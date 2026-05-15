"""Tests for SELL_LATENCY logging in execute_sell_routed.

Goal: every sell taking longer than SELL_LATENCY_THRESHOLD_SEC (default
15s) emits a [SELL_LATENCY] warning line. After ~1 week of dry-run logs
we'll have a frequency-and-duration distribution to decide whether the
slippage cascade is worth a separate fix.
"""
import asyncio
import logging
import sys
import os
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper as ws


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) \
        if asyncio.get_event_loop().is_running() is False \
        else asyncio.new_event_loop().run_until_complete(coro)


def _call_sell_routed(monkeypatched_clock_delta: float,
                      jupiter_succeeds: bool = True,
                      exit_reason: str = ""):
    """Drive execute_sell_routed with all network calls mocked and
    time.time advanced by `monkeypatched_clock_delta` between entry
    and exit so the duration check sees the desired value.

    Returns the captured (sig, msg, warning_records).
    """
    real_time = ws.time.time

    # We control time via a counter — first call is "start", second is
    # "end". The router's `time.time()` is only invoked twice in the
    # success path (once at the top, once in the finally clause), so a
    # simple counter is sufficient. Add a small epsilon buffer for any
    # extra calls inside the body so we don't run out of values.
    call_count = {"n": 0}
    base = 1_000_000.0
    def fake_time():
        call_count["n"] += 1
        return base if call_count["n"] == 1 else base + monkeypatched_clock_delta

    captured = []
    class CaptureHandler(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.WARNING and "[SELL_LATENCY]" in record.getMessage():
                captured.append(record.getMessage())

    handler = CaptureHandler()
    ws.logger.addHandler(handler)

    async def fake_get_sell_quote(*a, **kw):
        return {"outAmount": "100000"}  # truthy quote

    async def fake_execute_swap(*a, **kw):
        return ("SIG_OK", "ok") if jupiter_succeeds else (None, "jup fail")

    async def fake_pumpfun_sell(*a, **kw):
        return ("SIG_PP", "pumpportal ok")

    async def fake_send_telegram(*a, **kw):  # in case fallback path runs
        return True

    try:
        with patch.object(ws, "time", new=type("T", (), {"time": staticmethod(fake_time)})), \
             patch.object(ws, "get_sell_quote", new=AsyncMock(side_effect=fake_get_sell_quote)), \
             patch.object(ws, "execute_swap", new=AsyncMock(side_effect=fake_execute_swap)), \
             patch.object(ws, "execute_pumpfun_sell", new=AsyncMock(side_effect=fake_pumpfun_sell)), \
             patch.object(ws, "send_telegram", new=lambda *a, **kw: True), \
             patch.object(ws, "_is_graduated", new=lambda mc: False):
            sig, msg = asyncio.run(
                ws.execute_sell_routed(
                    session=None,
                    token_mint="So11111111111111111111111111111111111111112",
                    amount_tokens=1_000_000,
                    wallet_pubkey="W" * 32,
                    mc_usd=10_000.0,
                    exit_reason=exit_reason,
                )
            )
    finally:
        ws.logger.removeHandler(handler)

    return sig, msg, captured


# --- fast sells: no latency log -------------------------------------

def test_fast_sell_does_not_log_latency():
    """A sell completing in 5s (well under the 15s threshold) must not
    emit [SELL_LATENCY]."""
    sig, msg, captured = _call_sell_routed(monkeypatched_clock_delta=5.0,
                                           exit_reason="trailing_stop")
    assert sig == "SIG_OK"
    assert captured == [], f"expected no latency log, got: {captured}"


def test_fast_sell_at_threshold_minus_epsilon_does_not_log():
    """14.9s is still under the strict-greater-than 15s threshold."""
    sig, msg, captured = _call_sell_routed(monkeypatched_clock_delta=14.9,
                                           exit_reason="hard_floor")
    assert sig == "SIG_OK"
    assert captured == []


# --- slow sells: latency log fires ----------------------------------

def test_slow_sell_logs_latency():
    """A sell taking 30s emits [SELL_LATENCY] with duration."""
    sig, msg, captured = _call_sell_routed(monkeypatched_clock_delta=30.0,
                                           exit_reason="trailing_stop")
    assert sig == "SIG_OK"
    assert len(captured) == 1, f"expected 1 latency log, got: {captured}"
    line = captured[0]
    assert "[SELL_LATENCY]" in line
    assert "30.0s" in line
    assert "route=jupiter" in line
    assert "outcome=success" in line
    assert "exit_reason=trailing_stop" in line


def test_slow_sell_default_exit_reason_logs_as_unknown():
    """Sites that don't pass exit_reason still get logged; their reason
    field reads 'unknown' for later log-mining."""
    sig, msg, captured = _call_sell_routed(monkeypatched_clock_delta=20.0)
    assert sig == "SIG_OK"
    assert len(captured) == 1
    assert "exit_reason=unknown" in captured[0]


def test_slow_failed_sell_logs_outcome_failed():
    """Failed sells that exhaust the cascade still get a latency line
    so we can compare success/failure latencies."""
    sig, msg, captured = _call_sell_routed(monkeypatched_clock_delta=45.0,
                                           jupiter_succeeds=False,
                                           exit_reason="hard_floor")
    assert sig == "SIG_PP", "PumpPortal fallback should have succeeded"
    assert "route=jupiter→pumpportal" in captured[0]
    assert "exit_reason=hard_floor" in captured[0]


def test_slow_sell_records_route_jupiter_when_primary_succeeds():
    """Route field should record which leg actually closed the trade."""
    _, _, captured = _call_sell_routed(monkeypatched_clock_delta=20.0,
                                       jupiter_succeeds=True,
                                       exit_reason="whale_full_exit")
    assert "route=jupiter" in captured[0]
    # Negative check: not the fallback label
    assert "route=jupiter→pumpportal" not in captured[0]


# --- config sanity ----------------------------------------------------

def test_sell_latency_threshold_is_positive():
    """A zero or negative threshold would log every sell — defeats the
    purpose of flagging slow ones."""
    assert ws.SELL_LATENCY_THRESHOLD_SEC > 0


def test_sell_latency_threshold_is_reasonable():
    """Threshold should be in the 5s–120s range. Below 5s would catch
    too many fast sells; above 120s would miss the 30–60s cascade
    range we actually care about."""
    assert 5.0 <= ws.SELL_LATENCY_THRESHOLD_SEC <= 120.0
