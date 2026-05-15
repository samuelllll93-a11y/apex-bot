"""Tests for max-hold exit logic.

MAX_HOLD_MINUTES = 180 (3 hours) — secondary safety net beyond the hard
floor. Cuts any open position regardless of source or PnL state once the
time threshold is exceeded. Catches the slow-bleed scenario where a
position drifts at -20% for hours without deep-dumping to hard-floor
levels and never recovering.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper as ws


# --- threshold boundary ----------------------------------------------

def test_max_hold_just_under_threshold_does_not_fire():
    """Held 179 minutes (still under 180) → no exit."""
    assert ws._max_hold_check(elapsed_min=179.0) is None


def test_max_hold_at_threshold_fires():
    """Exactly 180 minutes → exit (boundary is inclusive)."""
    reason = ws._max_hold_check(elapsed_min=180.0)
    assert reason is not None
    assert "MAX HOLD" in reason


def test_max_hold_over_threshold_fires():
    """Held longer than 180 minutes → exit."""
    reason = ws._max_hold_check(elapsed_min=240.0)
    assert reason is not None


def test_max_hold_extreme_age_fires():
    """A position somehow still open after 24 hours definitely exits."""
    reason = ws._max_hold_check(elapsed_min=24 * 60.0)
    assert reason is not None


# --- very fresh positions never trigger ------------------------------

def test_max_hold_fresh_position_does_not_fire():
    """Just-opened position (5 seconds elapsed) is well under threshold."""
    assert ws._max_hold_check(elapsed_min=0.083) is None


def test_max_hold_zero_age_does_not_fire():
    """Edge case: clock skew making elapsed_min == 0."""
    assert ws._max_hold_check(elapsed_min=0.0) is None


# --- source-agnostic --------------------------------------------------

def test_max_hold_is_source_agnostic_by_construction():
    """The decision function takes only elapsed_min. It cannot look at
    source, so it is source-agnostic by construction. Pinning the
    signature prevents a future change from quietly adding a
    source-specific gate."""
    import inspect
    sig = inspect.signature(ws._max_hold_check)
    params = list(sig.parameters.keys())
    assert params == ["elapsed_min"], (
        f"_max_hold_check signature changed to {params} — adding a "
        f"source-dependent parameter would break the 'every position, "
        f"every source, same rule' contract."
    )


def test_max_hold_independent_of_pnl_or_target():
    """Max hold ignores pnl_pct and min_target_hit entirely — a 3hr-old
    +500% position still gets cut (the trail picks up if it's still
    rising), and a 3hr-old -50% position still gets cut (the hard
    floor would normally have caught it earlier; max hold is the
    backstop for the slow-bleed band)."""
    # The function doesn't take pnl_pct or min_target_hit — its sole
    # input is age. This test pins that contract.
    import inspect
    params = inspect.signature(ws._max_hold_check).parameters
    assert "pnl_pct" not in params, "max_hold must not gate on pnl"
    assert "min_target_hit" not in params, "max_hold must not gate on TP1"
    assert "source" not in params, "max_hold must not gate on source"


# --- config sanity ----------------------------------------------------

def test_max_hold_minutes_is_positive():
    """MAX_HOLD_MINUTES must be positive — a zero or negative value
    would force-exit every position on every monitor tick."""
    assert ws.MAX_HOLD_MINUTES > 0


def test_max_hold_minutes_is_reasonable():
    """Pin a reasonable upper bound so a config edit can't accidentally
    set it to 30 days. 24h is a sensible ceiling; the bot is meant for
    short-duration plays, not multi-day swing trades."""
    assert 30 <= ws.MAX_HOLD_MINUTES <= 24 * 60, (
        f"MAX_HOLD_MINUTES = {ws.MAX_HOLD_MINUTES} outside reasonable range "
        f"[30min, 24h] — bot is designed for short-duration trading."
    )


# --- routing wiring ---------------------------------------------------

def test_max_hold_uses_unified_sell_router_in_source():
    """Verify the max-hold branch in check_and_maybe_exit calls
    execute_sell_routed — which routes Jupiter primary, PumpPortal
    fallback per PREFER_JUPITER_SELLS=True. Done by source inspection
    since the function has many side effects that make end-to-end
    integration testing brittle without a full async test rig."""
    import inspect
    source = inspect.getsource(ws.check_and_maybe_exit)
    assert "_max_hold_check" in source, "max_hold helper must be called"
    # The max-hold execution block calls execute_sell_routed
    assert "max_hold_exit" in source, (
        "log_reason 'max_hold_exit' must appear in check_and_maybe_exit"
    )
    # Find the max-hold block and verify it routes correctly
    mh_start = source.find("# --- Max hold time-stop")
    mh_end   = source.find("# --- END MAX HOLD")
    assert mh_start > 0 and mh_end > mh_start, "max-hold block not found"
    mh_block = source[mh_start:mh_end]
    assert "execute_sell_routed" in mh_block, (
        "max-hold block must call execute_sell_routed (Jupiter+PumpPortal cascade)"
    )


def test_max_hold_fires_telegram_alert():
    """Verify the max-hold success path emits a Telegram alert."""
    import inspect
    source = inspect.getsource(ws.check_and_maybe_exit)
    mh_start = source.find("# --- Max hold time-stop")
    mh_end   = source.find("# --- END MAX HOLD")
    mh_block = source[mh_start:mh_end]
    assert "send_telegram" in mh_block, "max-hold block must send Telegram alert"
    assert "MAX HOLD" in mh_block, "Telegram label must mention MAX HOLD"
    assert "3hr" in mh_block or "max hold" in mh_block.lower(), (
        "Telegram message must indicate the 3hr / max hold trigger"
    )
