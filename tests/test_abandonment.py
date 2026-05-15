"""Tests for SELL_ABANDON_AFTER_FAILURES — per-position failed-sell
counter + _abandon_unsellable_position helper.

Why: without this counter, an unsellable bag retries forever every
monitor tick, burning RPC credits and producing hundreds of Telegram
errors. After SELL_ABANDON_AFTER_FAILURES consecutive failed full-exit
sells (HSF / HTP / generic MANNOS exit), the position is force-closed,
blacklisted, and the counter cleared.

These tests exercise the counter cycle and the helper directly. They
do not mock execute_sell_routed — that's deliberately covered by the
integration of the counter in check_and_maybe_exit, which is exercised
on the VPS in DRY_RUN before any live trading resumes.
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper as ws


# --- side-effect captures (replace ws-module functions per test) ---

class _Capture:
    """Holds buffers for one test's captured side effects."""
    def __init__(self):
        self.telegrams: list[str] = []
        self.warnings: list[str] = []
        self.record_trade_calls: list[float] = []
        self.save_positions_calls: int = 0
        self.save_blacklist_calls: int = 0


class _FakeLogger:
    def __init__(self, cap: _Capture):
        self.cap = cap
    def warning(self, msg, *a, **k):   self.cap.warnings.append(msg)
    def info(self, *a, **k):           pass
    def error(self, *a, **k):          pass
    def debug(self, *a, **k):          pass


def _install_captures():
    """Replace ws.send_telegram, ws.logger, ws._save_positions,
    ws._save_blacklist, ws._record_trade with capturing fakes.
    Returns (Capture, restore_fn)."""
    cap = _Capture()
    originals = {
        "send_telegram":    ws.send_telegram,
        "logger":           ws.logger,
        "_save_positions":  ws._save_positions,
        "_save_blacklist":  ws._save_blacklist,
        "_record_trade":    ws._record_trade,
    }
    def fake_telegram(msg):
        cap.telegrams.append(msg)
        return True
    def fake_save_positions():
        cap.save_positions_calls += 1
    def fake_save_blacklist():
        cap.save_blacklist_calls += 1
    def fake_record_trade(pnl_sol):
        cap.record_trade_calls.append(pnl_sol)

    ws.send_telegram    = fake_telegram
    ws.logger           = _FakeLogger(cap)
    ws._save_positions  = fake_save_positions
    ws._save_blacklist  = fake_save_blacklist
    ws._record_trade    = fake_record_trade

    def restore():
        for k, v in originals.items():
            setattr(ws, k, v)
    return cap, restore


def _reset_state():
    """Reset module-global state that the helper / counter touch."""
    ws.open_positions.clear()
    ws._token_blacklist.clear()
    ws._sell_failure_counts.clear()
    ws._stats["wins"] = 0
    ws._stats["losses"] = 0
    ws._stats["net_pnl_sol"] = 0.0


def _make_pos(entry_sol: float = 0.1, label: str = "TKN (Es9vMFrz)") -> dict:
    """Minimal position dict matching what check_and_maybe_exit would
    pass into the abandonment helper."""
    return {
        "entry_time":  1_700_000_000.0,
        "entry_sol":   entry_sol,
        "peak_sol":    entry_sol,
        "amount_tokens": 1_000_000,
        "token_label": label,
        "token_symbol": "TKN",
        "mc_entry":    50_000.0,
        "whale_name":  "peace",
        "source":      "whale_copy",
        "min_target_hit": False,
    }


TOKEN = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


# --- counter cycle tests ---------------------------------------------

def test_counter_increments_on_recorded_failure():
    """Successive `.get(mint, 0) + 1` writes give 1, 2, 3..."""
    _reset_state()
    for expected in range(1, 6):
        fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
        ws._sell_failure_counts[TOKEN] = fc
        assert fc == expected
        assert ws._sell_failure_counts[TOKEN] == expected


def test_counter_resets_via_pop_on_success():
    """A simulated success path pops the counter; the next failure
    starts at 1, not 6."""
    _reset_state()
    ws._sell_failure_counts[TOKEN] = 5

    # Simulate success path: pop the counter.
    ws._sell_failure_counts.pop(TOKEN, None)
    assert TOKEN not in ws._sell_failure_counts

    # Next failure starts at 1.
    fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
    ws._sell_failure_counts[TOKEN] = fc
    assert fc == 1


def test_counter_pop_on_missing_token_is_safe():
    """Popping a token that was never recorded must not raise."""
    _reset_state()
    ws._sell_failure_counts.pop(TOKEN, None)  # no KeyError
    assert TOKEN not in ws._sell_failure_counts


# --- threshold + below-threshold tests -------------------------------

def test_nine_failures_does_not_abandon():
    """At 9 consecutive failures (one below threshold) the helper
    must NOT have been called — position still tracked, no telegram."""
    _reset_state()
    cap, restore = _install_captures()
    try:
        ws.open_positions[TOKEN] = _make_pos()
        for _ in range(9):
            fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
            ws._sell_failure_counts[TOKEN] = fc
            if fc >= ws.SELL_ABANDON_AFTER_FAILURES:
                ws._abandon_unsellable_position(
                    ws.open_positions[TOKEN], TOKEN, "fail msg", "TEST TRIGGER"
                )

        assert TOKEN in ws.open_positions, "position must still be tracked at 9"
        assert ws._sell_failure_counts[TOKEN] == 9
        assert cap.telegrams == [], "no abandonment telegram below threshold"
        assert TOKEN not in ws._token_blacklist
    finally:
        restore()


def test_tenth_failure_triggers_abandonment():
    """At 10 consecutive failures the helper fires exactly once and
    the position is fully cleaned up."""
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos(entry_sol=0.1)
        ws.open_positions[TOKEN] = pos

        for _ in range(ws.SELL_ABANDON_AFTER_FAILURES):
            fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
            ws._sell_failure_counts[TOKEN] = fc
            if fc >= ws.SELL_ABANDON_AFTER_FAILURES:
                ws._abandon_unsellable_position(
                    pos, TOKEN, "ExceededSlippage Custom-17", "HARD SELL FLOOR (-35%)"
                )

        # Position cleaned up
        assert TOKEN not in ws.open_positions
        assert TOKEN not in ws._sell_failure_counts
        # Blacklisted
        assert TOKEN in ws._token_blacklist
        # Exactly one abandonment telegram
        assert len(cap.telegrams) == 1
        assert "POSITION ABANDONED" in cap.telegrams[0]
    finally:
        restore()


def test_successful_sell_after_five_failures_resets_counter():
    """The spec case: 5 failures, then a success, then another
    failure should start the counter at 1 — not 6, not abandon."""
    _reset_state()
    cap, restore = _install_captures()
    try:
        ws.open_positions[TOKEN] = _make_pos()

        # 5 failures
        for _ in range(5):
            fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
            ws._sell_failure_counts[TOKEN] = fc
        assert ws._sell_failure_counts[TOKEN] == 5

        # Simulate a successful sell — main pops the counter before del.
        ws._sell_failure_counts.pop(TOKEN, None)
        assert TOKEN not in ws._sell_failure_counts

        # Next failure starts fresh at 1.
        fc = ws._sell_failure_counts.get(TOKEN, 0) + 1
        ws._sell_failure_counts[TOKEN] = fc
        assert fc == 1
        assert TOKEN in ws.open_positions  # nothing abandoned
        assert cap.telegrams == []
    finally:
        restore()


# --- helper side-effects -----------------------------------------------

def test_abandon_removes_position_from_open_positions():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos()
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        assert TOKEN not in ws.open_positions
    finally:
        restore()


def test_abandon_blacklists_with_correct_duration():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos()
        ws.open_positions[TOKEN] = pos
        before = time.time()
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        bl_expiry = ws._token_blacklist[TOKEN]
        expected_offset = ws.BLACKLIST_MINUTES * 60
        # Allow ~2s slack for test execution time
        assert abs((bl_expiry - before) - expected_offset) < 2.0
    finally:
        restore()


def test_abandon_sends_telegram_with_expected_content():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos(label="ROT (testlabel)")
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(
            pos, TOKEN, "Jupiter: ExceededSlippage", "HARD SELL FLOOR (-35%)"
        )
        assert len(cap.telegrams) == 1
        msg = cap.telegrams[0]
        assert "POSITION ABANDONED" in msg
        assert "ROT (testlabel)" in msg
        assert TOKEN in msg
        assert str(ws.SELL_ABANDON_AFTER_FAILURES) in msg
        assert "Jupiter: ExceededSlippage" in msg
        assert "HARD SELL FLOOR (-35%)" in msg
        assert str(ws.BLACKLIST_MINUTES) in msg
    finally:
        restore()


def test_abandon_pops_counter():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos()
        ws.open_positions[TOKEN] = pos
        ws._sell_failure_counts[TOKEN] = 10
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        assert TOKEN not in ws._sell_failure_counts
    finally:
        restore()


def test_abandon_updates_stats_as_loss():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos(entry_sol=0.1)
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        assert ws._stats["losses"] == 1
        assert ws._stats["wins"] == 0
        # pnl_sol = -entry_sol = -0.1; net_pnl_sol is rounded to 6dp
        assert ws._stats["net_pnl_sol"] == round(-0.1, 6)
    finally:
        restore()


def test_abandon_records_trade():
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos(entry_sol=0.025)
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        assert cap.record_trade_calls == [-0.025]
    finally:
        restore()


def test_abandon_persists_positions_and_blacklist():
    """Both _save_positions and _save_blacklist must be called so the
    state survives a restart."""
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos()
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(pos, TOKEN, "err", "TEST")
        assert cap.save_positions_calls == 1
        assert cap.save_blacklist_calls == 1
    finally:
        restore()


def test_abandon_logs_warning_with_trigger():
    """Warning log must include the exit trigger for post-mortem."""
    _reset_state()
    cap, restore = _install_captures()
    try:
        pos = _make_pos()
        ws.open_positions[TOKEN] = pos
        ws._abandon_unsellable_position(
            pos, TOKEN, "err", "HARD TP (2.1x)"
        )
        assert len(cap.warnings) == 1
        assert "ABANDONED" in cap.warnings[0]
        assert "HARD TP (2.1x)" in cap.warnings[0]
    finally:
        restore()


# --- config sanity -----------------------------------------------------

def test_threshold_is_ten():
    """Pin the threshold so a config edit doesn't silently change
    abandonment behaviour."""
    assert ws.SELL_ABANDON_AFTER_FAILURES == 10


def test_sell_failure_counts_is_dict():
    assert isinstance(ws._sell_failure_counts, dict)


def test_threshold_is_positive_integer():
    """Zero or negative would mean every failure abandons immediately."""
    assert isinstance(ws.SELL_ABANDON_AFTER_FAILURES, int)
    assert ws.SELL_ABANDON_AFTER_FAILURES > 0
