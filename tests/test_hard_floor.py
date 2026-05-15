"""Tests for pre-TP1 hard floor exit logic.

The hard floor is the missing stop loss surfaced by the May 8-12 production
data analysis: trailing stop is gated behind min_target_hit (TP1 at +100%
gain), so positions that go straight down never arm anything and rot until
manual /sell. The hard floor fires regardless of source.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper as ws


# --- threshold boundary ----------------------------------------------

def test_hard_floor_below_threshold_does_not_fire():
    """Down -34% (still above the -35% floor) → no exit."""
    assert ws._hard_floor_check(pnl_pct=-34.0, min_target_hit=False) is None


def test_hard_floor_at_threshold_fires():
    """Exactly -35% → exit (boundary is inclusive)."""
    reason = ws._hard_floor_check(pnl_pct=-35.0, min_target_hit=False)
    assert reason is not None
    assert "HARD FLOOR" in reason


def test_hard_floor_well_below_threshold_fires():
    """-50% → exit."""
    reason = ws._hard_floor_check(pnl_pct=-50.0, min_target_hit=False)
    assert reason is not None
    assert "HARD FLOOR" in reason


def test_hard_floor_extreme_loss_fires():
    """-94% (the TARDFI-style rot scenario) → exit."""
    reason = ws._hard_floor_check(pnl_pct=-94.0, min_target_hit=False)
    assert reason is not None


# --- post-TP1 disable ------------------------------------------------

def test_hard_floor_does_not_fire_after_min_target_hit():
    """Post-TP1 the position is on a free ride; trailing stop owns the
    downside. Hard floor must not fire even on a deep drawdown."""
    assert ws._hard_floor_check(pnl_pct=-50.0, min_target_hit=True) is None
    assert ws._hard_floor_check(pnl_pct=-99.0, min_target_hit=True) is None


# --- positive PnL never triggers -------------------------------------

def test_hard_floor_positive_pnl_never_fires():
    """A position in profit must never hit the hard floor."""
    assert ws._hard_floor_check(pnl_pct=10.0, min_target_hit=False) is None
    assert ws._hard_floor_check(pnl_pct=0.0, min_target_hit=False) is None


# --- feature flag ----------------------------------------------------

def test_hard_floor_disabled_returns_none():
    """When HARD_FLOOR_ENABLED is False, the check must return None
    even on a deep loss, so the position falls through to the next
    check in the chain."""
    original = ws.HARD_FLOOR_ENABLED
    try:
        ws.HARD_FLOOR_ENABLED = False
        assert ws._hard_floor_check(pnl_pct=-50.0, min_target_hit=False) is None
        assert ws._hard_floor_check(pnl_pct=-94.0, min_target_hit=False) is None
    finally:
        ws.HARD_FLOOR_ENABLED = original


# --- source-agnostic --------------------------------------------------

def test_hard_floor_is_source_agnostic_by_construction():
    """The decision function takes only (pnl_pct, min_target_hit). It
    cannot look at source, so it is source-agnostic by construction.
    This test pins the signature in case a future change tries to
    sneak in a source-specific gate.
    """
    import inspect
    sig = inspect.signature(ws._hard_floor_check)
    params = list(sig.parameters.keys())
    assert params == ["pnl_pct", "min_target_hit"], (
        f"_hard_floor_check signature changed to {params} — adding a "
        f"source-dependent parameter would silently re-introduce the "
        f"momentum_scanner rotting-position bug."
    )


def test_hard_floor_fires_identically_across_sources():
    """Sanity check: running the same pnl through positions tagged as
    each source produces the same exit decision, since the decision
    function ignores source by design."""
    for source in ("whale_copy", "momentum_scanner", "cto_signal",
                   "dip_sniper", "cluster_buy"):
        # source isn't even a parameter — but the loop exists to make
        # the source-agnostic intent explicit in the test suite.
        result = ws._hard_floor_check(pnl_pct=-50.0, min_target_hit=False)
        assert result is not None, (
            f"hard floor should fire at -50% regardless of source "
            f"(checking {source})"
        )


# --- config sanity ----------------------------------------------------

def test_hard_floor_threshold_is_negative():
    """HARD_FLOOR_PCT must be negative — a positive value would mean
    'exit when up X%', which is what take-profit is for."""
    assert ws.HARD_FLOOR_PCT < 0


def test_hard_floor_threshold_is_reasonable():
    """Pin a reasonable range so a wild config edit doesn't silently
    destroy the bot (e.g. setting to -1% would exit everything;
    setting to -99% would re-introduce the rotting-position bug)."""
    assert -75.0 < ws.HARD_FLOOR_PCT < -10.0
