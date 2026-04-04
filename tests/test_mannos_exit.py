"""Tests for MANNOS tiered exit logic."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper


# --- get_exit_tier() -------------------------------------------------

def test_tier_low_confidence():
    """65-74 → 150% target, 20% trail, 45min."""
    t = whale_sniper.get_exit_tier(65)
    assert t["min_target_pct"] == 150
    assert t["trail_pct"] == 20
    assert t["time_stop_min"] == 45

def test_tier_low_confidence_upper_boundary():
    """74 is still tier 1."""
    t = whale_sniper.get_exit_tier(74)
    assert t["min_target_pct"] == 150

def test_tier_mid_confidence():
    """75-84 → 200% target, 25% trail, no time stop."""
    t = whale_sniper.get_exit_tier(75)
    assert t["min_target_pct"] == 200
    assert t["trail_pct"] == 25
    assert t["time_stop_min"] is None

def test_tier_mid_confidence_upper_boundary():
    """84 is still tier 2."""
    t = whale_sniper.get_exit_tier(84)
    assert t["min_target_pct"] == 200

def test_tier_high_confidence():
    """85+ → 400% target, 30% trail, no time stop."""
    t = whale_sniper.get_exit_tier(85)
    assert t["min_target_pct"] == 400
    assert t["trail_pct"] == 30
    assert t["time_stop_min"] is None

def test_tier_high_confidence_100():
    """100 still returns tier 3."""
    t = whale_sniper.get_exit_tier(100)
    assert t["min_target_pct"] == 400

def test_tier_below_65_defaults_to_tier1():
    """Scores below 65 (e.g., fail-open default) use tier 1."""
    t = whale_sniper.get_exit_tier(0)
    assert t["min_target_pct"] == 150

def test_tier_missing_score_uses_default():
    """Score of 70 (API fail-open default) uses tier 1."""
    t = whale_sniper.get_exit_tier(70)
    assert t["min_target_pct"] == 150


# --- _mannos_exit_check() -------------------------------------------

def test_hard_floor_triggers_before_min_target():
    """Down -20% before hitting min target → hard floor exit."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=-20.0,
        drop_from_peak=-20.0,
        elapsed_min=5.0,
        min_target_hit=False,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is not None
    assert "HARD FLOOR" in reason

def test_no_exit_before_min_target_small_loss():
    """Down -10% before min target — hard floor not triggered."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=-10.0,
        drop_from_peak=-10.0,
        elapsed_min=5.0,
        min_target_hit=False,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is None

def test_time_stop_fires_before_min_target():
    """Time stop fires even before min target is hit."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=50.0,
        drop_from_peak=0.0,
        elapsed_min=46.0,
        min_target_hit=False,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is not None
    assert "TIME STOP" in reason

def test_trail_fires_after_min_target():
    """After min target hit, trail stop of 20% from peak fires."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=120.0,
        drop_from_peak=-21.0,
        elapsed_min=20.0,
        min_target_hit=True,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is not None
    assert "MANNOS TRAIL" in reason

def test_no_exit_holding_above_min_target():
    """Holding above min target, trail not triggered → None."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=180.0,
        drop_from_peak=-5.0,
        elapsed_min=20.0,
        min_target_hit=True,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is None

def test_hard_floor_not_active_after_min_target():
    """Once min target hit, hard floor no longer applies — only trail matters."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=-15.0,
        drop_from_peak=-15.0,
        elapsed_min=10.0,
        min_target_hit=True,
        tier={"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45},
    )
    assert reason is None  # trail is only 15%, below 20% threshold

def test_time_stop_none_never_fires_before_min_target():
    """Tier 2/3 with time_stop_min=None — never exits on time alone."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=50.0,
        drop_from_peak=0.0,
        elapsed_min=9999.0,   # absurdly long time
        min_target_hit=False,
        tier={"min_target_pct": 200, "trail_pct": 25, "time_stop_min": None},
    )
    assert reason is None

def test_time_stop_none_never_fires_after_min_target():
    """Tier 2/3 with time_stop_min=None — no time exit even after min target hit."""
    reason = whale_sniper._mannos_exit_check(
        pnl_pct=250.0,
        drop_from_peak=-5.0,
        elapsed_min=9999.0,
        min_target_hit=True,
        tier={"min_target_pct": 200, "trail_pct": 25, "time_stop_min": None},
    )
    assert reason is None
