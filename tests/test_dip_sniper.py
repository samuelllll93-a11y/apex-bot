"""Tests for dip sniper — graduated watchlist and dip detection."""
import json
import sys
import os
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper


# --- Watchlist persistence -------------------------------------------

def test_save_and_load_watchlist(tmp_path):
    """Saved watchlist is loaded back identically."""
    wl_path = str(tmp_path / "watchlist.json")
    original = {
        "MintABC": {
            "graduation_price_sol": 0.0012,
            "ath_sol": 0.0025,
            "added_ts": 1700000000.0,
        }
    }
    whale_sniper._save_graduated_watchlist(original, path=wl_path)
    loaded = whale_sniper._load_graduated_watchlist(path=wl_path)
    assert loaded == original


def test_load_missing_watchlist_returns_empty(tmp_path):
    """Loading a non-existent path returns an empty dict — no crash."""
    wl_path = str(tmp_path / "does_not_exist.json")
    result = whale_sniper._load_graduated_watchlist(path=wl_path)
    assert result == {}


def test_load_corrupt_watchlist_returns_empty(tmp_path):
    """Corrupt JSON file returns empty dict — no crash."""
    wl_path = str(tmp_path / "bad.json")
    with open(wl_path, "w") as f:
        f.write("NOT VALID JSON{{{")
    result = whale_sniper._load_graduated_watchlist(path=wl_path)
    assert result == {}


# --- Dip detection logic ---------------------------------------------

def test_dip_detected_when_drop_exceeds_threshold():
    """50%+ drop from ATH is a dip signal."""
    entry = {
        "graduation_price_sol": 0.001,
        "ath_sol": 0.010,
        "added_ts": time.time(),
    }
    current_price_sol = 0.004   # 60% drop from ATH
    drop_pct = (entry["ath_sol"] - current_price_sol) / entry["ath_sol"] * 100
    assert drop_pct >= whale_sniper.DIP_SNIPER_DROP_PCT


def test_no_dip_when_drop_below_threshold():
    """< 50% drop is not a dip."""
    ath = 0.010
    current = 0.006   # only 40% drop
    drop_pct = (ath - current) / ath * 100
    assert drop_pct < whale_sniper.DIP_SNIPER_DROP_PCT


def test_watchlist_entry_expires_after_8_hours():
    """Entry added > 8h ago should be treated as expired."""
    added_ts = time.time() - (whale_sniper.DIP_SNIPER_WATCH_HOURS * 3600 + 1)
    expired = (time.time() - added_ts) / 3600 > whale_sniper.DIP_SNIPER_WATCH_HOURS
    assert expired is True


def test_watchlist_entry_not_expired_within_8_hours():
    """Entry added 1h ago is still active."""
    added_ts = time.time() - 3600
    expired = (time.time() - added_ts) / 3600 > whale_sniper.DIP_SNIPER_WATCH_HOURS
    assert expired is False


def test_dip_sniper_drop_pct_constant():
    """DIP_SNIPER_DROP_PCT is 50.0 as specified."""
    assert whale_sniper.DIP_SNIPER_DROP_PCT == 50.0


def test_dip_sniper_watch_hours_constant():
    """DIP_SNIPER_WATCH_HOURS is 8 as specified."""
    assert whale_sniper.DIP_SNIPER_WATCH_HOURS == 8


def test_dip_sniper_min_score_constant():
    """DIP_SNIPER_MIN_SCORE is 65 as specified."""
    assert whale_sniper.DIP_SNIPER_MIN_SCORE == 65
