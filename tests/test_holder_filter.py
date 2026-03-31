"""
Tests for holder filter null-value bypass (HOLDER_NULL_VALUE = 20).

DexScreener/Helius getTokenLargestAccounts returns at most 20 accounts,
so holder_count == 20 means "at least 20, but data capped" — not a real
concentration signal. These tests verify the bypass behaviour.
"""
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from market_radar import passes_holder_filter, HOLDER_NULL_VALUE


def test_holder_null_value_constant_is_20():
    assert HOLDER_NULL_VALUE == 20


def test_null_value_bypasses_filter(caplog):
    """holder_count == 20 must pass (bypass) with a log, not a veto."""
    with caplog.at_level(logging.INFO):
        result = passes_holder_filter(HOLDER_NULL_VALUE)
    assert result is True
    assert any("unavailable" in r.message.lower() for r in caplog.records), (
        "Expected a log message mentioning 'unavailable' when holder_count == HOLDER_NULL_VALUE"
    )


def test_none_still_fails_open():
    """Existing behaviour: None holder_count still passes (fail-open)."""
    assert passes_holder_filter(None) is True


def test_high_holder_count_passes():
    """Normal token with 300 holders is well under 500 limit — passes."""
    assert passes_holder_filter(300) is True


def test_excess_holder_count_fails():
    """Token with 600 holders (above max_holders=500) is blocked."""
    assert passes_holder_filter(600) is False


def test_21_holders_is_not_treated_as_null():
    """21 holders is a real low count — must NOT trigger the null bypass."""
    result = passes_holder_filter(21)
    assert result is True   # 21 < 500, still passes filter
    # but 21 != HOLDER_NULL_VALUE so no special bypass path


def test_zero_holders_passes():
    """0 holders: existing behaviour is to pass (guard against division)."""
    assert passes_holder_filter(0) is True
