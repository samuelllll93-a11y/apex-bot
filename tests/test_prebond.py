"""Tests for PumpFun prebond layer."""
import asyncio
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper


# --- prebond_decision() --------------------------------------------------

def test_prebond_decision_low_curve_proceed():
    """0-40% bonding → score 55, PROCEED."""
    score, action = whale_sniper.prebond_decision(20.0)
    assert action == "PROCEED"
    assert score == 55


def test_prebond_decision_mid_curve_proceed():
    """40-70% bonding → score 75, PROCEED."""
    score, action = whale_sniper.prebond_decision(55.0)
    assert action == "PROCEED"
    assert score == 75


def test_prebond_decision_high_curve_block():
    """70-100% bonding → BLOCK."""
    score, action = whale_sniper.prebond_decision(85.0)
    assert action == "BLOCK"
    assert score == 0


def test_prebond_decision_boundary_40_is_mid():
    """Exactly 40% is mid-range (score 75)."""
    score, action = whale_sniper.prebond_decision(40.0)
    assert action == "PROCEED"
    assert score == 75


def test_prebond_decision_boundary_70_is_block():
    """Exactly 70% triggers BLOCK."""
    score, action = whale_sniper.prebond_decision(70.0)
    assert action == "BLOCK"


def test_prebond_decision_zero_is_low():
    """0% bonding is low range (score 55)."""
    score, action = whale_sniper.prebond_decision(0.0)
    assert action == "PROCEED"
    assert score == 55


# --- fetch_prebond_progress() --------------------------------------------

def test_fetch_prebond_progress_not_graduated():
    """Returns (progress_pct, False) when complete=False."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value={
        "bonding_curve_progress": 45.5,
        "complete": False,
    })
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    async def _run():
        return await whale_sniper.fetch_prebond_progress(mock_session, "SomeMint123")

    pct, is_grad = asyncio.run(_run())
    assert pct == 45.5
    assert is_grad is False


def test_fetch_prebond_progress_graduated():
    """Returns (100.0, True) when complete=True."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value={
        "bonding_curve_progress": 100.0,
        "complete": True,
    })
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    async def _run():
        return await whale_sniper.fetch_prebond_progress(mock_session, "GraduatedMint")

    pct, is_grad = asyncio.run(_run())
    assert is_grad is True


def test_fetch_prebond_progress_api_error_fails_open():
    """Returns (None, False) on any exception — callers must treat as fail-open."""
    mock_session = MagicMock()
    mock_session.get = MagicMock(side_effect=Exception("connection error"))

    async def _run():
        return await whale_sniper.fetch_prebond_progress(mock_session, "ErrorMint")

    pct, is_grad = asyncio.run(_run())
    assert pct is None
    assert is_grad is False
