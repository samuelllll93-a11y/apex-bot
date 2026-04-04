"""Tests for Claude confidence scoring."""
import asyncio
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import whale_sniper


def test_get_claude_score_returns_int_in_range():
    """Score is always 0-100 when API responds normally."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="72")]

    async def _run():
        with patch.dict(os.environ, {"CLAUDE_API_KEY": "test-key"}):
            with patch("anthropic.AsyncAnthropic") as mock_cls:
                mock_client = AsyncMock()
                mock_client.messages.create = AsyncMock(return_value=mock_response)
                mock_cls.return_value = mock_client
                return await whale_sniper.get_claude_score(
                    "TokenMint123abc",
                    {"liquidity": {"usd": 50000}, "volume": {"m5": 12000, "h1": 90000},
                     "priceChange": {"m5": 2.1, "h1": 15.0}},
                    45.0,
                    "whale buy signal",
                )

    score = asyncio.run(_run())
    assert isinstance(score, int)
    assert 0 <= score <= 100
    assert score == 72


def test_get_claude_score_clamps_above_100():
    """Claude returning '150' is clamped to 100."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="150")]

    async def _run():
        with patch.dict(os.environ, {"CLAUDE_API_KEY": "test-key"}):
            with patch("anthropic.AsyncAnthropic") as mock_cls:
                mock_client = AsyncMock()
                mock_client.messages.create = AsyncMock(return_value=mock_response)
                mock_cls.return_value = mock_client
                return await whale_sniper.get_claude_score("abc", None, None, "")

    assert asyncio.run(_run()) == 100


def test_get_claude_score_clamps_below_0():
    """Claude returning '-10' is clamped to 0."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="-10")]

    async def _run():
        with patch.dict(os.environ, {"CLAUDE_API_KEY": "test-key"}):
            with patch("anthropic.AsyncAnthropic") as mock_cls:
                mock_client = AsyncMock()
                mock_client.messages.create = AsyncMock(return_value=mock_response)
                mock_cls.return_value = mock_client
                return await whale_sniper.get_claude_score("abc", None, None, "")

    assert asyncio.run(_run()) == 0


def test_get_claude_score_fails_open_on_api_error():
    """Returns 70 (default) when Anthropic API raises an exception."""
    async def _run():
        with patch.dict(os.environ, {"CLAUDE_API_KEY": "test-key"}):
            with patch("anthropic.AsyncAnthropic") as mock_cls:
                mock_client = AsyncMock()
                mock_client.messages.create = AsyncMock(side_effect=Exception("API error"))
                mock_cls.return_value = mock_client
                return await whale_sniper.get_claude_score("abc", None, None, "")

    assert asyncio.run(_run()) == 70


def test_get_claude_score_no_api_key_returns_70():
    """Returns 70 when CLAUDE_API_KEY is not set — no API call made."""
    async def _run():
        env = {k: v for k, v in os.environ.items() if k != "CLAUDE_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            return await whale_sniper.get_claude_score("abc", None, None, "")

    assert asyncio.run(_run()) == 70
