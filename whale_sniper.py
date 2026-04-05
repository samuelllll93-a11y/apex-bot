"""
APEX Whale Sniper
Monitors 4 whale wallets on Solana and mirrors their token buys via Jupiter.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
import time
import json
import aiohttp
import anthropic
import requests
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("whale_sniper")

# --- File logging: daily rotation, 7-day history ----------------------
_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(_log_dir, exist_ok=True)
_file_handler = TimedRotatingFileHandler(
    filename=os.path.join(_log_dir, "whale_sniper.log"),
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(_file_handler)
# ----------------------------------------------------------------------

# --- Config -----------------------------------------------------------

JUPITER_API      = "https://lite-api.jup.ag/swap/v1"
SOL_MINT         = "So11111111111111111111111111111111111111112"
WSOL_MINT        = "So11111111111111111111111111111111111111112"

DRY_RUN          = os.getenv("DRY_RUN", "True").lower() == "true"
BUY_AMOUNT_SOL   = float(os.getenv("BUY_AMOUNT_SOL", "0.1"))
MAX_SLIPPAGE_BPS      = int(os.getenv("MAX_SLIPPAGE_BPS", "1500"))
PRIORITY_FEE_LAMPORTS = int(os.getenv("PRIORITY_FEE_LAMPORTS", "100000"))

TX_CONFIRM_TIMEOUT_SEC = 30   # give up on confirmation after this many seconds
TX_CONFIRM_POLL_SEC    = 2    # poll getSignatureStatuses every N seconds (max 15 polls)

POLL_INTERVAL_SEC = 120  # 2-minute interval to reduce Helius credit usage
LOW_BALANCE_SOL   = float(os.getenv("LOW_BALANCE_SOL", "0.05"))  # skip trade + alert if below

# Whale wallets to track
WHALE_WALLETS: dict[str, str] = {
    "peace":    "7b88jCzsirGfLmFMyr7BXbCaDGTtuq8oDTWusqWvLv38",
    "crispy":   "EdbNfzVJjVZFsz1awBezeJpBaySLsckoZyPyaucy3g2R",
    "mannos":   "CAmNcBJ82xr1tzXrwZ6tZKwEFs26TG8kT6dJeR1bxjW9",
    "mr.putin": "8mzCDvq5JWJh6Cus7XYnnwL2JGCVUXA3bDqaXmzCG5hn",
    "peace2":   "6iZLfoaYvEAuuhnJEiSkwC9exmtMZehpkUVuFzb19sWc",
    # "early":  "Bv2BAw5UmKxv5SBMWYKqpsh6eXKNGM2RKxJGpGPk5vmb",  # disabled 2026-03-31
}

# Track the last seen signature per wallet to detect new txns
last_seen_sig: dict[str, str | None] = {name: None for name in WHALE_WALLETS}

# Whale activity log: name -> list of (mint, timestamp) for all buys
_whale_activity: dict[str, list[tuple[str, float]]] = {name: [] for name in WHALE_WALLETS}

CONVICTION_WINDOW_SEC = 1_800   # 30 min  — double-buy detection window
CONVICTION_MULTIPLIER = 1.5     # position multiplier on high-conviction signal
ACTIVITY_WINDOW_SEC   = 86_400  # 24 h    — HOT / COLD scoring window
HOT_THRESHOLD         = 3       # buys in 24 h to be classified HOT

# --- Sell / exit parameters -------------------------------------------
TAKE_PROFIT_PCT    = float(os.getenv("TAKE_PROFIT_PCT", "0.50")) * 100  # .env decimal → % (e.g. 0.38 = 38%)
TRAILING_STOP_PCT  = float(os.getenv("TRAILING_STOP_PCT", "0.10")) * 100  # .env decimal → % (e.g. 0.13 = 13%)
TIME_STOP_MIN      = int(os.getenv("TIME_STOP_MIN", "30"))  # minutes
POSITION_CHECK_SEC = 10     # how often the sell monitor loop runs

# --- Emergency exit parameters ----------------------------------------
EMERGENCY_DUMP_PCT        = 5.0  # emergency exit if down >5% right after buy
EMERGENCY_CHECK_DELAY_SEC = 5    # seconds after buy before emergency check runs

# --- DexScreener quality filter ---------------------------------------
DEXSCREENER_API       = "https://api.dexscreener.com/tokens/v1/solana"
MIN_DEX_LIQUIDITY_USD = 20_000
MIN_DEX_5M_VOLUME_USD = 10_000

# --- PumpFun prebond filter -------------------------------------------
PUMPFUN_API          = "https://frontend-api.pump.fun/coins"
PREBOND_POS_SIZE_PCT = 0.02   # 2% of current SOL balance for prebond entries

# --- MANNOS Autopilot -------------------------------------------------
# When True: mannos whale signals bypass DexScreener quality checks entirely.
# Prebond check and Claude scoring still run as normal.
MANNOS_AUTOPILOT = True   # set False to restore DexScreener filter for mannos

# --- MR.PUTIN Config --------------------------------------------------
# Ultra-early PumpFun entries: sub-$5k mcap, 1% position, 2h min hold, 3-day time stop
MRPUTIN_CONFIG: dict = {
    "max_mcap_usd":      5_000,   # Skip if mcap > $5k at signal time
    "bypass_dexscreener": True,
    "position_size_pct": 0.01,   # 1% of current SOL balance
    "hard_floor_pct":    -20.0,  # Stop loss from entry
    "trail_pct":         20.0,   # Trailing stop from peak
    "min_hold_mins":     120,    # Never sell before 2 hours
    "time_stop_mins":    4_320,  # Force exit after 3 days (3×24×60)
}

# --- Dip sniper -------------------------------------------------------
GRADUATED_WATCHLIST_PATH = "data/graduated_watchlist.json"
DIP_SNIPER_DROP_PCT      = 50.0   # trigger re-entry if price drops X% from ATH
DIP_SNIPER_MIN_SCORE     = 65     # minimum Claude score to enter a dip
DIP_SNIPER_WATCH_HOURS   = 8      # expire tokens from watchlist after X hours
DIP_SNIPER_CHECK_SEC     = 60     # how often the dip sniper loop runs

# In-memory graduated watchlist (loaded from disk at startup)
# mint → {graduation_price_sol: float, ath_sol: float, added_ts: float}
graduated_watchlist: dict[str, dict] = {}

# Open positions: token_mint → position dict (populated after every buy)
open_positions: dict[str, dict] = {}

# Blacklist: token_mint → expiry timestamp. Only set on TRAILING STOP exits.
# Take-profit and time-stop closures do NOT blacklist — re-entry on winners allowed.
_token_blacklist: dict[str, float] = {}
BLACKLIST_MINUTES = 45          # minutes to ban a token after a trailing stop loss

# Set once at startup in run() — lets confirm_transaction() reach the RPC
# without threading rpc_url through every intermediate function signature.
_rpc_url: str = ""

# --- Daily trade statistics (reset at midnight UTC) -------------------
_stats: dict = {
    "signals_detected":       0,   # every whale buy signal seen
    "cancelled_dexscreener":  0,   # filtered by liquidity / volume check
    "cancelled_prebond":      0,   # filtered by PumpFun bonding curve check
    "trades_executed":        0,   # buys that confirmed on-chain
    "wins":                   0,   # closed positions with PnL >= 0
    "losses":                 0,   # closed positions with PnL < 0
    "net_pnl_sol":            0.0, # running sum of (exit_sol - entry_sol)
}

# --- Per-trade log (for /summary command) -----------------------------
_trade_log: list[dict] = []   # [{ts: float, pnl_sol: float}, ...]
_SUMMARY_WINDOW_SEC = 43_200  # 12 hours


def _record_trade(pnl_sol: float) -> None:
    """Append a closed trade to the rolling trade log."""
    _trade_log.append({"ts": time.time(), "pnl_sol": round(pnl_sol, 6)})

# --- Helius rate tracker ----------------------------------------------

HELIUS_DAILY_WARN_LIMIT = 26_000   # ~800k credits/month ÷ 30 days
_helius_calls: int = 0
_helius_day_start: float = time.time()


def _track_helius_call() -> None:
    """Increment the Helius call counter and warn via Telegram if over daily limit."""
    global _helius_calls, _helius_day_start
    now = time.time()
    if now - _helius_day_start >= 86_400:
        _helius_calls = 0
        _helius_day_start = now
    _helius_calls += 1
    if _helius_calls == HELIUS_DAILY_WARN_LIMIT:
        msg = (
            f"⚠️ <b>APEX whale_sniper</b> — Helius daily limit reached\n"
            f"Made {_helius_calls:,} RPC calls today (≈800k credits/month threshold).\n"
            f"Consider reducing scan frequency."
        )
        logger.warning(f"Helius daily call limit hit: {_helius_calls:,}")
        send_telegram(msg)


# --- RPC helpers ------------------------------------------------------

def rpc_post(rpc_url: str, method: str, params: list) -> dict:
    _track_helius_call()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(rpc_url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_recent_signatures(rpc_url: str, wallet: str, limit: int = 10) -> list[dict]:
    """Return the most recent transaction signatures for a wallet."""
    try:
        result = rpc_post(
            rpc_url,
            "getSignaturesForAddress",
            [wallet, {"limit": limit, "commitment": "confirmed"}],
        )
        return result.get("result") or []
    except Exception as e:
        logger.warning(f"getSignaturesForAddress failed for {wallet[:8]}: {e}")
        return []


def get_transaction(rpc_url: str, sig: str) -> dict | None:
    """Fetch a parsed transaction by signature."""
    try:
        result = rpc_post(
            rpc_url,
            "getTransaction",
            [sig, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}],
        )
        return result.get("result")
    except Exception as e:
        logger.warning(f"getTransaction failed for {sig[:16]}: {e}")
        return None


def get_sol_balance(rpc_url: str, wallet_pubkey: str) -> float:
    """Return wallet SOL balance in SOL. Returns 0.0 on any RPC error."""
    if not wallet_pubkey:
        logger.error("get_sol_balance: wallet_pubkey is empty — WALLET_PUBLIC_KEY not set in .env")
        return 0.0
    try:
        result   = rpc_post(rpc_url, "getBalance",
                            [wallet_pubkey, {"commitment": "confirmed"}])
        raw      = result.get("result")
        rpc_err  = result.get("error")
        if rpc_err:
            logger.error(
                f"getBalance RPC error for {wallet_pubkey[:8]}…: {rpc_err} "
                f"— check WALLET_PUBLIC_KEY is a base58 address, not a private key"
            )
            return 0.0
        if raw is None:
            logger.error(
                f"getBalance returned null result for {wallet_pubkey[:8]}…"
                f" — full response: {result}"
            )
            return 0.0
        lamports = raw.get("value", 0)
        return lamports / 1_000_000_000
    except Exception as e:
        logger.error(f"getBalance exception for {wallet_pubkey[:8]}…: {e}")
        return 0.0


# --- DexScreener quality check ----------------------------------------

async def fetch_dexscreener(
    session: aiohttp.ClientSession,
    token_mint: str,
) -> dict | None:
    """
    Fetch the highest-liquidity Solana pair for token_mint from DexScreener.
    Returns None on any error — callers must fail-open (proceed with trade).
    """
    url = f"{DEXSCREENER_API}/{token_mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            resp.raise_for_status()
            data  = await resp.json()
            pairs = data if isinstance(data, list) else (data.get("pairs") or [])
            if not pairs:
                return None
            # Pick the pair with the highest USD liquidity
            return max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0)
    except Exception as e:
        logger.warning(f"DexScreener fetch failed for {token_mint[:8]}: {e}")
        return None


def passes_dex_quality(pair_data: dict) -> tuple[bool, str]:
    """Return (True, summary) if token meets minimum quality thresholds."""
    liq = (pair_data.get("liquidity") or {}).get("usd", 0) or 0
    v5m = (pair_data.get("volume")    or {}).get("m5",  0) or 0
    if liq < MIN_DEX_LIQUIDITY_USD:
        return False, f"liquidity ${liq:,.0f} below ${MIN_DEX_LIQUIDITY_USD:,}"
    if v5m < MIN_DEX_5M_VOLUME_USD:
        return False, f"5m vol ${v5m:,.0f} below ${MIN_DEX_5M_VOLUME_USD:,}"
    return True, f"liq=${liq:,.0f} 5m_vol=${v5m:,.0f}"


# --- PumpFun prebond layer --------------------------------------------

def prebond_decision(progress: float | None) -> tuple[int, str]:
    """
    Given bonding curve progress (0-100), return (score, action).
    action is "PROCEED", "BLOCK", or "GRADUATED".
      None:   score  0, GRADUATED (token already graduated — skip prebond scoring)
      0-40%:  score 55, PROCEED (early entry)
      40-70%: score 75, PROCEED (mid-curve momentum)
      70%+:   score  0, BLOCK  (too late — near graduation, thin exit window)
    """
    if progress is None:
        return 0, "GRADUATED"
    if progress >= 70.0:
        return 0, "BLOCK"
    elif progress >= 40.0:
        return 75, "PROCEED"
    else:
        return 55, "PROCEED"


async def fetch_prebond_progress(
    session: aiohttp.ClientSession,
    token_mint: str,
) -> tuple[float | None, bool]:
    """
    Query PumpFun API for bonding curve progress.
    Returns (progress_pct, is_graduated).
    Returns (None, False) on any error — callers must fail-open.
    """
    url = f"{PUMPFUN_API}/{token_mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            resp.raise_for_status()
            data     = await resp.json()
            is_grad  = bool(data.get("complete", False))
            raw_prog = data.get("bonding_curve_progress")
            if is_grad or raw_prog is None:
                # complete=True or missing curve field both mean graduated
                return 100.0, True
            return float(raw_prog), False
    except Exception as e:
        logger.debug(f"[PREBOND] PumpFun API failed for {token_mint[:8]}: {e} — fail-open")
        return None, False


async def fetch_pump_mcap(
    session: aiohttp.ClientSession,
    token_mint: str,
) -> float | None:
    """
    Fetch usd_market_cap from PumpFun API for the mr.putin mcap gate.
    Returns None on API failure — callers must fail-open.
    """
    url = f"{PUMPFUN_API}/{token_mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            mcap = data.get("usd_market_cap")
            return float(mcap) if mcap else None
    except Exception as e:
        logger.debug(f"[MR.PUTIN] pump.fun mcap fetch failed for {token_mint[:8]}: {e} — fail-open")
        return None


# --- Dip sniper watchlist helpers ------------------------------------

def _load_graduated_watchlist(path: str = GRADUATED_WATCHLIST_PATH) -> dict:
    """Load the graduated watchlist from disk. Returns {} on any error."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"[DIP SNIPER] Could not load watchlist from {path}: {e}")
        return {}


def _save_graduated_watchlist(
    watchlist: dict,
    path: str = GRADUATED_WATCHLIST_PATH,
) -> None:
    """Persist the graduated watchlist to disk. Silently swallows write errors."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(watchlist, f, indent=2)
    except Exception as e:
        logger.warning(f"[DIP SNIPER] Could not save watchlist to {path}: {e}")


def _add_to_graduated_watchlist(token_mint: str, graduation_price_sol: float) -> None:
    """
    Add a graduated token to the dip sniper watchlist.
    No-op if already present (preserves existing ATH data).
    """
    if token_mint in graduated_watchlist:
        return
    graduated_watchlist[token_mint] = {
        "graduation_price_sol": graduation_price_sol,
        "ath_sol":              graduation_price_sol,
        "added_ts":             time.time(),
    }
    _save_graduated_watchlist(graduated_watchlist)
    logger.info(
        f"[DIP SNIPER] Added {token_mint[:8]} to watchlist "
        f"(grad price {graduation_price_sol:.6f} SOL)"
    )


# --- Claude confidence scoring ----------------------------------------

async def get_claude_score(
    token_mint: str,
    dex_pair: dict | None,
    prebond_progress: float | None,
    context_note: str = "",
) -> int:
    """
    Ask Claude to score a token's short-term trading potential 0-100.
    Fails open at 70 if CLAUDE_API_KEY is absent or API call fails.
    Never logs the API key value.
    """
    api_key = os.getenv("CLAUDE_API_KEY", "")
    if not api_key:
        logger.warning("CLAUDE_API_KEY not set — Claude score defaulting to 70 (fail-open)")
        return 70

    liq  = (dex_pair.get("liquidity")   or {}).get("usd", 0)  if dex_pair else 0
    v5m  = (dex_pair.get("volume")      or {}).get("m5",  0)  if dex_pair else 0
    v1h  = (dex_pair.get("volume")      or {}).get("h1",  0)  if dex_pair else 0
    p5m  = (dex_pair.get("priceChange") or {}).get("m5",  0)  if dex_pair else 0
    p1h  = (dex_pair.get("priceChange") or {}).get("h1",  0)  if dex_pair else 0

    prompt = (
        "You are a Solana memecoin trading analyst. Score this token's short-term "
        "(1-2h) trading potential from 0 to 100 based on these on-chain metrics:\n\n"
        f"Liquidity USD:    ${liq:,.0f}\n"
        f"5m Volume USD:    ${v5m:,.0f}\n"
        f"1h Volume USD:    ${v1h:,.0f}\n"
        f"5m Price Change:  {p5m:+.1f}%\n"
        f"1h Price Change:  {p1h:+.1f}%\n"
    )
    if prebond_progress is not None:
        prompt += f"Bonding curve progress: {prebond_progress:.1f}%\n"
    if context_note:
        prompt += f"Context: {context_note}\n"
    prompt += (
        "\nRespond with ONLY a single integer 0-100. "
        "No explanation, no punctuation — just the number."
    )

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp   = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        score = max(0, min(100, int(resp.content[0].text.strip())))
        logger.info(f"[CLAUDE] {token_mint[:8]} scored {score}/100")
        return score
    except Exception as e:
        logger.warning(f"[CLAUDE] Scoring failed for {token_mint[:8]}: {e} — defaulting to 70")
        return 70


# --- Telegram message helpers -----------------------------------------

def _token_label(token_mint: str, dex_pair: dict | None) -> str:
    """Return 'SYMBOL (AbCd1234)' if DexScreener symbol available, else 'AbCd1234...'."""
    symbol = ((dex_pair or {}).get("baseToken") or {}).get("symbol", "")
    return f"{symbol} ({token_mint[:8]})" if symbol else token_mint[:8]


def _fmt_usd(value: float) -> str:
    """Format a USD value compactly: $1.23M / $456.7K / $789."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def _sol_price_from_dex(dex_pair: dict | None) -> float:
    """
    Derive SOL/USD price from a DexScreener pair.
    Uses priceUsd / priceNative (both are fields on every Solana pair).
    Returns 0.0 if either field is missing or zero.
    """
    if not dex_pair:
        return 0.0
    try:
        price_usd    = float(dex_pair.get("priceUsd")    or 0)
        price_native = float(dex_pair.get("priceNative") or 0)
        if price_usd > 0 and price_native > 0:
            return price_usd / price_native
    except (ValueError, TypeError):
        pass
    return 0.0


# --- MANNOS tiered exit logic -----------------------------------------

MANNOS_HARD_FLOOR_PCT = -20.0  # max loss before min target — protects against dumps

def get_exit_tier(claude_score: int) -> dict:
    """
    Return exit parameters for a given Claude confidence score.
    Tier 3 (85+): 400% min target, 30% trail, no time stop  [MANNOS max conviction]
    Tier 2 (75+): 200% min target, 25% trail, no time stop
    Tier 1 (<75): 150% min target, 20% trail, 45min time stop  [default / fail-open]
    time_stop_min=None means the position runs indefinitely until trail or hard floor.
    """
    if claude_score >= 85:
        return {"min_target_pct": 400, "trail_pct": 30, "time_stop_min": None}
    elif claude_score >= 75:
        return {"min_target_pct": 200, "trail_pct": 25, "time_stop_min": None}
    else:
        return {"min_target_pct": 150, "trail_pct": 20, "time_stop_min": 45}


def _mannos_exit_check(
    pnl_pct: float,
    drop_from_peak: float,
    elapsed_min: float,
    min_target_hit: bool,
    tier: dict,
) -> str | None:
    """
    Pure-function exit decision for MANNOS tiered trailing take profit.
    Returns an exit reason string, or None if position should be held.

    Before min_target_hit:
      - Hard floor at MANNOS_HARD_FLOOR_PCT (-20%) from entry
      - Time stop at tier["time_stop_min"] (skipped if None — Tier 2/3)

    After min_target_hit:
      - Trailing stop at tier["trail_pct"] below peak
      - Time stop at tier["time_stop_min"] (skipped if None — Tier 2/3)
    """
    if min_target_hit:
        if drop_from_peak <= -tier["trail_pct"]:
            return (
                f"MANNOS TRAIL {pnl_pct:+.1f}% "
                f"(peak drop {abs(drop_from_peak):.1f}% > trail {tier['trail_pct']}%)"
            )
        if tier["time_stop_min"] is not None and elapsed_min >= tier["time_stop_min"]:
            return f"TIME STOP ({elapsed_min:.0f}m | tier limit {tier['time_stop_min']}m)"
    else:
        if pnl_pct <= MANNOS_HARD_FLOOR_PCT:
            return f"HARD FLOOR {pnl_pct:+.1f}% (pre-target protection)"
        if tier["time_stop_min"] is not None and elapsed_min >= tier["time_stop_min"]:
            return f"TIME STOP ({elapsed_min:.0f}m | tier limit {tier['time_stop_min']}m)"
    return None


def _mrputin_exit_check(
    pnl_pct: float,
    drop_from_peak: float,
    elapsed_min: float,
) -> str | None:
    """
    Pure-function exit decision for MR.PUTIN wallet positions.

    Rules (all apply only after min_hold_mins):
      - Hard floor: -20% from entry (stop loss)
      - Trailing stop: -20% from peak
      - Time stop: 3 days (4320 min)

    Returns an exit reason string, or None if position should be held.
    """
    if elapsed_min < MRPUTIN_CONFIG["min_hold_mins"]:
        return None  # minimum hold period — never exit early
    if pnl_pct <= MRPUTIN_CONFIG["hard_floor_pct"]:
        return f"MR.PUTIN HARD FLOOR {pnl_pct:+.1f}%"
    if drop_from_peak <= -MRPUTIN_CONFIG["trail_pct"]:
        return (
            f"MR.PUTIN TRAIL {pnl_pct:+.1f}% "
            f"(peak drop {abs(drop_from_peak):.1f}% > {MRPUTIN_CONFIG['trail_pct']}%)"
        )
    if elapsed_min >= MRPUTIN_CONFIG["time_stop_mins"]:
        return f"MR.PUTIN TIME STOP ({elapsed_min:.0f}m | 3-day limit)"
    return None


async def dip_sniper_loop(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
) -> None:
    """
    Every DIP_SNIPER_CHECK_SEC seconds:
    1. Expire tokens older than DIP_SNIPER_WATCH_HOURS.
    2. Fetch current price via DexScreener for each watchlist token.
    3. Update ATH.
    4. If price dropped DIP_SNIPER_DROP_PCT% from ATH, call Claude.
    5. If Claude score >= DIP_SNIPER_MIN_SCORE and token not in open_positions, buy.
    """
    logger.info(f"Dip sniper started — watching {len(graduated_watchlist)} token(s)")
    rpc_url = _rpc_url

    while True:
        await asyncio.sleep(DIP_SNIPER_CHECK_SEC)

        now = time.time()
        # Expire old entries
        expired = [
            m for m, d in graduated_watchlist.items()
            if (now - d["added_ts"]) / 3600 > DIP_SNIPER_WATCH_HOURS
        ]
        for m in expired:
            del graduated_watchlist[m]
            logger.info(f"[DIP SNIPER] {m[:8]} | Action: EXPIRED (>{DIP_SNIPER_WATCH_HOURS}h)")
        if expired:
            _save_graduated_watchlist(graduated_watchlist)

        for token_mint, entry in list(graduated_watchlist.items()):
            # Skip if already in open positions — no double buy
            if token_mint in open_positions:
                logger.debug(f"[DIP SNIPER] {token_mint[:8]} | Action: WATCHING (already in position)")
                continue

            # Fetch current price from DexScreener
            pair = await fetch_dexscreener(session, token_mint)
            if pair is None:
                logger.debug(f"[DIP SNIPER] {token_mint[:8]} | Action: WATCHING (no DexScreener data)")
                continue

            current_price_sol = float((pair.get("priceNative") or 0) or 0)
            if current_price_sol <= 0:
                continue

            # Update ATH
            if current_price_sol > entry["ath_sol"]:
                graduated_watchlist[token_mint]["ath_sol"] = current_price_sol
                entry["ath_sol"] = current_price_sol

            ath      = entry["ath_sol"]
            drop_pct = (ath - current_price_sol) / ath * 100 if ath > 0 else 0.0

            logger.debug(
                f"[DIP SNIPER] {token_mint[:8]} | ATH: {ath:.6f} SOL | "
                f"Current: {current_price_sol:.6f} SOL | Drop: {drop_pct:.1f}% | Action: WATCHING"
            )

            if drop_pct < DIP_SNIPER_DROP_PCT:
                continue

            logger.info(
                f"[DIP SNIPER] {token_mint[:8]} | ATH: {ath:.6f} SOL | "
                f"Current: {current_price_sol:.6f} SOL | Drop: {drop_pct:.1f}% | Action: TRIGGERED"
            )

            # Get Claude score
            claude_score = await get_claude_score(
                token_mint, pair, None,
                f"dip sniper — {drop_pct:.0f}% drop from ATH of {ath:.6f} SOL"
            )
            if claude_score < DIP_SNIPER_MIN_SCORE:
                logger.info(
                    f"[DIP SNIPER] {token_mint[:8]} | Claude score {claude_score} < "
                    f"{DIP_SNIPER_MIN_SCORE} — skipping re-entry"
                )
                continue

            # Balance check
            sol_balance = get_sol_balance(rpc_url, wallet_pubkey)
            if sol_balance < LOW_BALANCE_SOL:
                logger.warning(
                    f"[DIP SNIPER] {token_mint[:8]} | LOW BALANCE {sol_balance:.4f} SOL — skip"
                )
                continue

            buy_sol    = round(sol_balance * PREBOND_POS_SIZE_PCT, 4)   # 2% of balance
            amount_lam = int(buy_sol * 1_000_000_000)
            quote      = await get_jupiter_quote(session, token_mint, amount_lam)
            if not quote:
                logger.error(f"[DIP SNIPER] {token_mint[:8]} | Jupiter quote failed — skipping")
                continue

            send_telegram(
                f"🎯 <b>DIP SNIPER</b> — <code>{token_mint[:8]}</code>\n"
                f"Dropped {drop_pct:.0f}% from ATH | Claude: {claude_score}/100\n"
                f"Buying {buy_sol} SOL worth…"
            )

            swap_sig, swap_msg = await execute_swap(session, quote, wallet_pubkey)
            if not swap_sig:
                logger.error(f"[DIP SNIPER] {token_mint[:8]} | Swap failed: {swap_msg}")
                continue

            token_units = int(quote.get("outAmount", 0))
            entry_sol   = int(quote.get("inAmount",  0)) / 1_000_000_000
            if token_units > 0:
                open_positions[token_mint] = {
                    "entry_time":     time.time(),
                    "entry_sol":      entry_sol,
                    "peak_sol":       entry_sol,
                    "amount_tokens":  token_units,
                    "whale":          "dip_sniper",
                    "buy_sol":        buy_sol,
                    "claude_score":   claude_score,
                    "min_target_hit": False,
                    "source":         "dip_sniper",
                }
                _stats["trades_executed"] += 1
                logger.info(
                    f"[DIP SNIPER] {token_mint[:8]} | Entered {token_units:,} tokens "
                    f"@ {entry_sol:.4f} SOL | Claude: {claude_score}"
                )
                send_telegram(
                    f"✅ <b>DIP SNIPER BUY</b> — <code>{token_mint[:8]}</code>\n"
                    f"Entry: {entry_sol:.4f} SOL | Score: {claude_score}/100\n"
                    f"Sig: <code>{swap_sig}</code>"
                )
                asyncio.create_task(emergency_dump_check(session, token_mint, wallet_pubkey))


# --- Trade detection --------------------------------------------------

def extract_token_buy(tx: dict, whale_address: str) -> str | None:
    """
    Inspect a parsed transaction for a token buy (SOL out, SPL token in).
    Returns the token mint address if a buy is detected, else None.
    Debug-level logs explain every None return so silent failures are visible.
    """
    if not tx:
        logger.debug("[extract_token_buy] tx is None/empty — skipping")
        return None

    meta = tx.get("meta") or {}
    if meta.get("err"):
        logger.debug(f"[extract_token_buy] tx has on-chain error: {meta['err']} — skipping")
        return None   # failed tx

    pre_balances  = meta.get("preTokenBalances")  or []
    post_balances = meta.get("postTokenBalances") or []

    if not post_balances:
        logger.debug("[extract_token_buy] postTokenBalances is empty — not a token buy tx")
        return None

    # uiAmount can be null for brand-new PumpFun pre-bond mints.
    # Fall back to raw integer amount ÷ 10^decimals so pre-bond buys are detected.
    def token_amount(b: dict) -> float:
        ui        = b.get("uiTokenAmount") or {}
        ui_amount = ui.get("uiAmount")
        if ui_amount is not None:
            return float(ui_amount)
        raw      = int(ui.get("amount", "0") or "0")
        decimals = int(ui.get("decimals", 0)  or 0)
        return raw / (10 ** decimals) if decimals >= 0 else float(raw)

    # Build maps: owner -> {mint: amount}
    def balance_map(balances: list) -> dict[str, dict[str, float]]:
        m: dict[str, dict[str, float]] = {}
        for b in balances:
            owner  = b.get("owner", "")
            mint   = b.get("mint", "")
            amount = token_amount(b)
            m.setdefault(owner, {})[mint] = amount
        return m

    pre  = balance_map(pre_balances)
    post = balance_map(post_balances)

    # Look for mints where the whale's balance increased
    whale_pre  = pre.get(whale_address,  {})
    whale_post = post.get(whale_address, {})

    if not whale_post:
        logger.debug(
            f"[extract_token_buy] whale {whale_address[:8]} not in postTokenBalances "
            f"— owners seen: {[o[:8] for o in list(post.keys())[:3]]}"
        )
        return None

    for mint, post_amount in whale_post.items():
        if mint in (SOL_MINT, WSOL_MINT):
            continue
        pre_amount = whale_pre.get(mint, 0.0)
        if post_amount > pre_amount:
            logger.info(
                f"Detected buy: whale {whale_address[:8]} "
                f"received {post_amount - pre_amount:.6f} of {mint[:8]}"
            )
            return mint

    logger.debug(
        f"[extract_token_buy] whale {whale_address[:8]} — no balance increase found "
        f"in {len(whale_post)} post-balance entries "
        f"(mints: {[m[:8] for m in whale_post if m not in (SOL_MINT, WSOL_MINT)][:3]})"
    )
    return None


# --- Jupiter swap -----------------------------------------------------

async def get_jupiter_quote(
    session: aiohttp.ClientSession,
    output_mint: str,
    amount_lamports: int,
) -> dict | None:
    params = {
        "inputMint":   SOL_MINT,
        "outputMint":  output_mint,
        "amount":      str(amount_lamports),
        "slippageBps": str(MAX_SLIPPAGE_BPS),
    }
    url = f"{JUPITER_API}/quote"
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"Jupiter quote attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
    return None


async def get_sell_quote(
    session: aiohttp.ClientSession,
    token_mint: str,
    amount_tokens: int,
) -> dict | None:
    """Get a Jupiter quote for selling amount_tokens of token_mint → SOL."""
    params = {
        "inputMint":   token_mint,
        "outputMint":  SOL_MINT,
        "amount":      str(amount_tokens),
        "slippageBps": str(MAX_SLIPPAGE_BPS),
    }
    url = f"{JUPITER_API}/quote"
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"Sell quote attempt {attempt + 1}/3 failed for {token_mint[:8]}: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
    return None


async def confirm_transaction(
    session: aiohttp.ClientSession,
    txid: str,
) -> tuple[bool, str]:
    """
    Poll Solana's getSignatureStatuses until the transaction confirms, fails,
    or TX_CONFIRM_TIMEOUT_SEC is reached.

    Returns:
      (True,  "confirmed in 4.2s")              — safe to open position
      (False, "tx failed on-chain: {err}")       — tx landed but reverted
      (False, "not confirmed within 30s")        — timeout / dropped
    In DRY_RUN mode returns (True, "DRY_RUN skip") without any network call.
    """
    if DRY_RUN:
        return True, "DRY_RUN skip"

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [[txid], {"searchTransactionHistory": True}],
    }
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed >= TX_CONFIRM_TIMEOUT_SEC:
            return False, f"not confirmed within {TX_CONFIRM_TIMEOUT_SEC}s"
        try:
            async with session.post(
                _rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                resp.raise_for_status()
                data  = await resp.json()
                value = ((data.get("result") or {}).get("value") or [None])[0]
                if value is None:
                    # Tx not yet propagated — keep waiting
                    await asyncio.sleep(TX_CONFIRM_POLL_SEC)
                    continue
                if value.get("err"):
                    return False, f"tx failed on-chain: {value['err']}"
                status = value.get("confirmationStatus", "")
                if status in ("confirmed", "finalized"):
                    return True, f"confirmed in {elapsed:.1f}s"
                # "processed" — seen but not yet in a confirmed block
        except Exception as e:
            logger.warning(f"Confirmation poll error ({txid[:16]}…): {e}")
        await asyncio.sleep(TX_CONFIRM_POLL_SEC)


async def execute_swap(
    session: aiohttp.ClientSession,
    quote: dict,
    wallet_pubkey: str,
) -> tuple[str | None, str]:
    """
    Submit a Jupiter swap and wait for on-chain confirmation.
    Returns (txid, message) on success, (None, reason) on any failure.
    In DRY_RUN mode returns ("DRY_RUN_SIG", "DRY_RUN") immediately.
    """
    if DRY_RUN:
        logger.info(
            f"[DRY RUN] Would swap {quote.get('inAmount')} lamports → "
            f"{quote.get('outAmount')} tokens ({quote.get('outputMint','?')[:8]})"
        )
        return "DRY_RUN_SIG", "DRY_RUN"

    payload = {
        "quoteResponse":             quote,
        "userPublicKey":             wallet_pubkey,
        "wrapAndUnwrapSol":          True,
        "dynamicComputeUnitLimit":   True,
        "prioritizationFeeLamports": PRIORITY_FEE_LAMPORTS,   # was "auto"
    }
    url  = f"{JUPITER_API}/swap"
    txid = None
    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                txid = (await resp.json()).get("txid")
                break   # submission accepted — move to confirmation
        except Exception as e:
            logger.error(f"Jupiter swap attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2)

    if not txid:
        return None, "Jupiter swap failed after 3 attempts"

    logger.info(
        f"TX submitted: {txid[:16]}… — waiting up to {TX_CONFIRM_TIMEOUT_SEC}s"
    )
    ok, reason = await confirm_transaction(session, txid)
    if not ok:
        logger.error(f"TX {txid[:16]}… confirmation failed: {reason}")
        return None, reason

    logger.info(f"TX {txid[:16]}… {reason}")
    return txid, reason


# --- Telegram ---------------------------------------------------------

def send_telegram(message: str) -> bool:
    """
    Send a Telegram message.  Returns True on success, False on any failure.
    Logs ERROR (not just WARNING) so failures are always visible in pm2 logs.
    """
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.error("send_telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alert dropped")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=5,
        )
        resp.raise_for_status()   # raises on 4xx / 5xx — was previously missing
        logger.info(f"Telegram alert sent (message_id={resp.json().get('result',{}).get('message_id')})")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"Telegram HTTP error {e.response.status_code}: {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# --- Position exit logic ----------------------------------------------

async def check_and_maybe_exit(
    session: aiohttp.ClientSession,
    token_mint: str,
    wallet_pubkey: str,
) -> None:
    """
    Evaluate one open position against all three exit conditions.
    Executes sell and clears position if any condition is met.
    """
    pos = open_positions.get(token_mint)
    if pos is None:
        return  # already closed by a concurrent check

    # Fetch current sell value
    sell_quote = await get_sell_quote(session, token_mint, pos["amount_tokens"])
    if sell_quote is None:
        logger.warning(
            f"[{token_mint[:8]}] sell quote failed this tick — retrying next cycle"
        )
        return  # keep position open; try again in POSITION_CHECK_SEC

    current_sol    = int(sell_quote.get("outAmount", 0)) / 1_000_000_000
    entry_sol      = pos["entry_sol"]
    peak_sol       = pos["peak_sol"]
    elapsed_min    = (time.time() - pos["entry_time"]) / 60

    # Update peak if price has moved up
    if current_sol > peak_sol:
        open_positions[token_mint]["peak_sol"] = current_sol
        peak_sol = current_sol

    pnl_pct       = (current_sol / entry_sol - 1) * 100 if entry_sol > 0 else 0.0
    drop_from_peak = (current_sol / peak_sol  - 1) * 100 if peak_sol  > 0 else 0.0

    logger.debug(
        f"[{token_mint[:8]}] hold — pnl={pnl_pct:+.1f}% | "
        f"peak_drop={drop_from_peak:.1f}% | {elapsed_min:.0f}m elapsed"
    )

    # --- MANNOS tiered exit ---
    claude_score   = pos.get("claude_score", 70)
    min_target_hit = pos.get("min_target_hit", False)
    tier           = get_exit_tier(claude_score)

    # Activate trailing stop once min target is reached (latches — never resets)
    if not min_target_hit and pnl_pct >= tier["min_target_pct"]:
        open_positions[token_mint]["min_target_hit"] = True
        min_target_hit = True
        logger.info(
            f"[EXIT] {token_mint[:8]} | Min target {tier['min_target_pct']}% reached | "
            f"Trail {tier['trail_pct']}% now active | Confidence tier: {claude_score}"
        )

    logger.debug(
        f"[EXIT] {token_mint[:8]} | Peak: {peak_sol:.4f} SOL | Current: {current_sol:.4f} SOL | "
        f"Trail: {tier['trail_pct']}% | Confidence tier: {claude_score} | "
        f"Action: {'TRAILING' if min_target_hit else 'HOLDING'}"
    )

    if pos.get("whale") == "mr.putin":
        exit_reason = _mrputin_exit_check(
            pnl_pct=pnl_pct,
            drop_from_peak=drop_from_peak,
            elapsed_min=elapsed_min,
        )
    else:
        exit_reason = _mannos_exit_check(
            pnl_pct=pnl_pct,
            drop_from_peak=drop_from_peak,
            elapsed_min=elapsed_min,
            min_target_hit=min_target_hit,
            tier=tier,
        )

    if exit_reason is None:
        return  # no exit condition met this tick

    # --- Execute sell --------------------------------------------------
    if DRY_RUN:
        sell_sig = "DRY_RUN_SELL_SIG"
        logger.info(
            f"[DRY RUN] Would sell {pos['amount_tokens']:,} tokens → "
            f"{current_sol:.4f} SOL ({exit_reason})"
        )
    else:
        sell_sig, sell_msg = await execute_swap(session, sell_quote, wallet_pubkey)
        if not sell_sig:
            logger.error(
                f"[{token_mint[:8]}] Sell swap failed ({sell_msg}) — "
                f"keeping position open, will retry next cycle. "
                f"Trigger was: {exit_reason}"
            )
            return  # don't clear position if live sell tx failed

    pnl_sign = "+" if pnl_pct >= 0 else ""
    emoji    = "💰" if pnl_pct >= 0 else "🛑"

    # Fetch exit MC for sell summary (non-blocking; fails open to "—")
    _sell_dex = await fetch_dexscreener(session, token_mint)
    mc_exit   = float((_sell_dex or {}).get("marketCap") or (_sell_dex or {}).get("fdv") or 0)
    mc_entry_stored  = pos.get("mc_entry", 0)
    token_label_sell = pos.get("token_label") or token_mint[:8]

    # Remove from open_positions *before* logging so monitor doesn't re-enter
    del open_positions[token_mint]

    # Blacklist on trailing stop ONLY — take profit and time stop allow re-entry
    if exit_reason.startswith("TRAILING STOP"):
        _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
        logger.info(
            f"[{token_mint[:8]}] Blacklisted for {BLACKLIST_MINUTES}min "
            f"(trailing stop loss — will not re-enter until cooldown expires)"
        )

    # Update daily stats
    if pnl_pct >= 0:
        _stats["wins"] += 1
    else:
        _stats["losses"] += 1
    _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + (current_sol - entry_sol), 6)
    _record_trade(current_sol - entry_sol)

    mc_entry_str = _fmt_usd(mc_entry_stored) if mc_entry_stored else "—"
    mc_exit_str  = _fmt_usd(mc_exit)         if mc_exit         else "—"

    logger.info(
        f"[{token_mint[:8]}] {exit_reason} | "
        f"Entry: {entry_sol:.4f} SOL | Exit: {current_sol:.4f} SOL | "
        f"PnL: {pnl_sign}{pnl_pct:.1f}%"
    )
    send_telegram(
        f"{emoji} <b>SELL</b> — {token_label_sell}\n"
        f"Reason: {exit_reason}\n"
        f"\n📊 <b>Trade Summary:</b>\n"
        f"  MC Entry:  {mc_entry_str}\n"
        f"  MC Exit:   {mc_exit_str}\n"
        f"  Entry:     {entry_sol:.4f} SOL\n"
        f"  Exit:      {current_sol:.4f} SOL\n"
        f"  PnL:       {pnl_sign}{pnl_pct:.1f}%\n"
        f"\nSig: <code>{sell_sig}</code>"
    )


# --- Emergency dump exit ----------------------------------------------

async def emergency_dump_check(
    session: aiohttp.ClientSession,
    token_mint: str,
    wallet_pubkey: str,
) -> None:
    """
    Fires EMERGENCY_CHECK_DELAY_SEC after position opens (via asyncio.create_task).
    If price is already down >EMERGENCY_DUMP_PCT from entry, sells immediately
    and blacklists the token.  Never blocks poll_whale().
    """
    await asyncio.sleep(EMERGENCY_CHECK_DELAY_SEC)
    pos = open_positions.get(token_mint)
    if pos is None:
        return  # already closed by monitor loop — nothing to do

    sell_quote = await get_sell_quote(session, token_mint, pos["amount_tokens"])
    if sell_quote is None:
        logger.warning(
            f"[{token_mint[:8]}] emergency check: sell quote failed "
            f"— normal monitor will handle"
        )
        return

    current_sol = int(sell_quote.get("outAmount", 0)) / 1_000_000_000
    entry_sol   = pos["entry_sol"]
    pnl_pct     = (current_sol / entry_sol - 1) * 100 if entry_sol > 0 else 0.0

    if pnl_pct > -EMERGENCY_DUMP_PCT:
        logger.debug(f"[{token_mint[:8]}] emergency check OK — pnl={pnl_pct:+.1f}%")
        return

    logger.info(
        f"[{token_mint[:8]}] IMMEDIATE DUMP DETECTED — "
        f"emergency exit (pnl={pnl_pct:.1f}%)"
    )

    if DRY_RUN:
        sell_sig = "DRY_RUN_SELL_SIG"
        logger.info(
            f"[DRY RUN] Would emergency-sell {pos['amount_tokens']:,} tokens "
            f"→ {current_sol:.4f} SOL"
        )
    else:
        sell_sig, sell_msg = await execute_swap(session, sell_quote, wallet_pubkey)
        if not sell_sig:
            logger.error(
                f"[{token_mint[:8]}] Emergency sell failed ({sell_msg}) "
                f"— normal monitor will handle"
            )
            return

    del open_positions[token_mint]
    _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
    logger.info(f"[{token_mint[:8]}] Blacklisted {BLACKLIST_MINUTES}min after emergency exit")

    _stats["losses"]      += 1
    _stats["net_pnl_sol"]  = round(_stats["net_pnl_sol"] + (current_sol - entry_sol), 6)
    _record_trade(current_sol - entry_sol)

    send_telegram(
        f"🛑 <b>EMERGENCY EXIT [{token_mint[:8]}]</b>\n"
        f"Immediate dump — down {abs(pnl_pct):.1f}% in {EMERGENCY_CHECK_DELAY_SEC}s\n"
        f"Entry: {entry_sol:.4f} SOL | Exit: {current_sol:.4f} SOL\n"
        f"Sig: <code>{sell_sig}</code>"
    )


# --- /summary command -------------------------------------------------

def _summary_message() -> str:
    """Build a 12-hour trade summary string for the /summary Telegram command."""
    now    = time.time()
    cutoff = now - _SUMMARY_WINDOW_SEC
    window = [t for t in _trade_log if t["ts"] >= cutoff]

    start_str = time.strftime("%H:%M UTC", time.gmtime(cutoff))
    end_str   = time.strftime("%H:%M UTC", time.gmtime(now))

    if not window:
        return (
            "📊 <b>12-Hour Trade Summary</b>\n"
            "━━━━━━━━━━━━━━━\n"
            f"🕐 Period: {start_str} → {end_str}\n"
            "No trades executed in the last 12 hours."
        )

    total  = len(window)
    wins   = [t for t in window if t["pnl_sol"] >= 0]
    losses = [t for t in window if t["pnl_sol"] <  0]
    n_wins = len(wins)
    n_loss = len(losses)

    win_rate  = n_wins / total * 100
    loss_rate = n_loss / total * 100
    total_pnl = sum(t["pnl_sol"] for t in window)
    avg_win   = sum(t["pnl_sol"] for t in wins)   / n_wins if n_wins else 0.0
    avg_loss  = sum(t["pnl_sol"] for t in losses) / n_loss if n_loss else 0.0

    pnl_sign  = "+" if total_pnl >= 0 else ""
    win_sign  = "+" if avg_win   >= 0 else ""
    loss_sign = "+" if avg_loss  >= 0 else ""

    return (
        "📊 <b>12-Hour Trade Summary</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"🕐 Period: {start_str} → {end_str}\n"
        f"📈 Total Trades: {total}\n"
        f"✅ Wins: {n_wins} ({win_rate:.1f}%)\n"
        f"❌ Losses: {n_loss} ({loss_rate:.1f}%)\n"
        f"💰 Total PnL: {pnl_sign}{total_pnl:.4f} SOL\n"
        f"📉 Avg Win: {win_sign}{avg_win:.4f} SOL\n"
        f"📈 Avg Loss: {loss_sign}{avg_loss:.4f} SOL\n"
        "━━━━━━━━━━━━━━━"
    )


async def _holdings_message(session: aiohttp.ClientSession) -> str:
    """
    Build a live holdings snapshot for the /holdings Telegram command.
    Fetches fresh MC from DexScreener and SOL price from CoinGecko for each position.
    Fails open — missing data shows '—' rather than crashing.
    """
    if not open_positions:
        return "📊 No current holdings"

    # Fetch SOL/USD price from CoinGecko once for all positions
    sol_usd = 0.0
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            cg_data = await resp.json()
            sol_usd = float((cg_data.get("solana") or {}).get("usd") or 0)
    except Exception as e:
        logger.debug(f"[HOLDINGS] CoinGecko fetch failed: {e} — USD estimate omitted")

    _NUM_EMOJIS = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines: list[str] = ["📊 <b>Current Holdings</b>"]
    total_entry_sol = 0.0
    total_worth_sol = 0.0

    for idx, (token_mint, pos) in enumerate(open_positions.items()):
        num     = _NUM_EMOJIS[idx] if idx < len(_NUM_EMOJIS) else f"{idx + 1}."
        entry_sol = pos.get("entry_sol", 0.0)
        mc_entry  = pos.get("mc_entry",  0.0)
        tl        = pos.get("token_label") or token_mint[:8]
        whale     = (pos.get("whale") or "?").upper()
        tp1_hit   = pos.get("min_target_hit", False)

        # Fresh MC from DexScreener
        dex_now = await fetch_dexscreener(session, token_mint)
        mc_now  = float((dex_now or {}).get("marketCap") or (dex_now or {}).get("fdv") or 0)

        # Worth and PnL derived from MC ratio
        if mc_entry and mc_now:
            worth_sol = entry_sol * (mc_now / mc_entry)
            pnl_pct   = (mc_now - mc_entry) / mc_entry * 100
        else:
            worth_sol = entry_sol
            pnl_pct   = 0.0

        total_entry_sol += entry_sol
        total_worth_sol += worth_sol

        color    = "🟢" if pnl_pct >= 0 else "🔴"
        pnl_sign = "+" if pnl_pct >= 0 else ""
        tp1_icon = "✅" if tp1_hit else "❌"

        lines.append(
            f"\n{num} <b>{tl}</b>\n"
            f"   Whale: {whale}\n"
            f"   Entry MC:  {_fmt_usd(mc_entry) if mc_entry else '—'}\n"
            f"   Current MC: {_fmt_usd(mc_now) if mc_now else '—'} "
            f"{color} {pnl_sign}{pnl_pct:.0f}%\n"
            f"   Entry: {entry_sol:.4f} SOL | Worth: ~{worth_sol:.4f} SOL\n"
            f"   TP1 hit: {tp1_icon}"
        )

    # Footer totals
    overall_pnl   = (total_worth_sol / total_entry_sol - 1) * 100 if total_entry_sol else 0.0
    overall_color = "🟢" if overall_pnl >= 0 else "🔴"
    overall_sign  = "+" if overall_pnl >= 0 else ""

    lines.append(
        f"\n💼 Total in: {total_entry_sol:.4f} SOL\n"
        f"📈 Total worth: ~{total_worth_sol:.4f} SOL\n"
        f"{overall_color} Overall: {overall_sign}{overall_pnl:.0f}%"
    )
    if sol_usd:
        lines.append(
            f"💵 Est. USD value: ~{_fmt_usd(total_worth_sol * sol_usd)}"
            f" (SOL @ ${sol_usd:,.2f})"
        )

    return "\n".join(lines)


def _register_commands() -> None:
    """Register bot commands in Telegram's command menu."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/setMyCommands",
            json={"commands": [
                {"command": "summary",  "description": "12-hour trade summary"},
                {"command": "holdings", "description": "Current open positions"},
            ]},
            timeout=5,
        )
        logger.info("Telegram commands registered (/summary, /holdings)")
    except Exception as e:
        logger.warning(f"setMyCommands failed: {e}")


async def telegram_command_loop() -> None:
    """Long-poll Telegram getUpdates and respond to /summary commands.

    Uses its own aiohttp session so the 30-second long-poll never competes
    with the shared session used by the whale poller and position monitor.
    """
    logger.info("Telegram command loop started — listening for /summary, /holdings")

    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("telegram_command_loop: credentials missing — /summary won't respond")
        return

    base_url       = f"https://api.telegram.org/bot{token}"
    last_update_id = 0

    async with aiohttp.ClientSession() as tg_session:
        fail_count = 0
        while True:
            try:
                params = {"timeout": 30, "offset": last_update_id + 1}
                async with tg_session.get(
                    f"{base_url}/getUpdates",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=40),
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                fail_count = 0  # reset backoff on any successful response
            except Exception as e:
                fail_count += 1
                delay = min(5 * (2 ** (fail_count - 1)), 300)
                logger.warning(
                    f"getUpdates failed ({type(e).__name__}: {e!r}) — "
                    f"retry in {delay}s (attempt {fail_count})"
                )
                await asyncio.sleep(delay)
                continue

            if not data.get("ok"):
                logger.error(
                    f"Telegram getUpdates error: {data.get('description', data)} "
                    f"(error_code={data.get('error_code')})"
                )
                await asyncio.sleep(5)
                continue

            for update in data.get("result", []):
                last_update_id = max(last_update_id, update.get("update_id", 0))
                msg  = update.get("message") or {}
                text = (msg.get("text") or "").strip()
                cid  = str(msg.get("chat", {}).get("id", ""))

                if text.startswith("/summary") and cid == chat_id:
                    reply = _summary_message()
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                            logger.info("/summary command handled")
                    except Exception as e:
                        logger.warning(f"/summary reply failed: {e}")

                elif text.startswith("/holdings") and cid == chat_id:
                    reply = await _holdings_message(tg_session)
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as r:
                            r.raise_for_status()
                            logger.info("/holdings command handled")
                    except Exception as e:
                        logger.warning(f"/holdings reply failed: {e}")


# --- Daily summary ----------------------------------------------------

def _send_daily_summary() -> None:
    """Format and send the midnight UTC daily stats summary."""
    total    = _stats["trades_executed"]
    wins     = _stats["wins"]
    losses   = _stats["losses"]
    net      = _stats["net_pnl_sol"]
    pnl_sign = "+" if net >= 0 else ""
    win_rate = f"{wins / total * 100:.0f}%" if total > 0 else "n/a"
    date_str = time.strftime("%Y-%m-%d", time.gmtime())
    msg = (
        f"📊 <b>APEX Daily Summary</b> ({date_str})\n\n"
        f"Signals detected:         {_stats['signals_detected']}\n"
        f"Cancelled (DexScreener):  {_stats['cancelled_dexscreener']}\n"
        f"Cancelled (prebond):      {_stats['cancelled_prebond']}\n"
        f"Trades executed:          {total}\n"
        f"Wins / Losses:            {wins} / {losses}  ({win_rate})\n"
        f"Net PnL:                  {pnl_sign}{net:.4f} SOL"
    )
    logger.info(
        f"DAILY SUMMARY | signals={_stats['signals_detected']} "
        f"cancelled_dex={_stats['cancelled_dexscreener']} "
        f"trades={total} W/L={wins}/{losses} pnl={pnl_sign}{net:.4f} SOL"
    )
    send_telegram(msg)


def _reset_stats() -> None:
    """Zero all daily counters — called immediately after midnight summary."""
    for key in ("signals_detected", "cancelled_dexscreener", "cancelled_prebond",
                "trades_executed", "wins", "losses"):
        _stats[key] = 0
    _stats["net_pnl_sol"] = 0.0


async def midnight_summary_loop() -> None:
    """Sleep until midnight UTC, send daily summary, reset stats, repeat."""
    while True:
        t = time.gmtime()
        secs = (23 - t.tm_hour) * 3600 + (59 - t.tm_min) * 60 + (60 - t.tm_sec)
        logger.info(
            f"Daily summary scheduled in "
            f"{secs // 3600}h {(secs % 3600) // 60}m"
        )
        await asyncio.sleep(secs)
        _send_daily_summary()
        _reset_stats()


# --- Startup diagnostics ----------------------------------------------

def startup_checks(rpc_url: str, wallet_pubkey: str) -> None:
    """
    Run at bot start.  Verifies wallet balance is readable and Telegram
    is reachable.  All findings logged at ERROR level so they are
    impossible to miss in pm2 logs.
    """
    # 1. Wallet balance --------------------------------------------------
    logger.info(f"Startup balance check — wallet: {wallet_pubkey[:8]}…")
    bal = get_sol_balance(rpc_url, wallet_pubkey)
    if bal > 0:
        logger.info(f"Wallet balance: {bal:.4f} SOL ✓")
    else:
        logger.error(
            f"Wallet balance read as 0.0 SOL — either wallet is empty, "
            f"WALLET_PUBLIC_KEY is wrong, or RPC returned an error above"
        )

    # 2. Telegram token format ------------------------------------------
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    token_valid = bool(re.match(r"^\d{8,12}:[A-Za-z0-9_-]{35,}$", token))
    logger.info(
        f"Telegram token: first10={token[:10]!r}  len={len(token)}  "
        f"format_valid={token_valid}  chat_id={chat_id!r}"
    )
    if not token_valid:
        logger.error(
            "TELEGRAM_BOT_TOKEN format looks wrong — expected '123456789:ABCdef…' "
            f"(got len={len(token)}, first10={token[:10]!r}). "
            "Check VPS .env for extra spaces, newlines, or truncation."
        )

    # 3. Live Telegram test message --------------------------------------
    logger.info("Sending startup Telegram test message…")
    ok = send_telegram("🤖 <b>APEX Whale Sniper</b> — startup OK\nBalance: "
                       f"{bal:.4f} SOL | DRY_RUN={DRY_RUN}")
    if ok:
        logger.info("Startup Telegram test: PASSED ✓")
    else:
        logger.error(
            "Startup Telegram test: FAILED — check errors above. "
            f"URL being called: https://api.telegram.org/bot{token[:10]}…/sendMessage"
        )

    # 4. Validate bot token via Telegram getMe -------------------------
    try:
        me_resp = requests.get(
            f"https://api.telegram.org/bot{token}/getMe",
            timeout=5,
        )
        me_data = me_resp.json()
        if me_data.get("ok"):
            bot_name = me_data["result"].get("username", "unknown")
            logger.info(f"Telegram bot identity: @{bot_name} ✓")
        else:
            logger.error(
                f"Telegram getMe FAILED — token is invalid or bot was revoked. "
                f"Description: {me_data.get('description')} "
                f"(error_code={me_data.get('error_code')})"
            )
    except Exception as e:
        logger.error(f"Telegram getMe check failed: {type(e).__name__}: {e}")


# --- Main loop --------------------------------------------------------

async def poll_whale(
    session: aiohttp.ClientSession,
    name: str,
    wallet: str,
    rpc_url: str,
    wallet_pubkey: str,
) -> None:
    """Check a single whale for new transactions and mirror buys."""
    sigs = get_recent_signatures(rpc_url, wallet, limit=5)
    if not sigs:
        return

    latest_sig = sigs[0].get("signature")

    # First run: just record baseline, don't trade
    if last_seen_sig[name] is None:
        last_seen_sig[name] = latest_sig
        logger.info(f"[{name}] baseline sig: {latest_sig[:16]}…")
        return

    # Find new signatures since last check
    new_sigs: list[str] = []
    for entry in sigs:
        sig = entry.get("signature")
        if sig == last_seen_sig[name]:
            break
        new_sigs.append(sig)

    if not new_sigs:
        return

    last_seen_sig[name] = latest_sig
    logger.info(f"[{name}] {len(new_sigs)} new txn(s)")

    for sig in new_sigs:
        tx = get_transaction(rpc_url, sig)
        token_mint = extract_token_buy(tx, wallet)

        if not token_mint:
            logger.debug(
                f"[WHALE] [{name}] {sig[:16]}… — processed, result: SKIPPED "
                f"(no token buy detected — see extract_token_buy debug above)"
            )
            continue

        logger.info(f"[{name}] BUY signal → {token_mint}")
        _stats["signals_detected"] += 1

        # Guard 1 — skip if we already hold this token (prevents same-cycle duplicates)
        if token_mint in open_positions:
            logger.info(
                f"[{name}] SKIP — {token_mint[:8]} already in open_positions "
                f"(duplicate buy signal ignored)"
            )
            continue

        # Guard 2 — skip if token is blacklisted after a trailing stop loss
        now_ts = time.time()
        expired_mints = [m for m, exp in _token_blacklist.items() if now_ts >= exp]
        for m in expired_mints:
            del _token_blacklist[m]
            logger.info(f"Blacklist expired — {m[:8]} re-enabled")
        if token_mint in _token_blacklist:
            remaining_min = (_token_blacklist[token_mint] - now_ts) / 60
            logger.info(
                f"[{name}] SKIP — {token_mint[:8]} blacklisted, "
                f"{remaining_min:.0f}min remaining after trailing stop"
            )
            continue

        # --- Conviction + activity tracking ----------------------------
        now = time.time()

        # Prune entries outside the 24 h activity window
        _whale_activity[name] = [
            (m, t) for m, t in _whale_activity[name]
            if now - t < ACTIVITY_WINDOW_SEC
        ]

        # Double conviction: same mint bought by this whale in last 30 min?
        prior_30m = [
            (m, t) for m, t in _whale_activity[name]
            if now - t < CONVICTION_WINDOW_SEC
        ]
        is_high_conviction = any(m == token_mint for m, t in prior_30m)

        # Record current buy, then recount
        _whale_activity[name].append((token_mint, now))
        buys_24h = len(_whale_activity[name])

        # HOT whale log — fires each time the threshold is crossed / maintained
        if buys_24h >= HOT_THRESHOLD:
            logger.info(f"[{name.upper()}] HOT WHALE 🔥 ({buys_24h} buys in 24h)")

        # --- Pre-trade SOL balance guard --------------------------------
        sol_balance = get_sol_balance(rpc_url, wallet_pubkey)
        if sol_balance < LOW_BALANCE_SOL:
            alert = (
                f"⚠️ <b>LOW BALANCE</b> — {sol_balance:.4f} SOL remaining\n"
                f"Skipping trade on <code>{token_mint[:8]}</code>\n"
                f"Top up wallet before next signal fires."
            )
            logger.warning(f"[{name}] LOW BALANCE {sol_balance:.4f} SOL — skipping trade on {token_mint[:8]}")
            send_telegram(alert)
            continue
        logger.info(f"[{name}] Balance OK: {sol_balance:.4f} SOL (min={LOW_BALANCE_SOL} SOL)")
        # ----------------------------------------------------------------

        # --- DexScreener quality check — Fix 2 -------------------------
        if (MANNOS_AUTOPILOT and name == "mannos") or name == "mr.putin":
            logger.info(
                f"[{name.upper()} AUTOPILOT] Bypassing DexScreener — direct entry "
                f"({token_mint[:8]})"
            )
            dex_pair = await fetch_dexscreener(session, token_mint)  # still fetch for Claude scoring
        else:
            dex_pair = await fetch_dexscreener(session, token_mint)
            if dex_pair is None:
                logger.warning(
                    f"[{name}] DexScreener unavailable for {token_mint[:8]} "
                    f"— proceeding (fail-open)"
                )
            else:
                dex_ok, dex_reason = passes_dex_quality(dex_pair)
                if not dex_ok:
                    logger.info(
                        f"[{name}] SKIP — DexScreener quality fail: {dex_reason} "
                        f"({token_mint[:8]})"
                    )
                    _stats["cancelled_dexscreener"] += 1
                    continue
                logger.info(f"[{name}] DexScreener OK — {dex_reason}")
        # ----------------------------------------------------------------

        # --- MR.PUTIN mcap gate ----------------------------------------
        if name == "mr.putin":
            _mrputin_mcap = await fetch_pump_mcap(session, token_mint)
            if _mrputin_mcap is not None and _mrputin_mcap > MRPUTIN_CONFIG["max_mcap_usd"]:
                logger.info(
                    f"[MR.PUTIN] SKIP — mcap ${_mrputin_mcap:,.0f} exceeds "
                    f"$5k threshold ({token_mint[:8]})"
                )
                _stats["cancelled_dexscreener"] += 1
                continue
            if _mrputin_mcap is not None:
                logger.info(f"[MR.PUTIN] mcap gate OK — ${_mrputin_mcap:,.0f} (≤$5k)")
            else:
                logger.info(f"[MR.PUTIN] mcap gate: pump.fun unavailable — fail-open ({token_mint[:8]})")
        # ----------------------------------------------------------------

        # --- PumpFun prebond check -------------------------------------
        prebond_pct, is_graduated = await fetch_prebond_progress(session, token_mint)
        prebond_buy_sol: float | None = None  # set to override BUY_AMOUNT_SOL for prebond entries

        if prebond_pct is None:
            # PumpFun API unreachable — not a pump.fun token or API down — fail-open
            logger.debug(f"[PREBOND] No pump.fun data for {token_mint[:8]} — proceeding normally")
        elif is_graduated:
            # Token already graduated — add to dip sniper watchlist, proceed to DexScreener
            logger.info(f"[PREBOND] {token_mint[:8]} already graduated — proceeding to DexScreener")
            if dex_pair is not None:
                grad_price = float((dex_pair.get("priceNative") or 0) or 0)
                _add_to_graduated_watchlist(token_mint, grad_price)
        else:
            pb_score, pb_action = prebond_decision(prebond_pct)
            if pb_action == "BLOCK":
                logger.info(
                    f"[PREBOND] BLOCKED — curve at {prebond_pct:.0f}%, too late for entry "
                    f"({token_mint[:8]})"
                )
                _stats["cancelled_prebond"] += 1
                continue
            # PROCEED — use 2% of current SOL balance as position size
            prebond_buy_sol = round(sol_balance * PREBOND_POS_SIZE_PCT, 4)
            logger.info(
                f"[PREBOND] Curve: {prebond_pct:.0f}% | Score: {pb_score} | "
                f"Position: 2% ({prebond_buy_sol} SOL) | Action: PROCEED"
            )
        # ----------------------------------------------------------------

        # --- Entry quote (immediate — no delay) ------------------------
        entry_quote = await get_jupiter_quote(
            session, token_mint, int(BUY_AMOUNT_SOL * 1_000_000_000)
        )
        if not entry_quote:
            logger.error(f"[{name}] SKIP — entry quote failed for {token_mint[:8]}")
            continue
        # ----------------------------------------------------------------

        # --- Claude confidence scoring ---------------------------------
        claude_score = await get_claude_score(
            token_mint,
            dex_pair,
            prebond_pct,   # None if not a pump.fun token or if graduated
            f"whale={name} signal, conviction={'high' if is_high_conviction else 'normal'}",
        )
        tier = get_exit_tier(claude_score)
        logger.info(
            f"[{name}] Claude score: {claude_score}/100 | "
            f"Tier: min_target={tier['min_target_pct']}% "
            f"trail={tier['trail_pct']}% "
            f"time={tier['time_stop_min']}m"
        )
        # ----------------------------------------------------------------

        # --- Position sizing — mr.putin > prebond > conviction > normal ---
        if name == "mr.putin":
            buy_sol = round(sol_balance * MRPUTIN_CONFIG["position_size_pct"], 4)
            logger.info(
                f"[MR.PUTIN] Position: {buy_sol} SOL (1% balance) | "
                f"min hold {MRPUTIN_CONFIG['min_hold_mins']}m | "
                f"time stop {MRPUTIN_CONFIG['time_stop_mins']}m"
            )
        elif prebond_buy_sol is not None:
            # Prebond entry: fixed 2% of balance (no conviction multiplier on prebond)
            buy_sol = prebond_buy_sol
            logger.info(f"[{name}] Prebond position size: {buy_sol} SOL (2% of balance)")
        elif is_high_conviction:
            buy_sol = round(BUY_AMOUNT_SOL * CONVICTION_MULTIPLIER, 4)
            logger.info(
                f"[{name.upper()}] HIGH CONVICTION sizing confirmed — "
                f"{buy_sol} SOL ({CONVICTION_MULTIPLIER}x normal)"
            )
        else:
            buy_sol = BUY_AMOUNT_SOL
        # ----------------------------------------------------------------

        # Use entry quote for normal size; re-fetch for non-standard sizes
        if buy_sol == BUY_AMOUNT_SOL:
            quote = entry_quote
        else:
            amount_lamports = int(buy_sol * 1_000_000_000)
            quote = await get_jupiter_quote(session, token_mint, amount_lamports)
            if not quote:
                logger.error(
                    f"[{name}] SKIP — conviction-sized quote failed for {token_mint[:8]}"
                )
                continue

        swap_sig, swap_msg = await execute_swap(session, quote, wallet_pubkey)

        if not swap_sig:
            logger.error(
                f"[{name}] Buy on {token_mint[:8]} did not confirm — "
                f"{swap_msg} — position NOT opened"
            )
            send_telegram(
                f"⚠️ <b>TX FAILED</b> — [{name.upper()}] buy on "
                f"<code>{token_mint[:8]}</code> did not confirm\n"
                f"Reason: {swap_msg}\n"
                f"Position NOT opened — no money spent"
            )
            continue

        # High conviction gets its own priority Telegram alert first
        token_label = _token_label(token_mint, dex_pair)
        mc_entry    = float(
            (dex_pair or {}).get("marketCap") or (dex_pair or {}).get("fdv") or 0
        )
        if is_high_conviction:
            send_telegram(
                f"🔥 <b>HIGH CONVICTION</b> — [{name.upper()}] bought "
                f"<code>{token_label}</code> twice in 30 mins\n"
                f"Position size: {buy_sol} SOL ({CONVICTION_MULTIPLIER}x normal)"
            )

        conviction_badge = "🔥 HIGH CONVICTION\n" if is_high_conviction else ""
        mc_entry_str     = _fmt_usd(mc_entry) if mc_entry else "—"

        if mc_entry:
            _sol_px    = _sol_price_from_dex(dex_pair)
            _entry_usd = buy_sol * _sol_px if _sol_px else 0.0
            _sell_frac = TAKE_PROFIT_PCT / 100          # e.g. 0.5
            _hold_frac = 1.0 - _sell_frac               # remaining after TP1
            _sell_pct  = f"{int(TAKE_PROFIT_PCT)}%"

            # TP1: sell sell_frac at 2x → returns sell_frac*2x of entry = full entry back
            _tp1_take  = _sell_frac * 2 * _entry_usd
            # TP2/TP3: hold_frac of tokens, priced at 5x and 10x of entry
            _tp2_worth = _hold_frac * 5  * _entry_usd
            _tp3_worth = _hold_frac * 10 * _entry_usd

            if _entry_usd:
                tp_block = (
                    f"\n\n🎯 <b>Targets:</b>\n"
                    f"  1️⃣ {_fmt_usd(mc_entry * 2)} (2x) → sell {_sell_pct} | take {_fmt_usd(_tp1_take)} back\n"
                    f"  2️⃣ {_fmt_usd(mc_entry * 5)} (5x) → free ride | worth {_fmt_usd(_tp2_worth)}\n"
                    f"  3️⃣ {_fmt_usd(mc_entry * 10)} (10x) → free ride | worth {_fmt_usd(_tp3_worth)}"
                )
            else:
                # SOL price unavailable — show MC targets without USD projections
                tp_block = (
                    f"\n\n🎯 <b>Targets:</b>\n"
                    f"  1️⃣ {_fmt_usd(mc_entry * 2)} (2x) → sell {_sell_pct}\n"
                    f"  2️⃣ {_fmt_usd(mc_entry * 5)} (5x) → free ride\n"
                    f"  3️⃣ {_fmt_usd(mc_entry * 10)} (10x) → free ride"
                )
        else:
            tp_block = ""
        msg = (
            f"🐋 <b>APEX WHALE COPY</b> [{name.upper()}]\n"
            f"{conviction_badge}"
            f"Token: <code>{token_label}</code>\n"
            f"Amount: {buy_sol} SOL\n"
            f"MC Entry: {mc_entry_str}\n"
            f"Whale: <code>{name.upper()}</code>\n"
            f"Our sig: <code>{swap_sig}</code>"
            f"{tp_block}"
        )
        logger.info(msg)
        send_telegram(msg)

        # --- Register open position for sell monitoring ----------------
        token_units = int(quote.get("outAmount", 0))
        entry_sol   = int(quote.get("inAmount",  0)) / 1_000_000_000
        if token_units > 0:
            open_positions[token_mint] = {
                "entry_time":    time.time(),
                "entry_sol":     entry_sol,
                "peak_sol":      entry_sol,   # starts equal to entry
                "amount_tokens": token_units,
                "whale":         name,
                "buy_sol":       buy_sol,
                "claude_score":  claude_score,
                "min_target_hit": False,
                "source":        "whale",
                "mc_entry":      mc_entry,
                "token_label":   token_label,
            }
            logger.info(
                f"[{token_mint[:8]}] Position opened — "
                f"{token_units:,} tokens | entry {entry_sol:.4f} SOL"
            )
            _stats["trades_executed"] += 1
            # Emergency dump check fires 5s after buy — non-blocking — Fix 4
            asyncio.create_task(emergency_dump_check(session, token_mint, wallet_pubkey))
        # ---------------------------------------------------------------


async def position_monitor_loop(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
) -> None:
    """Check all open positions for exit conditions every POSITION_CHECK_SEC seconds."""
    while True:
        await asyncio.sleep(POSITION_CHECK_SEC)
        count = len(open_positions)
        logger.info(f"Position monitor: checking {count} open position(s)")
        for token_mint in list(open_positions.keys()):
            await check_and_maybe_exit(session, token_mint, wallet_pubkey)


async def whale_poll_loop(
    session: aiohttp.ClientSession,
    rpc_url: str,
    wallet_pubkey: str,
) -> None:
    """Poll all whale wallets for new buys every POLL_INTERVAL_SEC seconds."""
    while True:
        tasks = [
            poll_whale(session, name, wallet, rpc_url, wallet_pubkey)
            for name, wallet in WHALE_WALLETS.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for wname, result in zip(WHALE_WALLETS.keys(), results):
            if isinstance(result, Exception):
                logger.error(
                    f"[WHALE] [{wname}] poll_whale raised unhandled exception: "
                    f"{type(result).__name__}: {result}"
                )
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def run():
    global _rpc_url
    rpc_url       = os.getenv("SOLANA_RPC", "")
    wallet_pubkey = os.getenv("WALLET_PUBLIC_KEY", "")   # base58 address — for balance checks + Jupiter
    wallet_key    = os.getenv("WALLET_PRIVATE_KEY", "")  # byte array    — for tx signing (live mode only)
    _rpc_url      = rpc_url                              # module-level — used by confirm_transaction()

    if not rpc_url:
        logger.error("SOLANA_RPC not set — exiting")
        return
    if not wallet_pubkey:
        logger.error(
            "WALLET_PUBLIC_KEY not set in .env — balance guard will always read 0.0 SOL. "
            "Add: WALLET_PUBLIC_KEY=<your base58 address>"
        )

    logger.info(f"Whale Sniper starting — DRY_RUN={DRY_RUN}")
    logger.info(f"Tracking {len(WHALE_WALLETS)} whales: {', '.join(WHALE_WALLETS)}")
    for wname in WHALE_WALLETS:
        logger.info(f"[{wname.upper()}] COLD WHALE ❄️ (no activity recorded yet)")

    _register_commands()
    startup_checks(rpc_url, wallet_pubkey)
    logger.info(f"Position monitor: {len(open_positions)} open position(s) at startup")
    now_ts = time.time()
    active_bl = {m: exp for m, exp in _token_blacklist.items() if exp > now_ts}
    if not active_bl:
        logger.info("Blacklist: empty")
    else:
        longest_min = (max(active_bl.values()) - now_ts) / 60
        logger.info(
            f"Blacklist: {len(active_bl)} token(s) active, "
            f"longest expiry {longest_min:.0f}min from now"
        )

    # Load dip sniper watchlist from disk
    graduated_watchlist.update(_load_graduated_watchlist())
    logger.info(f"Dip sniper watchlist loaded: {len(graduated_watchlist)} token(s)")

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            whale_poll_loop(session, rpc_url, wallet_pubkey),
            position_monitor_loop(session, wallet_pubkey),
            midnight_summary_loop(),
            telegram_command_loop(),
            dip_sniper_loop(session, wallet_pubkey),
        )


if __name__ == "__main__":
    asyncio.run(run())
