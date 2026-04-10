"""
APEX Whale Sniper
Monitors 4 whale wallets on Solana and mirrors their token buys via Jupiter.
"""

from __future__ import annotations

import os
import re
import asyncio
import base64
import logging
import time
import json
import aiohttp
import anthropic
import requests
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from solders.keypair import Keypair as SoldersKeypair
from solders.transaction import VersionedTransaction

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
MAX_SLIPPAGE_BPS      = int(os.getenv("MAX_SLIPPAGE_BPS", "2000"))
SELL_SLIPPAGE_BPS     = 2000   # 20% slippage for sells
PREFER_JUPITER_SELLS  = True   # Jupiter first for sells; PumpPortal as fallback
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
    # "mr.putin": "8mzCDvq5JWJh6Cus7XYnnwL2JGCVUXA3bDqaXmzCG5hn",  # disabled 2026-04-07
    "peace2":   "6iZLfoaYvEAuuhnJEiSkwC9exmtMZehpkUVuFzb19sWc",
    # "spsc":     "7S3E2L25kr6oN2cMP2GQ5tMEfg8jwcmoYo35vvv8rxhW",  # disabled 2026-04-08
    # "early":  "Bv2BAw5UmKxv5SBMWYKqpsh6eXKNGM2RKxJGpGPk5vmb",  # disabled 2026-03-31
}

# Track the last seen signature per wallet to detect new txns
last_seen_sig: dict[str, str | None] = {name: None for name in WHALE_WALLETS}

# Whale activity log: name -> list of (mint, timestamp) for all buys
_whale_activity: dict[str, list[tuple[str, float]]] = {name: [] for name in WHALE_WALLETS}

ACTIVITY_WINDOW_SEC   = 86_400  # 24 h    — HOT / COLD scoring window
HOT_THRESHOLD         = 3       # buys in 24 h to be classified HOT

# --- Sell / exit parameters -------------------------------------------
HARD_TP_MULT       = 2.1    # hard take-profit: sell 100% if current_sol >= entry_sol * this
TAKE_PROFIT_PCT    = float(os.getenv("TAKE_PROFIT_PCT", "0.50")) * 100  # .env decimal → % (e.g. 0.38 = 38%)
TRAILING_STOP_PCT  = float(os.getenv("TRAILING_STOP_PCT", "0.10")) * 100  # .env decimal → % (e.g. 0.13 = 13%)
TIME_STOP_MIN      = int(os.getenv("TIME_STOP_MIN", "30"))  # minutes
POSITION_CHECK_SEC = 10     # how often the sell monitor loop runs

# --- Emergency exit parameters ----------------------------------------
EMERGENCY_DUMP_PCT        = 40.0  # emergency exit if down >40% right after buy
EMERGENCY_CHECK_DELAY_SEC = 5    # seconds after buy before emergency check runs

# --- DexScreener quality filter ---------------------------------------
DEXSCREENER_API       = "https://api.dexscreener.com/tokens/v1/solana"
MIN_DEX_LIQUIDITY_USD    = 4_500
MIN_DEX_5M_VOLUME_USD    = 10_000
MIN_PUMP_VIRTUAL_LIQ_USD = 4_500

# --- PumpFun prebond filter -------------------------------------------
PUMPFUN_API          = "https://frontend-api.pump.fun/coins"
PREBOND_POS_SIZE_PCT = 0.02   # 2% of current SOL balance for prebond entries
GRADUATION_MC_USD    = 32_700  # PumpFun bonding curve graduation threshold

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
    "hard_floor_pct":    -35.0,  # Stop loss from entry
    "trail_pct":         20.0,   # Trailing stop from peak
    "min_hold_mins":     120,    # Never sell before 2 hours
    "time_stop_mins":    4_320,  # Force exit after 3 days (3×24×60)
}

# --- Token safety check -----------------------------------------------
MAX_TOP10_HOLDER_PCT = 35.0   # block if top 10 holders control > 35% of supply
MAX_DEV_HOLDER_PCT   = 8.0    # warn threshold; hard block fires at 15%
MAX_BUNDLE_HOLD_PCT  = 40.0   # block if bundle wallets collectively hold > 40% of supply
MIN_TX_COUNT         = 40     # warn (not block) if fewer than 40 txs seen

# --- Dip sniper -------------------------------------------------------
GRADUATED_WATCHLIST_PATH = "data/graduated_watchlist.json"
DIP_SNIPER_DROP_PCT      = 50.0   # trigger re-entry if price drops X% from ATH
DIP_SNIPER_MIN_SCORE     = 65     # minimum Claude score to enter a dip
WHALE_MIN_SCORE          = 50     # minimum Claude score to enter a whale copy
CTO_SIGNAL_BUY_SOL       = 0.15   # Fixed position size for DexAlert CTO signals
DIP_SNIPER_WATCH_HOURS   = 8      # expire tokens from watchlist after X hours
DIP_SNIPER_CHECK_SEC     = 60     # how often the dip sniper loop runs

# In-memory graduated watchlist (loaded from disk at startup)
# mint → {graduation_price_sol: float, ath_sol: float, added_ts: float}
graduated_watchlist: dict[str, dict] = {}

# Persistence paths — both files live next to the script
POSITIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "open_positions.json")
BLACKLIST_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "blacklist.json")

# Open positions: token_mint → position dict (populated after every buy)
open_positions: dict[str, dict] = {}

# Blacklist: token_mint → expiry timestamp. Only set on TRAILING STOP exits.
# Take-profit and time-stop closures do NOT blacklist — re-entry on winners allowed.
_token_blacklist: dict[str, float] = {}
BLACKLIST_MINUTES = 45          # minutes to ban a token after a trailing stop loss

# Set once at startup in run() — lets confirm_transaction() reach the RPC
# without threading rpc_url through every intermediate function signature.
_rpc_url: str = ""

# Set once at startup in run() — wallet keypair for signing PumpFun transactions.
_wallet_keypair: SoldersKeypair | None = None

# --- Daily trade statistics (reset at midnight UTC) -------------------
_stats: dict = {
    "signals_detected":       0,   # every whale buy signal seen
    "cancelled_dexscreener":  0,   # filtered by liquidity / volume check
    "cancelled_prebond":      0,   # filtered by PumpFun bonding curve check
    "cancelled_safety":       0,   # blocked by token safety check
    "trades_executed":        0,   # buys that confirmed on-chain
    "tp1_partials_executed":  0,   # successful TP1 partial sells at 2x
    "wins":                   0,   # closed positions with PnL >= 0
    "losses":                 0,   # closed positions with PnL < 0
    "net_pnl_sol":            0.0, # running sum of (exit_sol - entry_sol)
}

# --- Telegram chat IDs (multi-chat broadcast + control) ---------------
def _load_chat_ids() -> list[str]:
    """Load all Telegram chat IDs for alert broadcast from env.

    Includes TELEGRAM_CHAT_ID, TELEGRAM_CHAT_IDS (comma-separated),
    and TELEGRAM_CHAT_ID_2 so both users receive all alerts.
    """
    ids: list[str] = []
    multi = os.getenv("TELEGRAM_CHAT_IDS", "").strip()
    if multi:
        ids.extend(cid.strip() for cid in multi.split(",") if cid.strip())
    else:
        single = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if single:
            ids.append(single)
    secondary = os.getenv("TELEGRAM_CHAT_ID_2", "").strip()
    if secondary and secondary not in ids:
        ids.append(secondary)
    return ids

_telegram_chat_ids: list[str] = _load_chat_ids()

def _load_allowed_control_ids() -> set[str]:
    """Build set of chat IDs allowed to send commands and tap buttons.

    Includes all broadcast IDs plus TELEGRAM_CHAT_ID_2 (control-only).
    """
    ids = set(_telegram_chat_ids)
    secondary = os.getenv("TELEGRAM_CHAT_ID_2", "").strip()
    if secondary:
        ids.add(secondary)
    return ids

_allowed_control_ids: set[str] = _load_allowed_control_ids()

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


async def _arpc_post(
    session: aiohttp.ClientSession,
    rpc_url: str,
    method: str,
    params: list | dict,
) -> dict:
    """Async JSON-RPC POST with Helius call tracking. Raises on HTTP errors."""
    _track_helius_call()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(
        rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
    ) as resp:
        resp.raise_for_status()
        return await resp.json()


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


async def get_spl_token_balance(
    session: aiohttp.ClientSession,
    token_mint: str,
    wallet_pubkey: str,
) -> int:
    """Fetch the live on-chain SPL token balance (raw units, no decimals) for a mint.

    Uses getTokenAccountsByOwner with the same pattern as the safety-check
    helpers.  Returns 0 if the token account doesn't exist or on any RPC error.
    """
    rpc_url = _rpc_url
    if not rpc_url or not wallet_pubkey:
        logger.warning("[SPL BAL] rpc_url or wallet_pubkey not set — returning 0")
        return 0
    try:
        resp = await _arpc_post(
            session, rpc_url,
            "getTokenAccountsByOwner",
            [
                wallet_pubkey,
                {"mint": token_mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        )
        accts = (resp.get("result") or {}).get("value") or []
        total_raw = 0
        for acct in accts:
            info = (
                (acct.get("account") or {})
                .get("data", {})
                .get("parsed", {})
                .get("info", {})
            )
            amount_str = (info.get("tokenAmount") or {}).get("amount") or "0"
            total_raw += int(amount_str)
        return total_raw
    except Exception as e:
        logger.error(f"[SPL BAL] getTokenAccountsByOwner failed for {token_mint[:8]}: {e}")
        return 0


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


async def fetch_pumpfun_data(
    session: aiohttp.ClientSession,
    token_mint: str,
) -> dict | None:
    """
    Fetch token data directly from PumpFun for pre-graduation coins.
    Returns dict with: virtual_sol_reserves, usd_market_cap, created_timestamp,
    name, symbol, bonding_curve_progress. Returns None on failure (fail-open).
    Retries up to 3 times on 530 (temporary server error).
    """
    url = f"{PUMPFUN_API}/{token_mint}"
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 404:
                    return None
                if resp.status == 530:
                    logger.warning(
                        f"PumpFun 530 for {token_mint[:8]} — "
                        f"retry {attempt + 1}/3"
                    )
                    if attempt < 2:
                        await asyncio.sleep(2)
                        continue
                    return None
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.warning(f"PumpFun data fetch failed for {token_mint[:8]}: {e}")
            return None
    return None


def passes_pump_quality(pump_data: dict, sol_price_usd: float = 140.0) -> tuple[bool, str]:
    """
    Quality check for pre-graduation PumpFun coins using bonding curve data.
    Uses virtual_sol_reserves as liquidity proxy.
    Fail-open if data missing.
    """
    virtual_sol = pump_data.get("virtual_sol_reserves", 0) or 0
    # Convert lamports to SOL if needed (values > 1000 are likely lamports)
    if virtual_sol > 1000:
        virtual_sol = virtual_sol / 1_000_000_000
    virtual_liq_usd = virtual_sol * sol_price_usd
    mcap = pump_data.get("usd_market_cap", 0) or 0

    if virtual_liq_usd < MIN_PUMP_VIRTUAL_LIQ_USD:
        return False, f"PumpFun virtual liq ${virtual_liq_usd:,.0f} below ${MIN_PUMP_VIRTUAL_LIQ_USD:,}"
    return True, f"pump_liq=${virtual_liq_usd:,.0f} mcap=${mcap:,.0f}"


# --- Token safety check -----------------------------------------------

async def check_token_safety(
    session: aiohttp.ClientSession,
    token_mint: str,
    helius_url: str,
    whale_name: str,
    dex_pair: dict | None = None,
) -> tuple[bool, str]:
    """
    Run 4 concurrent safety checks on a token before entry.

    Checks (all run via asyncio.gather):
      1. Top-holder concentration  — block if top 10 > MAX_TOP10_HOLDER_PCT (35%)
      2. Dev wallet holdings       — warn at MAX_DEV_HOLDER_PCT (8%), block at >15%;
                                     also block if mint authority not revoked
      3. Bundle detection          — identifies wallets that bought in the first 3
                                     blocks, then fetches their current token balance;
                                     block if they collectively hold > MAX_BUNDLE_HOLD_PCT (40%)
      4. Tx count (warn-only)      — warn if total sigs < MIN_TX_COUNT (40)

    Every individual check fails open: an API error silently skips that check.
    Returns (safe, block_reason). safe=True → proceed with entry.
    Sends a Telegram notification with the full result card.
    """
    _DEV_HARD_BLOCK_PCT = 15.0   # hard block threshold; 8-15% warns but allows

    # Mutable result holders (written by inner coroutines via nonlocal)
    holder_pct:      float | None = None
    dev_pct:         float | None = None
    dev_status:      str          = "Dev wallet unknown"
    dev_blocks:      bool         = False
    mint_auth_block: bool         = False
    bundle_count:    int | None   = None
    bundle_hold_pct: float | None = None
    tx_count:        int          = 0

    # ----------------------------------------------------------------
    # Check 1 — top-holder concentration
    # ----------------------------------------------------------------
    async def _check_holders() -> None:
        nonlocal holder_pct
        try:
            accts_resp = await _arpc_post(
                session, helius_url,
                "getTokenLargestAccounts",
                [token_mint, {"commitment": "confirmed"}],
            )
            holders = (accts_resp.get("result") or {}).get("value") or []
            if not holders:
                return

            supply_resp = await _arpc_post(
                session, helius_url,
                "getTokenSupply",
                [token_mint, {"commitment": "confirmed"}],
            )
            total = float(
                ((supply_resp.get("result") or {}).get("value") or {}).get("uiAmount") or 0
            )
            if total <= 0:
                return

            top10 = sorted(
                holders, key=lambda h: float(h.get("uiAmount") or 0), reverse=True
            )[:10]
            top10_sum = sum(float(h.get("uiAmount") or 0) for h in top10)
            holder_pct = (top10_sum / total) * 100
            logger.info(
                f"[{token_mint[:8]}] Holder concentration: top 10 = {holder_pct:.1f}%"
            )
        except Exception as e:
            logger.debug(f"[{token_mint[:8]}] holder check skipped: {e}")

    # ----------------------------------------------------------------
    # Check 2 — dev wallet holdings + mint authority
    # ----------------------------------------------------------------
    async def _check_dev() -> None:
        nonlocal dev_pct, dev_status, dev_blocks, mint_auth_block
        try:
            # Creator address via Helius DAS getAsset
            asset_resp = await _arpc_post(
                session, helius_url,
                "getAsset",
                {"id": token_mint},
            )
            asset = asset_resp.get("result") or {}

            creator_addr: str | None = None
            creators = asset.get("creators") or []
            if creators:
                verified     = [c for c in creators if c.get("verified")]
                best         = verified[0] if verified else creators[0]
                creator_addr = best.get("address")
            if not creator_addr:
                creator_addr = asset.get("update_authority") or None
            if not creator_addr:
                auths = asset.get("authorities") or []
                if auths:
                    creator_addr = auths[0].get("address")

            if creator_addr:
                tok_resp = await _arpc_post(
                    session, helius_url,
                    "getTokenAccountsByOwner",
                    [
                        creator_addr,
                        {"mint": token_mint},
                        {"encoding": "jsonParsed", "commitment": "confirmed"},
                    ],
                )
                tok_accts = (tok_resp.get("result") or {}).get("value") or []
                dev_bal = 0.0
                for acct in tok_accts:
                    info = (
                        (acct.get("account") or {})
                        .get("data", {})
                        .get("parsed", {})
                        .get("info", {})
                    )
                    dev_bal += float(
                        (info.get("tokenAmount") or {}).get("uiAmount") or 0
                    )

                supply_resp = await _arpc_post(
                    session, helius_url,
                    "getTokenSupply",
                    [token_mint, {"commitment": "confirmed"}],
                )
                total = float(
                    ((supply_resp.get("result") or {}).get("value") or {}).get(
                        "uiAmount"
                    ) or 0
                )
                if total > 0:
                    dev_pct = (dev_bal / total) * 100
                    if dev_pct <= 1.0:
                        dev_status = "Dev fully out ✅"
                        dev_blocks = False
                    elif dev_pct <= _DEV_HARD_BLOCK_PCT:
                        dev_status = f"Dev holds {dev_pct:.1f}% ⚠️"
                        dev_blocks = False
                    else:
                        dev_status = f"Dev holds {dev_pct:.1f}% 🚨 BLOCK"
                        dev_blocks = True
                    logger.info(f"[{token_mint[:8]}] {dev_status}")
                else:
                    dev_status = "Dev wallet unknown"

            # Mint authority check via getAccountInfo
            acct_resp = await _arpc_post(
                session, helius_url,
                "getAccountInfo",
                [token_mint, {"encoding": "jsonParsed", "commitment": "confirmed"}],
            )
            acct_data = (
                ((acct_resp.get("result") or {}).get("value") or {}).get("data") or {}
            )
            if isinstance(acct_data, dict):
                parsed_info = acct_data.get("parsed", {}).get("info") or {}
                mint_auth   = parsed_info.get("mintAuthority")
                if mint_auth is not None:
                    mint_auth_block = True
                    logger.warning(
                        f"[{token_mint[:8]}] Mint authority ACTIVE: {mint_auth} 🚨"
                    )
                else:
                    logger.info(f"[{token_mint[:8]}] Mint authority revoked ✅")
        except Exception as e:
            logger.debug(f"[{token_mint[:8]}] dev check skipped: {e}")

    # ----------------------------------------------------------------
    # Check 3+4 — bundle detection + tx count (shared API call)
    #
    # Bundle logic:
    #   1. Identify wallets that signed transactions in the first 3 blocks
    #   2. Fetch each bundle wallet's current token balance concurrently
    #   3. BLOCK if their combined holding > MAX_BUNDLE_HOLD_PCT (40%)
    # ----------------------------------------------------------------
    async def _check_bundles_and_tx() -> None:
        nonlocal bundle_count, bundle_hold_pct, tx_count
        try:
            sigs_resp = await _arpc_post(
                session, helius_url,
                "getSignaturesForAddress",
                [token_mint, {"limit": 40, "commitment": "confirmed"}],
            )
            sigs     = sigs_resp.get("result") or []
            tx_count = len(sigs)
            logger.info(f"[{token_mint[:8]}] Tx count (sample): {tx_count}")

            if not sigs:
                bundle_count    = 0
                bundle_hold_pct = 0.0
                return

            # Oldest 3 unique slots = launch blocks
            first_slots: list[int] = []
            for s in reversed(sigs):
                slot = s.get("slot")
                if slot is not None and slot not in first_slots:
                    first_slots.append(slot)
                if len(first_slots) >= 3:
                    break

            first_slot_set = set(first_slots)
            early_sigs = [
                s["signature"]
                for s in sigs
                if s.get("slot") in first_slot_set and s.get("signature")
            ][:10]   # cap fetches at 10 getTransaction calls

            unique_signers: set[str] = set()
            for sig in early_sigs:
                try:
                    tx_resp = await _arpc_post(
                        session, helius_url,
                        "getTransaction",
                        [
                            sig,
                            {
                                "encoding": "jsonParsed",
                                "commitment": "confirmed",
                                "maxSupportedTransactionVersion": 0,
                            },
                        ],
                    )
                    acct_keys = (
                        ((tx_resp.get("result") or {}).get("transaction") or {})
                        .get("message", {})
                        .get("accountKeys") or []
                    )
                    if acct_keys:
                        first_key = acct_keys[0]
                        fee_payer = (
                            first_key if isinstance(first_key, str)
                            else first_key.get("pubkey", "")
                        )
                        if fee_payer:
                            unique_signers.add(fee_payer)
                except Exception:
                    pass   # fail-open per individual tx

            bundle_count = len(unique_signers)

            if not unique_signers:
                bundle_hold_pct = 0.0
                return

            # Fetch total supply independently (don't share with _check_holders
            # — they run concurrently and may not have completed yet)
            supply_resp = await _arpc_post(
                session, helius_url,
                "getTokenSupply",
                [token_mint, {"commitment": "confirmed"}],
            )
            total = float(
                ((supply_resp.get("result") or {}).get("value") or {}).get("uiAmount") or 0
            )
            if total <= 0:
                return

            # Fetch current token balance for each bundle wallet concurrently
            async def _wallet_balance(wallet: str) -> float:
                try:
                    resp = await _arpc_post(
                        session, helius_url,
                        "getTokenAccountsByOwner",
                        [
                            wallet,
                            {"mint": token_mint},
                            {"encoding": "jsonParsed", "commitment": "confirmed"},
                        ],
                    )
                    accts = (resp.get("result") or {}).get("value") or []
                    bal = 0.0
                    for acct in accts:
                        info = (
                            (acct.get("account") or {})
                            .get("data", {})
                            .get("parsed", {})
                            .get("info", {})
                        )
                        bal += float((info.get("tokenAmount") or {}).get("uiAmount") or 0)
                    return bal
                except Exception:
                    return 0.0   # fail-open per wallet

            balances        = await asyncio.gather(*[_wallet_balance(w) for w in unique_signers])
            bundle_hold_tot = sum(balances)
            bundle_hold_pct = (bundle_hold_tot / total) * 100

            logger.info(
                f"[{token_mint[:8]}] Bundle check: {bundle_count} wallet(s) in first "
                f"{len(first_slots)} block(s) — hold {bundle_hold_pct:.1f}% of supply"
            )
        except Exception as e:
            logger.debug(f"[{token_mint[:8]}] bundle check skipped: {e}")

    # ----------------------------------------------------------------
    # Run all checks concurrently
    # ----------------------------------------------------------------
    await asyncio.gather(
        _check_holders(),
        _check_dev(),
        _check_bundles_and_tx(),
        return_exceptions=True,
    )

    # ----------------------------------------------------------------
    # Build per-line status strings
    # ----------------------------------------------------------------

    # Holder status
    if holder_pct is None:
        holder_line  = "Holders: unknown ⚠️"
        holder_block = False   # fail-open
    elif holder_pct > MAX_TOP10_HOLDER_PCT:
        holder_line  = f"Holders: top 10 = {holder_pct:.0f}% 🚨"
        holder_block = True
    else:
        holder_line  = f"Holders: top 10 = {holder_pct:.0f}% ✅"
        holder_block = False

    # Bundle status — show wallet count + hold% regardless of pass/fail
    _bcount = bundle_count if bundle_count is not None else "?"
    _bpct   = f"{bundle_hold_pct:.0f}%" if bundle_hold_pct is not None else "?%"
    if bundle_hold_pct is None:
        bundle_line  = f"Bundles: {_bcount} wallets in first 3 blocks — held {_bpct} of supply ⚠️"
        bundle_block = False   # fail-open
    elif bundle_hold_pct > MAX_BUNDLE_HOLD_PCT:
        bundle_line  = f"Bundles: {_bcount} wallets in first 3 blocks — held {_bpct} of supply 🚨"
        bundle_block = True
    else:
        bundle_line  = f"Bundles: {_bcount} wallets in first 3 blocks — held {_bpct} of supply ✅"
        bundle_block = False

    # Tx count (warn-only)
    tx_line = f"Txs: {tx_count} ✅" if tx_count >= MIN_TX_COUNT else f"Txs: {tx_count} ⚠️"

    # ----------------------------------------------------------------
    # Collect block reasons
    # ----------------------------------------------------------------
    block_reasons: list[str] = []
    if dev_blocks:
        block_reasons.append(f"dev holds {dev_pct:.0f}% of supply")
    if mint_auth_block:
        block_reasons.append("mint authority not revoked")
    if holder_block:
        block_reasons.append(f"top 10 holders = {holder_pct:.0f}%")
    if bundle_block:
        block_reasons.append(f"bundle wallets hold {bundle_hold_pct:.0f}% of supply")

    is_safe = len(block_reasons) == 0

    # ----------------------------------------------------------------
    # Telegram notification
    # ----------------------------------------------------------------
    token_name = ((dex_pair or {}).get("baseToken") or {}).get("name") or "Unknown"
    verdict_line = (
        "✅ PASSED — proceeding to Claude score"
        if is_safe
        else f"❌ BLOCKED — {', '.join(block_reasons)}"
    )
    send_telegram(
        f"🔍 <b>SAFETY CHECK</b>\n"
        f"\n{token_name}\n"
        f"<code>{token_mint}</code>\n"
        f"\n  {dev_status}\n"
        f"  {holder_line}\n"
        f"  {bundle_line}\n"
        f"  {tx_line}\n"
        f"\n{verdict_line}"
    )

    # ----------------------------------------------------------------
    # Log summary line
    # ----------------------------------------------------------------
    dev_log     = (
        "out"              if "fully out" in dev_status
        else f"{dev_pct:.0f}%" if dev_pct is not None
        else "unknown"
    )
    result_icon = "✅" if is_safe else "❌"
    logger.info(
        f"[{token_mint[:8]}] Safety: "
        f"dev={dev_log}, "
        f"holders={'unknown' if holder_pct is None else f'{holder_pct:.0f}%'}, "
        f"bundles={'unknown' if bundle_hold_pct is None else f'{bundle_hold_pct:.0f}%'}, "
        f"txs={tx_count} {result_icon}"
    )

    return is_safe, "; ".join(block_reasons)


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


# --- Position + blacklist persistence ------------------------------------

def _save_positions() -> None:
    """Atomically persist open_positions to disk (temp-file + os.replace)."""
    tmp = POSITIONS_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(open_positions, f, indent=2)
        os.replace(tmp, POSITIONS_FILE)
    except Exception as e:
        logger.warning(f"[PERSIST] Could not save positions: {e}")


def _load_positions() -> dict:
    """Load open_positions from disk. Returns {{}} on missing file or parse error."""
    try:
        with open(POSITIONS_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"[PERSIST] Could not load positions from {POSITIONS_FILE}: {e}")
        return {}


def _save_blacklist() -> None:
    """Atomically persist _token_blacklist to disk (temp-file + os.replace)."""
    tmp = BLACKLIST_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(_token_blacklist, f, indent=2)
        os.replace(tmp, BLACKLIST_FILE)
    except Exception as e:
        logger.warning(f"[PERSIST] Could not save blacklist: {e}")


def _load_blacklist() -> dict:
    """Load _token_blacklist from disk. Returns {{}} on missing file or parse error."""
    try:
        with open(BLACKLIST_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"[PERSIST] Could not load blacklist from {BLACKLIST_FILE}: {e}")
        return {}


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
    pump_data: dict | None = None,
) -> tuple[int, list[str] | None]:
    """
    Ask Claude to score a token's short-term trading potential 0-100.

    Returns (score, bullets) where:
      - bullets is a list of up to 4 short reasoning strings on success
      - bullets is None if the API was unavailable or the call failed (fail-open)

    Fails open at (70, None) if CLAUDE_API_KEY is absent or API call fails.
    Never logs the API key value.
    """
    api_key = os.getenv("CLAUDE_API_KEY", "")
    if not api_key:
        logger.warning("CLAUDE_API_KEY not set — Claude score defaulting to 70 (fail-open)")
        return 70, None

    # --- Build mode-specific prompt ---
    _response_format = (
        "\nRespond in this exact format — no other text:\n"
        "SCORE: <integer 0-100>\n"
        "- <reason 1, max 10 words>\n"
        "- <reason 2, max 10 words>\n"
        "- <reason 3, max 10 words>\n"
        "- <reason 4, max 10 words>\n"
        "Omit bullet lines you don't need. No other text."
    )

    if pump_data is not None and dex_pair is None:
        # ── PRE-GRAD MODE ──────────────────────────────────────────────
        _vsol = pump_data.get("virtual_sol_reserves", 0) or 0
        if _vsol > 1000:
            _vsol = _vsol / 1_000_000_000
        _liq_usd = _vsol * 140.0
        _mcap = pump_data.get("usd_market_cap", 0) or 0
        _prog = prebond_progress if prebond_progress is not None else 0

        prompt = (
            "You are a Solana memecoin trading analyst scoring a PRE-GRADUATION "
            "pump.fun bonding curve token. This token has NO AMM pool yet.\n\n"
            "IMPORTANT: Volume and price change metrics are MEANINGLESS for bonding "
            "curve tokens — ignore them entirely.\n\n"
            "Pump.fun graduation threshold is $32,700 market cap. "
            "Bonding % = (mcap / 32700) * 100.\n\n"
            "On-chain metrics:\n"
            f"Market Cap USD:         ${_mcap:,.0f}\n"
            f"Bonding Curve Progress: {_prog:.1f}%\n"
            f"Virtual SOL Reserves:   {_vsol:.2f} SOL (${_liq_usd:,.0f} USD)\n"
        )
        if context_note:
            prompt += f"Context: {context_note}\n"
        prompt += (
            "\nScoring criteria (in order of importance):\n"
            "1. Bonding curve progress: 20-60% = good entry window, "
            "60-75% = getting late, 75%+ = too close to graduation score low, "
            "sub-20% = very early and risky\n"
            "2. Market cap: sub-$10k very early, $10-20k ideal, "
            "$20-25k late, above $25k too close to graduation\n"
            "3. Virtual SOL reserves as liquidity depth\n"
            "4. Whale conviction from context (HOT whale and high conviction "
            "should significantly boost score)\n\n"
            "Scoring guidance:\n"
            "80-100 = HOT whale + 20-60% bonding + $10-20k mcap\n"
            "60-79  = active whale + reasonable bonding progress\n"
            "40-59  = borderline — very early or getting late\n"
            "0-39   = bad entry (75%+ bonding or mcap above $25k or sub-5% bonding)"
        )
        prompt += _response_format
    else:
        # ── GRADUATED MODE ─────────────────────────────────────────────
        liq  = (dex_pair.get("liquidity")   or {}).get("usd", 0)  if dex_pair else 0
        v5m  = (dex_pair.get("volume")      or {}).get("m5",  0)  if dex_pair else 0
        v1h  = (dex_pair.get("volume")      or {}).get("h1",  0)  if dex_pair else 0
        p5m  = (dex_pair.get("priceChange") or {}).get("m5",  0)  if dex_pair else 0
        p1h  = (dex_pair.get("priceChange") or {}).get("h1",  0)  if dex_pair else 0

        prompt = (
            "You are a Solana memecoin trading analyst scoring a GRADUATED token "
            "that is live on a DEX with an AMM pool.\n\n"
            "On-chain metrics:\n"
            f"Liquidity USD:   ${liq:,.0f}\n"
            f"5m Volume USD:   ${v5m:,.0f}\n"
            f"1h Volume USD:   ${v1h:,.0f}\n"
            f"5m Price Change: {p5m:+.1f}%\n"
            f"1h Price Change: {p1h:+.1f}%\n"
        )
        if context_note:
            prompt += f"Context: {context_note}\n"
        prompt += (
            "\nScoring criteria (in order of importance):\n"
            "1. 5m volume — most important ($10k+ active, $50k+ strong momentum)\n"
            "2. Liquidity ($20k+ meaningful, $10k borderline)\n"
            "3. 1h volume (sustained activity vs one-off spike)\n"
            "4. 5m price change (positive good, extreme pump above +50% means "
            "late entry — score lower)\n"
            "5. 1h price change (overall trend direction)\n"
            "6. Whale conviction from context (HOT whale and high conviction "
            "should significantly boost score)\n\n"
            "Scoring guidance:\n"
            "80-100 = $20k+ liq + $30k+ 5m vol + positive trend + HOT whale\n"
            "60-79  = decent metrics + active whale\n"
            "40-59  = thin liquidity or mixed signals\n"
            "0-39   = dead volume or negative trend"
        )
        prompt += _response_format

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp   = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw    = resp.content[0].text.strip()
        lines  = [ln.strip() for ln in raw.splitlines() if ln.strip()]

        score   = 70  # fail-open default
        bullets: list[str] = []
        for line in lines:
            if line.upper().startswith("SCORE:"):
                try:
                    score = max(0, min(100, int(line.split(":", 1)[1].strip())))
                except (ValueError, IndexError):
                    pass
            elif line.startswith("-"):
                bullet = line.lstrip("- ").strip()
                if bullet:
                    bullets.append(bullet)
                if len(bullets) >= 4:
                    break

        logger.info(f"[CLAUDE] {token_mint[:8]} scored {score}/100 | {len(bullets)} reason(s)")
        return score, bullets
    except Exception as e:
        logger.warning(f"[CLAUDE] Scoring failed for {token_mint[:8]}: {e} — defaulting to 70")
        return 70, None


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


# --- MC lookup with DexScreener → PumpFun fallback -------------------

# Cooldown tracker for refresh button (mint → last refresh timestamp)
_last_refresh: dict[str, float] = {}


async def get_current_mc(
    session: aiohttp.ClientSession,
    token_mint: str,
) -> tuple[float, str]:
    """
    Fetch current market cap with DexScreener → PumpFun fallback.
    Returns (market_cap, source_label) where source_label is
    "DexScreener", "PumpFun", or "unknown".
    """
    # Try DexScreener first
    try:
        dex = await fetch_dexscreener(session, token_mint)
        if dex:
            mc = float(dex.get("marketCap") or dex.get("fdv") or 0)
            if mc > 0:
                return mc, "DexScreener"
    except Exception as e:
        logger.debug(f"[MC LOOKUP] DexScreener failed for {token_mint[:8]}: {e}")

    # Fall back to PumpFun
    try:
        pump = await fetch_pumpfun_data(session, token_mint)
        if pump:
            mc = float(pump.get("usd_market_cap") or 0)
            if mc > 0:
                return mc, "PumpFun"
    except Exception as e:
        logger.debug(f"[MC LOOKUP] PumpFun failed for {token_mint[:8]}: {e}")

    return 0.0, "unknown"


def _make_position_buttons(token_mint: str) -> list[list[dict]]:
    """Build the standard [Sell 50%] [Sell 100%] [Refresh] inline keyboard row."""
    return [
        [
            {"text": "Sell 50%",     "callback_data": f"sell|{token_mint}|50"},
            {"text": "Sell 100%",    "callback_data": f"sell|{token_mint}|100"},
            {"text": "\U0001f504 Refresh", "callback_data": f"refresh|{token_mint}"},
        ]
    ]


# --- Claude scoring Telegram alert ------------------------------------

def _send_claude_score_alert(
    token_label: str,
    score: int,
    bullets: list[str] | None,
    approved: bool,
    entry_blocked: bool,
) -> None:
    """
    Send a Telegram notification with the Claude scoring result.

    bullets=None  → API unavailable (fail-open path)
    bullets=[]    → API returned a score but no bullet reasons
    entry_blocked → True if the score caused the trade to be skipped
    """
    if bullets is None:
        score_line = "Score: unavailable ⚠️"
        bullet_block = ""
        action_line = "<i>Proceeding with fail-open score.</i>"
    else:
        verdict = "✅ APPROVED" if approved else "❌ REJECTED"
        score_line = f"Score: <b>{score}/100</b> {verdict}"
        if bullets:
            bullet_block = "\n" + "\n".join(f"  • {b}" for b in bullets)
        else:
            bullet_block = ""
        action_line = "<i>Entry blocked.</i>" if entry_blocked else "<i>Proceeding with entry.</i>"

    msg = (
        f"🤖 <b>CLAUDE SCORE</b> — {token_label}\n"
        f"{score_line}"
        f"{bullet_block}\n"
        f"{action_line}"
    )
    send_telegram(msg)


# --- MANNOS tiered exit logic -----------------------------------------

MANNOS_HARD_FLOOR_PCT = -35.0  # max loss before min target — protects against dumps

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
      - Hard floor at MANNOS_HARD_FLOOR_PCT (-35%) from entry
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
            claude_score, score_bullets = await get_claude_score(
                token_mint, pair, None,
                f"dip sniper — {drop_pct:.0f}% drop from ATH of {ath:.6f} SOL"
            )
            _dip_approved  = claude_score >= DIP_SNIPER_MIN_SCORE
            _dip_label     = _token_label(token_mint, pair)
            _send_claude_score_alert(
                token_label=_dip_label,
                score=claude_score,
                bullets=score_bullets,
                approved=_dip_approved,
                entry_blocked=not _dip_approved,
            )
            if not _dip_approved:
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

            # Use current MC for routing (dip sniper tokens are graduated by definition)
            _dip_mc, _ = await get_current_mc(session, token_mint)
            swap_sig, swap_msg = await execute_buy_routed(
                session, token_mint, buy_sol, wallet_pubkey, _dip_mc
            )

            send_telegram(
                f"🎯 <b>DIP SNIPER</b> — <code>{token_mint[:8]}</code>\n"
                f"Dropped {drop_pct:.0f}% from ATH | Claude: {claude_score}/100\n"
                f"Buying {buy_sol} SOL worth…"
            )
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
                _save_positions()
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
    url    = f"{JUPITER_API}/quote"
    delays = [5, 10, 15, 20, 25, 30, 35]   # wait longer between retries — gives Jupiter time to index new tokens
    for attempt in range(8):
        body = ""
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                body = await resp.text()
                resp.raise_for_status()
                return json.loads(body)
        except Exception as e:
            logger.error(
                f"Jupiter quote attempt {attempt + 1}/8 failed: {e} "
                f"| response: {body[:300]}"
            )
            if attempt < 7:
                await asyncio.sleep(delays[attempt])
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
        "slippageBps": str(SELL_SLIPPAGE_BPS),
    }
    url = f"{JUPITER_API}/quote"
    for attempt in range(2):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"Sell quote attempt {attempt + 1}/2 failed for {token_mint[:8]}: {e}")
            if attempt < 1:
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

    if _wallet_keypair is None:
        logger.error("[JUPITER SWAP] _wallet_keypair not loaded — WALLET_PRIVATE_KEY missing or invalid")
        return None, "keypair not loaded"

    payload = {
        "quoteResponse":             quote,
        "userPublicKey":             wallet_pubkey,
        "wrapAndUnwrapSol":          True,
        "dynamicComputeUnitLimit":   True,
        "prioritizationFeeLamports": PRIORITY_FEE_LAMPORTS,
    }
    url = f"{JUPITER_API}/swap"

    # Step 1: Get the serialized transaction from Jupiter
    swap_tx_b64: str | None = None
    for attempt in range(8):
        body = ""
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                body = await resp.text()
                resp.raise_for_status()
                swap_tx_b64 = json.loads(body).get("swapTransaction")
                if not swap_tx_b64:
                    raise RuntimeError(f"No swapTransaction in response: {body[:200]}")
                break
        except Exception as e:
            logger.error(
                f"Jupiter swap attempt {attempt + 1}/8 failed: {e} "
                f"| response: {body[:300]}"
            )
            if attempt < 7:
                await asyncio.sleep(2)

    if not swap_tx_b64:
        return None, "Jupiter swap failed after 8 attempts"

    # Step 2: Deserialize, sign, and submit the transaction
    try:
        tx_bytes = base64.b64decode(swap_tx_b64)
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [_wallet_keypair])
        signed_bytes = bytes(signed_tx)
    except Exception as exc:
        logger.error(f"[JUPITER SWAP] Transaction signing failed: {exc}")
        return None, f"tx signing failed: {exc}"

    encoded = base64.b64encode(signed_bytes).decode("utf-8")
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            encoded,
            {"encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed"},
        ],
    }

    txid: str | None = None
    for attempt in range(8):
        body_preview = ""
        try:
            async with session.post(
                _rpc_url,
                json=rpc_payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.text()
                body_preview = raw[:300]
                resp.raise_for_status()
                data = json.loads(raw)
                if "error" in data:
                    raise RuntimeError(f"RPC error: {data['error']}")
                txid = data.get("result")
                break
        except Exception as exc:
            logger.error(
                f"[JUPITER SWAP] sendTransaction attempt {attempt + 1}/8 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 7:
                await asyncio.sleep(2)

    if not txid:
        return None, "Jupiter sendTransaction failed after 8 attempts"

    logger.info(
        f"TX submitted: {txid[:16]}… — waiting up to {TX_CONFIRM_TIMEOUT_SEC}s"
    )
    ok, reason = await confirm_transaction(session, txid)
    if not ok:
        logger.error(f"TX {txid[:16]}… confirmation failed: {reason}")
        return None, reason

    logger.info(f"TX {txid[:16]}… {reason}")
    return txid, reason


_PUMPFUN_TRADE_URL = "https://pumpportal.fun/api/trade-local"


async def execute_pumpfun_buy(
    session: aiohttp.ClientSession,
    token_mint: str,
    buy_sol: float,
    wallet_pubkey: str,
) -> tuple[str | None, str]:
    """
    Buy a pre-graduation pump.fun token directly through its bonding curve
    via the PumpPortal trade-local API.

    Flow:
      1. POST to pumpportal.fun/api/trade-local → returns raw serialized tx bytes
      2. Deserialize as VersionedTransaction, sign with _wallet_keypair
      3. Submit via sendTransaction RPC, wait for confirmation

    Returns (txid, message) on success, (None, reason) on any failure.
    In DRY_RUN mode returns ("DRY_RUN_SIG", "DRY_RUN") immediately.
    """
    if DRY_RUN:
        logger.info(
            f"[PUMPFUN BUY] DRY RUN — would buy {buy_sol} SOL of {token_mint[:8]}"
        )
        return "DRY_RUN_SIG", "DRY_RUN"

    if _wallet_keypair is None:
        logger.error("[PUMPFUN BUY] _wallet_keypair not loaded — WALLET_PRIVATE_KEY missing or invalid")
        return None, "keypair not loaded"

    payload = {
        "publicKey":        wallet_pubkey,
        "action":           "buy",
        "mint":             token_mint,
        "denominatedInSol": "true",
        "amount":           buy_sol,
        "slippage":         20,
        "priorityFee":      0.005,
        "pool":             "pump",
    }

    tx_bytes: bytes | None = None
    delays = [5, 10, 15, 20, 25, 30, 35]
    for attempt in range(8):
        body_preview = ""
        try:
            async with session.post(
                _PUMPFUN_TRADE_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.read()
                body_preview = raw[:300].decode("utf-8", errors="replace")
                resp.raise_for_status()
                tx_bytes = raw
                break
        except aiohttp.ClientResponseError as exc:
            if exc.status == 400 and any(
                kw in body_preview.lower()
                for kw in ("migrated", "does not exist", "pump-amm")
            ):
                logger.warning(
                    f"[PUMPFUN BUY] Token migrated to pump-amm — use Jupiter "
                    f"(400: {body_preview[:120]})"
                )
                return None, "token migrated to pump-amm — use Jupiter"
            logger.error(
                f"[PUMPFUN BUY] Attempt {attempt + 1}/8 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 7:
                await asyncio.sleep(delays[attempt])
        except Exception as exc:
            logger.error(
                f"[PUMPFUN BUY] Attempt {attempt + 1}/8 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 7:
                await asyncio.sleep(delays[attempt])

    if not tx_bytes:
        return None, "PumpFun trade-local failed after 8 attempts"

    # Sign the transaction
    try:
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [_wallet_keypair])
        signed_bytes = bytes(signed_tx)
    except Exception as exc:
        logger.error(f"[PUMPFUN BUY] Transaction signing failed: {exc}")
        return None, f"tx signing failed: {exc}"

    # Submit via sendTransaction RPC
    encoded = base64.b64encode(signed_bytes).decode("utf-8")
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            encoded,
            {"encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed"},
        ],
    }

    txid: str | None = None
    for attempt in range(8):
        body_preview = ""
        try:
            async with session.post(
                _rpc_url,
                json=rpc_payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.text()
                body_preview = raw[:300]
                resp.raise_for_status()
                data = json.loads(raw)
                if "error" in data:
                    err_str = str(data["error"])
                    if "BondingCurveComplete" in err_str or "6005" in err_str or "liquidity migrated" in err_str.lower():
                        logger.warning(
                            f"[PUMPFUN BUY] Bonding curve complete — token graduated, skipping retries"
                        )
                        return None, "BondingCurveComplete — token graduated to Raydium"
                    raise RuntimeError(f"RPC error: {data['error']}")
                txid = data.get("result")
                break
        except Exception as exc:
            logger.error(
                f"[PUMPFUN BUY] sendTransaction attempt {attempt + 1}/8 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 7:
                await asyncio.sleep(delays[attempt])

    if not txid:
        return None, "PumpFun sendTransaction failed after 8 attempts"

    logger.info(
        f"[PUMPFUN BUY] TX submitted: {txid[:16]}… — waiting up to {TX_CONFIRM_TIMEOUT_SEC}s"
    )
    ok, reason = await confirm_transaction(session, txid)
    if not ok:
        logger.error(f"[PUMPFUN BUY] TX {txid[:16]}… confirmation failed: {reason}")
        return None, reason

    logger.info(f"[PUMPFUN BUY] TX {txid[:16]}… {reason}")
    return txid, reason


async def execute_pumpfun_sell(
    session: aiohttp.ClientSession,
    token_mint: str,
    amount_tokens: int,
    wallet_pubkey: str,
) -> tuple[str | None, str]:
    """
    Sell tokens on a pump.fun bonding curve via the PumpPortal trade-local API.
    Same flow as execute_pumpfun_buy but with action="sell".
    Returns (txid, message) on success, (None, reason) on any failure.
    In DRY_RUN mode returns ("DRY_RUN_SIG", "DRY_RUN") immediately.
    """
    if DRY_RUN:
        logger.info(
            f"[PUMPFUN SELL] DRY RUN — would sell {amount_tokens:,} tokens of {token_mint[:8]}"
        )
        return "DRY_RUN_SIG", "DRY_RUN"

    if _wallet_keypair is None:
        logger.error("[PUMPFUN SELL] _wallet_keypair not loaded")
        return None, "keypair not loaded"

    payload = {
        "publicKey":        wallet_pubkey,
        "action":           "sell",
        "mint":             token_mint,
        "amount":           amount_tokens,
        "denominatedInSol": "false",
        "slippage":         20,
        "priorityFee":      0.0005,
        "pool":             "pump",
    }
    logger.debug(f"[PUMPFUN SELL] payload: {payload}")

    tx_bytes: bytes | None = None
    for attempt in range(2):
        body_preview = ""
        try:
            async with session.post(
                _PUMPFUN_TRADE_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.read()
                body_preview = raw[:300].decode("utf-8", errors="replace")
                resp.raise_for_status()
                tx_bytes = raw
                break
        except Exception as exc:
            logger.error(
                f"[PUMPFUN SELL] Attempt {attempt + 1}/2 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 1:
                await asyncio.sleep(5)

    if not tx_bytes:
        return None, "PumpFun sell trade-local failed after 2 attempts"

    # Sign the transaction
    try:
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [_wallet_keypair])
        signed_bytes = bytes(signed_tx)
    except Exception as exc:
        logger.error(f"[PUMPFUN SELL] Transaction signing failed: {exc}")
        return None, f"tx signing failed: {exc}"

    # Submit via sendTransaction RPC
    encoded = base64.b64encode(signed_bytes).decode("utf-8")
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            encoded,
            {"encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed"},
        ],
    }

    txid: str | None = None
    for attempt in range(2):
        body_preview = ""
        try:
            async with session.post(
                _rpc_url,
                json=rpc_payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.text()
                body_preview = raw[:300]
                resp.raise_for_status()
                data = json.loads(raw)
                if "error" in data:
                    err_str = str(data["error"])
                    if "BondingCurveComplete" in err_str or "6005" in err_str:
                        logger.warning(
                            f"[PUMPFUN SELL] Bonding curve complete — "
                            f"token graduated, skipping retries"
                        )
                        return None, "BondingCurveComplete — token graduated"
                    raise RuntimeError(f"RPC error: {data['error']}")
                txid = data.get("result")
                break
        except Exception as exc:
            logger.error(
                f"[PUMPFUN SELL] sendTransaction attempt {attempt + 1}/2 failed: {exc} "
                f"| response: {body_preview}"
            )
            if attempt < 1:
                await asyncio.sleep(5)

    if not txid:
        return None, "PumpFun sell sendTransaction failed after 2 attempts"

    logger.info(
        f"[PUMPFUN SELL] TX submitted: {txid[:16]}… — waiting up to {TX_CONFIRM_TIMEOUT_SEC}s"
    )
    ok, reason = await confirm_transaction(session, txid)
    if not ok:
        logger.error(f"[PUMPFUN SELL] TX {txid[:16]}… confirmation failed: {reason}")
        return None, reason

    logger.info(f"[PUMPFUN SELL] TX {txid[:16]}… {reason}")
    return txid, reason


# --- Unified swap router -----------------------------------------------


def _is_graduated(mc_usd: float) -> bool:
    """Single source of truth: returns True if MC indicates post-graduation."""
    return mc_usd >= GRADUATION_MC_USD


async def execute_buy_routed(
    session: aiohttp.ClientSession,
    token_mint: str,
    buy_sol: float,
    wallet_pubkey: str,
    mc_usd: float,
) -> tuple[str | None, str]:
    """
    Unified buy router. Routes to PumpFun or Jupiter based on graduation status.
    - Pre-grad:  PumpFun only (no Jupiter fallback)
    - Post-grad: PumpFun first, Jupiter fallback on failure
    - MC unknown (0): live-fetch MC → route accordingly; default to Jupiter if still unknown
    Returns (txid, message) or (None, reason).
    """
    # If MC is unknown (PumpFun 530s or data unavailable), try a live lookup
    if mc_usd <= 0:
        mc_usd, mc_src = await get_current_mc(session, token_mint)
        if mc_usd > 0:
            logger.info(
                f"[ROUTER] MC=${mc_usd:,.0f} from {mc_src} — "
                f"routing to {'Jupiter (graduated)' if _is_graduated(mc_usd) else 'PumpFun (pre-grad)'}"
            )
        else:
            # Both sources failed — default to Jupiter as safe fallback
            logger.info(
                f"[ROUTER] MC fetch failed — defaulting to Jupiter (safe fallback)"
            )
            amount_lamports = int(buy_sol * 1_000_000_000)
            quote = await get_jupiter_quote(session, token_mint, amount_lamports)
            if not quote:
                return None, "MC unknown + Jupiter quote failed"
            return await execute_swap(session, quote, wallet_pubkey)

    if not _is_graduated(mc_usd):
        logger.info(f"[ROUTER] PumpFun (pre-grad) — MC ${mc_usd:,.0f} < ${GRADUATION_MC_USD:,}")
        return await execute_pumpfun_buy(session, token_mint, buy_sol, wallet_pubkey)

    # Post-graduation: try PumpFun first
    logger.info(f"[ROUTER] PumpFun (post-grad, primary) — MC ${mc_usd:,.0f}")
    sig, msg = await execute_pumpfun_buy(session, token_mint, buy_sol, wallet_pubkey)
    if sig:
        return sig, msg

    # PumpFun failed — fall back to Jupiter
    logger.info(f"[ROUTER] Jupiter (post-grad, fallback) — PumpFun failed: {msg}")
    amount_lamports = int(buy_sol * 1_000_000_000)
    quote = await get_jupiter_quote(session, token_mint, amount_lamports)
    if not quote:
        return None, f"Jupiter quote also failed after PumpFun: {msg}"
    return await execute_swap(session, quote, wallet_pubkey)


async def execute_sell_routed(
    session: aiohttp.ClientSession,
    token_mint: str,
    amount_tokens: int,
    wallet_pubkey: str,
    mc_usd: float,
) -> tuple[str | None, str]:
    """
    Unified sell router. Route order depends on PREFER_JUPITER_SELLS.
    Returns (txid, message) or (None, reason).
    """
    grad_label = "post-grad" if _is_graduated(mc_usd) else "pre-grad"

    if PREFER_JUPITER_SELLS:
        # --- Jupiter first, PumpPortal fallback ---
        logger.info(f"[ROUTER] Jupiter sell ({grad_label}, primary) — MC ${mc_usd:,.0f}")
        quote = await get_sell_quote(session, token_mint, amount_tokens)
        if quote:
            jup_sig, jup_msg = await execute_swap(session, quote, wallet_pubkey)
            if jup_sig:
                logger.info(f"[ROUTER] Sell succeeded via Jupiter")
                return jup_sig, f"{jup_msg} (via Jupiter)"
            jup_fail = jup_msg
        else:
            jup_fail = "Jupiter sell quote failed"

        logger.info(f"[ROUTER] PumpPortal sell ({grad_label}, fallback) — Jupiter failed: {jup_fail}")
        send_telegram(
            f"⚠️ <b>Jupiter sell failed</b> — attempting PumpPortal fallback\n"
            f"Reason: {jup_fail}"
        )
        sig, msg = await execute_pumpfun_sell(session, token_mint, amount_tokens, wallet_pubkey)
        if sig:
            logger.info(f"[ROUTER] Sell succeeded via PumpPortal fallback")
            return sig, f"{msg} (via PumpPortal fallback)"
        return None, f"Jupiter: {jup_fail} | PumpPortal: {msg}"

    else:
        # --- PumpPortal first, Jupiter fallback ---
        logger.info(f"[ROUTER] PumpPortal sell ({grad_label}, primary) — MC ${mc_usd:,.0f}")
        sig, msg = await execute_pumpfun_sell(session, token_mint, amount_tokens, wallet_pubkey)
        if sig:
            logger.info(f"[ROUTER] Sell succeeded via PumpPortal")
            return sig, f"{msg} (via PumpPortal)"

        logger.info(f"[ROUTER] Jupiter sell ({grad_label}, fallback) — PumpPortal failed: {msg}")
        send_telegram(
            f"⚠️ <b>PumpPortal sell failed</b> — attempting Jupiter fallback\n"
            f"Reason: {msg}"
        )
        quote = await get_sell_quote(session, token_mint, amount_tokens)
        if not quote:
            return None, f"PumpPortal: {msg} | Jupiter quote also failed"
        jup_sig, jup_msg = await execute_swap(session, quote, wallet_pubkey)
        if jup_sig:
            logger.info(f"[ROUTER] Sell succeeded via Jupiter fallback")
            return jup_sig, f"{jup_msg} (via Jupiter fallback)"
        return None, f"PumpPortal: {msg} | Jupiter: {jup_msg}"


# --- Telegram ---------------------------------------------------------

def send_telegram(message: str) -> bool:
    """
    Send a Telegram message to all authorised chat IDs.
    Returns True if at least one delivery succeeded.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not _telegram_chat_ids:
        logger.error("send_telegram: TELEGRAM_BOT_TOKEN or chat IDs not set — alert dropped")
        return False
    any_ok = False
    for chat_id in _telegram_chat_ids:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5,
            )
            resp.raise_for_status()
            logger.info(f"Telegram alert sent to {chat_id} (message_id={resp.json().get('result',{}).get('message_id')})")
            any_ok = True
        except requests.exceptions.HTTPError as e:
            logger.error(f"Telegram HTTP error for {chat_id}: {e.response.status_code}: {e.response.text}")
        except Exception as e:
            logger.error(f"Telegram send failed for {chat_id}: {e}")
    return any_ok


def send_telegram_with_buttons(
    message: str,
    inline_keyboard: list[list[dict]],
) -> bool:
    """
    Send a Telegram message with an inline keyboard to all authorised chat IDs.
    Returns True if at least one delivery succeeded.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not _telegram_chat_ids:
        logger.error("send_telegram_with_buttons: credentials not set — alert dropped")
        return False
    any_ok = False
    for chat_id in _telegram_chat_ids:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id":      chat_id,
                    "text":         message,
                    "parse_mode":   "HTML",
                    "reply_markup": {
                        "inline_keyboard": inline_keyboard,
                    },
                },
                timeout=5,
            )
            resp.raise_for_status()
            logger.info(
                f"Telegram alert with buttons sent to {chat_id} "
                f"(message_id={resp.json().get('result',{}).get('message_id')})"
            )
            any_ok = True
        except Exception as e:
            logger.error(f"Telegram send_with_buttons failed for {chat_id}: {e}")
    return any_ok


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

    entry_sol      = pos["entry_sol"]
    mc_entry       = pos.get("mc_entry", 0.0)

    # Fetch current MC for routing decisions and PnL estimation
    mc_now, mc_source = await get_current_mc(session, token_mint)

    # Log graduation transition
    if mc_entry and mc_entry < GRADUATION_MC_USD and mc_now >= GRADUATION_MC_USD:
        logger.info(
            f"[GRADUATION] {token_mint[:8]} has graduated — "
            f"switching to PumpFun/Jupiter routing for sells"
        )

    # Estimate current value: try Jupiter sell quote first, fall back to MC ratio
    sell_quote = await get_sell_quote(session, token_mint, pos["amount_tokens"])
    if sell_quote is not None:
        current_sol = int(sell_quote.get("outAmount", 0)) / 1_000_000_000
    elif mc_entry and mc_now:
        current_sol = entry_sol * (mc_now / mc_entry)
    else:
        logger.warning(
            f"[{token_mint[:8]}] sell quote and MC both unavailable — retrying next cycle"
        )
        return

    peak_sol       = pos["peak_sol"]
    elapsed_min    = (time.time() - pos["entry_time"]) / 60

    # Update peak if price has moved up
    if current_sol > peak_sol:
        open_positions[token_mint]["peak_sol"] = current_sol
        peak_sol = current_sol
        _save_positions()

    pnl_pct       = (current_sol / entry_sol - 1) * 100 if entry_sol > 0 else 0.0
    drop_from_peak = (current_sol / peak_sol  - 1) * 100 if peak_sol  > 0 else 0.0

    # Debug: MC-based hard sell floor check every cycle
    _hard_sell_mc = mc_entry * 0.65 if mc_entry else 0
    logger.debug(
        f"[{token_mint[:8]}] hold — pnl={pnl_pct:+.1f}% | "
        f"peak_drop={drop_from_peak:.1f}% | {elapsed_min:.0f}m elapsed | "
        f"MC now={mc_now:,.0f} | MC entry={mc_entry:,.0f} | "
        f"hard sell MC={_hard_sell_mc:,.0f}"
    )

    # --- MC-BASED HARD SELL FLOOR: sell 100% if MC drops to -35% of entry ---
    if mc_entry and mc_now and mc_now <= _hard_sell_mc:
        _hsf_label = pos.get("token_label") or token_mint[:8]
        _hsf_drop_pct = (mc_now / mc_entry - 1) * 100
        logger.info(
            f"[HARD SELL FLOOR] {token_mint[:8]} | MC now={mc_now:,.0f} <= "
            f"hard sell={_hard_sell_mc:,.0f} ({_hsf_drop_pct:+.1f}% from entry) — selling 100%"
        )

        # Fetch live on-chain balance and sync
        _hsf_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
        if _hsf_live_tokens <= 0:
            logger.warning(f"[HARD SELL FLOOR] {token_mint[:8]} | on-chain balance is 0 — aborting sell")
            send_telegram(
                f"⚠️ <b>HARD SELL FLOOR ABORTED</b> — {_hsf_label}\n"
                f"Wallet shows no token balance on-chain"
            )
            return
        open_positions[token_mint]["amount_tokens"] = _hsf_live_tokens
        _save_positions()

        if DRY_RUN:
            _hsf_sig = "DRY_RUN_HARD_SELL_FLOOR"
            logger.info(
                f"[DRY RUN] Hard sell floor would sell {_hsf_live_tokens:,} tokens → "
                f"{current_sol:.4f} SOL"
            )
        else:
            _hsf_sig, _hsf_msg = await execute_sell_routed(
                session, token_mint, _hsf_live_tokens, wallet_pubkey, mc_now
            )
            if not _hsf_sig:
                logger.error(
                    f"[HARD SELL FLOOR] {token_mint[:8]} | Sell failed ({_hsf_msg}) — "
                    f"keeping position open, will retry next cycle"
                )
                send_telegram(
                    f"⚠️ <b>HARD SELL FLOOR FAILED</b> — {_hsf_label}\n"
                    f"Reason: {_hsf_msg}\n"
                    f"Position stays open — will retry"
                )
                return

        # Success — close position
        _hsf_pnl_sol = current_sol - entry_sol
        del open_positions[token_mint]
        _save_positions()

        if pnl_pct >= 0:
            _stats["wins"] += 1
        else:
            _stats["losses"] += 1
        _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + _hsf_pnl_sol, 6)
        _record_trade(_hsf_pnl_sol)

        _hsf_pnl_sign = "+" if pnl_pct >= 0 else ""
        send_telegram(
            f"🛑 <b>HARD SELL FLOOR (-35%)</b> — {_hsf_label}\n"
            f"MC Entry: {_fmt_usd(mc_entry)} → MC Now: {_fmt_usd(mc_now)}\n"
            f"Received: {current_sol:.4f} SOL\n"
            f"PnL: {_hsf_pnl_sign}{pnl_pct:.1f}%\n"
            f"Sig: <code>{_hsf_sig}</code>"
        )
        logger.info(
            f"[HARD SELL FLOOR] {token_mint[:8]} | CLOSED | "
            f"MC {_fmt_usd(mc_entry)} → {_fmt_usd(mc_now)} | "
            f"PnL: {_hsf_pnl_sign}{pnl_pct:.1f}%"
        )
        return
    # --- END MC-BASED HARD SELL FLOOR ---

    # --- HARD TAKE-PROFIT: sell 100% if position reaches HARD_TP_MULT ---
    if current_sol >= entry_sol * HARD_TP_MULT:
        _htp_label = pos.get("token_label") or token_mint[:8]
        _htp_reason = f"HARD TP ({HARD_TP_MULT:.1f}x)"
        logger.info(
            f"[HARD TP] {token_mint[:8]} | current={current_sol:.4f} >= "
            f"entry={entry_sol:.4f} × {HARD_TP_MULT} — selling 100%"
        )

        # Fetch live on-chain balance and sync to stored position
        _htp_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
        if _htp_live_tokens <= 0:
            logger.warning(f"[HARD TP] {token_mint[:8]} | on-chain balance is 0 — aborting sell")
            send_telegram(
                f"⚠️ <b>HARD TP ABORTED</b> — {_htp_label}\n"
                f"Wallet shows no token balance on-chain"
            )
            return
        open_positions[token_mint]["amount_tokens"] = _htp_live_tokens
        _save_positions()

        if DRY_RUN:
            _htp_sig = "DRY_RUN_HARD_TP_SIG"
            logger.info(
                f"[DRY RUN] Hard TP would sell {_htp_live_tokens:,} tokens → "
                f"{current_sol:.4f} SOL"
            )
        else:
            _htp_sig, _htp_msg = await execute_sell_routed(
                session, token_mint, _htp_live_tokens, wallet_pubkey, mc_now
            )
            if not _htp_sig:
                logger.error(
                    f"[HARD TP] {token_mint[:8]} | Sell failed ({_htp_msg}) — "
                    f"keeping position open, will retry next cycle"
                )
                send_telegram(
                    f"⚠️ <b>HARD TP SELL FAILED</b> — {_htp_label}\n"
                    f"Reason: {_htp_msg}\n"
                    f"Position stays open — will retry"
                )
                return

        # Success — close position
        _htp_pnl_sol = current_sol - entry_sol
        del open_positions[token_mint]
        _save_positions()

        if pnl_pct >= 0:
            _stats["wins"] += 1
        else:
            _stats["losses"] += 1
        _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + _htp_pnl_sol, 6)
        _record_trade(_htp_pnl_sol)

        _htp_pnl_sign = "+" if pnl_pct >= 0 else ""
        send_telegram(
            f"🎯 <b>HARD TP ({HARD_TP_MULT:.1f}x)</b> — {_htp_label}\n"
            f"Received: {current_sol:.4f} SOL\n"
            f"PnL: {_htp_pnl_sign}{pnl_pct:.1f}%\n"
            f"Sig: <code>{_htp_sig}</code>"
        )
        logger.info(
            f"[HARD TP] {token_mint[:8]} | CLOSED | "
            f"Entry: {entry_sol:.4f} SOL | Exit: {current_sol:.4f} SOL | "
            f"PnL: {_htp_pnl_sign}{pnl_pct:.1f}%"
        )
        return
    # --- END HARD TAKE-PROFIT ---

    # --- MANNOS tiered exit ---
    claude_score   = pos.get("claude_score", 70)
    min_target_hit = pos.get("min_target_hit", False)
    tier           = get_exit_tier(claude_score)

    # --- TP1 partial sell at 2x -------------------------------------------
    # Fires once when the position hits 100% gain and has not yet had a
    # partial sell.  Sells TAKE_PROFIT_PCT of the token balance, records the
    # SOL received, reduces amount_tokens, and recalibrates entry/peak so the
    # remaining holding is effectively a free ride.  Fail-open: if the swap
    # fails, min_target_hit is still set and the full position continues under
    # trailing-stop logic on the next tick.
    if not min_target_hit and pnl_pct >= 100.0:
        _tp1_label    = pos.get("token_label") or token_mint[:8]
        _sell_frac    = TAKE_PROFIT_PCT / 100.0          # e.g. 0.50 when env=0.50

        # Fetch live on-chain balance and sync to stored position
        _tp1_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
        if _tp1_live_tokens <= 0:
            logger.warning(f"[TP1] {token_mint[:8]} | on-chain balance is 0 — aborting partial sell")
            send_telegram(
                f"⚠️ <b>TP1 ABORTED</b> — {_tp1_label}\n"
                f"Wallet shows no token balance on-chain"
            )
            return
        open_positions[token_mint]["amount_tokens"] = _tp1_live_tokens
        _save_positions()

        _sell_tokens  = int(_tp1_live_tokens * _sell_frac)
        _remain_tokens = _tp1_live_tokens - _sell_tokens

        logger.info(
            f"[TP1] {token_mint[:8]} | pnl={pnl_pct:+.1f}% — "
            f"partial sell: {_sell_tokens:,} tokens ({TAKE_PROFIT_PCT:.0f}%) | "
            f"remaining: {_remain_tokens:,} (live balance: {_tp1_live_tokens:,})"
        )

        _partial_received_sol = 0.0
        _tp1_sig              = None
        _tp1_ok               = False

        if DRY_RUN:
            # Estimate from MC ratio
            if mc_entry and mc_now:
                _partial_received_sol = (entry_sol * _sell_frac) * (mc_now / mc_entry)
            else:
                _partial_received_sol = entry_sol * _sell_frac * 2  # ~2x estimate
            _tp1_sig = "DRY_RUN_TP1_SIG"
            _tp1_ok  = True
            logger.info(
                f"[DRY RUN] TP1 would sell {_sell_tokens:,} tokens → "
                f"~{_partial_received_sol:.4f} SOL"
            )
        else:
            _tp1_sig, _tp1_msg = await execute_sell_routed(
                session, token_mint, _sell_tokens, wallet_pubkey, mc_now
            )
            if _tp1_sig:
                # Estimate SOL received from MC ratio (PumpFun sells don't return outAmount)
                if mc_entry and mc_now:
                    _partial_received_sol = (entry_sol * _sell_frac) * (mc_now / mc_entry)
                else:
                    _partial_received_sol = entry_sol * _sell_frac * 2
                _tp1_ok = True
            else:
                logger.warning(
                    f"[TP1] {token_mint[:8]} | sell failed ({_tp1_msg}) — "
                    f"fail-open: setting min_target_hit, keeping full position"
                )

        if _tp1_ok:
            # Reduce cost basis by SOL received; position is now a free ride.
            # Setting entry_sol ≈ 0 disables hard-floor protection naturally —
            # pnl_pct will always be extremely positive going forward.
            _new_entry = max(entry_sol - _partial_received_sol, 0.0001)
            open_positions[token_mint].update({
                "amount_tokens":    _remain_tokens,
                "min_target_hit":   True,
                "tp1_received_sol": _partial_received_sol,
                "entry_sol":        _new_entry,
                # Recalibrate peak to remaining position value so trailing stop
                # runs from the TP1 price level, not the old full-position peak.
                "peak_sol":         _partial_received_sol,
            })
            _stats["tp1_partials_executed"] += 1
        else:
            # Swap failed or quote unavailable — still arm trailing stop.
            open_positions[token_mint]["min_target_hit"] = True

        _save_positions()

        _tp1_trail_pct = tier["trail_pct"]
        if _tp1_ok:
            send_telegram(
                f"💰 <b>TP1 HIT</b> — {_tp1_label}\n"
                f"Sold {TAKE_PROFIT_PCT:.0f}% at 2x\n"
                f"Received: {_partial_received_sol:.4f} SOL (initial back)\n"
                f"Remainder: {_partial_received_sol:.4f} SOL riding free\n"
                f"Trailing stop: {_tp1_trail_pct:.0f}% from peak"
                + (f"\nSig: <code>{_tp1_sig}</code>" if not DRY_RUN else "")
            )
        else:
            send_telegram(
                f"💰 <b>TP1 HIT</b> — {_tp1_label}\n"
                f"⚠️ Partial sell failed — monitoring full position\n"
                f"Trailing stop: {_tp1_trail_pct:.0f}% from peak"
            )

        logger.info(
            f"[TP1] {token_mint[:8]} | "
            f"{'EXECUTED' if _tp1_ok else 'FAILED (fail-open)'} | "
            f"received={_partial_received_sol:.4f} SOL | "
            f"remaining_tokens={_remain_tokens:,}"
        )
        return   # re-evaluate remaining position fresh next tick
    # ----------------------------------------------------------------------

    # Activate trailing stop once min target is reached (latches — never resets)
    if not min_target_hit and pnl_pct >= tier["min_target_pct"]:
        open_positions[token_mint]["min_target_hit"] = True
        min_target_hit = True
        _save_positions()
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
    # Fetch live on-chain balance and sync to stored position
    _exit_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
    if _exit_live_tokens <= 0:
        _exit_label = pos.get("token_label") or token_mint[:8]
        logger.warning(
            f"[{token_mint[:8]}] on-chain balance is 0 — aborting sell "
            f"(trigger: {exit_reason})"
        )
        send_telegram(
            f"⚠️ <b>SELL ABORTED</b> — {_exit_label}\n"
            f"Trigger: {exit_reason}\n"
            f"Wallet shows no token balance on-chain"
        )
        return
    open_positions[token_mint]["amount_tokens"] = _exit_live_tokens
    _save_positions()

    if DRY_RUN:
        sell_sig = "DRY_RUN_SELL_SIG"
        logger.info(
            f"[DRY RUN] Would sell {_exit_live_tokens:,} tokens → "
            f"{current_sol:.4f} SOL ({exit_reason})"
        )
    else:
        sell_sig, sell_msg = await execute_sell_routed(
            session, token_mint, _exit_live_tokens, wallet_pubkey, mc_now
        )
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
    _save_positions()

    # Blacklist on trailing stop ONLY — take profit and time stop allow re-entry
    if exit_reason.startswith("TRAILING STOP"):
        _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
        _save_blacklist()
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

    entry_sol   = pos["entry_sol"]
    mc_entry    = pos.get("mc_entry", 0.0)

    # Get current MC for routing and price estimate
    mc_now, _ = await get_current_mc(session, token_mint)

    # Estimate current value: Jupiter quote first, MC ratio fallback
    sell_quote = await get_sell_quote(session, token_mint, pos["amount_tokens"])
    if sell_quote is not None:
        current_sol = int(sell_quote.get("outAmount", 0)) / 1_000_000_000
    elif mc_entry and mc_now:
        current_sol = entry_sol * (mc_now / mc_entry)
    else:
        logger.warning(
            f"[{token_mint[:8]}] emergency check: price unavailable "
            f"— normal monitor will handle"
        )
        return

    pnl_pct = (current_sol / entry_sol - 1) * 100 if entry_sol > 0 else 0.0

    logger.debug(
        f"[{token_mint[:8]}] emergency check: threshold={EMERGENCY_DUMP_PCT:.0f}% "
        f"| pnl={pnl_pct:+.1f}%"
    )

    if pnl_pct > -EMERGENCY_DUMP_PCT:
        return

    logger.info(
        f"[{token_mint[:8]}] IMMEDIATE DUMP DETECTED — "
        f"emergency exit (pnl={pnl_pct:.1f}%, threshold={EMERGENCY_DUMP_PCT:.0f}%)"
    )

    # Fetch live on-chain balance and sync to stored position
    _emg_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
    if _emg_live_tokens <= 0:
        _emg_label = pos.get("token_label") or token_mint[:8]
        logger.warning(f"[{token_mint[:8]}] on-chain balance is 0 — aborting emergency sell")
        send_telegram(
            f"⚠️ <b>EMERGENCY SELL ABORTED</b> — {_emg_label}\n"
            f"Wallet shows no token balance on-chain"
        )
        return
    open_positions[token_mint]["amount_tokens"] = _emg_live_tokens
    _save_positions()

    if DRY_RUN:
        sell_sig = "DRY_RUN_SELL_SIG"
        logger.info(
            f"[DRY RUN] Would emergency-sell {_emg_live_tokens:,} tokens "
            f"→ {current_sol:.4f} SOL"
        )
    else:
        sell_sig, sell_msg = await execute_sell_routed(
            session, token_mint, _emg_live_tokens, wallet_pubkey, mc_now
        )
        if not sell_sig:
            logger.error(
                f"[{token_mint[:8]}] Emergency sell failed ({sell_msg}) "
                f"— normal monitor will handle"
            )
            return

    del open_positions[token_mint]
    _save_positions()
    _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
    _save_blacklist()
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


async def _send_holdings_cards(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: str,
) -> None:
    """
    Send individual position cards with [Sell 50%] [Sell 100%] [Refresh] buttons
    for each open position, followed by a plain summary footer message.
    """
    if not open_positions:
        await _send_tg(session, base_url, chat_id, "\U0001f4ca No current holdings")
        return

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
        logger.debug(f"[HOLDINGS] CoinGecko fetch failed: {e} \u2014 USD estimate omitted")

    _NUM_EMOJIS = ["1\ufe0f\u20e3","2\ufe0f\u20e3","3\ufe0f\u20e3","4\ufe0f\u20e3","5\ufe0f\u20e3","6\ufe0f\u20e3","7\ufe0f\u20e3","8\ufe0f\u20e3","9\ufe0f\u20e3","\U0001f51f"]
    total_entry_sol = 0.0
    total_worth_sol = 0.0

    for idx, (token_mint, pos) in enumerate(open_positions.items()):
        num       = _NUM_EMOJIS[idx] if idx < len(_NUM_EMOJIS) else f"{idx + 1}."
        entry_sol = pos.get("entry_sol", 0.0)
        mc_entry  = pos.get("mc_entry",  0.0)

        # Fresh MC with fallback
        mc_now, mc_source = await get_current_mc(session, token_mint)

        # Worth and PnL derived from MC ratio
        if mc_entry and mc_now:
            worth_sol = entry_sol * (mc_now / mc_entry)
        else:
            worth_sol = entry_sol

        total_entry_sol += entry_sol
        total_worth_sol += worth_sol

        card = _build_position_message(token_mint, pos, mc_now, mc_source, num_emoji=num, sol_usd=sol_usd)
        await _send_tg_with_buttons(
            session, base_url, chat_id, card,
            _make_position_buttons(token_mint)
        )

    # Footer summary (plain message, no buttons)
    overall_pnl   = (total_worth_sol / total_entry_sol - 1) * 100 if total_entry_sol else 0.0
    overall_color = "\U0001f7e2" if overall_pnl >= 0 else "\U0001f534"
    overall_sign  = "+" if overall_pnl >= 0 else ""

    if sol_usd:
        entry_usd_str = f" (~{_fmt_usd(total_entry_sol * sol_usd)} USD)"
        worth_usd_str = f" (~{_fmt_usd(total_worth_sol * sol_usd)} USD)"
        sol_price_str = f"\n\U0001f4b5 SOL @ ${sol_usd:,.2f}"
    else:
        entry_usd_str = ""
        worth_usd_str = ""
        sol_price_str = ""

    footer = (
        f"\U0001f4bc Total in: {total_entry_sol:.4f} SOL{entry_usd_str}\n"
        f"\U0001f4c8 Total worth: ~{total_worth_sol:.4f} SOL{worth_usd_str}\n"
        f"{overall_color} Overall: {overall_sign}{overall_pnl:.0f}%"
        f"{sol_price_str}"
    )
    await _send_tg(session, base_url, chat_id, footer)


async def _send_home_dashboard(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: str,
) -> None:
    """Send the /home dashboard snapshot."""
    wallet_pubkey = os.getenv("WALLET_PUBLIC_KEY", "")

    # Live SOL balance
    sol_balance = get_sol_balance(_rpc_url, wallet_pubkey)

    # Live SOL/USD price
    sol_usd = 0.0
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            cg_data = await resp.json()
            sol_usd = float((cg_data.get("solana") or {}).get("usd") or 0)
    except Exception:
        pass

    usd_str = f" (~${sol_balance * sol_usd:,.2f} USD)" if sol_usd else ""

    # Stats
    trades = _stats["trades_executed"]
    wins   = _stats["wins"]
    losses = _stats["losses"]
    total  = wins + losses
    win_rate = (wins / total * 100) if total > 0 else 0.0

    msg = (
        "\U0001f3e0 <b>APEX SNIPER — HOME</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4b3 Wallet: <code>{wallet_pubkey}</code>\n"
        f"\U0001f4b0 Balance: {sol_balance:.4f} SOL{usd_str}\n"
        f"\U0001f7e2 Status: RUNNING\n"
        f"\U0001f3af Buy Limit: {BUY_AMOUNT_SOL} SOL\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"\U0001f4ca Trades Executed: {trades}\n"
        f"\u2705 Wins: {wins} | \u274c Losses: {losses} | \U0001f4c8 Win Rate: {win_rate:.0f}%"
    )

    await _send_tg(session, base_url, chat_id, msg)


def _register_commands() -> None:
    """Register bot commands in Telegram's command menu."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/setMyCommands",
            json={"commands": [
                {"command": "home",     "description": "Dashboard snapshot"},
                {"command": "summary",  "description": "12-hour trade summary"},
                {"command": "holdings", "description": "Current open positions"},
                {"command": "wallets",  "description": "Show tracked whale wallets"},
            ]},
            timeout=5,
        )
        logger.info("Telegram commands registered (/summary, /holdings, /wallets)")
    except Exception as e:
        logger.warning(f"setMyCommands failed: {e}")


async def telegram_command_loop() -> None:
    """Long-poll Telegram getUpdates and respond to /summary commands.

    Uses its own aiohttp session so the 30-second long-poll never competes
    with the shared session used by the whale poller and position monitor.
    """
    logger.info("Telegram command loop started — listening for /summary, /holdings")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token or not _allowed_control_ids:
        logger.warning("telegram_command_loop: credentials missing — /summary won't respond")
        return

    authorised_chats = _allowed_control_ids
    base_url         = f"https://api.telegram.org/bot{token}"
    last_update_id   = 0

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

                if text.startswith("/home") and cid in authorised_chats:
                    await _send_home_dashboard(tg_session, base_url, cid)
                    logger.info(f"/home command handled (chat {cid})")

                elif text.startswith("/summary") and cid in authorised_chats:
                    reply = _summary_message()
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid, "text": reply, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                            logger.info(f"/summary command handled (chat {cid})")
                    except Exception as e:
                        logger.warning(f"/summary reply failed: {e}")

                elif text.startswith("/holdings") and cid in authorised_chats:
                    await _send_holdings_cards(tg_session, base_url, cid)
                    logger.info(f"/holdings command handled (chat {cid})")

                elif text.startswith("/wallets") and cid in authorised_chats:
                    lines = ["\U0001f45b <b>Tracked Wallets</b>"]
                    for wname, waddr in WHALE_WALLETS.items():
                        lines.append(f"\n\U0001f40b <b>{wname.upper()}</b>\n<code>{waddr}</code>")
                    reply = "\n".join(lines)
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid, "text": reply, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                            logger.info(f"/wallets command handled (chat {cid})")
                    except Exception as e:
                        logger.warning(f"/wallets reply failed: {e}")

                # --- Inline sell/refresh button callbacks ------------------
                cbq = update.get("callback_query")
                if cbq:
                    cb_data = cbq.get("data", "")
                    cb_id   = cbq.get("id", "")
                    cb_chat = str((cbq.get("message") or {}).get("chat", {}).get("id", ""))

                    if cb_chat not in authorised_chats:
                        pass  # ignore callbacks from unknown chats
                    elif cb_data.startswith("sell|"):
                        await _handle_sell_callback(
                            tg_session, base_url, cb_id, cb_data, cb_chat
                        )
                    elif cb_data.startswith("refresh|"):
                        cb_msg_id = (cbq.get("message") or {}).get("message_id")
                        await _handle_refresh_callback(
                            tg_session, base_url, cb_id, cb_data,
                            cb_chat, cb_msg_id
                        )


async def _handle_sell_callback(
    tg_session: aiohttp.ClientSession,
    base_url: str,
    callback_id: str,
    cb_data: str,
    chat_id: str,
) -> None:
    """
    Process a sell button tap: sell|<token_mint>|<pct>
    Executes Jupiter swap, updates position, and replies with confirmation.
    """
    parts = cb_data.split("|")
    if len(parts) != 3:
        return
    _, token_mint, pct_str = parts
    try:
        sell_pct = int(pct_str)
    except ValueError:
        return

    pos = open_positions.get(token_mint)
    if not pos:
        await _answer_callback(tg_session, base_url, callback_id,
                               "Position not found — may already be closed.")
        return

    token_label = pos.get("token_label") or token_mint[:8]

    # Fetch live on-chain balance and sync to stored position
    wallet_pubkey_btn = os.getenv("WALLET_PUBLIC_KEY", "")
    live_tokens = await get_spl_token_balance(tg_session, token_mint, wallet_pubkey_btn)
    if live_tokens <= 0:
        await _answer_callback(tg_session, base_url, callback_id,
                               "No on-chain balance for this token.")
        await _send_tg(tg_session, base_url, chat_id,
                       f"⚠️ <b>SELL ABORTED</b> — {token_label}\n"
                       f"Wallet shows no token balance on-chain")
        return
    open_positions[token_mint]["amount_tokens"] = live_tokens
    _save_positions()

    sell_tokens = int(live_tokens * sell_pct / 100)
    if sell_tokens <= 0:
        await _answer_callback(tg_session, base_url, callback_id,
                               "Sell amount too small.")
        return

    logger.info(
        f"[SELL BUTTON] {token_label} — selling {sell_pct}% "
        f"({sell_tokens:,} of {live_tokens:,} live tokens)"
    )

    # Acknowledge the button press immediately
    await _answer_callback(tg_session, base_url, callback_id,
                           f"Selling {sell_pct}% of {token_label}...")

    # Get MC for routing
    mc_now, _ = await get_current_mc(tg_session, token_mint)

    # Estimate expected SOL from MC ratio
    entry_sol    = pos["entry_sol"]
    mc_entry     = pos.get("mc_entry", 0.0)
    if mc_entry and mc_now:
        expected_sol = (entry_sol * sell_pct / 100) * (mc_now / mc_entry)
    else:
        expected_sol = entry_sol * sell_pct / 100  # fallback: assume flat

    if DRY_RUN:
        sell_sig = "DRY_RUN_MANUAL_SELL"
        logger.info(
            f"[DRY RUN] Would manual-sell {sell_tokens:,} tokens of {token_label} "
            f"→ ~{expected_sol:.4f} SOL"
        )
    else:
        if _wallet_keypair is None:
            logger.error(f"[SELL BUTTON] _wallet_keypair is None — cannot sign sell tx")
            await _send_tg(tg_session, base_url, chat_id,
                           f"⚠️ <b>SELL FAILED</b> — {token_label}\n"
                           f"Wallet keypair not loaded — check WALLET_PRIVATE_KEY in .env")
            return
        wallet_pubkey = os.getenv("WALLET_PUBLIC_KEY", "")
        sell_sig, sell_msg = await execute_sell_routed(
            tg_session, token_mint, sell_tokens, wallet_pubkey, mc_now
        )
        if not sell_sig:
            await _send_tg(tg_session, base_url, chat_id,
                           f"⚠️ <b>SELL FAILED</b> — {token_label}\n{sell_msg}")
            return

    pnl_pct   = (expected_sol / (entry_sol * sell_pct / 100) - 1) * 100 if entry_sol > 0 else 0.0
    pnl_sign  = "+" if pnl_pct >= 0 else ""

    if sell_pct >= 100:
        # Full sell — remove position
        _pnl_sol = expected_sol - entry_sol
        del open_positions[token_mint]
        _save_positions()

        if _pnl_sol >= 0:
            _stats["wins"] += 1
        else:
            _stats["losses"] += 1
        _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + _pnl_sol, 6)
        _record_trade(_pnl_sol)

        await _send_tg(tg_session, base_url, chat_id,
            f"✅ <b>SOLD 100%</b> — {token_label}\n"
            f"Tokens sold: {sell_tokens:,}\n"
            f"SOL received: {expected_sol:.4f}\n"
            f"PnL: {pnl_sign}{pnl_pct:.1f}%\n"
            f"Position closed."
            + (f"\nSig: <code>{sell_sig}</code>" if not DRY_RUN else "")
        )
    else:
        # Partial sell — update position
        remain_tokens = live_tokens - sell_tokens
        # Reduce entry_sol proportionally to reflect the partial exit
        remain_entry  = entry_sol * (1 - sell_pct / 100)
        open_positions[token_mint].update({
            "amount_tokens": remain_tokens,
            "buy_sol":       pos["buy_sol"] * (1 - sell_pct / 100),
            "entry_sol":     max(remain_entry, 0.0001),
        })
        _save_positions()

        await _send_tg(tg_session, base_url, chat_id,
            f"✅ <b>SOLD {sell_pct}%</b> — {token_label}\n"
            f"Tokens sold: {sell_tokens:,} | Remaining: {remain_tokens:,}\n"
            f"SOL received: {expected_sol:.4f}\n"
            f"PnL on portion: {pnl_sign}{pnl_pct:.1f}%"
            + (f"\nSig: <code>{sell_sig}</code>" if not DRY_RUN else "")
        )

    logger.info(
        f"[SELL BUTTON] {token_label} — {sell_pct}% sell complete | "
        f"sig={sell_sig[:16] if sell_sig else 'N/A'}… | "
        f"received={expected_sol:.4f} SOL"
    )


async def _answer_callback(
    session: aiohttp.ClientSession,
    base_url: str,
    callback_id: str,
    text: str,
) -> None:
    """Answer a Telegram callback query (dismiss the loading spinner on the button)."""
    try:
        async with session.post(
            f"{base_url}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            r.raise_for_status()
    except Exception as e:
        logger.warning(f"answerCallbackQuery failed: {e}")


async def _send_tg(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: str,
    text: str,
) -> None:
    """Send a plain Telegram message via an existing aiohttp session."""
    try:
        async with session.post(
            f"{base_url}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            r.raise_for_status()
    except Exception as e:
        logger.warning(f"_send_tg failed: {e}")


async def _send_tg_with_buttons(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: str,
    text: str,
    inline_keyboard: list[list[dict]],
) -> None:
    """Send a Telegram message with inline keyboard via an existing aiohttp session."""
    try:
        async with session.post(
            f"{base_url}/sendMessage",
            json={
                "chat_id":      chat_id,
                "text":         text,
                "parse_mode":   "HTML",
                "reply_markup": {"inline_keyboard": inline_keyboard},
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            r.raise_for_status()
    except Exception as e:
        logger.warning(f"_send_tg_with_buttons failed: {e}")


async def _edit_message_with_buttons(
    session: aiohttp.ClientSession,
    base_url: str,
    chat_id: str,
    message_id: int,
    text: str,
    inline_keyboard: list[list[dict]],
) -> None:
    """Edit an existing Telegram message in place, preserving inline keyboard."""
    try:
        async with session.post(
            f"{base_url}/editMessageText",
            json={
                "chat_id":      chat_id,
                "message_id":   message_id,
                "text":         text,
                "parse_mode":   "HTML",
                "reply_markup": {"inline_keyboard": inline_keyboard},
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            r.raise_for_status()
    except Exception as e:
        logger.warning(f"editMessageText failed: {e}")


def _build_position_message(
    token_mint: str,
    pos: dict,
    mc_now: float,
    mc_source: str,
    num_emoji: str = "",
    sol_usd: float = 0.0,
) -> str:
    """
    Build a position card message used by both buy alert refresh and /holdings.
    Returns formatted HTML string.
    """
    entry_sol = pos.get("entry_sol", 0.0)
    mc_entry  = pos.get("mc_entry", 0.0)
    tl        = pos.get("token_label") or token_mint[:8]
    whale     = (pos.get("whale") or "?").upper()
    tp1_hit   = pos.get("min_target_hit", False)
    buy_sol   = pos.get("buy_sol", 0.0)
    swap_sig  = pos.get("swap_sig", "")

    # Hard sell MC
    hard_sell_mc = mc_entry * 0.65 if mc_entry else 0
    hard_sell_str = f" | Hard Sell MC: {_fmt_usd(hard_sell_mc)} (\u221235%)" if mc_entry else ""
    mc_entry_str = _fmt_usd(mc_entry) if mc_entry else "\u2014"

    # PnL and worth
    if mc_entry and mc_now:
        pnl_pct   = (mc_now - mc_entry) / mc_entry * 100
        worth_sol = entry_sol * (mc_now / mc_entry)
    else:
        pnl_pct   = 0.0
        worth_sol = entry_sol

    color    = "\U0001f7e2" if pnl_pct >= 0 else "\U0001f534"
    pnl_sign = "+" if pnl_pct >= 0 else ""
    tp1_icon = "\u2705" if tp1_hit else "\u274c"

    mc_now_str = _fmt_usd(mc_now) if mc_now else "\u2014"

    # Format entry and worth with optional USD
    if sol_usd:
        entry_str = f"{entry_sol:.4f} SOL (~{_fmt_usd(entry_sol * sol_usd)} USD)"
        worth_str = f"~{worth_sol:.4f} SOL (~{_fmt_usd(worth_sol * sol_usd)} USD)"
    else:
        entry_str = f"{entry_sol:.4f} SOL"
        worth_str = f"~{worth_sol:.4f} SOL"

    prefix = f"{num_emoji} " if num_emoji else ""
    lines = [
        f"{prefix}<b>{tl}</b>",
        f"   Whale: {whale}",
        f"   MC Entry: {mc_entry_str}{hard_sell_str}",
        f"   Current MC: {mc_now_str} {color} {pnl_sign}{pnl_pct:.0f}% (via {mc_source})",
        f"   Entry: {entry_str} | Worth: {worth_str}",
        f"   TP1 hit: {tp1_icon}",
    ]

    return "\n".join(lines)


async def _handle_refresh_callback(
    tg_session: aiohttp.ClientSession,
    base_url: str,
    callback_id: str,
    cb_data: str,
    chat_id: str,
    message_id: int | None,
) -> None:
    """
    Process a refresh button tap: refresh|<token_mint>
    Fetches current MC and edits the original message in place.
    """
    parts = cb_data.split("|")
    if len(parts) != 2:
        return
    _, token_mint = parts

    # 10-second cooldown per mint
    now = time.time()
    last = _last_refresh.get(token_mint, 0.0)
    if now - last < 10:
        await _answer_callback(tg_session, base_url, callback_id,
                               "\u23f3 Please wait a few seconds before refreshing")
        return

    _last_refresh[token_mint] = now

    pos = open_positions.get(token_mint)
    if not pos:
        await _answer_callback(tg_session, base_url, callback_id,
                               "Position not found \u2014 may already be closed.")
        return

    await _answer_callback(tg_session, base_url, callback_id,
                           "Refreshing...")

    # Fetch fresh MC
    mc_now, mc_source = await get_current_mc(tg_session, token_mint)

    # Build refreshed message
    msg = _build_position_message(token_mint, pos, mc_now, mc_source)

    if message_id:
        await _edit_message_with_buttons(
            tg_session, base_url, chat_id, message_id,
            msg, _make_position_buttons(token_mint)
        )
    else:
        await _send_tg(tg_session, base_url, chat_id, msg)

    logger.info(
        f"[REFRESH] {token_mint[:8]} | MC={_fmt_usd(mc_now)} via {mc_source}"
    )


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
    token_valid = bool(re.match(r"^\d{8,12}:[A-Za-z0-9_-]{35,}$", token))
    logger.info(
        f"Telegram token: first10={token[:10]!r}  len={len(token)}  "
        f"format_valid={token_valid}  chat_ids={_telegram_chat_ids}"
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

        # Guard 0 — panic sell check: wait 10s and confirm whale still holds
        logger.info(f"[{name}] Waiting 10s to confirm {token_mint[:8]} isn't a panic sell...")
        await asyncio.sleep(10)
        _recent_sigs_raw = get_recent_signatures(rpc_url, wallet, limit=5)
        _sold_quick = False
        for _rentry in (_recent_sigs_raw or []):
            _rsig = _rentry.get("signature", "") if isinstance(_rentry, dict) else _rentry
            if _rsig == sig:
                break
            _rtx = get_transaction(rpc_url, _rsig)
            if not _rtx:
                continue
            _meta = _rtx.get("meta") or {}
            _pre  = {b.get("mint"): float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                     for b in (_meta.get("preTokenBalances") or [])
                     if b.get("owner") == wallet}
            _post = {b.get("mint"): float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                     for b in (_meta.get("postTokenBalances") or [])
                     if b.get("owner") == wallet}
            if _pre.get(token_mint, 0.0) > 0 and _post.get(token_mint, 0.0) < _pre.get(token_mint, 0.0):
                _sold_quick = True
                break
        if _sold_quick:
            logger.info(
                f"[{name}] SKIP — {token_mint[:8]} panic sell detected within 10s "
                f"(whale sold immediately after buying)"
            )
            send_telegram(
                f"⚡ <b>PANIC SELL DETECTED</b> — <code>{token_mint[:8]}</code>\n"
                f"Whale: <b>{name}</b>\n"
                f"Whale bought then sold within 10s — trade skipped."
            )
            continue

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

        # --- Activity tracking ---------------------------------------------
        now = time.time()

        # Prune entries outside the 24 h activity window
        _whale_activity[name] = [
            (m, t) for m, t in _whale_activity[name]
            if now - t < ACTIVITY_WINDOW_SEC
        ]

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

        # --- Pump.fun status (prefetch — used by DexScreener gate AND prebond logic) ---
        # Fetching once here avoids a duplicate API call later.
        # Fail-open: (None, False) means pump.fun unreachable or token not on pump.fun.
        prebond_pct, is_graduated = await fetch_prebond_progress(session, token_mint)
        prebond_buy_sol: float | None = None  # set to override BUY_AMOUNT_SOL for prebond entries
        pump_data:       dict | None  = None  # populated for pre-graduation coins
        # ----------------------------------------------------------------

        # --- Quality gate — always try PumpFun first; fall back to DexScreener ---
        # PumpFun is checked for ALL wallets first since 90% of signals are
        # pre-graduation coins not yet on DexScreener. DexScreener is only used
        # as a fallback for graduated tokens.
        _bypass_quality = (MANNOS_AUTOPILOT and name == "mannos") or name == "mr.putin"

        # Step 1: Always fetch PumpFun data regardless of prebond_pct result.
        # fetch_prebond_progress may fail even when the token IS on pump.fun,
        # so we fetch pump_data independently to ensure Claude always has metrics.
        if pump_data is None:
            pump_data = await fetch_pumpfun_data(session, token_mint)

        if pump_data is not None and not (pump_data.get("complete", False)):
            # Pre-graduation coin confirmed via PumpFun — use PumpFun data
            _prog = pump_data.get("bonding_curve_progress", prebond_pct or 0)
            logger.info(
                f"[{name.upper()}] Pre-graduation ({_prog:.0f}%) "
                f"— using PumpFun data for quality + Claude ({token_mint[:8]})"
            )
            if _bypass_quality:
                logger.info(
                    f"[{name.upper()}] PumpFun quality check bypassed "
                    f"({'MANNOS_AUTOPILOT' if name == 'mannos' else 'mr.putin sub-$5k'})"
                )
            else:
                _ok, _reason = passes_pump_quality(pump_data)
                if not _ok:
                    logger.info(
                        f"[{name.upper()}] SKIP — PumpFun quality fail: {_reason} "
                        f"({token_mint[:8]})"
                    )
                    _stats["cancelled_dexscreener"] += 1
                    continue
                logger.info(f"[{name.upper()}] PumpFun quality OK — {_reason}")
            dex_pair = None  # no DexScreener pair for pre-graduation coins
        elif prebond_pct is not None and not is_graduated:
            # PumpFun data fetch failed but prebond_progress says pre-graduation
            # Fail-open: skip quality check rather than falling to DexScreener
            # (DexScreener always shows $0 liquidity for pre-grad coins)
            logger.info(
                f"[{name.upper()}] Pre-grad ({prebond_pct:.0f}%) but PumpFun data "
                f"unavailable — fail-open, skipping quality check ({token_mint[:8]})"
            )
            dex_pair = None
        else:
            # Graduated or PumpFun unavailable — fall back to DexScreener
            pump_data = None  # don't pass stale pump_data to Claude for graduated coins
            dex_pair = await fetch_dexscreener(session, token_mint)
            if dex_pair:
                if _bypass_quality:
                    logger.info(
                        f"[{name.upper()}] DexScreener quality check bypassed "
                        f"({'MANNOS_AUTOPILOT' if name == 'mannos' else 'mr.putin sub-$5k'})"
                    )
                else:
                    _ok, _reason = passes_dex_quality(dex_pair)
                    if not _ok:
                        logger.info(
                            f"[{name.upper()}] SKIP — DexScreener quality fail: {_reason} "
                            f"({token_mint[:8]})"
                        )
                        _stats["cancelled_dexscreener"] += 1
                        continue
                    logger.info(f"[{name.upper()}] DexScreener quality OK — {_reason}")
            else:
                logger.info(
                    f"[{name.upper()}] DexScreener unavailable — fail-open, proceeding "
                    f"({token_mint[:8]})"
                )
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
        # prebond_pct and is_graduated were already fetched above — no second API call.
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

        # --- Token safety check ----------------------------------------
        # Skip for mannos (MANNOS_AUTOPILOT) and mr.putin (sub-$5k, no clean data)
        if not (MANNOS_AUTOPILOT and name == "mannos") and name != "mr.putin":
            _safe, _block_reason = await check_token_safety(
                session, token_mint, rpc_url, name, dex_pair=dex_pair
            )
            if not _safe:
                logger.info(
                    f"[{name}] SKIP — safety check failed: {_block_reason} "
                    f"({token_mint[:8]})"
                )
                _stats["cancelled_safety"] += 1
                _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
                _save_blacklist()
                continue
        else:
            logger.info(
                f"[{name.upper()}] Safety check skipped "
                f"({'MANNOS_AUTOPILOT' if name == 'mannos' else 'mr.putin sub-$5k'})"
            )
        # ----------------------------------------------------------------

        # (Entry quote is fetched at the swap site after sizing — see routing block below)
        # ----------------------------------------------------------------

        # --- Claude confidence scoring ---------------------------------
        if MANNOS_AUTOPILOT and name == "mannos":
            # Autopilot: skip scoring entirely — proceed straight to buy.
            # Use fixed high-conviction tier: 200% min target, 25% trail, no time stop.
            claude_score = 75   # maps to Tier 2 in get_exit_tier()
            tier = get_exit_tier(claude_score)
            logger.info(
                f"[MANNOS AUTOPILOT] Claude score skipped — high-conviction tier: "
                f"min_target={tier['min_target_pct']}% "
                f"trail={tier['trail_pct']}% "
                f"time={tier['time_stop_min']}m"
            )
        else:
            claude_score, score_bullets = await get_claude_score(
                token_mint,
                dex_pair,
                prebond_pct,   # None if not a pump.fun token or if graduated
                f"whale={name} signal",
                pump_data=pump_data,
            )
            _whale_approved = claude_score >= WHALE_MIN_SCORE
            _whale_label    = _token_label(token_mint, dex_pair)
            _send_claude_score_alert(
                token_label=_whale_label,
                score=claude_score,
                bullets=score_bullets,
                approved=_whale_approved,
                entry_blocked=not _whale_approved,
            )
            if not _whale_approved:
                logger.info(
                    f"[{name}] Claude score {claude_score} < {WHALE_MIN_SCORE} "
                    f"(WHALE_MIN_SCORE) — skipping entry for {token_mint[:8]}"
                )
                _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
                _save_blacklist()
                continue
            tier = get_exit_tier(claude_score)
            logger.info(
                f"[{name}] Claude score: {claude_score}/100 | "
                f"Tier: min_target={tier['min_target_pct']}% "
                f"trail={tier['trail_pct']}% "
                f"time={tier['time_stop_min']}m"
            )
        # ----------------------------------------------------------------

        # --- Position sizing — mr.putin > prebond > normal ----------------
        if name == "mr.putin":
            buy_sol = round(sol_balance * MRPUTIN_CONFIG["position_size_pct"], 4)
            logger.info(
                f"[MR.PUTIN] Position: {buy_sol} SOL (1% balance) | "
                f"min hold {MRPUTIN_CONFIG['min_hold_mins']}m | "
                f"time stop {MRPUTIN_CONFIG['time_stop_mins']}m"
            )
        elif prebond_buy_sol is not None:
            buy_sol = prebond_buy_sol
            logger.info(f"[{name}] Prebond position size: {buy_sol} SOL (2% of balance)")
        else:
            buy_sol = BUY_AMOUNT_SOL
        # ----------------------------------------------------------------

        # Determine MC for routing — use pump_data or dex_pair, live lookup if both missing
        _buy_mc = float((pump_data or {}).get("usd_market_cap") or 0) or \
                  float((dex_pair or {}).get("marketCap") or (dex_pair or {}).get("fdv") or 0)
        if _buy_mc <= 0:
            _buy_mc, _mc_src = await get_current_mc(session, token_mint)
            if _buy_mc > 0:
                logger.info(
                    f"[{name.upper()}] MC=${_buy_mc:,.0f} (from live lookup, {_mc_src})"
                )
            else:
                logger.warning(
                    f"[{name.upper()}] MC unknown — router will default to Jupiter"
                )
        # --- Mint suffix routing: non-"pump" mints are likely on Raydium already ---
        if not token_mint.endswith("pump"):
            logger.info(
                f"[{name.upper()}] Mint does not end in 'pump' — skipping PumpFun, "
                f"routing directly to Jupiter ({token_mint[:8]})"
            )
            _amount_lamports = int(buy_sol * 1_000_000_000)
            _jup_quote = await get_jupiter_quote(session, token_mint, _amount_lamports)
            if _jup_quote:
                swap_sig, swap_msg = await execute_swap(session, _jup_quote, wallet_pubkey)
            else:
                swap_sig, swap_msg = None, "Jupiter quote failed (non-pump mint)"
        else:
            swap_sig, swap_msg = await execute_buy_routed(
                session, token_mint, buy_sol, wallet_pubkey, _buy_mc
            )

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

        token_label = _token_label(token_mint, dex_pair)
        mc_entry    = float(
            (dex_pair or {}).get("marketCap") or (dex_pair or {}).get("fdv") or 0
        )
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

        hard_sell_mc = mc_entry * 0.65 if mc_entry else 0
        hard_sell_str = f" | Hard Sell MC: {_fmt_usd(hard_sell_mc)} (\u221235%)" if mc_entry else ""

        msg = (
            f"\U0001f40b <b>APEX WHALE COPY</b> [{name.upper()}]\n"
            f"Token: <code>{token_label}</code>\n"
            f"Amount: {buy_sol} SOL\n"
            f"MC Entry: {mc_entry_str}{hard_sell_str}\n"
            f"Whale: <code>{name.upper()}</code>\n"
            f"Our sig: <code>{swap_sig}</code>"
            f"{tp_block}"
        )
        logger.info(msg)
        send_telegram_with_buttons(msg, _make_position_buttons(token_mint))

        # --- Register open position for sell monitoring ----------------
        # No quote object in routed flow — use buy_sol as entry, fetch
        # live on-chain token balance to get accurate amount_tokens.
        entry_sol = buy_sol
        try:
            token_units = await get_spl_token_balance(session, token_mint, wallet_pubkey)
        except Exception as exc:
            logger.warning(f"[{token_mint[:8]}] SPL balance fetch failed: {exc} — saving with 0")
            token_units = 0

        try:
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
            _save_positions()
            logger.info(
                f"[{token_mint[:8]}] Position opened — "
                f"{token_units:,} tokens | entry {entry_sol:.4f} SOL"
            )
        except Exception as exc:
            logger.error(
                f"[{token_mint[:8]}] CRITICAL — position save failed: {exc} "
                f"(buy was successful, sig={swap_sig})"
            )
            send_telegram(
                f"🚨 <b>POSITION SAVE FAILED</b>\n"
                f"Token: {token_label}\n"
                f"Buy sig: <code>{swap_sig}</code>\n"
                f"Manual intervention needed!"
            )
        _stats["trades_executed"] += 1
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
    global _rpc_url, _wallet_keypair
    rpc_url       = os.getenv("SOLANA_RPC", "")
    wallet_pubkey = os.getenv("WALLET_PUBLIC_KEY", "")   # base58 address — for balance checks + Jupiter
    wallet_key    = os.getenv("WALLET_PRIVATE_KEY", "")  # base58 or JSON byte array — for tx signing
    _rpc_url      = rpc_url                              # module-level — used by confirm_transaction()

    # Load wallet keypair for PumpFun transaction signing (skipped in DRY_RUN)
    if wallet_key and not DRY_RUN:
        try:
            # Support three formats:
            #   1. JSON byte-array:         [228,29,168,...]
            #   2. Bare comma-separated:    228, 29, 168, ...
            #   3. Base58 string:           4dZ7a...
            stripped = wallet_key.strip()
            if stripped.startswith("["):
                key_bytes = bytes(json.loads(stripped))
                _wallet_keypair = SoldersKeypair.from_bytes(key_bytes)
            elif "," in stripped:
                key_bytes = bytes(int(b.strip()) for b in stripped.split(",") if b.strip())
                _wallet_keypair = SoldersKeypair.from_bytes(key_bytes)
            else:
                _wallet_keypair = SoldersKeypair.from_base58_string(stripped)
            logger.info(f"Wallet keypair loaded — pubkey: {str(_wallet_keypair.pubkey())[:12]}…")
        except Exception as exc:
            logger.error(f"Failed to load wallet keypair from WALLET_PRIVATE_KEY: {exc}")
    elif not DRY_RUN:
        logger.warning("WALLET_PRIVATE_KEY not set — PumpFun buys will fail in live mode")

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

    # Restore persisted state from disk
    _restored_positions = _load_positions()
    open_positions.update(_restored_positions)
    logger.info(f"Position monitor: {len(open_positions)} open position(s) at startup "
                f"({'restored from disk' if _restored_positions else 'none'})")

    _restored_blacklist = _load_blacklist()
    _token_blacklist.update(_restored_blacklist)

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


# --- External signal API ---------------------------------------------------
# handle_cto_signal() is called directly by DexAlert scanner


async def handle_cto_signal(
    session: aiohttp.ClientSession,
    token_mint: str,
    token_name: str,
    token_symbol: str,
    rpc_url: str,
    wallet_pubkey: str,
) -> None:
    """
    Entry point for DexAlert CTO verified signals.
    Runs the full Apex pipeline: quality check -> safety -> Claude -> buy.
    """
    PREFIX = "[CTO SIGNAL]"

    # Step 1 — Duplicate check
    if token_mint in open_positions:
        logger.info(f"{PREFIX} {token_symbol} already in open_positions — skipping")
        return
    if token_mint in _token_blacklist and _token_blacklist[token_mint] > time.time():
        logger.info(f"{PREFIX} {token_symbol} is blacklisted — skipping")
        return

    # Step 2 — Balance check
    sol_balance = get_sol_balance(rpc_url, wallet_pubkey)
    if sol_balance < LOW_BALANCE_SOL:
        logger.warning(
            f"{PREFIX} SOL balance {sol_balance:.4f} below minimum "
            f"{LOW_BALANCE_SOL} — skipping"
        )
        return

    # Step 3 — Quality check (fail-open)
    dex_pair: dict | None = None
    pump_data: dict | None = None
    prebond_pct: float | None = None
    is_graduated = False

    try:
        prebond_pct, is_graduated = await fetch_prebond_progress(session, token_mint)
    except Exception as e:
        logger.warning(f"{PREFIX} fetch_prebond_progress failed: {e} — fail-open")

    if not is_graduated:
        # Pre-graduation — use PumpFun quality check
        try:
            pump_data = await fetch_pumpfun_data(session, token_mint)
            if pump_data:
                ok, reason = passes_pump_quality(pump_data)
                if not ok:
                    logger.info(f"{PREFIX} Pump quality fail: {reason} — skipping")
                    return
        except Exception as e:
            logger.warning(f"{PREFIX} PumpFun quality check failed: {e} — fail-open")
    else:
        # Graduated — use DexScreener quality check
        try:
            dex_pair = await fetch_dexscreener(session, token_mint)
            if dex_pair:
                ok, reason = passes_dex_quality(dex_pair)
                if not ok:
                    logger.info(f"{PREFIX} Dex quality fail: {reason} — skipping")
                    return
        except Exception as e:
            logger.warning(f"{PREFIX} DexScreener quality check failed: {e} — fail-open")

    # Step 4 — Safety check
    try:
        safe, safety_msg = await check_token_safety(
            session, token_mint, rpc_url, whale_name="cto_signal", dex_pair=dex_pair
        )
        if not safe:
            logger.info(f"{PREFIX} Safety fail: {safety_msg} — blacklisting + skipping")
            _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
            return
    except Exception as e:
        logger.warning(f"{PREFIX} Safety check failed: {e} — fail-open")

    # Step 5 — Claude scoring
    try:
        claude_score, bullets = await get_claude_score(
            token_mint,
            dex_pair=dex_pair,
            prebond_progress=prebond_pct,
            context_note="cto_signal=DexAlert verified CTO",
            pump_data=pump_data,
        )
    except Exception as e:
        logger.warning(f"{PREFIX} Claude scoring failed: {e} — fail-open at 70")
        claude_score, bullets = 70, None

    if claude_score < WHALE_MIN_SCORE:
        logger.info(
            f"{PREFIX} Claude score {claude_score} below min {WHALE_MIN_SCORE} — NO-GO"
        )
        return

    # Step 6 — Execute buy
    buy_sol = CTO_SIGNAL_BUY_SOL

    if DRY_RUN:
        logger.info(
            f"{PREFIX} [DRY RUN] Would buy {buy_sol} SOL of {token_symbol} "
            f"({token_mint[:8]}) — Claude: {claude_score}"
        )
        send_telegram(
            f"🔵 {PREFIX} <b>[DRY RUN]</b> Would buy {token_symbol}\n"
            f"Mint: <code>{token_mint[:8]}</code>...\n"
            f"Size: {buy_sol} SOL\n"
            f"Claude: {claude_score}/100\n"
            f"Source: DexAlert verified CTO"
        )
        return

    # Determine MC for routing — live lookup if local data missing
    _cto_mc = float((pump_data or {}).get("usd_market_cap") or 0) or \
              float((dex_pair or {}).get("marketCap") or (dex_pair or {}).get("fdv") or 0)
    if _cto_mc <= 0:
        _cto_mc, _mc_src = await get_current_mc(session, token_mint)
        if _cto_mc > 0:
            logger.info(
                f"{PREFIX} MC=${_cto_mc:,.0f} (from live lookup, {_mc_src})"
            )
        else:
            logger.warning(
                f"{PREFIX} MC unknown — router will default to Jupiter"
            )
    swap_sig, swap_msg = await execute_buy_routed(
        session, token_mint, buy_sol, wallet_pubkey, _cto_mc
    )

    if not swap_sig:
        logger.error(f"{PREFIX} Buy failed: {swap_msg} — position NOT opened")
        send_telegram(
            f"⚠️ {PREFIX} <b>TX FAILED</b> — {token_symbol}\n"
            f"Mint: <code>{token_mint[:8]}</code>\n"
            f"Reason: {swap_msg}"
        )
        return

    # Step 7 — Record position
    token_label = _token_label(token_mint, dex_pair)
    mc_entry = float(
        (dex_pair or {}).get("marketCap") or (dex_pair or {}).get("fdv") or 0
    )

    entry_sol = buy_sol
    try:
        token_units = await get_spl_token_balance(session, token_mint, wallet_pubkey)
    except Exception as exc:
        logger.warning(f"{PREFIX} SPL balance fetch failed: {exc} — saving with 0")
        token_units = 0

    try:
        open_positions[token_mint] = {
            "entry_time":     time.time(),
            "entry_sol":      entry_sol,
            "peak_sol":       entry_sol,
            "amount_tokens":  token_units,
            "whale":          "cto_signal",
            "buy_sol":        buy_sol,
            "claude_score":   claude_score,
            "min_target_hit": False,
            "source":         "cto_signal",
            "mc_entry":       mc_entry,
            "token_label":    token_label,
        }
        _save_positions()
        logger.info(
            f"{PREFIX} Position opened — {token_symbol} | "
            f"{token_units:,} tokens | entry {entry_sol:.4f} SOL"
        )
    except Exception as exc:
        logger.error(
            f"{PREFIX} CRITICAL — position save failed: {exc} "
            f"(buy was successful, sig={swap_sig})"
        )
        send_telegram(
            f"🚨 <b>POSITION SAVE FAILED</b>\n"
            f"Token: {token_label}\n"
            f"Buy sig: <code>{swap_sig}</code>\n"
            f"Manual intervention needed!"
        )
    _stats["trades_executed"] += 1
    asyncio.create_task(emergency_dump_check(session, token_mint, wallet_pubkey))

    # Step 8 — Telegram alert with sell buttons
    _cto_hard_sell_mc = mc_entry * 0.65 if mc_entry else 0
    _cto_hard_sell_str = f" | Hard Sell MC: {_fmt_usd(_cto_hard_sell_mc)} (\u221235%)" if mc_entry else ""
    _cto_mc_str = _fmt_usd(mc_entry) if mc_entry else "\u2014"

    _cto_msg = (
        f"\U0001f3af {PREFIX} <b>Bought {token_symbol}</b>\n"
        f"Mint: <code>{token_mint[:8]}</code>...\n"
        f"Size: {buy_sol} SOL\n"
        f"MC Entry: {_cto_mc_str}{_cto_hard_sell_str}\n"
        f"Claude: {claude_score}/100\n"
        f"Source: DexAlert verified CTO\n"
        f"Sig: <code>{swap_sig}</code>"
    )
    send_telegram_with_buttons(_cto_msg, _make_position_buttons(token_mint))


if __name__ == "__main__":
    asyncio.run(run())
