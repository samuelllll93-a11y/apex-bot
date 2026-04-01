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
import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("whale_sniper")

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
    "peace":  "7b88jCzsirGfLmFMyr7BXbCaDGTtuq8oDTWusqWvLv38",
    "crispy": "EdbNfzVJjVZFsz1awBezeJpBaySLsckoZyPyaucy3g2R",
    "mannos": "CAmNcBJ82xr1tzXrwZ6tZKwEFs26TG8kT6dJeR1bxjW9",
    "early":  "Bv2BAw5UmKxv5SBMWYKqpsh6eXKNGM2RKxJGpGPk5vmb",
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
TAKE_PROFIT_PCT    = 50.0   # exit if SOL value up ≥ 50 % from entry
TRAILING_STOP_PCT  = 10.0   # exit if SOL value drops ≥ 10 % from peak
TIME_STOP_MIN      = 30     # force-close after this many minutes
POSITION_CHECK_SEC = 10     # how often the sell monitor loop runs

# --- Entry confirmation parameters ------------------------------------
ENTRY_DELAY_SEC  = 60     # wait before executing entry
ENTRY_DUMP_PCT   = 3.0    # skip if price drops >3% in the delay window

# --- Emergency exit parameters ----------------------------------------
EMERGENCY_DUMP_PCT        = 5.0  # emergency exit if down >5% right after buy
EMERGENCY_CHECK_DELAY_SEC = 5    # seconds after buy before emergency check runs

# --- DexScreener quality filter ---------------------------------------
DEXSCREENER_API       = "https://api.dexscreener.com/tokens/v1/solana"
MIN_DEX_LIQUIDITY_USD = 20_000
MIN_DEX_5M_VOLUME_USD = 10_000

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
    "cancelled_entry_delay":  0,   # filtered by 60s price-dump test
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
            pairs = data.get("pairs") or []
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


# --- Trade detection --------------------------------------------------

def extract_token_buy(tx: dict, whale_address: str) -> str | None:
    """
    Inspect a parsed transaction for a token buy (SOL out, SPL token in).
    Returns the token mint address if a buy is detected, else None.
    """
    if not tx:
        return None

    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None   # failed tx

    pre_balances  = meta.get("preTokenBalances")  or []
    post_balances = meta.get("postTokenBalances") or []

    # Build maps: owner -> {mint: amount}
    def balance_map(balances: list) -> dict[str, dict[str, float]]:
        m: dict[str, dict[str, float]] = {}
        for b in balances:
            owner  = b.get("owner", "")
            mint   = b.get("mint", "")
            amount = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            m.setdefault(owner, {})[mint] = amount
        return m

    pre  = balance_map(pre_balances)
    post = balance_map(post_balances)

    # Look for mints where the whale's balance increased
    whale_pre  = pre.get(whale_address,  {})
    whale_post = post.get(whale_address, {})

    for mint, post_amount in whale_post.items():
        if mint in (SOL_MINT, WSOL_MINT):
            continue
        pre_amount = whale_pre.get(mint, 0.0)
        if post_amount > pre_amount:
            logger.info(
                f"Detected buy: whale {whale_address[:8]} "
                f"received {post_amount - pre_amount:.4f} of {mint[:8]}"
            )
            return mint

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

    # Check exit conditions (take profit first — never cut a winner early)
    exit_reason = None
    if pnl_pct >= TAKE_PROFIT_PCT:
        exit_reason = f"TAKE PROFIT +{pnl_pct:.1f}%"
    elif drop_from_peak <= -TRAILING_STOP_PCT:
        exit_reason = (
            f"TRAILING STOP {pnl_pct:+.1f}% "
            f"(dropped {abs(drop_from_peak):.1f}% from peak)"
        )
    elif elapsed_min >= TIME_STOP_MIN:
        exit_reason = f"TIME STOP ({elapsed_min:.0f}m elapsed)"

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

    logger.info(
        f"[{token_mint[:8]}] {exit_reason} | "
        f"Entry: {entry_sol:.4f} SOL | Exit: {current_sol:.4f} SOL | "
        f"PnL: {pnl_sign}{pnl_pct:.1f}%"
    )
    send_telegram(
        f"{emoji} <b>SELL [{token_mint[:8]}]</b>\n"
        f"Reason: {exit_reason}\n"
        f"Entry: {entry_sol:.4f} SOL | Exit: {current_sol:.4f} SOL\n"
        f"PnL: {pnl_sign}{pnl_pct:.1f}%\n"
        f"Sig: <code>{sell_sig}</code>"
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


def _register_commands() -> None:
    """Register /summary in Telegram's bot command menu."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/setMyCommands",
            json={"commands": [{"command": "summary", "description": "12-hour trade summary"}]},
            timeout=5,
        )
        logger.info("Telegram /summary command registered")
    except Exception as e:
        logger.warning(f"setMyCommands failed: {e}")


async def telegram_command_loop() -> None:
    """Long-poll Telegram getUpdates and respond to /summary commands.

    Uses its own aiohttp session so the 30-second long-poll never competes
    with the shared session used by the whale poller and position monitor.
    """
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("telegram_command_loop: credentials missing — /summary won't respond")
        return

    base_url       = f"https://api.telegram.org/bot{token}"
    last_update_id = 0

    async with aiohttp.ClientSession() as tg_session:
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
            except Exception as e:
                logger.warning(f"getUpdates failed: {e}")
                await asyncio.sleep(5)
                continue

            if not data.get("ok"):
                logger.error(f"Telegram getUpdates error: {data.get('description', data)}")
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
        f"Cancelled (60s dump):     {_stats['cancelled_entry_delay']}\n"
        f"Trades executed:          {total}\n"
        f"Wins / Losses:            {wins} / {losses}  ({win_rate})\n"
        f"Net PnL:                  {pnl_sign}{net:.4f} SOL"
    )
    logger.info(
        f"DAILY SUMMARY | signals={_stats['signals_detected']} "
        f"cancelled_dex={_stats['cancelled_dexscreener']} "
        f"cancelled_delay={_stats['cancelled_entry_delay']} "
        f"trades={total} W/L={wins}/{losses} pnl={pnl_sign}{net:.4f} SOL"
    )
    send_telegram(msg)


def _reset_stats() -> None:
    """Zero all daily counters — called immediately after midnight summary."""
    for key in ("signals_detected", "cancelled_dexscreener", "cancelled_entry_delay",
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

        # --- 60-second entry delay with price-direction check — Fix 1 --
        send_telegram(
            f"⏳ <b>[{name.upper()}]</b> signal detected on "
            f"<code>{token_mint[:8]}</code>\n"
            f"Waiting {ENTRY_DELAY_SEC}s to confirm price direction…"
        )

        probe_lamports = int(BUY_AMOUNT_SOL * 1_000_000_000)
        quote_t0 = await get_jupiter_quote(session, token_mint, probe_lamports)
        if not quote_t0:
            logger.error(f"[{name}] SKIP — T=0 quote failed for {token_mint[:8]}")
            continue
        price_t0 = int(quote_t0.get("outAmount", 0))

        # Sleep releases the event loop — position_monitor_loop continues normally.
        # Note: asyncio.gather waits for all whale coroutines, so the next poll
        # cycle is delayed ~60s when an active signal is in progress.
        await asyncio.sleep(ENTRY_DELAY_SEC)

        quote_t60 = await get_jupiter_quote(session, token_mint, probe_lamports)
        if not quote_t60:
            logger.error(f"[{name}] SKIP — T=60 quote failed for {token_mint[:8]}")
            continue
        price_t60 = int(quote_t60.get("outAmount", 0))

        # Higher outAmount at T=60 = same SOL buys more tokens = token is cheaper = dumped
        dump_pct = (price_t60 / price_t0 - 1) * 100 if price_t0 > 0 else 0.0

        if dump_pct > ENTRY_DUMP_PCT:
            logger.info(
                f"[{name}] Entry cancelled — {token_mint[:8]} dropped "
                f"{dump_pct:.1f}% in {ENTRY_DELAY_SEC}s window"
            )
            _stats["cancelled_entry_delay"] += 1
            continue

        logger.info(
            f"[{name}] Entry confirmed — {token_mint[:8]} price held/rose "
            f"in {ENTRY_DELAY_SEC}s window (dump_pct={dump_pct:.1f}%)"
        )
        # ----------------------------------------------------------------

        # --- Position sizing — conviction multiplier only after quality gates — Fix 3
        if is_high_conviction:
            buy_sol = round(BUY_AMOUNT_SOL * CONVICTION_MULTIPLIER, 4)
            logger.info(
                f"[{name.upper()}] HIGH CONVICTION sizing confirmed — "
                f"{buy_sol} SOL (all quality gates passed)"
            )
        else:
            buy_sol = BUY_AMOUNT_SOL
        # ----------------------------------------------------------------

        # Reuse T=60 quote for normal size; re-fetch only for conviction size
        if buy_sol == BUY_AMOUNT_SOL:
            quote = quote_t60
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
        if is_high_conviction:
            send_telegram(
                f"🔥 <b>HIGH CONVICTION</b> — [{name.upper()}] bought "
                f"<code>{token_mint[:8]}</code> twice in 30 mins\n"
                f"Position size: {buy_sol} SOL ({CONVICTION_MULTIPLIER}x normal)"
            )

        conviction_badge = "🔥 HIGH CONVICTION\n" if is_high_conviction else ""
        msg = (
            f"🐋 <b>APEX WHALE COPY</b> [{name.upper()}]\n"
            f"{conviction_badge}"
            f"Token: <code>{token_mint}</code>\n"
            f"Amount: {buy_sol} SOL\n"
            f"Whale sig: <code>{sig}</code>\n"
            f"Our sig: <code>{swap_sig}</code>"
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
        await asyncio.gather(*tasks, return_exceptions=True)
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

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            whale_poll_loop(session, rpc_url, wallet_pubkey),
            position_monitor_loop(session, wallet_pubkey),
            midnight_summary_loop(),
            telegram_command_loop(),
        )


if __name__ == "__main__":
    asyncio.run(run())
