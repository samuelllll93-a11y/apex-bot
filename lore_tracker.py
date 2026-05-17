"""
APEX Lore Tracker — observation-only wallet performance monitor.

Polls ~117 Solana wallets defined in state/lore_wallets.json every 15
minutes (60 min for dormant wallets), reconstructs their trades from
on-chain activity, aggregates by trader group, and reports top
performers + Claude-driven promote/demote recommendations via Telegram.

NO TRADING. Lore tracker never holds keys and never submits
transactions. It is a separate PM2 process, crash-isolated from
whale_sniper.py, sharing only the Telegram chat IDs and the Helius
RPC endpoint (via SOLANA_RPC in .env).

PHASE 1 SCOPE: scaffold + polling only. Reads wallets, polls Helius
for recent signatures, logs activity. No trade reconstruction, no
metrics, no group aggregation, no Telegram digests beyond a startup
ping. Phases 2-4 add those layers.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from zoneinfo import ZoneInfo

import aiohttp
import anthropic
import requests
from dotenv import load_dotenv


# --- Module setup -----------------------------------------------------

load_dotenv()

logger = logging.getLogger("lore_tracker")
logger.setLevel(logging.INFO)
# Avoid duplicate console output if root logger also has handlers.
logger.propagate = False
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(_console_handler)

_HERE = os.path.dirname(os.path.abspath(__file__))
_log_dir = os.path.join(_HERE, "logs")
os.makedirs(_log_dir, exist_ok=True)
_file_handler = TimedRotatingFileHandler(
    filename=os.path.join(_log_dir, "lore_tracker.log"),
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(_file_handler)


# --- Config -----------------------------------------------------------

LORE_WALLETS_FILE          = os.path.join(_HERE, "state", "lore_wallets.json")
LORE_METRICS_FILE          = os.path.join(_HERE, "state", "lore_metrics.json")
LORE_OPEN_POSITIONS_FILE   = os.path.join(_HERE, "state", "lore_open_positions.json")
LORE_CLOSED_TRADES_FILE    = os.path.join(_HERE, "state", "lore_closed_trades.json")
LORE_SCHEDULER_STATE_FILE  = os.path.join(_HERE, "state", "lore_scheduler_state.json")

# Daily top-3 fires at this Perth-local hour. UTC+8 — so 9am Perth =
# 01:00 UTC. The scheduler loop fires whenever (current Perth hour) is
# >= this AND today's date string is not yet recorded in
# lore_scheduler_state.json. That makes restarts mid-day idempotent and
# misses get re-fired automatically.
LORE_TOP3_PERTH_HOUR = 9
PERTH_TZ = ZoneInfo("Australia/Perth")

# 3-day Claude analysis fires Mon/Thu/Sun (weekday() values: Mon=0,
# Thu=3, Sun=6) at the same 09:00 Perth hour. Same scheduler pattern
# as the daily top-3 — restart-safe via last_lore_analysis_date in
# lore_scheduler_state.json.
LORE_ANALYSIS_WEEKDAYS    = {0, 3, 6}    # Mon, Thu, Sun
LORE_ANALYSIS_PERTH_HOUR  = 9
LORE_ANALYSIS_WINDOW_DAYS = 3

# Claude API config — matches whale_sniper's pattern. claude-haiku-4-5
# is the current fast/cheap tier (used by whale_sniper for Claude
# scoring at line 1188). At ~30 calls/month × ~13k tokens each the
# monthly spend is well under $1.
LORE_CLAUDE_MODEL      = "claude-haiku-4-5"
LORE_CLAUDE_MAX_TOKENS = 1500

# Rolling window for closed-trade metrics — entries older than this
# are pruned on each tick's write. 30 days matches the user spec for
# the per-wallet metrics window.
LORE_METRICS_WINDOW_DAYS = 30

# Dust threshold for declaring a position "fully closed". When
# tokens_held drops below this fraction of the position's original
# token count, the entry is removed from open_positions so it
# doesn't linger forever for tokens that round to ~0 after a sell.
LORE_DUST_FRACTION = 0.001   # 0.1% of original tokens

# Cold-start cap: when a wallet has no last_processed_sig (first
# time we poll it), process only the most recent N signatures to
# seed metrics instead of trying to backfill its entire history.
# Subsequent ticks process only sigs newer than last_processed_sig.
LORE_COLD_START_SIG_LIMIT = 5

# Lamports per SOL — used to convert native SOL balance deltas
# (preBalances/postBalances are in lamports).
LAMPORTS_PER_SOL = 1_000_000_000

# SOL/USD price cache — refreshed lazily once per tick via Jupiter
# quote (1 SOL → USDC). Used to normalise USDC/USDT trades to SOL
# for the per-wallet metrics window. Marked APPROXIMATE in records.
LORE_SOL_USD_CACHE_SEC = 300   # 5 min cache

# Active wallets are polled every ACTIVE interval; dormant wallets
# (no trade activity in LORE_DORMANT_THRESHOLD_DAYS) are polled less
# often to conserve Helius credits.
LORE_POLL_INTERVAL_ACTIVE_MIN  = 15
LORE_POLL_INTERVAL_DORMANT_MIN = 60
LORE_DORMANT_THRESHOLD_DAYS    = 14

# Per-wallet signature fetch limit — small to stay light. Phase 2
# (trade reconstruction) may need to bump this if a single 15-min
# window produces more than 10 sigs for an active wallet.
LORE_RECENT_SIG_LIMIT = 10

# Outbound Helius RPC pacing. Phase 1 smoke against the shared
# SOLANA_RPC endpoint at concurrency=5 produced a 429 storm on 77/117
# wallets — Helius enforces a per-second rate cap on top of the
# monthly credit limit. Fix: dedicated HELIUS_API_KEY endpoint for
# isolation (see main()) PLUS strictly sequential calls with a small
# inter-call gap as defence-in-depth.
#
# 117 wallets × (~100ms call + 150ms gap) ≈ 30s per tick, well
# inside the 15min interval.
LORE_RPC_GAP_SEC = 0.15

# Quote currencies — treated like SOL for cost-basis purposes when
# phase 2 reconstructs trades. SPL "out" of these = funding a buy;
# SPL "in" of these = realising a sell into stables.
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {SOL_MINT, USDC_MINT, USDT_MINT}

# Helius credit warning — ~800k credits/month ÷ 30 days. Mirrors the
# whale_sniper threshold; the lore tracker counter is independent so
# both processes can watch their own budget.
HELIUS_DAILY_WARN_LIMIT = 26_000


# --- Helpers (duplicated from whale_sniper.py for crash-isolation) ---
# These mirror the whale_sniper logic but live in this module so the
# lore tracker PM2 process imports nothing from whale_sniper. See the
# phase 1 design note: importing whale_sniper would attach a duplicate
# file handler to whale_sniper.log and pull in unrelated module-level
# state.

def _load_chat_ids() -> list[str]:
    """Load all Telegram chat IDs for alert broadcast from env.

    Includes TELEGRAM_CHAT_ID, TELEGRAM_CHAT_IDS (comma-separated),
    and TELEGRAM_CHAT_ID_2 — matches whale_sniper's loader so both
    users receive lore alerts on the same channels as trade alerts.
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


def _send_telegram_to(token: str, chat_ids: list[str], message: str) -> bool:
    """Inner send helper — POST to one bot's sendMessage endpoint for
    each chat_id in chat_ids. Returns True if any delivery succeeded.
    Logs errors per chat without raising.
    """
    if not token or not chat_ids:
        logger.error("send_telegram: token or chat IDs not set — alert dropped")
        return False
    any_ok = False
    for chat_id in chat_ids:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5,
            )
            resp.raise_for_status()
            any_ok = True
        except requests.exceptions.HTTPError as e:
            logger.error(f"Telegram HTTP {e.response.status_code} for {chat_id}: {e.response.text[:200]}")
        except Exception as e:
            logger.error(f"Telegram send failed for {chat_id}: {e}")
    return any_ok


def send_telegram(message: str) -> bool:
    """Broadcast a message via the PRIMARY bot (TELEGRAM_BOT_TOKEN)
    to all authorised chat IDs. Used by scheduled reports (daily
    top-3, 3-day Claude analysis) so they land on the same channels
    as whale_sniper's existing alerts.
    """
    return _send_telegram_to(
        os.getenv("TELEGRAM_BOT_TOKEN", ""),
        _telegram_chat_ids,
        message,
    )


def send_telegram_lore_reply(chat_id: str, message: str) -> bool:
    """Reply via the SECONDARY bot (TELEGRAM_BOT_TOKEN_LORE) to a
    specific chat ID — used by /lore command replies so the response
    comes back through the same bot the user typed into. Falls
    silently to False if the lore bot token is not configured.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN_LORE", "").strip()
    if not token:
        logger.error("send_telegram_lore_reply: TELEGRAM_BOT_TOKEN_LORE not set — reply dropped")
        return False
    return _send_telegram_to(token, [str(chat_id)], message)


# Helius call tracker — independent counter for the lore tracker
# process so we can see its credit consumption separately from
# whale_sniper's. Logged hourly at INFO level + Telegram warning at
# the daily threshold.
_helius_calls: int = 0
_helius_day_start: float = time.time()
_helius_last_hourly_log: float = time.time()
_helius_warned_today: bool = False


def _track_helius_call() -> None:
    """Increment the Helius call counter. Reset daily; warn once per
    day when crossing HELIUS_DAILY_WARN_LIMIT.
    """
    global _helius_calls, _helius_day_start, _helius_warned_today
    now = time.time()
    if now - _helius_day_start >= 86_400:
        logger.info(f"[HELIUS] Daily counter reset (was {_helius_calls:,})")
        _helius_calls = 0
        _helius_day_start = now
        _helius_warned_today = False
    _helius_calls += 1
    if _helius_calls >= HELIUS_DAILY_WARN_LIMIT and not _helius_warned_today:
        _helius_warned_today = True
        msg = (
            f"⚠️ <b>APEX lore_tracker</b> — Helius daily limit reached\n"
            f"Made {_helius_calls:,} RPC calls today (~800k credits/month threshold).\n"
            f"Consider reducing scan frequency or extending dormant detection."
        )
        logger.warning(f"Helius daily call limit hit: {_helius_calls:,}")
        send_telegram(msg)


def _maybe_log_hourly_helius_stats() -> None:
    """Emit an INFO line with the running Helius call total once per
    hour so we can watch credit consumption during dry-run.
    """
    global _helius_last_hourly_log
    now = time.time()
    if now - _helius_last_hourly_log >= 3_600:
        hours_into_day = (now - _helius_day_start) / 3_600 or 0.001
        rate_per_hour = _helius_calls / hours_into_day
        logger.info(
            f"[HELIUS] Running total: {_helius_calls:,} calls today "
            f"(~{rate_per_hour:.0f}/hr, threshold {HELIUS_DAILY_WARN_LIMIT:,}/day)"
        )
        _helius_last_hourly_log = now


async def _arpc_post(
    session: aiohttp.ClientSession,
    rpc_url: str,
    method: str,
    params: list | dict,
) -> dict:
    """Async JSON-RPC POST with Helius call tracking. Raises on HTTP
    errors. Mirrors whale_sniper's helper but uses this module's
    independent counter.
    """
    _track_helius_call()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(
        rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
    ) as resp:
        resp.raise_for_status()
        return await resp.json()


# --- Lore wallet loading ----------------------------------------------

def _load_lore_wallets() -> dict[str, dict]:
    """Load the wallet roster from state/lore_wallets.json.

    Expected schema per entry:
      {"<address>": {"name": "...", "emoji": "...", "group": "..."}}

    Returns the parsed dict. Raises if the file is missing or malformed
    — lore tracker cannot function without its roster, and silent
    fail-open would mask a deployment mistake.
    """
    with open(LORE_WALLETS_FILE, "r") as f:
        data = json.load(f)
    if not isinstance(data, dict) or not data:
        raise ValueError(f"{LORE_WALLETS_FILE} is empty or not a JSON object")
    return data


def _atomic_write_json(path: str, data) -> None:
    """Write JSON to `path` atomically: write to .tmp, fsync, then
    os.replace. Mirrors whale_sniper's pattern for state files so a
    crash mid-write can never leave a half-written JSON on disk.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_json_or(path: str, default):
    """Load JSON from `path` or return `default` if missing/empty.
    Logs a WARNING on parse error but returns `default` rather than
    raising — phase 2 state files are derived and recoverable from
    on-chain data; an unparseable file shouldn't kill the tracker.
    """
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to load {path}: {type(e).__name__}: {e} — using default")
        return default


def _wallet_log_tag(addr: str, meta: dict) -> str:
    """Build a grep-friendly tag for log lines.
    Format: '[group/name addr8]' or '[? addr8]' for entries with no
    group/name metadata.
    """
    group = (meta or {}).get("group") or "?"
    name  = (meta or {}).get("name")  or "?"
    return f"[{group}/{name} {addr[:8]}]"


# --- Trade reconstruction ---------------------------------------------
# Classifies a Helius getTransaction response into a buy or sell from
# the lore wallet's perspective using only on-chain pre/post balances
# (no protocol-specific decoding). PnL = (sell SOL value - cost basis
# fraction) where SOL value is the wallet's native SOL delta (with fee
# added back if the wallet was fee payer) plus any USDC/USDT delta
# normalised via a cached Jupiter spot quote.

_SOL_USD_RATE_CACHE = {"rate": 0.0, "fetched_at": 0.0}


async def _get_sol_usd_rate(session: aiohttp.ClientSession) -> float:
    """Return current SOL/USD rate via Jupiter spot quote. Cached
    LORE_SOL_USD_CACHE_SEC. Falls back to last known rate (or 150.0)
    on any error so trade reconstruction never blocks on a price
    fetch failure.
    """
    now = time.time()
    if (
        _SOL_USD_RATE_CACHE["rate"] > 0
        and now - _SOL_USD_RATE_CACHE["fetched_at"] < LORE_SOL_USD_CACHE_SEC
    ):
        return _SOL_USD_RATE_CACHE["rate"]
    try:
        url = "https://lite-api.jup.ag/swap/v1/quote"
        params = {
            "inputMint":   SOL_MINT,
            "outputMint":  USDC_MINT,
            "amount":      str(LAMPORTS_PER_SOL),   # 1 SOL
            "slippageBps": "50",
        }
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        # USDC has 6 decimals — outAmount is in micro-USDC
        rate = int(data.get("outAmount", 0)) / 1_000_000
        if rate > 0:
            _SOL_USD_RATE_CACHE["rate"] = rate
            _SOL_USD_RATE_CACHE["fetched_at"] = now
            return rate
    except Exception as e:
        logger.warning(f"SOL/USD rate fetch failed: {type(e).__name__}: {e}")
    return _SOL_USD_RATE_CACHE["rate"] or 150.0


async def _get_transaction(
    session: aiohttp.ClientSession,
    rpc_url: str,
    sig: str,
) -> dict | None:
    """Fetch a parsed transaction by signature. Returns None on any
    RPC error so the caller can skip and move on.
    """
    try:
        result = await _arpc_post(
            session, rpc_url, "getTransaction",
            [sig, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }],
        )
        return result.get("result")
    except aiohttp.ClientResponseError as e:
        if e.status == 429:
            logger.warning(f"[TX {sig[:16]}] Helius 429 — skipping this tick")
        else:
            logger.warning(f"[TX {sig[:16]}] getTransaction HTTP {e.status}: {e.message}")
        return None
    except Exception as e:
        logger.warning(f"[TX {sig[:16]}] getTransaction failed: {type(e).__name__}: {e}")
        return None


def _wallet_token_deltas(tx: dict, wallet_addr: str) -> dict[str, float]:
    """Return {mint: ui_amount_delta} for SPL tokens owned by the
    wallet across this txn. Positive = received, negative = sent.
    Tokens with zero net delta are excluded.

    Handles the case where a token account appears in only one of
    pre/postTokenBalances (a newly opened or fully closed token
    account) by treating the missing side as 0.
    """
    meta = tx.get("meta") or {}
    pre  = meta.get("preTokenBalances")  or []
    post = meta.get("postTokenBalances") or []

    # Build {(account_index, mint): ui_amount} for each side, scoped
    # to entries owned by the wallet only.
    def index(entries):
        out: dict[tuple[int, str], float] = {}
        for e in entries:
            if e.get("owner") != wallet_addr:
                continue
            mint = e.get("mint")
            if not mint:
                continue
            amt = (e.get("uiTokenAmount") or {}).get("uiAmount")
            # uiAmount can be None when the account holds zero
            out[(e.get("accountIndex"), mint)] = float(amt or 0.0)
        return out

    pre_idx  = index(pre)
    post_idx = index(post)

    # Sum deltas per mint across all the wallet's token accounts
    # for that mint (most wallets have one, but be safe).
    deltas: dict[str, float] = {}
    all_keys = set(pre_idx) | set(post_idx)
    for key in all_keys:
        _, mint = key
        delta = post_idx.get(key, 0.0) - pre_idx.get(key, 0.0)
        if delta == 0.0:
            continue
        deltas[mint] = deltas.get(mint, 0.0) + delta

    # Strip mints that net out to zero after summing
    return {m: d for m, d in deltas.items() if d != 0.0}


def _wallet_sol_delta(tx: dict, wallet_addr: str) -> float:
    """Return the wallet's native SOL delta in SOL units (positive =
    received). Adds the txn fee back if the wallet was the fee payer
    (accountKeys index 0) so the trade-side SOL movement isn't
    distorted by the priority fee.
    """
    meta = tx.get("meta") or {}
    pre_balances  = meta.get("preBalances")  or []
    post_balances = meta.get("postBalances") or []
    fee_lamports  = int(meta.get("fee") or 0)
    keys = (((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or [])

    # accountKeys entries with jsonParsed encoding are dicts with
    # "pubkey" — handle both dict and bare-string fallbacks.
    def keystr(k):
        if isinstance(k, dict):
            return k.get("pubkey")
        return k

    wallet_idx = None
    for i, k in enumerate(keys):
        if keystr(k) == wallet_addr:
            wallet_idx = i
            break
    if wallet_idx is None:
        return 0.0
    if wallet_idx >= len(pre_balances) or wallet_idx >= len(post_balances):
        return 0.0

    lamport_delta = post_balances[wallet_idx] - pre_balances[wallet_idx]
    # Fee payer is accountKeys[0]. If the wallet paid the fee, add it
    # back so the SOL delta reflects only the swap-side movement.
    if wallet_idx == 0:
        lamport_delta += fee_lamports
    return lamport_delta / LAMPORTS_PER_SOL


def _extract_trade(
    tx: dict,
    wallet_addr: str,
    sig: str,
    sol_usd_rate: float,
) -> dict | None:
    """Classify a successful txn as a BUY or SELL of one non-quote
    token from the wallet's perspective. Returns a trade dict or
    None if the txn is not a recognisable single-token swap.

    Logic:
      1. Compute wallet's SOL delta and USDC+USDT deltas (the
         "quote" side, normalised to SOL).
      2. Compute per-mint deltas for non-quote SPL tokens.
      3. If exactly one non-quote token moved AND its sign opposes
         the quote-side movement, classify as BUY (token +, quote -)
         or SELL (token -, quote +). Multi-token swaps or
         same-direction movements skip silently.
    """
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return None   # failed txn — didn't actually move funds

    sol_delta = _wallet_sol_delta(tx, wallet_addr)
    tok_deltas = _wallet_token_deltas(tx, wallet_addr)

    # Split quote-currency deltas (USDC/USDT) from non-quote
    quote_sol_value = sol_delta
    if USDC_MINT in tok_deltas:
        usdc_delta_sol = tok_deltas.pop(USDC_MINT) / sol_usd_rate
        quote_sol_value += usdc_delta_sol
    if USDT_MINT in tok_deltas:
        usdt_delta_sol = tok_deltas.pop(USDT_MINT) / sol_usd_rate
        quote_sol_value += usdt_delta_sol

    # Filter out tokens whose net delta is essentially zero
    tok_deltas = {m: d for m, d in tok_deltas.items() if abs(d) > 1e-9}

    if len(tok_deltas) == 0:
        return None   # no SPL token movement — pure transfer or SOL move
    if len(tok_deltas) > 1:
        # Token-to-token rotation or multi-token movement — beyond
        # the scope of SOL-balance-delta PnL. Skip with debug.
        logger.debug(
            f"[TX {sig[:16]}] multi-token movement ({len(tok_deltas)} mints), skipping"
        )
        return None

    token_mint, token_delta = next(iter(tok_deltas.items()))
    blocktime = float(tx.get("blockTime") or time.time())

    # BUY: token in (+), quote out (-)
    if token_delta > 0 and quote_sol_value < 0:
        return {
            "kind":        "buy",
            "wallet":      wallet_addr,
            "token_mint":  token_mint,
            "token_units": token_delta,
            "sol_value":   abs(quote_sol_value),
            "timestamp":   blocktime,
            "signature":   sig,
        }
    # SELL: token out (-), quote in (+)
    if token_delta < 0 and quote_sol_value > 0:
        return {
            "kind":        "sell",
            "wallet":      wallet_addr,
            "token_mint":  token_mint,
            "token_units": abs(token_delta),
            "sol_value":   quote_sol_value,
            "timestamp":   blocktime,
            "signature":   sig,
        }
    # Same-direction or zero movement — ambiguous. Common when the
    # txn was a deposit / withdrawal / liquidity provision rather
    # than a trade.
    return None


def _apply_trade(
    trade: dict,
    open_positions: dict,
    closed_trades: list,
) -> dict | None:
    """Mutate open_positions and closed_trades in place to reflect
    the trade. Buys add to a position (averaging cost basis);
    sells realise PnL on the proportional cost basis and append a
    record to closed_trades.

    Returns:
      - For BUYS: the open-position dict that was updated (no
        closed-trades record is produced).
      - For SELLS that landed: the closed-trade record just
        appended to closed_trades.
      - For SELLS skipped because we have no recorded buy
        (pre-baseline history): None.

    The return value lets the caller distinguish landed sells from
    skipped ones for accurate per-sig logging without re-walking
    closed_trades[-1] (which previously caused a stale-PnL bug
    when a sell was skipped).
    """
    wallet = trade["wallet"]
    mint   = trade["token_mint"]

    if trade["kind"] == "buy":
        wpos = open_positions.setdefault(wallet, {})
        existing = wpos.get(mint)
        if existing:
            existing["cost_basis_sol"] = round(
                float(existing.get("cost_basis_sol") or 0) + trade["sol_value"], 9
            )
            existing["tokens_held"] = round(
                float(existing.get("tokens_held") or 0) + trade["token_units"], 9
            )
            existing["last_buy_at"]  = trade["timestamp"]
            existing["buys_count"]   = int(existing.get("buys_count") or 1) + 1
            return existing
        new_pos = {
            "cost_basis_sol":   round(trade["sol_value"], 9),
            "tokens_held":      round(trade["token_units"], 9),
            "tokens_at_open":   round(trade["token_units"], 9),
            "opened_at":        trade["timestamp"],
            "last_buy_at":      trade["timestamp"],
            "buys_count":       1,
        }
        wpos[mint] = new_pos
        return new_pos

    # trade["kind"] == "sell" — look up without creating an entry,
    # so a skipped sell doesn't leave an empty wallet dict behind in
    # open_positions (which would pollute state file writes).
    wpos = open_positions.get(wallet) or {}
    pos = wpos.get(mint)
    if not pos or float(pos.get("tokens_held") or 0) <= 0:
        logger.debug(
            f"[TRADE] {wallet[:8]} sold {mint[:8]} with no recorded buy — "
            f"skipping (pre-baseline history)"
        )
        return None

    tokens_held    = float(pos["tokens_held"])
    cost_basis_sol = float(pos["cost_basis_sol"])
    tokens_sold    = min(trade["token_units"], tokens_held)
    fraction_sold  = tokens_sold / tokens_held if tokens_held > 0 else 0.0
    realised_cost  = cost_basis_sol * fraction_sold
    realised_pnl   = trade["sol_value"] - realised_cost
    realised_pct   = (realised_pnl / realised_cost * 100) if realised_cost > 0 else 0.0
    hold_minutes   = max(
        0.0, (trade["timestamp"] - float(pos.get("opened_at") or trade["timestamp"])) / 60.0
    )

    record = {
        "wallet":         wallet,
        "token_mint":     mint,
        "entry_time":     float(pos.get("opened_at") or trade["timestamp"]),
        "exit_time":      trade["timestamp"],
        "entry_sol":      round(realised_cost, 9),
        "exit_sol":       round(trade["sol_value"], 9),
        "pnl_sol":        round(realised_pnl, 9),
        "pnl_pct":        round(realised_pct, 4),
        "hold_minutes":   round(hold_minutes, 2),
        "fraction_sold":  round(fraction_sold, 6),
        "signature":      trade["signature"],
    }
    closed_trades.append(record)

    # Reduce the position
    new_tokens = tokens_held - tokens_sold
    new_basis  = cost_basis_sol - realised_cost
    tokens_at_open = float(pos.get("tokens_at_open") or pos.get("tokens_held") or 1.0)
    if new_tokens <= tokens_at_open * LORE_DUST_FRACTION:
        # Position fully closed (or rounded down to dust)
        del wpos[mint]
        if not wpos:
            del open_positions[wallet]
    else:
        pos["tokens_held"]    = round(new_tokens, 9)
        pos["cost_basis_sol"] = round(max(new_basis, 0.0), 9)

    return record


def _prune_closed_trades(closed_trades: list) -> int:
    """Remove entries older than LORE_METRICS_WINDOW_DAYS. Returns
    the number of entries dropped. Called once per tick before write.
    """
    cutoff = time.time() - LORE_METRICS_WINDOW_DAYS * 86_400
    before = len(closed_trades)
    closed_trades[:] = [t for t in closed_trades if float(t.get("exit_time") or 0) >= cutoff]
    return before - len(closed_trades)


def _recompute_wallet_metrics(
    wallet: str,
    metrics: dict,
    closed_trades: list,
) -> None:
    """Refresh derived rolling-window metrics for one wallet from
    the closed_trades log. Preserves last_processed_sig and any
    other per-wallet bookkeeping fields already in metrics.
    """
    wallet_trades = [t for t in closed_trades if t.get("wallet") == wallet]
    trade_count   = len(wallet_trades)
    win_count     = sum(1 for t in wallet_trades if float(t.get("pnl_sol") or 0) > 0)
    pnl_sum       = sum(float(t.get("pnl_sol") or 0) for t in wallet_trades)
    hold_sum      = sum(float(t.get("hold_minutes") or 0) for t in wallet_trades)

    entry = metrics.setdefault(wallet, {})
    entry.update({
        "trade_count_30d":  trade_count,
        "win_count_30d":    win_count,
        "win_rate_30d":     round(win_count / trade_count, 4) if trade_count else 0.0,
        "total_pnl_sol_30d": round(pnl_sum, 9),
        "avg_hold_min":     round(hold_sum / trade_count, 2) if trade_count else 0.0,
        # last_activity_ts is updated on each processed trade in
        # _poll_one_tick; if we have closed trades, max(exit_time) is
        # a safe lower bound when buys-without-sells haven't refreshed it.
        "last_activity_ts": max(
            float(entry.get("last_activity_ts") or 0),
            max((float(t.get("exit_time") or 0) for t in wallet_trades), default=0.0),
        ),
    })


# --- Group aggregation + top-3 reporting ------------------------------
# Group field in lore_wallets.json identifies the trader behind a set
# of addresses. Multiple wallets per group is the norm (e.g. mannos
# has "mannos" + "mannos2"). The top-3 ranking uses summed group PnL
# over the last 24h. Overall 30-day group metrics are also exposed
# via _aggregate_group_metrics for future /lore on-demand use.

def _wallets_by_group(wallets: dict[str, dict]) -> dict[str, list[str]]:
    """Return {group_name: [addr, ...]} from the wallet roster.
    Wallets with missing/empty group fall under '?'.
    """
    by_group: dict[str, list[str]] = {}
    for addr, meta in wallets.items():
        group = ((meta or {}).get("group") or "?").strip() or "?"
        by_group.setdefault(group, []).append(addr)
    return by_group


def _group_emoji(group: str, wallets: dict[str, dict]) -> str:
    """Pick a representative emoji for a group — first non-empty
    emoji across the group's members in wallet roster order. Returns
    '' if no member has one.
    """
    for addr, meta in wallets.items():
        m = meta or {}
        if (m.get("group") or "").strip() == group:
            emoji = (m.get("emoji") or "").strip()
            if emoji:
                return emoji
    return ""


def _aggregate_group_metrics(
    wallets: dict[str, dict],
    metrics: dict,
) -> dict[str, dict]:
    """Combine per-wallet rolling-30-day metrics into per-group totals.

    Returns {group_name: {
        members: [addr, ...],
        trade_count: int,
        win_count:   int,
        win_rate:    float,           # 0..1
        pnl_sol:     float,
        last_activity_ts: float,
        active_member_count: int,     # members with last_activity_ts >= 14d cutoff
        dormant_member_count: int,
        is_active_group: bool,        # True if at least one active member
    }}.

    Wallets with no metrics entry contribute zeros to counts and PnL
    but still count toward member rosters.
    """
    cutoff = time.time() - LORE_DORMANT_THRESHOLD_DAYS * 86_400
    by_group = _wallets_by_group(wallets)
    out: dict[str, dict] = {}
    for group, members in by_group.items():
        trade_count = 0
        win_count   = 0
        pnl_sum     = 0.0
        last_ts     = 0.0
        active_n    = 0
        dormant_n   = 0
        for addr in members:
            entry = metrics.get(addr) or {}
            trade_count += int(entry.get("trade_count_30d") or 0)
            win_count   += int(entry.get("win_count_30d")   or 0)
            pnl_sum     += float(entry.get("total_pnl_sol_30d") or 0.0)
            ts = float(entry.get("last_activity_ts") or 0)
            if ts > last_ts:
                last_ts = ts
            # "Active" only if we've recorded a last_activity_ts and
            # it's within the dormant cutoff. Wallets with no
            # recorded activity yet count as neither — too early to
            # tell.
            if ts > 0:
                if ts >= cutoff:
                    active_n += 1
                else:
                    dormant_n += 1
        out[group] = {
            "members":              members,
            "trade_count":          trade_count,
            "win_count":            win_count,
            "win_rate":             (win_count / trade_count) if trade_count else 0.0,
            "pnl_sol":              round(pnl_sum, 9),
            "last_activity_ts":     last_ts,
            "active_member_count":  active_n,
            "dormant_member_count": dormant_n,
            "is_active_group":      active_n > 0,
        }
    return out


def _top3_pnl_24h(
    wallets: dict[str, dict],
    closed_trades: list,
) -> list[dict]:
    """Compute the top-3 groups by realised PnL over the last 24h.

    Filters closed_trades to those whose exit_time is within the
    last 86,400 seconds, buckets them by group (looked up from
    wallets[addr].group), sums pnl_sol / trade_count / win_count
    per group, and returns the top 3 sorted by pnl_sol descending.

    Returns fewer than 3 entries when fewer groups had qualifying
    activity. Returns an empty list when no group did.
    """
    now    = time.time()
    cutoff = now - 86_400
    addr_to_group: dict[str, str] = {}
    for addr, meta in wallets.items():
        addr_to_group[addr] = ((meta or {}).get("group") or "?").strip() or "?"

    groups: dict[str, dict] = {}
    for t in closed_trades:
        exit_t = float(t.get("exit_time") or 0)
        if exit_t < cutoff:
            continue
        wallet = t.get("wallet") or ""
        group  = addr_to_group.get(wallet, "?")
        g = groups.setdefault(group, {
            "group":       group,
            "trade_count": 0,
            "win_count":   0,
            "pnl_sol":     0.0,
        })
        g["trade_count"] += 1
        pnl = float(t.get("pnl_sol") or 0)
        if pnl > 0:
            g["win_count"] += 1
        g["pnl_sol"] += pnl

    for g in groups.values():
        g["pnl_sol"]  = round(g["pnl_sol"], 9)
        g["win_rate"] = (g["win_count"] / g["trade_count"]) if g["trade_count"] else 0.0
        g["emoji"]    = _group_emoji(g["group"], wallets)

    return sorted(groups.values(), key=lambda x: x["pnl_sol"], reverse=True)[:3]


def _format_top3_message(top3: list[dict]) -> str:
    """Render the top-3 Telegram body. Returns the empty-activity
    message verbatim when top3 is [].
    """
    now_perth = datetime.now(PERTH_TZ)
    timestamp_str = now_perth.strftime("%Y-%m-%d %H:%M Perth")

    if not top3:
        return (
            f"🏆 <b>LORE TOP 3 — Last 24h</b>\n\n"
            f"No qualifying activity in last 24h.\n\n"
            f"Updated: {timestamp_str}"
        )

    lines = ["🏆 <b>LORE TOP 3 — Last 24h</b>", ""]
    for i, g in enumerate(top3, 1):
        sign       = "+" if g["pnl_sol"] >= 0 else ""
        emoji_part = f" {g['emoji']}" if g.get("emoji") else ""
        wr_pct     = int(round(g["win_rate"] * 100))
        trade_word = "trade" if g["trade_count"] == 1 else "trades"
        lines.append(
            f"{i}. {g['group']}{emoji_part}  "
            f"{sign}{g['pnl_sol']:.3f} SOL  "
            f"({g['trade_count']} {trade_word}, {wr_pct}% WR)"
        )
    lines.append("")
    lines.append(f"Updated: {timestamp_str}")
    return "\n".join(lines)


def _compose_top3_now(wallets: dict[str, dict]) -> str:
    """Compose-only path: load closed trades from disk, compute top-3,
    return the rendered string without sending. Used by --send-top3-now
    (after sending) and could be used by phase 4's /lore handler.
    """
    closed_trades = _load_json_or(LORE_CLOSED_TRADES_FILE, [])
    if not isinstance(closed_trades, list):
        closed_trades = []
    top3 = _top3_pnl_24h(wallets, closed_trades)
    return _format_top3_message(top3), top3


async def _send_daily_top3(wallets: dict[str, dict]) -> int:
    """Compute the current top-3 and send it to all authorised
    Telegram chat IDs. Returns the number of groups in the top-3
    (0..3) so callers can log whether anything qualified.
    """
    msg, top3 = _compose_top3_now(wallets)
    logger.info(
        f"[TOP3] sending daily top-3 — {len(top3)} group(s) with 24h activity"
    )
    # send_telegram is sync requests-based (~5s timeout); a brief
    # block on the event loop is acceptable and matches whale_sniper's
    # pattern. Same applies to scheduled and on-demand paths.
    send_telegram(msg)
    return len(top3)


async def _top3_scheduler_loop(wallets: dict[str, dict]) -> None:
    """Fire _send_daily_top3 once per day at or after 09:00 Perth.
    Idempotent across restarts via last_top3_sent_date persisted to
    LORE_SCHEDULER_STATE_FILE.
    """
    sched_state = _load_json_or(LORE_SCHEDULER_STATE_FILE, {})
    if not isinstance(sched_state, dict):
        sched_state = {}
    logger.info(
        f"[TOP3] scheduler started — daily 09:00 Perth, "
        f"last sent: {sched_state.get('last_top3_sent_date') or 'never'}"
    )

    while True:
        try:
            now_perth     = datetime.now(PERTH_TZ)
            today_str     = now_perth.date().isoformat()
            last_sent     = sched_state.get("last_top3_sent_date")
            past_send_hr  = now_perth.hour >= LORE_TOP3_PERTH_HOUR
            already_sent  = (last_sent == today_str)

            if past_send_hr and not already_sent:
                logger.info(
                    f"[TOP3] firing daily send — Perth time "
                    f"{now_perth.strftime('%H:%M')} >= {LORE_TOP3_PERTH_HOUR:02d}:00, "
                    f"not yet sent today ({today_str})"
                )
                try:
                    await _send_daily_top3(wallets)
                except Exception as e:
                    logger.error(
                        f"[TOP3] send failed: {type(e).__name__}: {e}",
                        exc_info=True,
                    )
                    # Don't mark as sent — try again next minute.
                else:
                    sched_state["last_top3_sent_date"] = today_str
                    try:
                        _atomic_write_json(LORE_SCHEDULER_STATE_FILE, sched_state)
                    except OSError as e:
                        logger.error(f"[TOP3] scheduler state write failed: {e}")
        except Exception as e:
            logger.error(
                f"[TOP3] scheduler tick failed: {type(e).__name__}: {e}",
                exc_info=True,
            )
        await asyncio.sleep(60)


# --- 3-day Claude analysis --------------------------------------------
# Mon/Thu/Sun 9am Perth: aggregate the last 3 days of group activity,
# send to Claude (haiku 4.5) with a strict-JSON prompt requesting
# PROMOTE/WATCH/DEMOTE recommendations, parse, render to Telegram.

def _aggregate_3day_group_data(
    wallets: dict[str, dict],
    metrics: dict,
    closed_trades: list,
) -> dict:
    """Build the per-group 3-day rollup that gets sent to Claude.
    Only groups with at least one trade in the window appear in the
    output — Claude shouldn't waste tokens analysing inactive groups.
    """
    now    = time.time()
    cutoff = now - LORE_ANALYSIS_WINDOW_DAYS * 86_400
    addr_to_group: dict[str, str] = {}
    for addr, meta in wallets.items():
        addr_to_group[addr] = ((meta or {}).get("group") or "?").strip() or "?"

    # Bucket recent trades by group + accumulate stats
    g_stats: dict[str, dict] = {}
    for t in closed_trades:
        exit_t = float(t.get("exit_time") or 0)
        if exit_t < cutoff:
            continue
        wallet = t.get("wallet") or ""
        group  = addr_to_group.get(wallet, "?")
        g = g_stats.setdefault(group, {
            "trades_3d":          0,
            "wins_3d":            0,
            "total_pnl_sol_3d":   0.0,
            "hold_min_sum":       0.0,
            "last_activity_ts":   0.0,
        })
        g["trades_3d"]        += 1
        pnl = float(t.get("pnl_sol") or 0)
        if pnl > 0:
            g["wins_3d"] += 1
        g["total_pnl_sol_3d"] += pnl
        g["hold_min_sum"]     += float(t.get("hold_minutes") or 0)
        if exit_t > g["last_activity_ts"]:
            g["last_activity_ts"] = exit_t

    # Cross-reference with the wallet roster for member counts +
    # active/dormant breakdowns + representative emoji
    by_group  = _wallets_by_group(wallets)
    dormant_cutoff = now - LORE_DORMANT_THRESHOLD_DAYS * 86_400

    groups_out = []
    for group, stats in g_stats.items():
        members      = by_group.get(group, [])
        active_n     = 0
        dormant_n    = 0
        for addr in members:
            ts = float((metrics.get(addr) or {}).get("last_activity_ts") or 0)
            if ts > 0:
                if ts >= dormant_cutoff:
                    active_n += 1
                else:
                    dormant_n += 1
        last_act_iso = ""
        if stats["last_activity_ts"]:
            last_act_iso = datetime.fromtimestamp(
                stats["last_activity_ts"], PERTH_TZ
            ).strftime("%Y-%m-%d %H:%M Perth")

        win_rate = (stats["wins_3d"] / stats["trades_3d"]) if stats["trades_3d"] else 0.0
        avg_hold = (stats["hold_min_sum"] / stats["trades_3d"]) if stats["trades_3d"] else 0.0
        groups_out.append({
            "group":                group,
            "emoji":                _group_emoji(group, wallets),
            "member_count":         len(members),
            "active_members":       active_n,
            "dormant_members":      dormant_n,
            "trades_3d":            stats["trades_3d"],
            "wins_3d":              stats["wins_3d"],
            "win_rate_3d":          round(win_rate, 4),
            "total_pnl_sol_3d":     round(stats["total_pnl_sol_3d"], 6),
            "avg_hold_min":         round(avg_hold, 2),
            "last_activity_perth":  last_act_iso,
        })

    # Most-traded group first so Claude sees high-signal items at top
    groups_out.sort(key=lambda g: g["trades_3d"], reverse=True)

    now_perth = datetime.now(PERTH_TZ).strftime("%Y-%m-%d %H:%M Perth")
    return {
        "window_days":  LORE_ANALYSIS_WINDOW_DAYS,
        "as_of_perth":  now_perth,
        "groups":       groups_out,
    }


_LORE_ANALYSIS_SCHEMA_DESCRIPTION = (
    "Output schema (STRICT JSON — no markdown, no code fences, no preamble, "
    "no trailing prose):\n"
    "{\n"
    '  "summary": string,  // 2-3 sentence overview of the past 3 days\n'
    '  "promote": [\n'
    '    {"group": string, "reasoning": string}  // 2-3 sentence reasoning\n'
    "  ],\n"
    '  "watch": [\n'
    '    {"group": string, "reasoning": string}\n'
    "  ],\n"
    '  "demote": [\n'
    '    {"group": string, "reasoning": string}\n'
    "  ]\n"
    "}\n"
    "\n"
    "Rules:\n"
    "- promote: traders with strong consistency + positive total_pnl_sol_3d\n"
    "- watch: declining win rate, mixed signals, or fading activity\n"
    "- demote: negative PnL, very low win rate, or unrecoverable losses\n"
    "- Only include groups that appear in the input data\n"
    "- Empty arrays are valid when no candidates fit a category\n"
    "- Each group name must match exactly the 'group' field in the input\n"
)


def _build_lore_analysis_prompt(data: dict) -> tuple[str, str]:
    """Return (system_prompt, user_prompt) for the 3-day analysis.
    Kept separate so tests can assert prompt structure without
    invoking the API.
    """
    system_prompt = (
        "You are a Solana trading performance analyst reviewing 3-day "
        "metrics for trader groups (each group = one trader operating "
        "one or more wallets). You output ONLY valid JSON matching the "
        "user-supplied schema, with no preamble, no code fences, and no "
        "trailing prose. Reasoning is concrete and references the "
        "numbers in the input data."
    )
    user_prompt = (
        f"Here is the 3-day group performance data ({data['window_days']} "
        f"days back from {data['as_of_perth']}). "
        f"{len(data['groups'])} groups had at least one closed trade in "
        f"the window.\n\n"
        f"DATA:\n{json.dumps(data, indent=2)}\n\n"
        f"{_LORE_ANALYSIS_SCHEMA_DESCRIPTION}\n"
        f"Respond with the JSON object only."
    )
    return system_prompt, user_prompt


def _parse_lore_analysis_response(raw: str) -> dict | None:
    """Robustly parse Claude's response. Strips code fences if the
    model returns ```json ... ``` despite instructions. Returns the
    parsed dict on success, None on parse failure (logged with the
    raw response for diagnosis).
    """
    text = (raw or "").strip()
    # Remove ```json prefix and ``` suffix if present
    if text.startswith("```"):
        # Find first newline after the opening fence
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
        if text.endswith("```"):
            text = text[: -3]
        text = text.strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(
            f"[CLAUDE] Failed to parse lore analysis JSON: {e} | "
            f"raw response: {raw[:500]!r}"
        )
        return None

    # Basic shape validation — missing keys default to empty containers
    if not isinstance(parsed, dict):
        logger.error(f"[CLAUDE] Parsed JSON is not an object: {type(parsed).__name__}")
        return None
    return {
        "summary": str(parsed.get("summary") or "").strip(),
        "promote": parsed.get("promote") if isinstance(parsed.get("promote"), list) else [],
        "watch":   parsed.get("watch")   if isinstance(parsed.get("watch"),   list) else [],
        "demote":  parsed.get("demote")  if isinstance(parsed.get("demote"),  list) else [],
    }


def _format_lore_analysis_message(
    parsed: dict,
    wallets: dict[str, dict],
) -> str:
    """Render Claude's parsed response into the Telegram body per
    the user-spec format. Sections with empty arrays are omitted.
    Group emoji is looked up via _group_emoji so the message matches
    the daily top-3 visual style.
    """
    now_perth     = datetime.now(PERTH_TZ).strftime("%Y-%m-%d %H:%M Perth")
    lines = ["🧠 <b>LORE ANALYSIS — 3-day review</b>", ""]

    summary = (parsed.get("summary") or "").strip()
    if summary:
        lines.append(summary)
        lines.append("")

    def _section(emoji_label: str, items: list) -> None:
        if not items:
            return
        lines.append(emoji_label)
        for it in items:
            if not isinstance(it, dict):
                continue
            group     = (it.get("group") or "").strip()
            reasoning = (it.get("reasoning") or "").strip()
            if not group:
                continue
            emoji = _group_emoji(group, wallets)
            emoji_part = f" {emoji}" if emoji else ""
            lines.append(f"• {group}{emoji_part} — {reasoning}")
        lines.append("")

    _section("✅ <b>PROMOTE</b>", parsed.get("promote") or [])
    _section("👀 <b>WATCH</b>",   parsed.get("watch")   or [])
    _section("❌ <b>DEMOTE</b>",  parsed.get("demote")  or [])

    lines.append(f"Updated: {now_perth}")
    return "\n".join(lines).rstrip() + "\n"


async def _request_lore_analysis_from_claude(data: dict) -> dict | None:
    """Call Claude with the 3-day data, return the parsed
    recommendations dict, or None on any failure. Failures are
    logged with enough context to diagnose later without leaking
    the API key.
    """
    api_key = os.getenv("CLAUDE_API_KEY", "").strip()
    if not api_key:
        logger.error("CLAUDE_API_KEY not set in .env — lore analysis aborted")
        return None
    system_prompt, user_prompt = _build_lore_analysis_prompt(data)
    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model=LORE_CLAUDE_MODEL,
            max_tokens=LORE_CLAUDE_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        # Anthropic SDK returns a list of content blocks; concatenate
        # text-type blocks defensively.
        raw_parts = []
        for block in (resp.content or []):
            block_text = getattr(block, "text", None)
            if block_text:
                raw_parts.append(block_text)
        raw = "".join(raw_parts).strip()
        if not raw:
            logger.error("[CLAUDE] Empty response from lore analysis")
            return None
        logger.info(
            f"[CLAUDE] Lore analysis received — "
            f"{getattr(resp.usage, 'input_tokens', '?')} in / "
            f"{getattr(resp.usage, 'output_tokens', '?')} out tokens"
        )
        return _parse_lore_analysis_response(raw)
    except Exception as e:
        logger.error(f"[CLAUDE] Lore analysis call failed: {type(e).__name__}: {e}")
        return None


async def _send_lore_analysis(
    wallets: dict[str, dict],
    *,
    reply_chat_id: str | None = None,
) -> bool:
    """Orchestrate: aggregate 3-day data → call Claude → format →
    send. When reply_chat_id is provided, sends via the SECONDARY
    bot to that single chat (used by the /lore command). Otherwise
    broadcasts via the PRIMARY bot to all authorised chat IDs (used
    by the Mon/Thu/Sun scheduler).

    Returns True if a Telegram message was sent (whether the
    analysis itself succeeded or fell back to an error notification).
    Returns False only when even the error notification couldn't be
    delivered (no chat IDs / no token).
    """
    metrics       = _load_json_or(LORE_METRICS_FILE,       {})
    closed_trades = _load_json_or(LORE_CLOSED_TRADES_FILE, [])
    if not isinstance(metrics, dict):       metrics = {}
    if not isinstance(closed_trades, list): closed_trades = []

    data = _aggregate_3day_group_data(wallets, metrics, closed_trades)

    def _do_send(body: str) -> bool:
        if reply_chat_id:
            return send_telegram_lore_reply(reply_chat_id, body)
        return send_telegram(body)

    if not data["groups"]:
        body = (
            "🧠 <b>LORE ANALYSIS — 3-day review</b>\n\n"
            "No qualifying group activity in the last 3 days.\n\n"
            f"Updated: {data['as_of_perth']}"
        )
        logger.info("[CLAUDE] Skipping API call — no 3-day group activity")
        return _do_send(body)

    parsed = await _request_lore_analysis_from_claude(data)
    if parsed is None:
        body = (
            "🧠 <b>LORE ANALYSIS — 3-day review</b>\n\n"
            "⚠️ Analysis failed — see logs/lore_tracker.log for details.\n\n"
            f"Updated: {data['as_of_perth']}"
        )
        return _do_send(body)

    body = _format_lore_analysis_message(parsed, wallets)
    return _do_send(body)


async def _lore_analysis_scheduler_loop(wallets: dict[str, dict]) -> None:
    """Fire _send_lore_analysis on Mon/Thu/Sun at or after 09:00 Perth.
    Same restart-safe pattern as the daily top-3 scheduler — persists
    last_lore_analysis_date in lore_scheduler_state.json.
    """
    sched_state = _load_json_or(LORE_SCHEDULER_STATE_FILE, {})
    if not isinstance(sched_state, dict):
        sched_state = {}
    weekday_labels = {0: "Mon", 3: "Thu", 6: "Sun"}
    sched_days = ", ".join(weekday_labels[d] for d in sorted(LORE_ANALYSIS_WEEKDAYS))
    logger.info(
        f"[CLAUDE] Lore analysis scheduler started — {sched_days} at 09:00 Perth, "
        f"last sent: {sched_state.get('last_lore_analysis_date') or 'never'}"
    )

    while True:
        try:
            now_perth     = datetime.now(PERTH_TZ)
            today_str     = now_perth.date().isoformat()
            last_sent     = sched_state.get("last_lore_analysis_date")
            past_send_hr  = now_perth.hour >= LORE_ANALYSIS_PERTH_HOUR
            is_sched_day  = now_perth.weekday() in LORE_ANALYSIS_WEEKDAYS
            already_sent  = (last_sent == today_str)

            if is_sched_day and past_send_hr and not already_sent:
                logger.info(
                    f"[CLAUDE] Firing scheduled lore analysis — "
                    f"{now_perth.strftime('%a %H:%M')} Perth, last sent: {last_sent or 'never'}"
                )
                try:
                    await _send_lore_analysis(wallets)
                except Exception as e:
                    logger.error(
                        f"[CLAUDE] Scheduled analysis failed: {type(e).__name__}: {e}",
                        exc_info=True,
                    )
                else:
                    sched_state["last_lore_analysis_date"] = today_str
                    # Re-load + re-write so we don't trample on the top-3
                    # scheduler's last_top3_sent_date (both write the same
                    # file). Belt-and-braces against race conditions.
                    try:
                        on_disk = _load_json_or(LORE_SCHEDULER_STATE_FILE, {})
                        if isinstance(on_disk, dict):
                            on_disk["last_lore_analysis_date"] = today_str
                            _atomic_write_json(LORE_SCHEDULER_STATE_FILE, on_disk)
                            sched_state = on_disk
                    except OSError as e:
                        logger.error(f"[CLAUDE] Scheduler state write failed: {e}")
        except Exception as e:
            logger.error(
                f"[CLAUDE] Lore scheduler tick failed: {type(e).__name__}: {e}",
                exc_info=True,
            )
        await asyncio.sleep(60)


# --- /lore Telegram command listener (SECOND bot) ---------------------
# Polls TELEGRAM_BOT_TOKEN_LORE getUpdates and replies to /lore via the
# same bot. Runs on a completely separate Telegram bot to avoid the
# collision problem with whale_sniper's existing command listener
# documented in phase 4 design memo: two pollers on one bot would race
# for updates and silently drop ~50% of commands.

async def _lore_command_loop(wallets: dict[str, dict]) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN_LORE", "").strip()
    if not token:
        logger.error(
            "TELEGRAM_BOT_TOKEN_LORE not set — /lore command DISABLED. "
            "Scheduled reports + top-3 still active."
        )
        return  # task exits cleanly; the rest of the bot keeps running

    base_url       = f"https://api.telegram.org/bot{token}"
    last_update_id = 0
    fail_count     = 0
    authorised     = set(_telegram_chat_ids)
    logger.info(
        f"[LORE CMD] Listener started on secondary bot — authorised chats: "
        f"{sorted(authorised) if authorised else 'NONE (no chat IDs configured)'}"
    )

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params = {"timeout": 30, "offset": last_update_id + 1}
                async with session.get(
                    f"{base_url}/getUpdates",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=40),
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                fail_count = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fail_count += 1
                delay = min(5 * (2 ** (fail_count - 1)), 300)
                logger.warning(
                    f"[LORE CMD] getUpdates failed ({type(e).__name__}: {e!r}) — "
                    f"retry in {delay}s (attempt {fail_count})"
                )
                await asyncio.sleep(delay)
                continue

            if not data.get("ok"):
                logger.error(
                    f"[LORE CMD] getUpdates error: {data.get('description', data)}"
                )
                await asyncio.sleep(5)
                continue

            for update in data.get("result", []):
                last_update_id = max(last_update_id, update.get("update_id", 0))
                msg  = update.get("message") or {}
                text = (msg.get("text") or "").strip()
                cid  = str(msg.get("chat", {}).get("id", ""))

                if not text.startswith("/lore"):
                    continue
                if authorised and cid not in authorised:
                    logger.warning(
                        f"[LORE CMD] /lore from unauthorised chat {cid} — ignored"
                    )
                    continue

                logger.info(f"[LORE CMD] /lore received from chat {cid} — running analysis")
                try:
                    await _send_lore_analysis(wallets, reply_chat_id=cid)
                except Exception as e:
                    logger.error(
                        f"[LORE CMD] /lore handling failed: {type(e).__name__}: {e}",
                        exc_info=True,
                    )
                    send_telegram_lore_reply(
                        cid,
                        "⚠️ /lore failed — see logs/lore_tracker.log",
                    )


# --- Polling ----------------------------------------------------------
# Phase 1: poll all wallets at the ACTIVE interval. Dormant
# classification arrives in phase 2 once metrics exist. The scaffolding
# for dormant-bucket polling is in place — see _is_wallet_dormant — but
# it returns False for every wallet in phase 1.

def _is_wallet_dormant(addr: str, metrics: dict) -> bool:
    """Return True if the wallet should be polled at the dormant
    (60min) cadence instead of active (15min). A wallet is dormant
    when we have a recorded last_activity_ts and it's older than
    LORE_DORMANT_THRESHOLD_DAYS.

    Wallets with no recorded activity (never seen a recognised
    trade) are NOT dormant — they need active polling so we can
    discover their first trade. Otherwise newly added wallets
    would never get the polling cadence they need to seed metrics.
    """
    entry = metrics.get(addr) or {}
    last  = float(entry.get("last_activity_ts") or 0)
    if last <= 0:
        return False
    cutoff = time.time() - LORE_DORMANT_THRESHOLD_DAYS * 86_400
    return last < cutoff


async def _get_recent_signatures(
    session: aiohttp.ClientSession,
    rpc_url: str,
    addr: str,
    meta: dict,
    limit: int = LORE_RECENT_SIG_LIMIT,
) -> list[dict]:
    """Fetch the most recent signatures for one wallet. Returns []
    on any RPC error so the poll loop continues for other wallets.
    429s are logged at WARNING and treated as recoverable.

    Pacing (sequential calls with LORE_RPC_GAP_SEC between) is
    handled by the caller in _poll_one_tick.
    """
    tag = _wallet_log_tag(addr, meta)
    try:
        result = await _arpc_post(
            session, rpc_url, "getSignaturesForAddress",
            [addr, {"limit": limit, "commitment": "confirmed"}],
        )
        sigs = result.get("result") or []
        return sigs if isinstance(sigs, list) else []
    except aiohttp.ClientResponseError as e:
        if e.status == 429:
            logger.warning(f"{tag} Helius 429 rate-limited — continuing, will retry next tick")
        else:
            logger.warning(f"{tag} getSignaturesForAddress HTTP {e.status}: {e.message}")
        return []
    except Exception as e:
        logger.warning(f"{tag} getSignaturesForAddress failed: {type(e).__name__}: {e}")
        return []


async def _poll_one_tick(
    session: aiohttp.ClientSession,
    rpc_url: str,
    wallets: dict[str, dict],
    metrics: dict,
    open_positions: dict,
    closed_trades: list,
    poll_dormant_this_tick: bool,
) -> None:
    """One pass of the poll loop. Fetches new signatures per wallet,
    reconstructs trades from on-chain pre/post balances, applies
    them to open_positions and closed_trades, recomputes per-wallet
    metrics, and persists all three state files atomically at the
    end of the tick.

    Wallet bucket (active vs dormant) determines whether this tick
    visits it. Calls are strictly sequential with LORE_RPC_GAP_SEC
    between to avoid Helius 429 rate-limit bursts.
    """
    active_pool   = []
    dormant_pool  = []
    for addr, meta in wallets.items():
        if _is_wallet_dormant(addr, metrics):
            dormant_pool.append((addr, meta))
        else:
            active_pool.append((addr, meta))

    to_poll = active_pool[:]
    if poll_dormant_this_tick:
        to_poll.extend(dormant_pool)

    logger.info(
        f"[POLL] tick start — {len(active_pool)} active, "
        f"{len(dormant_pool)} dormant ({'incl. dormant' if poll_dormant_this_tick else 'active only'})"
    )

    # One SOL/USD fetch per tick (cached 5min internally — usually a
    # no-op). Done early so it doesn't compete with the per-wallet
    # processing for Jupiter bandwidth.
    sol_usd_rate = await _get_sol_usd_rate(session)

    tick_start          = time.time()
    sigs_observed       = 0
    txns_processed      = 0
    buys_recorded       = 0
    sells_recorded      = 0
    skipped_unrecognised = 0
    err_count           = 0

    for addr, meta in to_poll:
        tag = _wallet_log_tag(addr, meta)
        try:
            sigs = await _get_recent_signatures(session, rpc_url, addr, meta)
        except Exception as e:
            err_count += 1
            logger.warning(f"{tag} unexpected error fetching sigs: {type(e).__name__}: {e}")
            sigs = []
        sigs_observed += len(sigs)

        if not sigs:
            await asyncio.sleep(LORE_RPC_GAP_SEC)
            continue

        # Determine which sigs are NEW vs already-processed. sigs
        # arrive newest-first from getSignaturesForAddress; we want
        # to process oldest-first so buys land before sells in time
        # order. Stop at the first sig that equals last_processed_sig.
        entry = metrics.setdefault(addr, {})
        last_processed = entry.get("last_processed_sig")
        new_sigs: list[dict] = []
        for s in sigs:
            if s.get("signature") == last_processed:
                break
            new_sigs.append(s)

        # Cold-start cap: on the first poll we've ever done for this
        # wallet, only process the most recent N signatures. Backfilling
        # everything would burn RPC credits and we'd still miss anything
        # past the 10-sig fetch window.
        if last_processed is None and len(new_sigs) > LORE_COLD_START_SIG_LIMIT:
            logger.info(
                f"{tag} cold start — capping to {LORE_COLD_START_SIG_LIMIT} most recent of "
                f"{len(new_sigs)} signatures"
            )
            new_sigs = new_sigs[:LORE_COLD_START_SIG_LIMIT]

        new_sigs.reverse()   # oldest first

        if not new_sigs:
            # Nothing new this tick — pace and move on.
            await asyncio.sleep(LORE_RPC_GAP_SEC)
            continue

        logger.info(f"{tag} {len(new_sigs)} new signature(s) to reconstruct")
        wallet_buys  = 0
        wallet_sells = 0

        for s in new_sigs:
            sig = s.get("signature")
            if not sig:
                continue
            await asyncio.sleep(LORE_RPC_GAP_SEC)
            tx = await _get_transaction(session, rpc_url, sig)
            if tx is None:
                # RPC error — don't advance last_processed past this so
                # we retry next tick. Break out of the new_sigs loop;
                # the entries we've already processed are committed via
                # the running open_positions/closed_trades mutation.
                logger.warning(
                    f"{tag} getTransaction returned None for {sig[:16]} — "
                    f"halting reconstruction for this wallet this tick"
                )
                break
            txns_processed += 1
            # "Active" is decoupled from "PnL-reconstructible". Any
            # successfully fetched txn for the wallet counts as
            # activity — covers (a) sell-only sub-wallets, (b)
            # pre-baseline holders unwinding into our window,
            # (c) creator wallets receiving allocations they later
            # sell. Without this, those wallets would be flagged
            # dormant despite trading actively.
            blocktime = float(tx.get("blockTime") or time.time())
            entry["last_activity_ts"] = max(
                float(entry.get("last_activity_ts") or 0),
                blocktime,
            )

            trade = _extract_trade(tx, addr, sig, sol_usd_rate)
            if trade is None:
                skipped_unrecognised += 1
                continue
            applied = _apply_trade(trade, open_positions, closed_trades)
            if trade["kind"] == "buy":
                buys_recorded  += 1
                wallet_buys    += 1
                logger.info(
                    f"{tag} BUY  {trade['token_mint'][:8]} "
                    f"{trade['token_units']:,.4f} for {trade['sol_value']:.4f} SOL"
                )
            elif applied is not None:
                # Landed sell — `applied` is the closed-trade record
                # just appended, so PnL fields are accurate per-sell.
                sells_recorded += 1
                wallet_sells   += 1
                logger.info(
                    f"{tag} SELL {trade['token_mint'][:8]} for "
                    f"{trade['sol_value']:.4f} SOL "
                    f"(PnL {applied['pnl_sol']:+.4f} SOL / "
                    f"{applied['pnl_pct']:+.1f}%, "
                    f"held {applied['hold_minutes']:.1f}m)"
                )
            else:
                # Sell of a position we have no cost basis for.
                # _apply_trade already logged at DEBUG; bump the
                # skip counter so the tick summary reflects it.
                skipped_unrecognised += 1
                logger.debug(
                    f"{tag} SELL {trade['token_mint'][:8]} skipped — "
                    f"no recorded buy (pre-baseline)"
                )
            # Advance the watermark to this sig only after successful
            # apply — so a mid-batch failure leaves the unprocessed
            # tail to retry next tick.
            entry["last_processed_sig"] = sig
        else:
            # All new_sigs processed without break — fast-forward the
            # watermark to the newest sig in the batch in case some
            # were skipped (unrecognised) without advancing it.
            if new_sigs:
                entry["last_processed_sig"] = new_sigs[-1]["signature"]

        if wallet_buys or wallet_sells:
            _recompute_wallet_metrics(addr, metrics, closed_trades)

        await asyncio.sleep(LORE_RPC_GAP_SEC)

    # End-of-tick housekeeping: prune old closed trades, persist all
    # three state files atomically.
    pruned = _prune_closed_trades(closed_trades)
    try:
        _atomic_write_json(LORE_METRICS_FILE, metrics)
        _atomic_write_json(LORE_OPEN_POSITIONS_FILE, open_positions)
        _atomic_write_json(LORE_CLOSED_TRADES_FILE, closed_trades)
    except OSError as e:
        logger.error(f"[POLL] state file write failed: {type(e).__name__}: {e}")

    tick_dur = time.time() - tick_start
    logger.info(
        f"[POLL] tick complete — {len(to_poll)} wallets in {tick_dur:.1f}s | "
        f"sigs_seen={sigs_observed} txns={txns_processed} "
        f"buys={buys_recorded} sells={sells_recorded} "
        f"skipped={skipped_unrecognised} errors={err_count} pruned={pruned}"
    )
    _maybe_log_hourly_helius_stats()


async def _poll_loop(rpc_url: str, wallets: dict[str, dict]) -> None:
    """Run the polling loop forever at ACTIVE cadence. Every Nth tick
    (where N = DORMANT / ACTIVE) also polls dormant wallets. State
    files are loaded once at start and persisted at the end of each
    tick via _poll_one_tick.
    """
    metrics:        dict = _load_json_or(LORE_METRICS_FILE,        {})
    open_positions: dict = _load_json_or(LORE_OPEN_POSITIONS_FILE, {})
    closed_trades:  list = _load_json_or(LORE_CLOSED_TRADES_FILE,  [])
    if not isinstance(metrics, dict):        metrics = {}
    if not isinstance(open_positions, dict): open_positions = {}
    if not isinstance(closed_trades, list):  closed_trades = []
    logger.info(
        f"[POLL] state restored — metrics for {len(metrics)} wallets, "
        f"{sum(len(v) for v in open_positions.values())} open positions across "
        f"{len(open_positions)} wallets, {len(closed_trades)} closed trades"
    )

    interval_sec   = LORE_POLL_INTERVAL_ACTIVE_MIN * 60
    dormant_every  = max(1, LORE_POLL_INTERVAL_DORMANT_MIN // LORE_POLL_INTERVAL_ACTIVE_MIN)
    tick = 0

    async with aiohttp.ClientSession() as session:
        while True:
            tick += 1
            poll_dormant_this_tick = (tick % dormant_every == 0)
            try:
                await _poll_one_tick(
                    session, rpc_url, wallets, metrics,
                    open_positions, closed_trades, poll_dormant_this_tick,
                )
            except Exception as e:
                logger.error(f"[POLL] tick {tick} failed: {type(e).__name__}: {e}", exc_info=True)
            await asyncio.sleep(interval_sec)


# --- Entry point ------------------------------------------------------

def _resolve_lore_rpc_url() -> tuple[str, str]:
    """Return (rpc_url, isolation_status) for lore tracker's RPC endpoint.

    Preference: dedicated HELIUS_API_KEY → constructed Helius URL.
    Fallback: SOLANA_RPC (shared with whale_sniper — rate budgets
    are NOT isolated; warned at startup).

    isolation_status is one of:
      'isolated'   — dedicated HELIUS_API_KEY in use, distinct from SOLANA_RPC
      'shared_key' — HELIUS_API_KEY matches the key embedded in SOLANA_RPC
      'fallback'   — HELIUS_API_KEY unset; using SOLANA_RPC
      'none'       — neither available; lore tracker cannot poll
    """
    helius_key = os.getenv("HELIUS_API_KEY", "").strip()
    solana_rpc = os.getenv("SOLANA_RPC", "").strip()

    if helius_key:
        lore_rpc = f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
        # Defensive: detect when the dedicated key is the same key
        # already embedded in SOLANA_RPC (i.e. one Helius account
        # serving both processes).
        if solana_rpc and helius_key in solana_rpc:
            return lore_rpc, "shared_key"
        return lore_rpc, "isolated"

    if solana_rpc:
        return solana_rpc, "fallback"
    return "", "none"


async def main() -> None:
    rpc_url, isolation = _resolve_lore_rpc_url()
    if isolation == "none":
        logger.error(
            "Neither HELIUS_API_KEY nor SOLANA_RPC set in .env — "
            "lore tracker cannot poll"
        )
        return
    if isolation == "isolated":
        logger.info(
            "Helius isolation: lore tracker using dedicated HELIUS_API_KEY "
            "endpoint (rate budget separate from whale_sniper)"
        )
    elif isolation == "shared_key":
        logger.warning(
            "HELIUS_API_KEY matches the key embedded in SOLANA_RPC — "
            "lore tracker is NOT rate-budget-isolated from whale_sniper. "
            "Continuing on shared Helius account."
        )
    elif isolation == "fallback":
        logger.warning(
            "HELIUS_API_KEY not set — lore tracker falling back to "
            "SOLANA_RPC (shared rate budget with whale_sniper)"
        )

    wallets = _load_lore_wallets()
    group_count = len({(v or {}).get("group") for v in wallets.values()})

    logger.info(
        f"Lore tracker starting — {len(wallets)} wallets across {group_count} groups"
    )
    logger.info(
        f"Poll cadence: active={LORE_POLL_INTERVAL_ACTIVE_MIN}min, "
        f"dormant={LORE_POLL_INTERVAL_DORMANT_MIN}min, "
        f"dormant threshold={LORE_DORMANT_THRESHOLD_DAYS}d, "
        f"sequential pacing={LORE_RPC_GAP_SEC*1000:.0f}ms gap"
    )

    send_telegram(
        f"🔭 <b>Lore tracker online</b> — observing "
        f"{len(wallets)} wallets across {group_count} groups\n"
        f"Phase 1: polling only (no trade reconstruction yet)\n"
        f"Active poll: every {LORE_POLL_INTERVAL_ACTIVE_MIN}min"
    )

    # Install signal handlers for clean shutdown so PM2 stop works
    # without a long termination wait.
    stop_event = asyncio.Event()

    def _on_signal(signame: str) -> None:
        logger.info(f"Received {signame} — shutting down lore tracker cleanly")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(
                getattr(signal, sig_name), _on_signal, sig_name
            )
        except (NotImplementedError, AttributeError):
            # Windows doesn't support add_signal_handler; not a concern
            # on the Linux VPS but keeps local testing portable.
            pass

    poll_task     = asyncio.create_task(_poll_loop(rpc_url, wallets))
    top3_task     = asyncio.create_task(_top3_scheduler_loop(wallets))
    analysis_task = asyncio.create_task(_lore_analysis_scheduler_loop(wallets))
    cmd_task      = asyncio.create_task(_lore_command_loop(wallets))
    tasks = [poll_task, top3_task, analysis_task, cmd_task]
    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        logger.info("Lore tracker stopped")


async def _send_top3_oneshot() -> None:
    """Manual one-shot: load wallets + closed trades, compute the
    current top-3, send to Telegram, exit. Used by --send-top3-now
    for smoke testing. Does NOT touch lore_scheduler_state.json —
    the next scheduled run will still fire as expected.
    """
    wallets = _load_lore_wallets()
    msg, top3 = _compose_top3_now(wallets)
    logger.info(
        f"[TOP3] manual one-shot — composed top-3 with {len(top3)} group(s)"
    )
    # Print the rendered message so we see what's about to go out.
    print("----- TOP-3 MESSAGE PREVIEW -----")
    print(msg)
    print("----- END PREVIEW -----")
    sent_ok = send_telegram(msg)
    logger.info(f"[TOP3] manual one-shot — telegram sent: {sent_ok}")


async def _send_lore_analysis_oneshot() -> None:
    """Manual one-shot for the 3-day Claude analysis. Loads wallets,
    runs the full aggregate → Claude → format → send pipeline,
    broadcasts via the PRIMARY bot, exits. Does NOT touch
    last_lore_analysis_date — using --send-lore-now does not block
    or shift the next scheduled Mon/Thu/Sun run.
    """
    wallets = _load_lore_wallets()
    logger.info("[CLAUDE] manual one-shot — composing 3-day analysis")
    sent_ok = await _send_lore_analysis(wallets)
    logger.info(f"[CLAUDE] manual one-shot — telegram delivered: {sent_ok}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="APEX lore tracker — observation-only wallet monitor."
    )
    p.add_argument(
        "--send-top3-now",
        action="store_true",
        help=(
            "Compute the current top-3 from on-disk state and send to "
            "Telegram once, then exit. Manual smoke trigger; does NOT "
            "update the scheduler's last_top3_sent_date."
        ),
    )
    p.add_argument(
        "--send-lore-now",
        action="store_true",
        help=(
            "Run the 3-day Claude analysis once and broadcast via the "
            "PRIMARY bot, then exit. Manual smoke trigger; does NOT "
            "update the scheduler's last_lore_analysis_date."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.send_top3_now:
        asyncio.run(_send_top3_oneshot())
    elif args.send_lore_now:
        asyncio.run(_send_lore_analysis_oneshot())
    else:
        asyncio.run(main())
