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

import asyncio
import json
import logging
import os
import signal
import time
from logging.handlers import TimedRotatingFileHandler

import aiohttp
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


def send_telegram(message: str) -> bool:
    """Send a Telegram message to all authorised chat IDs.
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
            any_ok = True
        except requests.exceptions.HTTPError as e:
            logger.error(f"Telegram HTTP {e.response.status_code} for {chat_id}: {e.response.text[:200]}")
        except Exception as e:
            logger.error(f"Telegram send failed for {chat_id}: {e}")
    return any_ok


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


def _wallet_log_tag(addr: str, meta: dict) -> str:
    """Build a grep-friendly tag for log lines.
    Format: '[group/name addr8]' or '[? addr8]' for entries with no
    group/name metadata.
    """
    group = (meta or {}).get("group") or "?"
    name  = (meta or {}).get("name")  or "?"
    return f"[{group}/{name} {addr[:8]}]"


# --- Polling ----------------------------------------------------------
# Phase 1: poll all wallets at the ACTIVE interval. Dormant
# classification arrives in phase 2 once metrics exist. The scaffolding
# for dormant-bucket polling is in place — see _is_wallet_dormant — but
# it returns False for every wallet in phase 1.

def _is_wallet_dormant(addr: str, metrics: dict) -> bool:
    """Return True if the wallet should be polled at the dormant
    (60min) cadence instead of active (15min). Phase 1 stub: always
    False. Phase 2 fills this in based on metrics[addr]['last_activity_ts'].
    """
    # Placeholder so phase 2 can plug in without restructuring the
    # poll loop.
    _ = (addr, metrics)
    return False


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
    poll_dormant_this_tick: bool,
) -> None:
    """One pass of the poll loop. Phase 1 just fetches sigs and logs
    counts — no trade reconstruction. Wallet bucket (active vs
    dormant) determines whether this tick visits it.

    Calls are strictly sequential with LORE_RPC_GAP_SEC between to
    avoid Helius 429 rate-limit bursts (see phase 1 smoke fix).
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

    tick_start = time.time()
    total_sigs = 0
    err_count  = 0
    rate_limit_count = 0
    for addr, meta in to_poll:
        try:
            sigs = await _get_recent_signatures(session, rpc_url, addr, meta)
        except Exception as e:
            err_count += 1
            logger.warning(f"{_wallet_log_tag(addr, meta)} unexpected error: {type(e).__name__}: {e}")
            sigs = []
        sig_count = len(sigs)
        total_sigs += sig_count
        if sig_count == 0:
            # _get_recent_signatures already logged the reason at WARNING
            # if it was a 429 or other RPC failure. We only need a count
            # of 429s here for the tick summary — re-fetch from log would
            # be costly, so we approximate via a separate counter wired
            # in via the future protocol-specific decode pass.
            pass
        else:
            tag = _wallet_log_tag(addr, meta)
            logger.info(f"{tag} {sig_count} recent sigs (phase 1: not yet reconstructed)")
        # Pacing gap — defence in depth even with dedicated Helius key
        await asyncio.sleep(LORE_RPC_GAP_SEC)

    tick_dur = time.time() - tick_start
    logger.info(
        f"[POLL] tick complete — {len(to_poll)} wallets polled in {tick_dur:.1f}s, "
        f"{total_sigs} total sigs observed, {err_count} errors"
    )
    _maybe_log_hourly_helius_stats()


async def _poll_loop(rpc_url: str, wallets: dict[str, dict]) -> None:
    """Run the polling loop forever at ACTIVE cadence. Every Nth tick
    (where N = DORMANT / ACTIVE) also polls dormant wallets.
    """
    metrics: dict = {}   # Phase 2 will load from LORE_METRICS_FILE here.
    interval_sec   = LORE_POLL_INTERVAL_ACTIVE_MIN * 60
    dormant_every  = max(1, LORE_POLL_INTERVAL_DORMANT_MIN // LORE_POLL_INTERVAL_ACTIVE_MIN)
    tick = 0

    async with aiohttp.ClientSession() as session:
        while True:
            tick += 1
            poll_dormant_this_tick = (tick % dormant_every == 0)
            try:
                await _poll_one_tick(
                    session, rpc_url, wallets, metrics, poll_dormant_this_tick
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

    poll_task = asyncio.create_task(_poll_loop(rpc_url, wallets))
    try:
        await stop_event.wait()
    finally:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        logger.info("Lore tracker stopped")


if __name__ == "__main__":
    asyncio.run(main())
