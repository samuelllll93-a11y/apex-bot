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
SELL_SLIPPAGE_BPS     = 3000   # 30% slippage for sells (was 20% — bumped
                               # after a wave of Custom-17 "ExceededSlippage"
                               # errors from pAMMBay on rapidly-dumping tokens)

# Per-position consecutive-failure tracking. After this many back-to-back
# sell failures, the position is abandoned (logged + blacklisted) to stop
# the monitor loop retrying an unsellable bag forever (zero-liquidity
# tokens with 100% peak drops were responsible for ~7k Custom-17 errors
# in the preceding 24h).
SELL_ABANDON_AFTER_FAILURES = 10
_sell_failure_counts: dict[str, int] = {}
PREFER_JUPITER_SELLS  = True   # Jupiter first for sells; PumpPortal as fallback

# Pre-TP1 hard floor — the missing stop loss. Before commit history shows
# the trailing stop is gated behind min_target_hit (TP1 at +100% gain), so
# any position that goes straight down arms nothing and rots until manual
# /sell. 5-day production data (May 8-12) confirmed multi-hour holds at
# -80% to -94% on momentum_scanner positions for exactly this reason. The
# hard floor fires regardless of source (whale, momentum_scanner, cto,
# dip_sniper, cluster) — it operates on pnl_pct only.
#
# -35% is the starting threshold. At 30% sell slippage (SELL_SLIPPAGE_BPS)
# the realised loss after retries can be -50% to -60%; a tighter floor
# (-25%) would trigger more often but might cut positions that would have
# recovered. Tune via config.
HARD_FLOOR_PCT     = -35.0
HARD_FLOOR_ENABLED = True
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
    # "peace2":   "6iZLfoaYvEAuuhnJEiSkwC9exmtMZehpkUVuFzb19sWc",  # disabled 2026-04-21
    # "pi":       "9XVWfqzavraezfvS38v8xcZepHYxcNKtnNMEKpPXEwTN",  # disabled 2026-05-02
    # "bigboy": "HYWo71Wk9PNDe5sBaRKazPnVyGnQDiwgXCFKvgAQ1ENp",  # disabled 2026-04-19
    "bullishness": "FMWUQvEjMrX4foJ8GPxpf82fSPHTkjqz9LWSgQ5EBTtQ",
    # "spsc":     "7S3E2L25kr6oN2cMP2GQ5tMEfg8jwcmoYo35vvv8rxhW",  # disabled 2026-04-08
    # "early":  "Bv2BAw5UmKxv5SBMWYKqpsh6eXKNGM2RKxJGpGPk5vmb",  # disabled 2026-03-31
}

# Lore wallet tracker — populated from state/lore_wallets.json at startup.
# This is a separate, larger pool of wallets we just OBSERVE (no copy-trading).
# Sam edits state/lore_wallets.json directly; format is {address: alias}.
LORE_WALLETS: dict[str, str] = {}

# Track the last seen signature per wallet to detect new txns
last_seen_sig: dict[str, str | None] = {name: None for name in WHALE_WALLETS}

# Whale activity log: name -> list of (mint, timestamp) for all buys
_whale_activity: dict[str, list[tuple[str, float]]] = {name: [] for name in WHALE_WALLETS}

ACTIVITY_WINDOW_SEC   = 86_400  # 24 h    — HOT / COLD scoring window
HOT_THRESHOLD         = 3       # buys in 24 h to be classified HOT

# --- Whale cluster detection ------------------------------------------
# When 2+ tracked whales buy the same token inside CLUSTER_WINDOW_SEC we
# treat it as a conviction signal. 3+ = MEGA cluster (forces Tier 3
# exits, bypasses blacklist cooldown).
CLUSTER_WINDOW_SEC = 30 * 60    # 30 min sliding window

# token_mint → list of (whale_name, ts) — latest entry per whale wins
_recent_whale_buys: dict[str, list[tuple[str, float]]] = {}
# token_mint → highest cluster tier already alerted (0=none, 2=cluster, 3=mega)
_cluster_alerts_sent: dict[str, int] = {}


def _cluster_prune_and_record(token_mint: str, whale_name: str, now: float) -> None:
    """Append this whale's buy signal to _recent_whale_buys[token_mint] and
    drop any entries older than CLUSTER_WINDOW_SEC. Also opportunistically
    garbage-collect other tokens so the dict doesn't grow unbounded.
    """
    cutoff = now - CLUSTER_WINDOW_SEC
    _recent_whale_buys.setdefault(token_mint, []).append((whale_name, now))
    # Prune this token
    _recent_whale_buys[token_mint] = [
        (w, ts) for (w, ts) in _recent_whale_buys[token_mint] if ts >= cutoff
    ]
    # Opportunistic sweep of everything else — cheap, keeps memory bounded
    for _m in list(_recent_whale_buys.keys()):
        if _m == token_mint:
            continue
        _recent_whale_buys[_m] = [
            (w, ts) for (w, ts) in _recent_whale_buys[_m] if ts >= cutoff
        ]
        if not _recent_whale_buys[_m]:
            del _recent_whale_buys[_m]
            _cluster_alerts_sent.pop(_m, None)


def _cluster_unique_whales(token_mint: str) -> list[str]:
    """Return ordered list of unique whale names who bought this token
    inside the cluster window (oldest first). Empty if none tracked."""
    seen: list[str] = []
    for (w, _ts) in _recent_whale_buys.get(token_mint, []):
        if w not in seen:
            seen.append(w)
    return seen


def _cluster_window_span_min(token_mint: str, now: float) -> int:
    """How many minutes span the oldest→newest cluster entry — for alert text."""
    entries = _recent_whale_buys.get(token_mint, [])
    if not entries:
        return 0
    oldest = min(ts for (_w, ts) in entries)
    return int((now - oldest) / 60)

# --- Sell / exit parameters -------------------------------------------
TAKE_PROFIT_PCT    = float(os.getenv("TAKE_PROFIT_PCT", "0.75")) * 100  # .env decimal → % (e.g. 0.75 = 75%)
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

# --- Momentum scanner -------------------------------------------------
# DexScreener-based three-tier scanner. Runs every 10 minutes. Candidates
# come from the trending (token-profiles), boosts, and search-by-volume
# endpoints; each candidate is classified into Tier 1/2/3 based on age
# from pairCreatedAt and filtered + TA-checked per-tier. Surviving
# candidates get a Claude score; buy above tier threshold, watch within
# 8 points of threshold, silent skip otherwise.
MOMENTUM_SCAN_INTERVAL_SEC    = 600           # 10 min main loop cadence
MOMENTUM_ALERT_COOLDOWN_SEC   = 4 * 3600      # 4-hour dedup per mint

# Per-tier Claude score thresholds (buy; watch = buy - MOMENTUM_WATCH_DELTA)
MOMENTUM_TIER1_BUY_SCORE      = 60            # Fresh grads (<3h)
MOMENTUM_TIER2_BUY_SCORE      = 58            # Reawakening (3h-7d)
MOMENTUM_TIER3_BUY_SCORE      = 58            # Second leg (7d-30d)
MOMENTUM_WATCH_DELTA          = 8             # score ≥ buy-8 → watch alert

# Age tier boundaries (hours)
MOMENTUM_TIER1_MAX_AGE_HOURS  = 3.0
MOMENTUM_TIER2_MAX_AGE_HOURS  = 24.0 * 7      # 7 days
MOMENTUM_TIER3_MAX_AGE_HOURS  = 24.0 * 30     # 30 days

# DexScreener endpoint pagination / throttle
MOMENTUM_CANDIDATES_MAX       = 50            # cap unique mints per cycle
MOMENTUM_PER_COIN_SLEEP_SEC   = 0.3           # throttle between per-mint fetches

# Graduated-pump dedup: pump.fun mints end in "pump"; if mcap exceeds
# this cap they're treated as already-runaway tokens and skipped.
# Set above the $32_700 graduation threshold so newly-graduated tokens
# (the $32.7k-$40k just-on-Raydium cohort) still flow into Tier 1 for
# evaluation rather than being silently filtered.
MOMENTUM_PUMP_SKIP_MCAP_USD   = 40_000

# mint → last-alerted timestamp (cooldown dedup)
_momentum_alerted: dict[str, float] = {}
# Rolling event log for the analyst: list of (ts, event_type) where
# event_type ∈ {"scanned", "watched", "bought", "low_score"}.
# Pruned to the last 24h on each analyst tick.
_momentum_events: list[tuple[float, str]] = []


# --- Claude analyst ---------------------------------------------------
ANALYST_BRIEF_INTERVAL_HOURS = 6   # cadence of the analyst market brief
# Scheduling + snapshot state for the hourly market brief, the 6-hour
# strategy check, and the on-demand /analyse command. All three share
# one async loop (hourly_analyst_loop) plus a Telegram command handler.
_analyst_last_brief:          float = 0.0    # ts of last hourly brief sent
_analyst_last_strategy_check: float = 0.0    # ts of last 6-hour strategy check
_analyst_prev_snapshot:       dict  = {}     # previous hour's pump.fun stats for compare

# --- MANNOS Autopilot (now global) -----------------------------------
# When True: ALL whale signals (except mr.putin, which keeps its own config)
# bypass quality checks, token-safety gating, and Claude scoring. Balance
# check, blacklist check, emergency dump guard, position tracking, and
# Telegram alerts remain active. Flag name retained for backward compat.
MANNOS_AUTOPILOT = True   # set False to restore quality/safety/claude gating

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


def _is_autopilot(name: str) -> bool:
    """Return True if this whale should skip quality/safety/Claude gating.
    mr.putin keeps its own dedicated config — all other whales go autopilot
    when MANNOS_AUTOPILOT is enabled (global autopilot mode)."""
    return MANNOS_AUTOPILOT and name != "mr.putin"


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
CTO_SIGNAL_BUY_SOL       = 0.02   # Fixed position size for DexAlert CTO signals
DIP_SNIPER_WATCH_HOURS   = 8      # expire tokens from watchlist after X hours
DIP_SNIPER_CHECK_SEC     = 60     # how often the dip sniper loop runs

# In-memory graduated watchlist (loaded from disk at startup)
# mint → {graduation_price_sol: float, ath_sol: float, added_ts: float}
graduated_watchlist: dict[str, dict] = {}

# Persistence paths — both files live next to the script
POSITIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "open_positions.json")
BLACKLIST_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "blacklist.json")

# Daily intelligence report — trade log lives in state/ subdir (created on demand)
_STATE_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state")
TRADE_LOG_FILE    = os.path.join(_STATE_DIR, "trade_log.json")
TRADED_TOKENS_FILE = os.path.join(_STATE_DIR, "traded_tokens.json")

# Tokens we've already traded and exited — never rebuy these.
# Populated on every full-close; persisted to TRADED_TOKENS_FILE.
_traded_tokens: set[str] = set()
# Momentum scanner watch-only events (no buy — for daily report signal count)
MOMENTUM_WATCH_LOG_FILE = os.path.join(_STATE_DIR, "momentum_watches.json")

# Lore wallet tracker — observation-only wallet intelligence database.
LORE_WALLETS_FILE          = os.path.join(_STATE_DIR, "lore_wallets.json")
WALLET_LORE_FILE           = os.path.join(_STATE_DIR, "wallet_lore.json")
LORE_POLL_INTERVAL_SEC     = 120
LORE_MIN_STAGGER_SEC       = 0.5   # don't poll faster than this even with many wallets
LORE_INACTIVE_DAYS         = 30
LORE_MIN_TRADES_FOR_REPORT = 5
LORE_RECENT_TRADES_KEEP    = 20
_WALLET_LORE: dict[str, dict] = {}
DAILY_REPORT_UTC_HOURS = (11, 23)   # 11:00 UTC = 21:00 AEST, 23:00 UTC = 09:00 AEST (UTC+10, no DST)
DAILY_REPORT_WINDOW_DAYS = 7

# --- Persistent data store (~/apex-data) ------------------------------
# Deep history for the /insights command + weekly deep-dive. Daily-bucketed
# JSON for structured data, per-hour/per-day text files for Claude outputs.
APEX_DATA_DIR             = os.path.expanduser("~/apex-data")
APEX_DATA_TRADES          = os.path.join(APEX_DATA_DIR, "trades")
APEX_DATA_SIGNALS         = os.path.join(APEX_DATA_DIR, "signals")
APEX_DATA_ERRORS          = os.path.join(APEX_DATA_DIR, "errors")
APEX_DATA_WHALES          = os.path.join(APEX_DATA_DIR, "whale_activity")
APEX_DATA_REPORTS_BRIEFS  = os.path.join(APEX_DATA_DIR, "reports", "briefs")
APEX_DATA_REPORTS_DAILY   = os.path.join(APEX_DATA_DIR, "reports", "daily")
APEX_DATA_ANALYSIS        = os.path.join(APEX_DATA_DIR, "analysis")
APEX_DATA_ANALYSIS_WEEKLY = os.path.join(APEX_DATA_DIR, "analysis", "weekly")


def _apex_data_day_file(subdir: str, ts: float | None = None, ext: str = ".json") -> str:
    """Return the daily-bucketed path inside `subdir` for timestamp `ts`
    (default now): e.g. {subdir}/2026-04-20.json."""
    if ts is None:
        ts = time.time()
    day = time.strftime("%Y-%m-%d", time.gmtime(ts))
    return os.path.join(subdir, f"{day}{ext}")


def _apex_data_append_json(path: str, record: dict) -> None:
    """Load-modify-atomic-replace append. Creates parent dirs, never raises."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if not isinstance(data, list):
                data = []
        except (FileNotFoundError, json.JSONDecodeError):
            data = []
        data.append(record)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as exc:
        logger.error(f"[APEX DATA] append {path} failed: {exc}")


def _apex_data_save_text(path: str, content: str) -> None:
    """Write a text file (overwrite). Creates parent dirs, never raises."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
    except Exception as exc:
        logger.error(f"[APEX DATA] save_text {path} failed: {exc}")


def _apex_log_trade_record(record: dict) -> None:
    """Append a closed-trade dict to trades/YYYY-MM-DD.json keyed by entry_time."""
    ts = float(record.get("entry_time") or record.get("exit_time") or time.time())
    _apex_data_append_json(_apex_data_day_file(APEX_DATA_TRADES, ts), record)


def _apex_log_error(token_mint: str, whale: str | None, reason: str,
                    ctx: dict | None = None) -> None:
    """Record a failed transaction with reason + context for later analysis."""
    rec = {
        "ts":         time.time(),
        "token_mint": token_mint or "unknown",
        "whale":      whale or "unknown",
        "reason":     reason or "unknown",
        "context":    ctx or {},
    }
    _apex_data_append_json(_apex_data_day_file(APEX_DATA_ERRORS), rec)


def _apex_log_signal(source: str, action: str, details: dict) -> None:
    """Record a signal outcome (momentum buy/watch/skip, CTO signal, etc.)."""
    rec = {"ts": time.time(), "source": source, "action": action}
    rec.update(details or {})
    _apex_data_append_json(_apex_data_day_file(APEX_DATA_SIGNALS), rec)


def _apex_log_whale_activity(whale_name: str, token_mint: str, apex_bought: bool,
                             reason_if_skipped: str | None = None,
                             extra: dict | None = None) -> None:
    """Every whale buy signal apex detected, with outcome + reason."""
    rec: dict = {
        "ts":                time.time(),
        "whale_name":        whale_name or "unknown",
        "token_mint":        token_mint or "unknown",
        "apex_bought":       bool(apex_bought),
        "reason_if_skipped": None if apex_bought else (reason_if_skipped or "unknown"),
    }
    if extra:
        rec.update(extra)
    _apex_data_append_json(_apex_data_day_file(APEX_DATA_WHALES), rec)


def _apex_migrate_legacy_trade_log() -> None:
    """One-shot: if apex-data/trades/ is empty and state/trade_log.json
    exists, split it into per-day files. Idempotent (checks for existing
    files and skips if any .json already in trades/)."""
    try:
        os.makedirs(APEX_DATA_TRADES, exist_ok=True)
        existing = [f for f in os.listdir(APEX_DATA_TRADES) if f.endswith(".json")]
        if existing:
            logger.info(
                f"[APEX DATA] migration skipped — trades/ already has "
                f"{len(existing)} file(s)"
            )
            return
        if not os.path.exists(TRADE_LOG_FILE):
            logger.info(
                f"[APEX DATA] migration skipped — no legacy {TRADE_LOG_FILE} found"
            )
            return
        with open(TRADE_LOG_FILE, "r") as f:
            legacy = json.load(f)
        if not isinstance(legacy, list):
            logger.warning("[APEX DATA] legacy trade_log.json is not a list — skip")
            return
        by_day: dict[str, list] = {}
        for rec in legacy:
            ts = float(rec.get("entry_time") or rec.get("exit_time") or time.time())
            day = time.strftime("%Y-%m-%d", time.gmtime(ts))
            by_day.setdefault(day, []).append(rec)
        for day, records in by_day.items():
            p = os.path.join(APEX_DATA_TRADES, f"{day}.json")
            with open(p, "w") as f:
                json.dump(records, f, indent=2)
        logger.info(
            f"[APEX DATA] migrated {len(legacy)} trade(s) → "
            f"{len(by_day)} daily file(s) in {APEX_DATA_TRADES}"
        )
    except Exception as exc:
        logger.error(f"[APEX DATA] migration failed: {exc}", exc_info=True)


def _real_pnl(pos: dict, exit_proceeds_sol: float) -> tuple[float, float]:
    """Return (pnl_sol, pnl_pct) computed against the ORIGINAL entry cost,
    summing in any partial-exit proceeds already booked on this position.

    After TP1 (and partial mirror sells), `entry_sol` on the live pos dict
    is intentionally shrunk so pnl_pct stays positive — this disables the
    hard-floor exit on the free-ride remainder. The side-effect is that
    displays and logs computed from (current_sol / shrunk_entry_sol) can
    balloon into tens of thousands of percent, which is misleading.

    `original_entry_sol` is captured once at open and never mutated.
    `tp1_received_sol` accumulates SOL proceeds from every partial exit
    (TP1 *and* partial mirror sells).

    For positions opened before these fields existed, fall back to
    reconstructing original_entry as (current entry_sol + tp1_received_sol),
    which matches the invariant after one TP1 event.
    """
    original_entry = float(pos.get("original_entry_sol") or 0)
    tp1_received   = float(pos.get("tp1_received_sol") or 0)
    if original_entry <= 0:
        original_entry = float(pos.get("entry_sol") or 0) + tp1_received
    total_proceeds = tp1_received + float(exit_proceeds_sol or 0)
    if original_entry <= 0:
        return 0.0, 0.0
    pnl_sol = total_proceeds - original_entry
    pnl_pct = (total_proceeds / original_entry - 1) * 100
    return pnl_sol, pnl_pct


def _log_trade(pos: dict, exit_reason: str, exit_sol: float, token_mint: str) -> None:
    """Append a closed-position record to state/trade_log.json.

    exit_reason should be one of: mirror_sell, trailing_stop, time_stop,
    take_profit, emergency_dump, manual_sell, or a free-form string for
    edge cases. Called at every site that clears a position from
    open_positions. Fails silently (logged) so the main trade flow never
    crashes on a logging error.
    """
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        entry_time  = float(pos.get("entry_time") or 0)
        exit_time   = time.time()
        hold_mins   = max(0.0, (exit_time - entry_time) / 60) if entry_time else 0.0

        # Real PnL accounts for any partial exits (TP1 / partial mirror sells)
        # whose proceeds were booked into tp1_received_sol before this final
        # exit fired. Falls back gracefully for legacy positions.
        original_entry = float(pos.get("original_entry_sol") or 0)
        tp1_received   = float(pos.get("tp1_received_sol") or 0)
        if original_entry <= 0:
            original_entry = float(pos.get("entry_sol") or 0) + tp1_received
        pnl_sol, pnl_pct = _real_pnl(pos, exit_sol)

        # Normalise legacy source tags to the canonical taxonomy so the
        # daily report can bucket uniformly: whale → whale_copy.
        _source = pos.get("source") or "unknown"
        if _source == "whale":
            _source = "whale_copy"

        record = {
            "token_mint":   token_mint,
            "token_symbol": pos.get("token_symbol") or pos.get("token_label") or token_mint[:8],
            "whale_name":   pos.get("whale_name") or pos.get("whale") or "unknown",
            "entry_time":   entry_time,
            "exit_time":    exit_time,
            "hold_time_mins": round(hold_mins, 2),
            "entry_sol":          round(original_entry, 6),   # original cost basis
            "tp1_received_sol":   round(tp1_received, 6),     # booked partials
            "exit_sol":           round(exit_sol, 6),         # this final exit only
            "total_proceeds_sol": round(tp1_received + exit_sol, 6),
            "pnl_sol":      round(pnl_sol, 6),
            "pnl_pct":      round(pnl_pct, 2),
            "entry_mcap":   float(pos.get("mc_entry") or 0),
            "exit_reason":  exit_reason,
            "was_hot_whale": bool(pos.get("was_hot_whale", False)),
            "buys_24h_at_entry": int(pos.get("buys_24h_at_entry") or 0),
            "was_pregrad":  bool(pos.get("was_pregrad", False)),
            "conviction":   pos.get("conviction") or "normal",
            "source":       _source,
            # Momentum-scanner metadata — present only when source == momentum_scanner
            "momentum_score":     pos.get("momentum_score"),
            "bonding_pct_entry":  pos.get("bonding_pct_entry"),
            "velocity_pct":       pos.get("velocity_pct"),
            "replies_at_entry":   pos.get("replies_at_entry"),
            # Cluster metadata — present only when source == cluster_buy
            "cluster_size":       pos.get("cluster_size"),
            # CTO review metadata — present only when source == cto_signal
            "cto_review_decision": pos.get("cto_review_decision"),
            "cto_review_pct":      pos.get("cto_review_pct"),
        }

        try:
            with open(TRADE_LOG_FILE, "r") as f:
                log = json.load(f)
            if not isinstance(log, list):
                log = []
        except (FileNotFoundError, json.JSONDecodeError):
            log = []

        log.append(record)

        tmp = TRADE_LOG_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(log, f, indent=2)
        os.replace(tmp, TRADE_LOG_FILE)

        # Also append to the persistent per-day store under ~/apex-data/trades/
        _apex_log_trade_record(record)

        logger.info(
            f"[TRADE LOG] {record['token_symbol']} | {exit_reason} | "
            f"PnL {pnl_sol:+.4f} SOL ({pnl_pct:+.1f}%) | "
            f"hold {hold_mins:.0f}m | whale={record['whale_name']}"
        )
    except Exception as exc:
        logger.error(f"[TRADE LOG] Failed to log trade for {token_mint[:8]}: {exc}", exc_info=True)

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


# --- Honeypot freeze-authority check ----------------------------------
# Tokens whose mint still has a freezeAuthority can have holders' token
# accounts frozen by the issuer — a classic rug pattern. Custom-17 errors
# from Token-2022 at TransferChecked ("Error: Account is frozen") were
# ~7k/day in logs before this guard was added. Called before every buy
# site; fail-open on RPC errors so flaky RPCs don't block legitimate buys.

HONEYPOT_BLACKLIST_HOURS = 24


async def check_freeze_authority(
    session: aiohttp.ClientSession,
    rpc_url: str,
    mint: str,
) -> tuple[bool, str]:
    """Return (is_safe, detail).

    (True, "no freeze authority")        — mint safe to trade
    (False, "<authority_pubkey>")         — freeze authority is set → honeypot
    (True, "rpc error: …" / "missing …") — fail-open on any RPC or parse
                                           issue (don't block genuine buys
                                           on transient RPC errors).
    """
    if not rpc_url or not mint:
        return True, "rpc_url or mint missing (fail-open)"
    try:
        result = await _arpc_post(
            session, rpc_url,
            "getAccountInfo",
            [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        )
        value = (result.get("result") or {}).get("value")
        if not value:
            return True, "mint account not found (fail-open)"
        info = (
            ((value.get("data") or {}).get("parsed") or {}).get("info") or {}
        )
        freeze = info.get("freezeAuthority")
        if freeze:
            return False, str(freeze)
        return True, "no freeze authority"
    except Exception as exc:
        logger.warning(
            f"[HONEYPOT CHECK] {mint[:8]} RPC error "
            f"({type(exc).__name__}): {exc} — fail-open"
        )
        return True, f"rpc error: {type(exc).__name__}"


async def _honeypot_guard(
    session: aiohttp.ClientSession,
    rpc_url: str,
    mint: str,
    symbol: str,
    source_label: str,
) -> bool:
    """Wraps check_freeze_authority with the common side-effects:
    blacklist 24h, Telegram alert, error log. Returns True to proceed,
    False to skip the buy. Fail-open on RPC errors (also returns True)."""
    safe, detail = await check_freeze_authority(session, rpc_url, mint)
    if safe:
        return True

    logger.info(f"[HONEYPOT] {mint[:8]} — freeze authority active, skipping")
    _token_blacklist[mint] = time.time() + HONEYPOT_BLACKLIST_HOURS * 3600
    try:
        _save_blacklist()
    except Exception as exc:
        logger.warning(f"[HONEYPOT] blacklist save failed: {exc}")

    send_telegram(
        f"🍯 <b>HONEYPOT DETECTED</b> — {symbol} ({mint[:8]})\n"
        f"CA: <code>{mint}</code>\n"
        f"Freeze authority active — buy skipped"
    )
    _apex_log_error(
        mint, source_label, "honeypot_freeze_authority",
        {"freeze_authority": detail, "blacklist_hours": HONEYPOT_BLACKLIST_HOURS},
    )
    return False


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


def _save_traded_tokens() -> None:
    """Atomically persist _traded_tokens to disk."""
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        tmp = TRADED_TOKENS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(sorted(_traded_tokens), f, indent=2)
        os.replace(tmp, TRADED_TOKENS_FILE)
    except Exception as e:
        logger.warning(f"[PERSIST] Could not save traded tokens: {e}")


def _load_traded_tokens() -> set[str]:
    """Load traded tokens from disk. Returns empty set on missing file or parse error."""
    try:
        with open(TRADED_TOKENS_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(m) for m in data}
        return set()
    except FileNotFoundError:
        return set()
    except Exception as e:
        logger.warning(f"[PERSIST] Could not load traded tokens from {TRADED_TOKENS_FILE}: {e}")
        return set()


def _mark_token_traded(token_mint: str) -> None:
    """Record token as fully exited — never rebuy. Persists to disk."""
    if not token_mint or token_mint in _traded_tokens:
        return
    _traded_tokens.add(token_mint)
    _save_traded_tokens()


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


HYBRID_PREGRAD_TRAIL_PCT   = 30.0   # trail for positions entered pre-graduation
HYBRID_GRADUATED_TRAIL_PCT = 35.0   # trail for positions entered after graduation


def _hybrid_trail_pct(pos: dict) -> float:
    """Return the trailing-stop width for this position: 20% pre-grad, 25% graduated."""
    return HYBRID_PREGRAD_TRAIL_PCT if pos.get("was_pregrad", False) else HYBRID_GRADUATED_TRAIL_PCT


def _hard_floor_check(pnl_pct: float, min_target_hit: bool) -> str | None:
    """Pre-TP1 hard floor exit decision. Returns an exit reason string when
    the position has dropped past HARD_FLOOR_PCT from entry and TP1 has
    not fired yet; otherwise None.

    Post-TP1 (min_target_hit=True) the trailing stop owns the downside —
    by then the entry basis has been recalibrated and the remainder is
    effectively a free ride, so hard floor would never fire correctly
    anyway.

    Source-agnostic: applies to whale_copy, momentum_scanner, cto_signal,
    dip_sniper, and cluster_buy positions identically.
    """
    if not HARD_FLOOR_ENABLED:
        return None
    if min_target_hit:
        return None
    if pnl_pct <= HARD_FLOOR_PCT:
        return f"HARD FLOOR ({pnl_pct:+.1f}% <= {HARD_FLOOR_PCT:.0f}%)"
    return None


def _hybrid_exit_check(pos: dict, drop_from_peak: float) -> str | None:
    """Primary exit: a simple percent-from-peak trailing stop with NO time
    stop, NO hard floor, and NO min-target gating. Trail width depends on
    whether the token was pre-graduation at entry (20%) vs already
    graduated (25%). The -25% drawdown alert still fires on the way down
    but doesn't sell; only a peak-drop ≥ trail_pct or the whale-full-exit
    emergency (handled separately) closes the position.
    Returns the exit_reason string on trigger, else None."""
    trail_pct = _hybrid_trail_pct(pos)
    if drop_from_peak <= -trail_pct:
        tag = "pre-grad" if pos.get("was_pregrad", False) else "graduated"
        return (
            f"TRAILING STOP ({trail_pct:.0f}% {tag}, peak drop "
            f"{abs(drop_from_peak):.1f}%)"
        )
    return None


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

    Primary exit is whale mirror sell (handled separately).
    Secondary safety net:
      - Trailing stop at tier["trail_pct"] below peak (after min_target_hit)
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

            # Honeypot guard — skip any mint with an active freeze authority
            if not await _honeypot_guard(session, _rpc_url, token_mint,
                                         symbol="?", source_label="dip_sniper"):
                continue

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
                    "entry_time":         time.time(),
                    "entry_sol":          entry_sol,
                    "original_entry_sol": entry_sol,   # never mutated — real PnL baseline
                    "tp1_received_sol":   0.0,          # accumulates partial-exit proceeds
                    "peak_sol":           entry_sol,
                    "amount_tokens":      token_units,
                    "whale":              "dip_sniper",
                    "buy_sol":            buy_sol,
                    "claude_score":       claude_score,
                    "min_target_hit":     False,
                    "alerted_25pct_down": False,
                    "source":             "dip_sniper",
                }
                _save_positions()
                _stats["trades_executed"] += 1
                logger.info(
                    f"[DIP SNIPER] {token_mint[:8]} | Entered {token_units:,} tokens "
                    f"@ {entry_sol:.4f} SOL | Claude: {claude_score}"
                )
                logger.info(f"[BUY/SELL SIG] {token_mint[:8]} dip_sniper_buy sig={swap_sig}")
                send_telegram(
                    f"✅ <b>DIP SNIPER BUY</b> — <code>{token_mint[:8]}</code>\n"
                    f"CA: <code>{token_mint}</code>\n"
                    f"Entry: {entry_sol:.4f} SOL | Score: {claude_score}/100"
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
            # PumpPortal returns the detailed reason in exc.message (HTTP
            # status line), while body_preview is usually just "Bad Request".
            # Check both so migrated-token detection actually fires.
            _haystack = f"{body_preview} {getattr(exc, 'message', '') or ''}".lower()
            if exc.status == 400 and any(
                kw in _haystack
                for kw in ("migrated", "does not exist", "pump-amm")
            ):
                logger.warning(
                    f"[PUMPFUN BUY] Token migrated to pump-amm — use Jupiter "
                    f"(400: {(getattr(exc, 'message', '') or body_preview)[:160]})"
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

def _abandon_unsellable_position(
    pos: dict,
    token_mint: str,
    sell_msg: str,
    *,
    abandon_qualifier: str = "",
    exit_reason: str = "",
) -> None:
    """Finalize an unsellable position after SELL_ABANDON_AFTER_FAILURES
    consecutive sell failures. Logs the trade as 'abandoned_unsellable',
    removes the position, blacklists the token, updates daily stats with
    realised PnL (computed against exit_sol = 0 so any prior TP1 / mirror
    proceeds still count via _real_pnl), and sends the abandonment alert.

    abandon_qualifier is a path-specific phrase interpolated into the
    user-facing messages — "whale_full_exit" for the whale-exit path so
    the message reads "consecutive whale_full_exit sell failures", "" for
    the generic trailing-stop / hard-floor / max-hold paths.

    exit_reason is the trigger string that originally fired the sell;
    included in the structured warning log when present, omitted when "".
    """
    _exit_label = pos.get("token_label") or token_mint[:8]
    abn_real_sol, abn_real_pct = _real_pnl(pos, 0.0)
    _log_trade(pos, "abandoned_unsellable", 0.0, token_mint)
    del open_positions[token_mint]
    _save_positions()
    _mark_token_traded(token_mint)
    _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
    _save_blacklist()
    _sell_failure_counts.pop(token_mint, None)
    if abn_real_pct >= 0:
        _stats["wins"] += 1
    else:
        _stats["losses"] += 1
    _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + abn_real_sol, 6)
    _record_trade(abn_real_sol)
    sign = "+" if abn_real_pct >= 0 else ""
    qualifier_phrase = f"{abandon_qualifier} " if abandon_qualifier else ""
    send_telegram(
        f"🗑 <b>POSITION ABANDONED</b> — {_exit_label}\n"
        f"CA: <code>{token_mint}</code>\n"
        f"Reason: unsellable after {SELL_ABANDON_AFTER_FAILURES} "
        f"consecutive {qualifier_phrase}sell failures\n"
        f"Last error: {sell_msg[:200]}\n"
        f"Final PnL: {sign}{abn_real_pct:.1f}% "
        f"({sign}{abn_real_sol:.4f} SOL)\n"
        f"Token blacklisted for {BLACKLIST_MINUTES}min."
    )
    trigger_phrase = f"| trigger was: {exit_reason} " if exit_reason else ""
    logger.warning(
        f"[{token_mint[:8]}] ABANDONED after "
        f"{SELL_ABANDON_AFTER_FAILURES} consecutive "
        f"{qualifier_phrase}sell failures {trigger_phrase}| real PnL: "
        f"{sign}{abn_real_pct:.1f}%"
    )


def _finalize_successful_exit(
    pos: dict,
    token_mint: str,
    current_sol: float,
    log_reason: str,
    *,
    blacklist: bool,
) -> tuple[float, float]:
    """Shared housekeeping after a successful exit sell. Computes realised
    PnL via _real_pnl (accounts for prior TP1 / partial-mirror proceeds),
    writes the trade record, removes the position from open_positions,
    marks the token as traded, optionally blacklists it (trailing-stop
    exits blacklist; whale-full-exit and hard-floor / max-hold do not),
    updates daily stats, resets the per-position sell-failure counter, and
    records the trade for the rolling PnL window.

    Returns (real_pnl_sol, real_pnl_pct) so callers can compose their own
    Telegram message — success messages differ enough between paths
    (whale dump vs trade summary) that they stay bespoke at the call site.
    """
    _sell_failure_counts.pop(token_mint, None)
    real_pnl_sol, real_pnl_pct = _real_pnl(pos, current_sol)
    _log_trade(pos, log_reason, current_sol, token_mint)
    del open_positions[token_mint]
    _save_positions()
    _mark_token_traded(token_mint)
    if blacklist:
        _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
        _save_blacklist()
    if real_pnl_pct >= 0:
        _stats["wins"] += 1
    else:
        _stats["losses"] += 1
    _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + real_pnl_sol, 6)
    _record_trade(real_pnl_sol)
    return real_pnl_sol, real_pnl_pct


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

    # Trailing-stop arming: peak tracking and trailing-exit checks only run
    # AFTER TP1 has fired (min_target_hit=True). Pre-TP1 the position holds
    # unless whale_full_exit / emergency_dump / TP1 triggers — see below.
    _trail_armed = pos.get("min_target_hit", False)

    # Update peak only after TP1; otherwise leave peak_sol at its last value
    # (set to _partial_received_sol at TP1) so the trailing stop runs from
    # the post-TP1 baseline rather than an arbitrary pre-TP1 high.
    if _trail_armed and current_sol > peak_sol:
        open_positions[token_mint]["peak_sol"] = current_sol
        peak_sol = current_sol
        _save_positions()

    pnl_pct       = (current_sol / entry_sol - 1) * 100 if entry_sol > 0 else 0.0
    drop_from_peak = (current_sol / peak_sol  - 1) * 100 if peak_sol  > 0 else 0.0

    logger.debug(
        f"[{token_mint[:8]}] hold — pnl={pnl_pct:+.1f}% | "
        f"peak_drop={drop_from_peak:.1f}% | {elapsed_min:.0f}m elapsed | "
        f"MC now={mc_now:,.0f} | MC entry={mc_entry:,.0f}"
    )

    # --- -25% drawdown alert (one-shot per position) -------------------
    if pnl_pct <= -25.0 and not pos.get("alerted_25pct_down", False):
        whale_name = pos.get("whale", "unknown")
        send_telegram(
            f"⚠️ <b>DOWN 25%</b> — <code>{token_mint[:8]}</code>\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Whale: {whale_name}\n"
            f"Entry: {entry_sol:.6f} SOL | Current: {current_sol:.6f} SOL\n"
            f"PnL: {pnl_pct:.1f}%"
        )
        open_positions[token_mint]["alerted_25pct_down"] = True
        logger.info(f"[{token_mint[:8]}] -25% drawdown alert sent (whale={whale_name})")
    # -------------------------------------------------------------------

    # --- WHALE BALANCE WATCH: only a full whale exit (balance → 0) forces a sell ---
    _whale_wallet = pos.get("whale_wallet")
    _whale_name   = pos.get("whale_name")
    _whale_entry_bal = pos.get("whale_entry_balance", 0) or 0

    # Re-hydrate a zero entry balance. The initial fetch at position open
    # can fail (RPC flake, or the whale's token account hasn't propagated
    # on-chain yet — we react within seconds of the whale's buy). If it
    # stays 0, the mirror-sell block below is permanently gated OFF and we
    # miss every whale exit. On the first monitor tick where we see 0,
    # re-fetch and adopt whatever balance exists now as the entry baseline.
    if _whale_wallet and _whale_entry_bal <= 0:
        _rehydrated = await get_spl_token_balance(session, token_mint, _whale_wallet)
        if _rehydrated > 0:
            _whale_entry_bal = _rehydrated
            open_positions[token_mint]["whale_entry_balance"]   = _rehydrated
            open_positions[token_mint]["whale_current_balance"] = _rehydrated
            _save_positions()
            logger.info(
                f"[MIRROR] {token_mint[:8]} | Re-hydrated whale_entry_balance "
                f"0 → {_rehydrated:,} (initial entry fetch returned 0, mirror "
                f"detection now armed for {_whale_name})"
            )
        else:
            logger.debug(
                f"[MIRROR] {token_mint[:8]} | whale balance still 0 — "
                f"mirror detection disabled this tick (will retry next cycle)"
            )

    # --- WHALE FULL-EXIT EMERGENCY ------------------------------------
    # New hybrid strategy: ignore partial whale sells entirely (trailing stop
    # is the primary exit). A whale fully dumping (balance → 0) is the only
    # whale signal that still forces an emergency sell regardless of trail.
    if _whale_wallet and _whale_entry_bal > 0:
        try:
            _whale_now_bal = await get_spl_token_balance(session, token_mint, _whale_wallet)
        except Exception as exc:
            logger.debug(f"[WHALE] {token_mint[:8]} | whale balance fetch failed: {exc}")
            _whale_now_bal = _whale_entry_bal  # fail-open: assume no change

        open_positions[token_mint]["whale_current_balance"] = _whale_now_bal

        if _whale_now_bal == 0:
            # Whale fully exited — treat as emergency and close our bag.
            _exit_label = pos.get("token_label") or token_mint[:8]
            logger.info(
                f"[WHALE FULL EXIT] {token_mint[:8]} | {_whale_name} dumped 100% — "
                f"emergency exit triggered"
            )

            _live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
            if _live_tokens <= 0:
                logger.warning(
                    f"[WHALE FULL EXIT] {token_mint[:8]} | on-chain balance is 0 — "
                    f"nothing to sell; closing position record"
                )
                # Still record the close so we don't keep evaluating a phantom pos.
                _log_trade(pos, "whale_full_exit", 0.0, token_mint)
                del open_positions[token_mint]
                _save_positions()
                _mark_token_traded(token_mint)
                send_telegram(
                    f"🚨 <b>WHALE FULLY EXITED</b> — {_whale_name} dumped 100% — "
                    f"emergency exit triggered\n"
                    f"Token: {_exit_label} (wallet empty, position record closed)\n"
                    f"CA: <code>{token_mint}</code>"
                )
                return
            open_positions[token_mint]["amount_tokens"] = _live_tokens
            _save_positions()

            if DRY_RUN:
                _wfe_sig = "DRY_RUN_WHALE_FULL_EXIT_SIG"
                logger.info(
                    f"[DRY RUN] whale_full_exit would sell {_live_tokens:,} → "
                    f"~{current_sol:.4f} SOL"
                )
            else:
                _wfe_sig, _wfe_msg = await execute_sell_routed(
                    session, token_mint, _live_tokens, wallet_pubkey, mc_now
                )
                if not _wfe_sig:
                    # Cap consecutive whale_full_exit retries the same way the
                    # trailing-stop path does — otherwise a zero-liquidity zombie
                    # token retries forever every monitor tick.
                    _wfe_fc = _sell_failure_counts.get(token_mint, 0) + 1
                    _sell_failure_counts[token_mint] = _wfe_fc
                    logger.error(
                        f"[WHALE FULL EXIT] {token_mint[:8]} | sell failed "
                        f"({_wfe_msg}) — attempt {_wfe_fc}/{SELL_ABANDON_AFTER_FAILURES}"
                    )
                    _apex_log_error(
                        token_mint, _whale_name, "whale_full_exit_sell_failed",
                        {"msg": _wfe_msg, "attempt": _wfe_fc},
                    )
                    if _wfe_fc >= SELL_ABANDON_AFTER_FAILURES:
                        _abandon_unsellable_position(
                            pos, token_mint, _wfe_msg,
                            abandon_qualifier="whale_full_exit",
                        )
                        return
                    send_telegram(
                        f"⚠️ <b>WHALE FULL EXIT — SELL FAILED</b>\n"
                        f"Token: {_exit_label}\n"
                        f"CA: <code>{token_mint}</code>\n"
                        f"Reason: {_wfe_msg}\n"
                        f"Attempt {_wfe_fc}/{SELL_ABANDON_AFTER_FAILURES} — "
                        f"position stays open, will retry"
                    )
                    _save_positions()
                    return

            # Successful sell — finalize via shared helper.
            _wfe_real_sol, _wfe_real_pct = _finalize_successful_exit(
                pos, token_mint, current_sol, "whale_full_exit",
                blacklist=False,
            )
            _sign = "+" if _wfe_real_pct >= 0 else ""
            logger.info(f"[BUY/SELL SIG] {token_mint[:8]} whale_full_exit sig={_wfe_sig}")
            send_telegram(
                f"🚨 <b>WHALE FULLY EXITED</b> — {_whale_name} dumped 100% — "
                f"emergency exit triggered\n"
                f"Token: {_exit_label}\n"
                f"CA: <code>{token_mint}</code>\n"
                f"Received: ~{current_sol:.4f} SOL\n"
                f"PnL: {_sign}{_wfe_real_pct:.1f}% ({_sign}{_wfe_real_sol:.4f} SOL)"
            )
            logger.info(
                f"[WHALE FULL EXIT] {token_mint[:8]} | CLOSED | "
                f"Whale: {_whale_name} | PnL: {_sign}{_wfe_real_pct:.1f}%"
            )
            return

        # Any other delta (partial sell, buy more, unchanged) is intentionally
        # ignored under the hybrid strategy — trailing stop governs exits.
        _save_positions()
    # --- END WHALE FULL-EXIT EMERGENCY --------------------------------

    # --- Pre-TP1 hard floor --------------------------------------------
    # Force-exit when pnl drops past HARD_FLOOR_PCT before TP1 arms.
    # This is the only stop loss for positions that never reach +100%
    # (e.g. momentum_scanner picks that go straight down) and the
    # earliest stop loss for whale_copy positions other than a full
    # whale dump.
    min_target_hit = pos.get("min_target_hit", False)
    _hf_exit_reason = _hard_floor_check(pnl_pct, min_target_hit)
    if _hf_exit_reason is not None:
        _hf_label = pos.get("token_label") or token_mint[:8]
        _hf_symbol = pos.get("token_symbol") or _hf_label
        _hf_live_tokens = await get_spl_token_balance(session, token_mint, wallet_pubkey)
        if _hf_live_tokens <= 0:
            logger.warning(
                f"[{token_mint[:8]}] on-chain balance is 0 — aborting hard floor "
                f"sell (trigger: {_hf_exit_reason})"
            )
            send_telegram(
                f"⚠️ <b>HARD FLOOR ABORTED</b> — {_hf_label}\n"
                f"CA: <code>{token_mint}</code>\n"
                f"Trigger: {_hf_exit_reason}\n"
                f"Wallet shows no token balance on-chain"
            )
            return
        open_positions[token_mint]["amount_tokens"] = _hf_live_tokens
        _save_positions()

        if DRY_RUN:
            _hf_sig = "DRY_RUN_HARD_FLOOR_SIG"
            logger.info(
                f"[DRY RUN] hard_floor would sell {_hf_live_tokens:,} → "
                f"~{current_sol:.4f} SOL ({_hf_exit_reason})"
            )
        else:
            _hf_sig, _hf_msg = await execute_sell_routed(
                session, token_mint, _hf_live_tokens, wallet_pubkey, mc_now
            )
            if not _hf_sig:
                _hf_fc = _sell_failure_counts.get(token_mint, 0) + 1
                _sell_failure_counts[token_mint] = _hf_fc
                logger.error(
                    f"[{token_mint[:8]}] hard floor sell failed ({_hf_msg}) — "
                    f"attempt {_hf_fc}/{SELL_ABANDON_AFTER_FAILURES}. "
                    f"Trigger was: {_hf_exit_reason}"
                )
                _apex_log_error(
                    token_mint, pos.get("whale_name") or pos.get("whale"),
                    "hard_floor_sell_failed",
                    {"msg": _hf_msg, "trigger": _hf_exit_reason, "attempt": _hf_fc},
                )
                if _hf_fc >= SELL_ABANDON_AFTER_FAILURES:
                    _abandon_unsellable_position(
                        pos, token_mint, _hf_msg,
                        exit_reason=_hf_exit_reason,
                    )
                    return
                return  # retry next tick — position stays open

        # Successful sell — finalize via shared helper. Blacklist on
        # hard-floor exits for the same reason trailing-stop blacklists:
        # the token already dumped on us once, avoid re-entry during
        # cooldown.
        _hf_real_sol, _hf_real_pct = _finalize_successful_exit(
            pos, token_mint, current_sol, "hard_floor",
            blacklist=True,
        )
        _hf_sign = "+" if _hf_real_pct >= 0 else ""
        logger.info(
            f"[{token_mint[:8]}] hard_floor sig={_hf_sig} | real PnL: "
            f"{_hf_sign}{_hf_real_pct:.1f}% ({_hf_sign}{_hf_real_sol:.4f} SOL)"
        )
        send_telegram(
            f"🛡️ <b>HARD FLOOR</b> — {_hf_symbol} hard floor at "
            f"{_hf_sign}{_hf_real_pct:.1f}% — exiting\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Trigger: {_hf_exit_reason}\n"
            f"Realised PnL: {_hf_sign}{_hf_real_pct:.1f}% "
            f"({_hf_sign}{_hf_real_sol:.4f} SOL)"
        )
        return
    # --- END HARD FLOOR ------------------------------------------------

    # --- MANNOS tiered exit (secondary safety net) ---
    claude_score   = pos.get("claude_score", 70)
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
                f"CA: <code>{token_mint}</code>\n"
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
                _apex_log_error(
                    token_mint, pos.get("whale_name") or pos.get("whale"),
                    "tp1_sell_failed", {"msg": _tp1_msg},
                )

        if _tp1_ok:
            # Reduce cost basis by SOL received; position is now a free ride.
            # Setting entry_sol ≈ 0 disables hard-floor protection naturally —
            # pnl_pct against this shrunk basis will stay extremely positive,
            # preventing the MANNOS hard floor from firing on the remainder.
            # For real PnL reporting we instead use original_entry_sol +
            # tp1_received_sol via _real_pnl() — see check_and_maybe_exit.
            _new_entry = max(entry_sol - _partial_received_sol, 0.0001)
            # Accumulate (don't overwrite) — a prior partial mirror sell may
            # have already booked proceeds into this field.
            _prev_partial = float(pos.get("tp1_received_sol") or 0.0)
            open_positions[token_mint].update({
                "amount_tokens":    _remain_tokens,
                "min_target_hit":   True,
                "tp1_received_sol": _prev_partial + _partial_received_sol,
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

        # Hybrid strategy — trail width is the SAME as the primary exit (20%
        # pre-grad, 25% graduated). The tier-derived number would be wrong.
        _tp1_trail_pct = _hybrid_trail_pct(pos)
        if _tp1_ok:
            logger.info(f"[BUY/SELL SIG] {token_mint[:8]} tp1 sig={_tp1_sig}")
            send_telegram(
                f"💰 <b>TP1 HIT</b> — {_tp1_label}\n"
                f"CA: <code>{token_mint}</code>\n"
                f"Sold {TAKE_PROFIT_PCT:.0f}% at 2x\n"
                f"Received: {_partial_received_sol:.4f} SOL (initial back)\n"
                f"Remainder: {_partial_received_sol:.4f} SOL riding free\n"
                f"Trailing stop: {_tp1_trail_pct:.0f}% from peak"
            )
        else:
            send_telegram(
                f"💰 <b>TP1 HIT</b> — {_tp1_label}\n"
                f"CA: <code>{token_mint}</code>\n"
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

    # --- Hybrid primary exit: trailing stop only ----------------------
    # Trailing stop is ARMED only after TP1 fires (min_target_hit=True).
    # Pre-TP1 the position holds — exits come only from whale_full_exit
    # (handled above) or emergency_dump_check (separate task). Post-TP1
    # the remainder rides with the tier trail width (30% pre-grad / 35%
    # graduated) from the post-TP1 peak.
    if not _trail_armed:
        logger.debug(
            f"[EXIT] {token_mint[:8]} | trailing stop NOT armed (pre-TP1) | "
            f"pnl={pnl_pct:+.1f}% — holding"
        )
        return  # no exit until TP1 hits

    _trail_pct_current = _hybrid_trail_pct(pos)
    logger.debug(
        f"[EXIT] {token_mint[:8]} | Peak: {peak_sol:.4f} SOL | "
        f"Current: {current_sol:.4f} SOL | Trail: {_trail_pct_current:.0f}% "
        f"({'pre-grad' if pos.get('was_pregrad', False) else 'graduated'}) | "
        f"peak_drop={drop_from_peak:.1f}%"
    )
    exit_reason = _hybrid_exit_check(pos, drop_from_peak)

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
            f"CA: <code>{token_mint}</code>\n"
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
            # Increment consecutive-failure counter for this position.
            # Zombies with zero liquidity were previously retrying every
            # monitor tick forever — cap attempts and abandon if exhausted.
            _fc = _sell_failure_counts.get(token_mint, 0) + 1
            _sell_failure_counts[token_mint] = _fc
            logger.error(
                f"[{token_mint[:8]}] Sell swap failed ({sell_msg}) — "
                f"attempt {_fc}/{SELL_ABANDON_AFTER_FAILURES}. "
                f"Trigger was: {exit_reason}"
            )
            _apex_log_error(
                token_mint, pos.get("whale_name") or pos.get("whale"),
                "trailing_stop_sell_failed",
                {"msg": sell_msg, "trigger": exit_reason, "attempt": _fc},
            )
            if _fc >= SELL_ABANDON_AFTER_FAILURES:
                _abandon_unsellable_position(
                    pos, token_mint, sell_msg,
                    exit_reason=exit_reason,
                )
                return
            return  # don't clear position if live sell tx failed — will retry next cycle

    # Successful sell — gather display values, classify log_reason, then
    # delegate housekeeping to _finalize_successful_exit.
    _original_entry = float(pos.get("original_entry_sol") or entry_sol or 0) or entry_sol
    _tp1_rec        = float(pos.get("tp1_received_sol") or 0.0)

    # Fetch exit MC for sell summary (non-blocking; fails open to "—")
    _sell_dex = await fetch_dexscreener(session, token_mint)
    mc_exit   = float((_sell_dex or {}).get("marketCap") or (_sell_dex or {}).get("fdv") or 0)
    mc_entry_stored  = pos.get("mc_entry", 0)
    token_label_sell = pos.get("token_label") or token_mint[:8]

    # Classify reason from the human-readable exit_reason string.
    # whale_full_exit never reaches this block (handled inline above) but
    # the branch is kept for defensive future-proofing.
    _er_lower = (exit_reason or "").lower()
    if   "whale_full_exit" in _er_lower or "whale full exit" in _er_lower:
        _log_reason = "whale_full_exit"
    elif "trailing" in _er_lower:   _log_reason = "trailing_stop"
    elif "time stop" in _er_lower:  _log_reason = "time_stop"
    elif "hard floor" in _er_lower: _log_reason = "hard_floor"
    elif "take profit" in _er_lower or "take-profit" in _er_lower or "tp" in _er_lower:
        _log_reason = "take_profit"
    else:                           _log_reason = exit_reason or "unknown"

    # Blacklist on trailing stop ONLY — take profit and time stop allow re-entry
    _blacklist_this_exit = exit_reason.startswith("TRAILING STOP")

    _real_pnl_sol, _real_pnl_pct = _finalize_successful_exit(
        pos, token_mint, current_sol, _log_reason,
        blacklist=_blacklist_this_exit,
    )
    if _blacklist_this_exit:
        logger.info(
            f"[{token_mint[:8]}] Blacklisted for {BLACKLIST_MINUTES}min "
            f"(trailing stop loss — will not re-enter until cooldown expires)"
        )

    pnl_sign = "+" if _real_pnl_pct >= 0 else ""
    emoji    = "💰" if _real_pnl_pct >= 0 else "🛑"

    mc_entry_str = _fmt_usd(mc_entry_stored) if mc_entry_stored else "—"
    mc_exit_str  = _fmt_usd(mc_exit)         if mc_exit         else "—"
    _partial_line = (
        f"  Partial sold: {_tp1_rec:.4f} SOL (TP1/mirror)\n" if _tp1_rec > 0 else ""
    )

    logger.info(
        f"[{token_mint[:8]}] {exit_reason} | "
        f"Entry: {_original_entry:.4f} SOL | Partial+Exit: "
        f"{_tp1_rec:.4f}+{current_sol:.4f} SOL | "
        f"PnL: {pnl_sign}{_real_pnl_pct:.1f}% ({pnl_sign}{_real_pnl_sol:.4f} SOL)"
    )
    logger.info(f"[BUY/SELL SIG] {token_mint[:8]} trailing_stop sig={sell_sig}")
    send_telegram(
        f"{emoji} <b>SELL</b> — {token_label_sell}\n"
        f"CA: <code>{token_mint}</code>\n"
        f"Reason: {exit_reason}\n"
        f"\n📊 <b>Trade Summary:</b>\n"
        f"  MC Entry:  {mc_entry_str}\n"
        f"  MC Exit:   {mc_exit_str}\n"
        f"  Entry:     {_original_entry:.4f} SOL\n"
        f"{_partial_line}"
        f"  Final exit: {current_sol:.4f} SOL\n"
        f"  PnL:       {pnl_sign}{_real_pnl_pct:.1f}% "
        f"({pnl_sign}{_real_pnl_sol:.4f} SOL)"
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
            f"CA: <code>{token_mint}</code>\n"
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
            _apex_log_error(
                token_mint, pos.get("whale_name") or pos.get("whale"),
                "emergency_dump_sell_failed", {"msg": sell_msg},
            )
            return

    _emg_real_sol, _emg_real_pct = _real_pnl(pos, current_sol)
    _emg_original_entry = float(pos.get("original_entry_sol") or entry_sol or 0) or entry_sol
    _log_trade(pos, "emergency_dump", current_sol, token_mint)
    del open_positions[token_mint]
    _save_positions()
    _mark_token_traded(token_mint)
    _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
    _save_blacklist()
    logger.info(f"[{token_mint[:8]}] Blacklisted {BLACKLIST_MINUTES}min after emergency exit")

    _stats["losses"]      += 1
    _stats["net_pnl_sol"]  = round(_stats["net_pnl_sol"] + _emg_real_sol, 6)
    _record_trade(_emg_real_sol)

    logger.info(f"[BUY/SELL SIG] {token_mint[:8]} emergency_dump sig={sell_sig}")
    send_telegram(
        f"🛑 <b>EMERGENCY EXIT [{token_mint[:8]}]</b>\n"
        f"CA: <code>{token_mint}</code>\n"
        f"Immediate dump — down {abs(_emg_real_pct):.1f}% in {EMERGENCY_CHECK_DELAY_SEC}s\n"
        f"Entry: {_emg_original_entry:.4f} SOL | Exit: {current_sol:.4f} SOL"
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
                {"command": "report",   "description": "7-day Claude trading intelligence report"},
                {"command": "analyse",  "description": "On-demand Claude state analysis"},
                {"command": "insights", "description": "Deep Claude insights over all historical data"},
                {"command": "cleartrades", "description": "Clear the never-rebuy list (allow rebuys again)"},
                {"command": "walletlore", "description": "Wallet intelligence report (Claude analysis)"},
            ]},
            timeout=5,
        )
        logger.info("Telegram commands registered (/summary, /holdings, /wallets, /report, /analyse, /insights, /cleartrades)")
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

                elif text.startswith("/walletlore") and cid in authorised_chats:
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid,
                                  "text": "📚 Building wallet intelligence report... (Claude)",
                                  "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                    except Exception as e:
                        logger.warning(f"/walletlore ack failed: {e}")
                    asyncio.create_task(
                        _send_walletlore_report(triggered_by=f"/walletlore chat={cid}"),
                        name=f"walletlore-{cid}",
                    )
                    logger.info(f"/walletlore command handled (chat {cid})")

                elif text.startswith("/insights") and cid in authorised_chats:
                    # Fast ack, then full history read + sonnet-4-6 analysis
                    # in a background task (can take 10-20s end-to-end).
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid,
                                  "text": "🔍 Reading all stored data... (Claude)",
                                  "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                    except Exception as e:
                        logger.warning(f"/insights ack failed: {e}")
                    asyncio.create_task(
                        _send_insights(
                            tg_session,
                            reply_chat_id=cid,
                            triggered_by=f"/insights chat={cid}",
                            weekly=False,
                        ),
                        name=f"insights-{cid}",
                    )
                    logger.info(f"/insights command handled (chat {cid})")

                elif text.startswith("/analyse") and cid in authorised_chats:
                    # Fast ack, then heavy analysis in a background task so
                    # the command loop stays responsive during the Claude call
                    # and per-position live-PnL quote fetches.
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid,
                                  "text": "🔍 Analysing current state… (Claude)",
                                  "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                    except Exception as e:
                        logger.warning(f"/analyse ack failed: {e}")
                    asyncio.create_task(
                        _send_state_analysis(
                            tg_session,
                            os.getenv("WALLET_PUBLIC_KEY", ""),
                            cid,
                        ),
                        name=f"analyse-{cid}",
                    )
                    logger.info(f"/analyse command handled (chat {cid})")

                elif text.startswith("/report") and cid in authorised_chats:
                    # Acknowledge fast, then kick the heavy Claude call off
                    # as a background task so the command loop stays responsive.
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid,
                                  "text": "📝 Building 7-day report… (Claude)",
                                  "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                    except Exception as e:
                        logger.warning(f"/report ack failed: {e}")
                    asyncio.create_task(
                        _send_daily_report(triggered_by=f"/report chat={cid}"),
                        name=f"daily-report-{cid}",
                    )
                    logger.info(f"/report command handled (chat {cid})")

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

                elif text.startswith("/cleartrades") and cid in authorised_chats:
                    cleared_count = len(_traded_tokens)
                    _traded_tokens.clear()
                    _save_traded_tokens()
                    reply = (
                        f"\u267b\ufe0f <b>Never-rebuy list cleared</b>\n"
                        f"Removed {cleared_count} token(s) — rebuys now allowed on all."
                    )
                    try:
                        async with tg_session.post(
                            f"{base_url}/sendMessage",
                            json={"chat_id": cid, "text": reply, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r:
                            r.raise_for_status()
                            logger.info(f"/cleartrades command handled (chat {cid}) — cleared {cleared_count} token(s)")
                    except Exception as e:
                        logger.warning(f"/cleartrades reply failed: {e}")

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
        # Full sell — real PnL across original entry + any prior partial proceeds
        _manual_real_sol, _manual_real_pct = _real_pnl(pos, expected_sol)
        _manual_sign = "+" if _manual_real_pct >= 0 else ""
        _log_trade(pos, "manual_sell", expected_sol, token_mint)
        del open_positions[token_mint]
        _save_positions()
        _mark_token_traded(token_mint)

        if _manual_real_pct >= 0:
            _stats["wins"] += 1
        else:
            _stats["losses"] += 1
        _stats["net_pnl_sol"] = round(_stats["net_pnl_sol"] + _manual_real_sol, 6)
        _record_trade(_manual_real_sol)

        logger.info(f"[BUY/SELL SIG] {token_mint[:8]} manual_sell_100 sig={sell_sig}")
        await _send_tg(tg_session, base_url, chat_id,
            f"✅ <b>SOLD 100%</b> — {token_label}\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Tokens sold: {sell_tokens:,}\n"
            f"SOL received: {expected_sol:.4f}\n"
            f"PnL: {_manual_sign}{_manual_real_pct:.1f}% "
            f"({_manual_sign}{_manual_real_sol:.4f} SOL)\n"
            f"Position closed."
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

        logger.info(f"[BUY/SELL SIG] {token_mint[:8]} manual_sell_partial sig={sell_sig}")
        await _send_tg(tg_session, base_url, chat_id,
            f"✅ <b>SOLD {sell_pct}%</b> — {token_label}\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Tokens sold: {sell_tokens:,} | Remaining: {remain_tokens:,}\n"
            f"SOL received: {expected_sol:.4f}\n"
            f"PnL on portion: {pnl_sign}{pnl_pct:.1f}%"
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
        f"   MC Entry: {mc_entry_str}",
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

        # --- Cluster detection --------------------------------------------
        # Record this whale's buy signal in the 30-min sliding window, then
        # count how many unique whales have bought this same token. 2+ =
        # CLUSTER (send alert, normal flow), 3+ = MEGA CLUSTER (send alert,
        # force Tier 3 exits + bypass blacklist).
        _cluster_now = time.time()
        _cluster_prune_and_record(token_mint, name, _cluster_now)
        _cluster_whales   = _cluster_unique_whales(token_mint)
        _cluster_size     = len(_cluster_whales)
        _is_mega_cluster  = _cluster_size >= 3
        _is_cluster       = _cluster_size >= 2

        if _is_cluster:
            _prev_tier     = _cluster_alerts_sent.get(token_mint, 0)
            _current_tier  = 3 if _is_mega_cluster else 2
            if _current_tier > _prev_tier:
                # Best-effort DexScreener fetch for symbol + mcap in the alert.
                _clus_dex = await fetch_dexscreener(session, token_mint)
                _clus_sym = (
                    ((_clus_dex or {}).get("baseToken") or {}).get("symbol", "")
                    or token_mint[:8]
                )
                _clus_mcap = float(
                    (_clus_dex or {}).get("marketCap")
                    or (_clus_dex or {}).get("fdv")
                    or 0
                )
                _clus_mcap_str = f"${_clus_mcap:,.0f}" if _clus_mcap else "—"
                _clus_span_min = _cluster_window_span_min(token_mint, _cluster_now) or 1
                _clus_wallets  = " + ".join(_cluster_whales)

                if _is_mega_cluster:
                    _alert = (
                        f"🚨 <b>MEGA CLUSTER</b> — {_cluster_size} whales in agreement\n"
                        f"Token: {_clus_sym} | MCap: {_clus_mcap_str}\n"
                        f"CA: <code>{token_mint}</code>\n"
                        f"Wallets: {_clus_wallets}\n"
                        f"All bought within {_clus_span_min}m\n"
                        f"→ HIGH CONVICTION AUTO-BUY"
                    )
                else:
                    _alert = (
                        f"🔥 <b>CLUSTER BUY</b> — 2 whales in agreement\n"
                        f"Token: {_clus_sym} | MCap: {_clus_mcap_str}\n"
                        f"CA: <code>{token_mint}</code>\n"
                        f"Wallets: {_clus_wallets}\n"
                        f"Both bought within {_clus_span_min}m"
                    )
                send_telegram(_alert)
                _cluster_alerts_sent[token_mint] = _current_tier
                logger.info(
                    f"[CLUSTER] {token_mint[:8]} | tier={_current_tier} | "
                    f"whales={_cluster_whales} | span={_clus_span_min}m"
                )
        # ------------------------------------------------------------------

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
                f"CA: <code>{token_mint}</code>\n"
                f"Whale: <b>{name}</b>\n"
                f"Whale bought then sold within 10s — trade skipped."
            )
            _apex_log_whale_activity(name, token_mint, False, "panic_sell_10s")
            continue

        # Guard 1 — skip if we already hold this token (prevents double-buying)
        if token_mint in open_positions:
            _existing_whale = open_positions[token_mint].get("whale_name") or open_positions[token_mint].get("whale", "?")
            logger.info(
                f"[{name}] SKIP — already holding {token_mint[:8]}, not adding to position "
                f"(originally entered via {_existing_whale})"
            )
            _apex_log_whale_activity(name, token_mint, False, "already_holding",
                                     {"held_by": _existing_whale})
            continue

        # Guard 2 — skip if token is blacklisted after a trailing stop loss
        now_ts = time.time()
        expired_mints = [m for m, exp in _token_blacklist.items() if now_ts >= exp]
        for m in expired_mints:
            del _token_blacklist[m]
            logger.info(f"Blacklist expired — {m[:8]} re-enabled")
        if token_mint in _token_blacklist:
            remaining_min = (_token_blacklist[token_mint] - now_ts) / 60
            if _is_mega_cluster:
                # MEGA CLUSTER overrides blacklist cooldown — 3+ whales
                # agreeing inside 30m outweighs the prior trailing-stop signal.
                logger.info(
                    f"[{name}] MEGA CLUSTER override — {token_mint[:8]} was "
                    f"blacklisted with {remaining_min:.0f}min left, bypassing"
                )
            else:
                logger.info(
                    f"[{name}] SKIP — {token_mint[:8]} blacklisted, "
                    f"{remaining_min:.0f}min remaining after trailing stop"
                )
                _apex_log_whale_activity(name, token_mint, False, "blacklisted",
                                         {"remaining_min": round(remaining_min, 1)})
                continue

        # Guard 3 — never rebuy tokens we've already traded and exited
        if token_mint in _traded_tokens:
            logger.info(f"[{name}] SKIP — {token_mint[:8]} already traded and exited, not rebuying")
            _apex_log_whale_activity(name, token_mint, False, "already_traded", {})
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
            _apex_log_whale_activity(name, token_mint, False, "low_balance",
                                     {"sol_balance": round(sol_balance, 4)})
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
        _bypass_quality = _is_autopilot(name) or name == "mr.putin"

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
                    f"({'AUTOPILOT' if _is_autopilot(name) else 'mr.putin sub-$5k'})"
                )
            else:
                _ok, _reason = passes_pump_quality(pump_data)
                if not _ok:
                    logger.info(
                        f"[{name.upper()}] SKIP — PumpFun quality fail: {_reason} "
                        f"({token_mint[:8]})"
                    )
                    _stats["cancelled_dexscreener"] += 1
                    _apex_log_whale_activity(name, token_mint, False,
                                             "pumpfun_quality_fail", {"detail": _reason})
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
                        f"({'AUTOPILOT' if _is_autopilot(name) else 'mr.putin sub-$5k'})"
                    )
                else:
                    _ok, _reason = passes_dex_quality(dex_pair)
                    if not _ok:
                        logger.info(
                            f"[{name.upper()}] SKIP — DexScreener quality fail: {_reason} "
                            f"({token_mint[:8]})"
                        )
                        _stats["cancelled_dexscreener"] += 1
                        _apex_log_whale_activity(name, token_mint, False,
                                                 "dexscreener_quality_fail", {"detail": _reason})
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
        # Skip for autopilot whales (global autopilot) and mr.putin (sub-$5k, no clean data)
        if not _is_autopilot(name) and name != "mr.putin":
            _safe, _block_reason = await check_token_safety(
                session, token_mint, rpc_url, name, dex_pair=dex_pair
            )
            if not _safe:
                logger.info(
                    f"[{name}] SKIP — safety check failed: {_block_reason} "
                    f"({token_mint[:8]})"
                )
                _stats["cancelled_safety"] += 1
                _apex_log_whale_activity(name, token_mint, False,
                                         "safety_check_failed", {"detail": _block_reason})
                _token_blacklist[token_mint] = time.time() + BLACKLIST_MINUTES * 60
                _save_blacklist()
                continue
        else:
            logger.info(
                f"[{name.upper()}] Safety check skipped "
                f"({'AUTOPILOT' if _is_autopilot(name) else 'mr.putin sub-$5k'})"
            )
        # ----------------------------------------------------------------

        # (Entry quote is fetched at the swap site after sizing — see routing block below)
        # ----------------------------------------------------------------

        # --- Claude confidence scoring ---------------------------------
        if _is_autopilot(name):
            # Autopilot: skip scoring entirely — proceed straight to buy.
            # MEGA CLUSTER (3+ whales in 30m) bumps to Tier 3 (400% min
            # target, 30% trail, no time stop) per user spec. Normal
            # autopilot stays Tier 2 (200% / 25% / no time stop).
            if _is_mega_cluster:
                claude_score = 85   # → Tier 3 in get_exit_tier()
                _tier_label  = f"{name.upper()} AUTOPILOT + MEGA CLUSTER"
            else:
                claude_score = 75   # → Tier 2 in get_exit_tier()
                _tier_label  = f"{name.upper()} AUTOPILOT"
            tier = get_exit_tier(claude_score)
            logger.info(
                f"[{_tier_label}] Claude score skipped — tier: "
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
        # --- Honeypot guard: skip mints whose freeze authority is still set ---
        _hp_symbol = ((dex_pair or {}).get("baseToken") or {}).get("symbol") or "?"
        if not await _honeypot_guard(session, rpc_url, token_mint,
                                     symbol=_hp_symbol, source_label=name):
            _apex_log_whale_activity(name, token_mint, False,
                                     "honeypot_freeze_authority")
            continue

        # --- Max entry mcap filter: skip if mcap > $50k (silent skip, log only) ---
        _mcap_pair = await fetch_dexscreener(session, token_mint)
        _entry_mcap = float(
            (_mcap_pair or {}).get("marketCap") or (_mcap_pair or {}).get("fdv") or 0
        )
        if _entry_mcap > 50000:
            logger.info(
                f"[{name}] SKIP — {token_mint[:8]} mcap ${_entry_mcap:,.0f} "
                f"above $50k max entry threshold"
            )
            _apex_log_whale_activity(name, token_mint, False, "mcap_too_high",
                                     {"mcap_usd": _entry_mcap})
            continue

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
            _apex_log_error(token_mint, name, "whale_buy_failed", {"msg": swap_msg})
            _apex_log_whale_activity(name, token_mint, False,
                                     "buy_tx_failed", {"msg": swap_msg})
            send_telegram(
                f"⚠️ <b>TX FAILED</b> — [{name.upper()}] buy on "
                f"<code>{token_mint[:8]}</code> did not confirm\n"
                f"CA: <code>{token_mint}</code>\n"
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

        msg = (
            f"\U0001f40b <b>APEX WHALE COPY</b> [{name.upper()}]\n"
            f"Token: <code>{token_label}</code>\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Amount: {buy_sol} SOL\n"
            f"MC Entry: {mc_entry_str}\n"
            f"Whale: <code>{name.upper()}</code>"
            f"{tp_block}"
        )
        logger.info(msg)
        # Sig removed from user-facing alert — keep in logs for operator debugging.
        logger.info(f"[BUY SIG] {token_mint[:8]} whale={name} sig={swap_sig}")
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

        # Capture whale's token balance at entry for mirror-sell tracking
        try:
            _whale_entry_bal = await get_spl_token_balance(session, token_mint, wallet)
        except Exception as exc:
            logger.warning(f"[{token_mint[:8]}] Whale balance fetch failed: {exc} — saving with 0")
            _whale_entry_bal = 0

        try:
            # Capture signal context for the daily intelligence report.
            # pump_data.complete=False at entry means the token was pre-graduation.
            _was_pregrad = bool(pump_data is not None and not pump_data.get("complete", False))
            _was_hot     = bool(buys_24h >= HOT_THRESHOLD)
            _conviction  = "high" if _was_hot else "normal"
            _tok_symbol  = ((dex_pair or {}).get("baseToken") or {}).get("symbol", "") or None

            # Source tag: cluster detection wins over plain whale-copy so
            # the daily report can split cluster trades from solo copies.
            _pos_source = "cluster_buy" if _is_cluster else "whale_copy"

            open_positions[token_mint] = {
                "entry_time":            time.time(),
                "entry_sol":             entry_sol,
                "original_entry_sol":    entry_sol,   # never mutated — real PnL baseline
                "tp1_received_sol":      0.0,          # accumulates partial-exit proceeds
                "peak_sol":              entry_sol,   # starts equal to entry
                "amount_tokens":         token_units,
                "whale":                 name,
                "buy_sol":               buy_sol,
                "claude_score":          claude_score,
                "min_target_hit":        False,
                "alerted_25pct_down":    False,
                "source":                _pos_source,
                "cluster_size":          _cluster_size if _is_cluster else None,
                "mc_entry":              mc_entry,
                "token_label":           token_label,
                "token_symbol":          _tok_symbol,
                "whale_wallet":          wallet,
                "whale_name":            name,
                "whale_entry_balance":   _whale_entry_bal,
                "whale_current_balance": _whale_entry_bal,
                # Daily-report context
                "was_hot_whale":         _was_hot,
                "buys_24h_at_entry":     int(buys_24h),
                "was_pregrad":           _was_pregrad,
                "conviction":            _conviction,
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
            logger.error(f"[BUY/SELL SIG] {token_mint[:8]} position_save_failed sig={swap_sig}")
            _apex_log_error(token_mint, name, "position_save_failed",
                            {"msg": str(exc), "sig": swap_sig})
            send_telegram(
                f"🚨 <b>POSITION SAVE FAILED</b>\n"
                f"Token: {token_label}\n"
                f"CA: <code>{token_mint}</code>\n"
                f"Manual intervention needed!"
            )
        _stats["trades_executed"] += 1
        _apex_log_whale_activity(name, token_mint, True, None, {
            "buy_sol":      buy_sol,
            "mc_entry":     mc_entry,
            "claude_score": claude_score,
            "cluster_size": _cluster_size if _is_cluster else None,
        })
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

    # One-shot migration: split state/trade_log.json into ~/apex-data/trades/
    # if that directory is currently empty. Safe to run every startup —
    # skips if any *.json already exists in the target.
    _apex_migrate_legacy_trade_log()
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

    _traded_tokens.update(_load_traded_tokens())
    logger.info(f"Traded-tokens list: {len(_traded_tokens)} token(s) will not be rebought")

    # Re-arm CTO post-entry reviews that were pending across restart. The task
    # sleeps until entry_time + CTO_REVIEW_WAIT_SEC, so stale entries fire
    # immediately and fresh entries wait out the remaining window.
    _cto_wallet_pubkey = os.getenv("WALLET_PUBLIC_KEY", "")
    _rehydrated_reviews = 0
    for _mint, _pos in list(open_positions.items()):
        if _pos.get("source") == "cto_signal" and _pos.get("cto_review_pending"):
            _symbol = (_pos.get("token_label") or "").split(" ", 1)[0] or _mint[:8]
            asyncio.create_task(
                cto_review_task(_mint, _symbol, _cto_wallet_pubkey),
                name=f"cto-review-rehydrate-{_symbol}",
            )
            _rehydrated_reviews += 1
    if _rehydrated_reviews:
        logger.info(f"[CTO REVIEW] Rehydrated {_rehydrated_reviews} pending review task(s) after restart")

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

    # --- Lore wallet tracker bootstrap ---------------------------------
    # lore_wallets.json is the operator-curated address→alias map.
    # wallet_lore.json is the accumulated stats DB; reload it so restarts
    # don't lose history.
    LORE_WALLETS.update(_load_lore_wallets_input())
    _WALLET_LORE.update(_load_wallet_lore())
    logger.info(
        f"[LORE] config loaded — {len(LORE_WALLETS)} wallet(s) in "
        f"lore_wallets.json, {len(_WALLET_LORE)} record(s) in wallet_lore.json"
    )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            whale_poll_loop(session, rpc_url, wallet_pubkey),
            position_monitor_loop(session, wallet_pubkey),
            midnight_summary_loop(),
            telegram_command_loop(),
            dip_sniper_loop(session, wallet_pubkey),
            daily_report_loop(),
            momentum_scanner_loop(session, wallet_pubkey),
            hourly_analyst_loop(session, wallet_pubkey),
            cto_queue_loop(session, rpc_url, wallet_pubkey),
            lore_wallet_poll_loop(session, rpc_url),
            lore_weekly_loop(),
        )


# --- Daily intelligence report ----------------------------------------

def _load_trade_log() -> list[dict]:
    """Load trade_log.json safely. Returns [] on any error."""
    try:
        with open(TRADE_LOG_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    except Exception as exc:
        logger.warning(f"[DAILY REPORT] _load_trade_log failed: {exc}")
        return []


def _recent_trades(days: int) -> list[dict]:
    """Return trades from the last `days` days, sorted oldest → newest."""
    cutoff = time.time() - days * 86_400
    trades = [t for t in _load_trade_log() if float(t.get("exit_time") or 0) >= cutoff]
    trades.sort(key=lambda t: float(t.get("exit_time") or 0))
    return trades


def _normalise_source(src: str | None) -> str:
    """Canonical source bucket. Legacy records may use 'whale' for whale_copy."""
    if not src:
        return "unknown"
    if src == "whale":
        return "whale_copy"
    return src


def _trades_basic_stats(trades: list[dict]) -> dict:
    """Pre-compute aggregates so Claude gets a clean summary alongside raw rows.

    Now also buckets trades by source (whale_copy / momentum_scanner /
    cto_signal / cluster_buy / dip_sniper / …) with wins, losses, win_rate,
    total_pnl for each — enables the daily report to compare strategies.
    """
    total = len(trades)
    if total == 0:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "total_pnl_sol": 0.0, "by_source": {},
        }
    wins    = sum(1 for t in trades if float(t.get("pnl_sol") or 0) >= 0)
    losses  = total - wins
    total_pnl = sum(float(t.get("pnl_sol") or 0) for t in trades)

    by_source: dict[str, dict] = {}
    for t in trades:
        src = _normalise_source(t.get("source"))
        b   = by_source.setdefault(src, {
            "total": 0, "wins": 0, "losses": 0,
            "total_pnl_sol": 0.0, "best_pnl_sol": None, "worst_pnl_sol": None,
        })
        pnl = float(t.get("pnl_sol") or 0)
        b["total"]         += 1
        b["total_pnl_sol"] += pnl
        if pnl >= 0:
            b["wins"]   += 1
        else:
            b["losses"] += 1
        if b["best_pnl_sol"]  is None or pnl > b["best_pnl_sol"]:
            b["best_pnl_sol"]  = pnl
        if b["worst_pnl_sol"] is None or pnl < b["worst_pnl_sol"]:
            b["worst_pnl_sol"] = pnl
    for b in by_source.values():
        b["win_rate"]      = (b["wins"] / b["total"] * 100) if b["total"] else 0.0
        b["total_pnl_sol"] = round(b["total_pnl_sol"], 4)
        if b["best_pnl_sol"]  is not None: b["best_pnl_sol"]  = round(b["best_pnl_sol"], 4)
        if b["worst_pnl_sol"] is not None: b["worst_pnl_sol"] = round(b["worst_pnl_sol"], 4)

    return {
        "total":         total,
        "wins":          wins,
        "losses":        losses,
        "win_rate":      (wins / total) * 100 if total else 0.0,
        "total_pnl_sol": round(total_pnl, 4),
        "by_source":     by_source,
    }


def _momentum_report_stats(trades: list[dict], days: int) -> dict:
    """Momentum-specific aggregates for the daily report block."""
    cutoff = time.time() - days * 86_400
    mom_trades = [
        t for t in trades
        if _normalise_source(t.get("source")) == "momentum_scanner"
    ]
    watches = _load_momentum_watches()
    watches_window = [w for w in watches if float(w.get("ts") or 0) >= cutoff]

    bought = len(mom_trades)
    watch_count = len(watches_window)
    signals = bought + watch_count
    if bought == 0:
        return {
            "signals":    signals,
            "bought":     0,
            "watch_only": watch_count,
            "win_rate":   0.0,
            "total_pnl_sol": 0.0,
            "best_trade": None,
            "worst_trade": None,
            # threshold-analysis buckets (empty here)
            "by_score_band":   {},
            "avg_bonding_pct": None,
            "avg_velocity_pct": None,
            "velocity_winners_avg_velocity": None,
            "velocity_losers_avg_velocity":  None,
        }

    wins      = sum(1 for t in mom_trades if float(t.get("pnl_sol") or 0) >= 0)
    total_pnl = sum(float(t.get("pnl_sol") or 0) for t in mom_trades)
    best      = max(mom_trades, key=lambda t: float(t.get("pnl_sol") or 0))
    worst     = min(mom_trades, key=lambda t: float(t.get("pnl_sol") or 0))

    # Score-band win-rate comparison: 75+ vs 60-74 (watch-only so no trades,
    # but keep bucket in case thresholds change). We only have trades for
    # 75+ by definition (score < 75 doesn't buy), so 75+ is the live band.
    bands: dict[str, dict] = {}
    for t in mom_trades:
        s = int(t.get("momentum_score") or t.get("claude_score") or 0)
        band = "75+" if s >= 75 else "60-74" if s >= 60 else "<60"
        b = bands.setdefault(band, {"total": 0, "wins": 0, "total_pnl_sol": 0.0})
        b["total"]         += 1
        if float(t.get("pnl_sol") or 0) >= 0:
            b["wins"] += 1
        b["total_pnl_sol"] += float(t.get("pnl_sol") or 0)
    for b in bands.values():
        b["win_rate"]      = (b["wins"] / b["total"] * 100) if b["total"] else 0.0
        b["total_pnl_sol"] = round(b["total_pnl_sol"], 4)

    # Averages
    bondings   = [float(t["bonding_pct_entry"]) for t in mom_trades if t.get("bonding_pct_entry") is not None]
    velocities = [float(t["velocity_pct"])      for t in mom_trades if t.get("velocity_pct")      is not None]
    win_velocities  = [float(t["velocity_pct"]) for t in mom_trades if t.get("velocity_pct") is not None and float(t.get("pnl_sol") or 0) >= 0]
    lose_velocities = [float(t["velocity_pct"]) for t in mom_trades if t.get("velocity_pct") is not None and float(t.get("pnl_sol") or 0) <  0]

    return {
        "signals":       signals,
        "bought":        bought,
        "watch_only":    watch_count,
        "win_rate":      (wins / bought * 100) if bought else 0.0,
        "total_pnl_sol": round(total_pnl, 4),
        "best_trade":  {
            "symbol":  best.get("token_symbol"),
            "pnl_sol": round(float(best.get("pnl_sol") or 0), 4),
            "pnl_pct": round(float(best.get("pnl_pct") or 0), 2),
        },
        "worst_trade": {
            "symbol":  worst.get("token_symbol"),
            "pnl_sol": round(float(worst.get("pnl_sol") or 0), 4),
            "pnl_pct": round(float(worst.get("pnl_pct") or 0), 2),
        },
        "by_score_band":   bands,
        "avg_bonding_pct": round(sum(bondings) / len(bondings), 2)   if bondings   else None,
        "avg_velocity_pct": round(sum(velocities) / len(velocities), 2) if velocities else None,
        "velocity_winners_avg_velocity": round(sum(win_velocities) / len(win_velocities), 2)   if win_velocities  else None,
        "velocity_losers_avg_velocity":  round(sum(lose_velocities) / len(lose_velocities), 2) if lose_velocities else None,
    }


def _format_momentum_section(m: dict) -> str:
    """Build the Telegram-friendly momentum block (goes above Claude's analysis)."""
    if m["signals"] == 0 and m["bought"] == 0:
        return "⚡ <b>MOMENTUM SCANNER</b>\nNo signals in window."
    pnl_sign = "+" if m["total_pnl_sol"] >= 0 else ""
    best = m.get("best_trade")  or {}
    worst = m.get("worst_trade") or {}
    best_str = (
        f"{best.get('symbol','?')} {('+' if best.get('pnl_pct', 0) >= 0 else '')}{best.get('pnl_pct', 0):.1f}%"
        if best else "—"
    )
    worst_str = (
        f"{worst.get('symbol','?')} {('+' if worst.get('pnl_pct', 0) >= 0 else '')}{worst.get('pnl_pct', 0):.1f}%"
        if worst else "—"
    )
    return (
        f"⚡ <b>MOMENTUM SCANNER</b>\n"
        f"Signals: {m['signals']} | Bought: {m['bought']} | "
        f"Watch only: {m['watch_only']}\n"
        f"Win rate: {m['win_rate']:.0f}% | PnL: {pnl_sign}{m['total_pnl_sol']:.4f} SOL\n"
        f"Best: {best_str} | Worst: {worst_str}"
    )


async def _ask_claude_for_daily_analysis(trades: list[dict], stats: dict,
                                         momentum: dict) -> str:
    """Send the last-7d trade rows + momentum metadata to Claude and return
    its markdown-ish analysis. Fails open with a stub so the report still
    lands on Telegram if Claude is unreachable or CLAUDE_API_KEY is missing.
    """
    api_key = os.getenv("CLAUDE_API_KEY", "")
    if not api_key:
        return "<i>(Claude analysis unavailable — CLAUDE_API_KEY not set)</i>"

    # Keep the payload lean — send only the fields Claude needs to analyse.
    rows = []
    for t in trades:
        rows.append({
            "symbol":      t.get("token_symbol") or "?",
            "source":      _normalise_source(t.get("source")),
            "whale":       t.get("whale_name") or "?",
            "hold_mins":   t.get("hold_time_mins"),
            "pnl_sol":     t.get("pnl_sol"),
            "pnl_pct":     t.get("pnl_pct"),
            "entry_mcap":  t.get("entry_mcap"),
            "exit_reason": t.get("exit_reason"),
            "hot_whale":   t.get("was_hot_whale"),
            "pregrad":     t.get("was_pregrad"),
            "conviction":  t.get("conviction"),
            # Momentum-specific fields (None for non-momentum trades)
            "momentum_score":    t.get("momentum_score"),
            "bonding_pct_entry": t.get("bonding_pct_entry"),
            "velocity_pct":      t.get("velocity_pct"),
            "replies_at_entry":  t.get("replies_at_entry"),
            "cluster_size":      t.get("cluster_size"),
        })
    prompt = (
        "You are a Solana memecoin trading analyst reviewing the last 7 days of "
        "an Apex trading bot that runs multiple strategies concurrently:\n"
        "  • whale_copy — mirrors trades from 6 tracked wallets\n"
        "  • cluster_buy — 2+ whales converging on the same token (higher conviction)\n"
        "  • momentum_scanner — independent pump.fun scanner (bonding 20-70%, "
        "mcap $8k-$25k, replies ≥20, velocity ≥2.5% in 30s, Claude score ≥75 auto-buys)\n"
        "  • cto_signal — DexAlert verified community-takeover pings\n\n"
        "EXIT STRATEGY (hybrid, recently switched from whale-mirror to trailing-stop):\n"
        "  • Primary exit: pure percent-from-peak trailing stop — 20% for positions "
        "entered pre-graduation, 25% for positions entered after graduation. No time "
        "stop, no hard floor, no tiered min-target gating.\n"
        "  • TP1 sells 75% at 2× to lock initial. The remaining 25% free-rides under "
        "the same 20%/25% trail.\n"
        "  • Whale FULL exit (whale dumps 100% of their bag) is an emergency sell — "
        "exit_reason = 'whale_full_exit'. Whale partial sells are IGNORED.\n"
        "  • -25% drawdown triggers an alert but NOT a sell.\n"
        "  • emergency_dump fires within 5s of entry if price immediately craters.\n\n"
        f"Stats (aggregate + by_source breakdown): {json.dumps(stats)}\n"
        f"Momentum-scanner specifics: {json.dumps(momentum)}\n"
        f"Raw trades (chronological): {json.dumps(rows)}\n\n"
        "Produce a concise Telegram-friendly report (HTML allowed: <b>, <i>). "
        "No markdown fences, no walls of text, use line breaks and emojis. "
        "Cover ALL of these sections:\n"
        "  • 🏆 Best & worst performing wallets / sources (by total PnL + win rate)\n"
        "  • 🎯 Best entry conditions (mcap bands, pre-grad vs graduated, HOT vs normal)\n"
        "  • 🪢 Exit-strategy review — compare trailing_stop vs whale_full_exit vs "
        "take_profit vs time_stop/hard_floor (legacy). Are the 20%/25% trail widths "
        "too tight or too loose given how many trailing_stops closed winners early "
        "vs prevented deep losses? Are whale_full_exit triggers saving trades or "
        "exiting at the bottom? Count occurrences per reason and compute avg PnL.\n"
        "  • 📉 Patterns in losses (exit reasons, hold times, mcap bands)\n"
        "  • ⚡ Momentum scanner review — compare its PnL/win-rate vs whale_copy. "
        "Is the 75 score threshold right (raise/lower)? Is the 2.5% velocity-in-30s "
        "filter catching good coins or too many duds? Is the 20-70% bonding range "
        "still correct? Use the by_score_band and velocity winners/losers numbers.\n"
        "  • 💡 Exactly 3 specific actionable suggestions — at least ONE must be "
        "about momentum-scanner parameter tuning, and at least ONE must be about "
        "the trailing-stop width (suggest keeping, tightening, or widening with "
        "evidence from the trailing_stop trades' PnL distribution).\n"
        "Keep total under 2800 characters. Lead each section with its emoji + "
        "a <b>bold header</b>. Use short bullets starting with '•'. Do NOT emit "
        "raw <tags> like <5min> or <HOT> — they break Telegram HTML parsing."
    )

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if not text:
            return "<i>(Claude returned empty response)</i>"
        return text
    except Exception as exc:
        logger.error(f"[DAILY REPORT] Claude call failed: {exc}")
        return f"<i>(Claude analysis unavailable — {type(exc).__name__})</i>"


_TG_ALLOWED_TAG_PATTERN = re.compile(
    r'</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|a|tg-spoiler)(?:\s[^>]{0,200})?/?>',
    re.IGNORECASE,
)


def _sanitize_tg_html(text: str) -> str:
    """Make arbitrary text safe to embed in Telegram HTML parse_mode.

    Telegram HTML accepts only a fixed tag whitelist; unknown tags like
    Claude emitting a literal <5min> or <HOT> — or even an orphaned '<' in
    "price <200k" — cause the whole message to be rejected with
    "can't parse entities".

    Approach: extract whitelisted tags into NUL-bracketed placeholders,
    HTML-entity-escape everything else (&, <, > all of them), then restore
    the saved tags verbatim. NUL bytes don't appear in Claude output so the
    placeholder can't collide with real text.
    """
    saved: list[str] = []

    def _stash(m: re.Match) -> str:
        saved.append(m.group(0))
        return f"\x00TAG{len(saved) - 1}\x00"

    tmp = _TG_ALLOWED_TAG_PATTERN.sub(_stash, text)
    # Escape order matters: & first, otherwise subsequent < / > escapes
    # would be re-escaped into &amp;lt;.
    tmp = tmp.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    for i, tag in enumerate(saved):
        tmp = tmp.replace(f"\x00TAG{i}\x00", tag, 1)
    return tmp


def _chunk_html_for_telegram(body: str, max_len: int = 4000) -> list[str]:
    """Split an HTML message body into Telegram-safe chunks ≤ max_len chars.

    Breaks at line boundaries where possible so bold/italic tags opened on
    one line and closed on the same line stay intact. A line that itself
    exceeds max_len is hard-sliced as a last resort — rare for Claude
    output which is always short-lined.
    """
    if len(body) <= max_len:
        return [body]
    chunks: list[str] = []
    current = ""
    for line in body.split("\n"):
        if len(line) > max_len:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), max_len):
                chunks.append(line[i:i + max_len])
            continue
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > max_len:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _send_telegram_capture_error(message: str) -> tuple[bool, str]:
    """Like send_telegram() but also returns the Telegram error text on failure.

    send_telegram() swallows exceptions and returns a bare bool, which loses
    the actual Telegram description (e.g. "Bad Request: can't parse entities").
    This variant iterates the same chat IDs, captures per-chat errors, and
    returns (any_ok, combined_error_str) so the daily-report caller can
    log what actually went wrong.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not _telegram_chat_ids:
        return False, "TELEGRAM_BOT_TOKEN or chat IDs not set"
    any_ok = False
    errors: list[str] = []
    for chat_id in _telegram_chat_ids:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=10,
            )
            resp.raise_for_status()
            any_ok = True
        except requests.exceptions.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:400]
            except Exception:
                pass
            errors.append(f"{chat_id} HTTP {e.response.status_code}: {body}")
        except Exception as e:
            errors.append(f"{chat_id} {type(e).__name__}: {e}")
    return any_ok, "; ".join(errors) if errors else ""


async def _send_daily_report(triggered_by: str = "scheduler") -> str:
    """Build + send the daily intelligence report. Returns a short status string.

    Long reports are auto-chunked at line boundaries into Telegram-safe
    pieces (≤ 4000 chars each, below the 4096 HTML ceiling). Each chunk is
    sent independently; a failure on one chunk is logged with the exact
    Telegram error body rather than a bare bool.
    """
    trades   = _recent_trades(DAILY_REPORT_WINDOW_DAYS)
    stats    = _trades_basic_stats(trades)
    momentum = _momentum_report_stats(trades, DAILY_REPORT_WINDOW_DAYS)
    date_str = time.strftime("%Y-%m-%d", time.gmtime())

    if stats["total"] == 0 and momentum["signals"] == 0:
        body = (
            f"📊 <b>APEX DAILY REPORT</b> — {date_str}\n\n"
            f"No closed trades or momentum signals in the last "
            f"{DAILY_REPORT_WINDOW_DAYS} days."
        )
    else:
        analysis_raw = await _ask_claude_for_daily_analysis(trades, stats, momentum)
        # Claude sometimes emits angle-bracketed tokens like <5min> or <HOT>
        # that Telegram's HTML parser rejects ("Unsupported start tag"). Strip
        # anything outside the Telegram whitelist before embedding.
        analysis = _sanitize_tg_html(analysis_raw)
        pnl_sign = "+" if stats["total_pnl_sol"] >= 0 else ""
        momentum_block = _format_momentum_section(momentum)
        body = (
            f"📊 <b>APEX DAILY REPORT</b> — {date_str}\n\n"
            f"Overall: {stats['total']} trades | "
            f"{stats['win_rate']:.0f}% win rate | "
            f"{pnl_sign}{stats['total_pnl_sol']:.4f} SOL\n\n"
            f"{momentum_block}\n\n"
            f"{analysis}"
        )

    # Persist the daily report as a text file: reports/daily/YYYY-MM-DD.txt
    _apex_data_save_text(
        os.path.join(APEX_DATA_REPORTS_DAILY, f"{date_str}.txt"),
        body,
    )

    chunks = _chunk_html_for_telegram(body, max_len=4000)
    total_chunks = len(chunks)
    logger.info(
        f"[DAILY REPORT] built {len(body)} chars → {total_chunks} chunk(s) "
        f"({stats['total']} trades, triggered_by={triggered_by})"
    )

    sent_count  = 0
    failed_msgs: list[str] = []
    for idx, chunk in enumerate(chunks, 1):
        # Prefix multi-part messages so recipients see ordering; keep the
        # header chunk unprefixed so it still begins with the main title.
        if total_chunks > 1 and idx > 1:
            chunk = f"<i>(part {idx}/{total_chunks})</i>\n\n{chunk}"
        ok, err = _send_telegram_capture_error(chunk)
        if ok:
            sent_count += 1
        else:
            failed_msgs.append(f"chunk {idx}/{total_chunks} ({len(chunk)} chars): {err}")
            logger.error(
                f"[DAILY REPORT] send failed — chunk {idx}/{total_chunks}, "
                f"{len(chunk)} chars: {err}"
            )

    if sent_count == total_chunks:
        status = f"sent {total_chunks} chunk(s) ({triggered_by})"
    elif sent_count > 0:
        status = (
            f"partial {sent_count}/{total_chunks} chunk(s) ({triggered_by}) | "
            f"failures: {failed_msgs[0][:200]}"
        )
    else:
        status = f"send failed ({triggered_by}) | {failed_msgs[0][:300] if failed_msgs else 'unknown'}"
    logger.info(f"[DAILY REPORT] {status} — {stats['total']} trades, "
                f"pnl={stats['total_pnl_sol']:+.4f} SOL")
    return status


async def daily_report_loop() -> None:
    """Sleep until the next DAILY_REPORT_UTC_HOURS slot, fire report, repeat.
    23:00 UTC == 09:00 AEST (morning), 11:00 UTC == 21:00 AEST (evening).
    Weekly deep-dive still fires only on Saturday 23:00 UTC."""
    while True:
        now = time.gmtime()
        # Seconds until the soonest scheduled hour (today or rolling to tomorrow)
        candidates = []
        for hr in DAILY_REPORT_UTC_HOURS:
            secs = (hr - now.tm_hour) * 3600 - now.tm_min * 60 - now.tm_sec
            if secs <= 0:
                secs += 86_400
            candidates.append((secs, hr))
        secs_to_next, target_hr = min(candidates)
        logger.info(
            f"[DAILY REPORT] Next run in "
            f"{secs_to_next // 3600}h {(secs_to_next % 3600) // 60}m "
            f"(target {target_hr:02d}:00 UTC, slots {DAILY_REPORT_UTC_HOURS})"
        )
        await asyncio.sleep(secs_to_next)
        try:
            await _send_daily_report(triggered_by="scheduler")
        except Exception as exc:
            logger.error(f"[DAILY REPORT] Scheduler tick crashed: {exc}", exc_info=True)

        # Weekly deep-dive: Saturday 23:00 UTC only (= 09:00 AEST Sunday).
        # time.gmtime().tm_wday: 5 = Saturday. Gate on hour so the 11:00 UTC
        # tick doesn't also re-fire it.
        try:
            _gm = time.gmtime()
            if _gm.tm_wday == 5 and _gm.tm_hour == 23:
                logger.info("[INSIGHTS] Saturday 23:00 UTC — firing weekly deep-dive")
                async with aiohttp.ClientSession() as _wk_sess:
                    await _send_insights(
                        _wk_sess, reply_chat_id=None,
                        triggered_by="weekly_deep_dive", weekly=True,
                    )
        except Exception as exc:
            logger.error(f"[INSIGHTS] weekly deep-dive crashed: {exc}", exc_info=True)


# --- Momentum scanner -----------------------------------------------------

def _log_momentum_watch(record: dict) -> None:
    """Append a momentum watch-only event to state/momentum_watches.json.
    Used by the daily report to count 'Watch only' signals that didn't buy."""
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        try:
            with open(MOMENTUM_WATCH_LOG_FILE, "r") as f:
                log = json.load(f)
            if not isinstance(log, list):
                log = []
        except (FileNotFoundError, json.JSONDecodeError):
            log = []
        log.append(record)
        tmp = MOMENTUM_WATCH_LOG_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(log, f, indent=2)
        os.replace(tmp, MOMENTUM_WATCH_LOG_FILE)
    except Exception as exc:
        logger.error(f"[MOMENTUM] watch log append failed: {exc}")


def _load_momentum_watches() -> list[dict]:
    try:
        with open(MOMENTUM_WATCH_LOG_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    except Exception as exc:
        logger.warning(f"[MOMENTUM] watch log load failed: {exc}")
        return []


async def _fetch_pumpfun_latest(session: aiohttp.ClientSession) -> list[dict]:
    """Pull the latest-trade-active feed. Primary endpoint is the
    client-api heroku proxy (which fronts PumpPortal's data) because
    frontend-api.pump.fun has been 530'ing. Falls back to the original
    endpoint if the proxy fails.
    Returns [] on any error so the scanner fail-opens instead of crashing."""
    # 50 is plenty for the hourly-brief market snapshot; full momentum
    # scanner now uses DexScreener and no longer depends on this helper.
    qs = (
        "?offset=0&limit=50"
        "&sort=last_trade_timestamp&order=DESC&includeNsfw=false"
    )
    endpoints = [
        f"https://client-api-2-74b1891ee9f9.herokuapp.com/coins{qs}",
        f"https://frontend-api.pump.fun/coins{qs}",
    ]
    for url in endpoints:
        label = "heroku-proxy" if "herokuapp" in url else "frontend-api"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                status = resp.status
                if status != 200:
                    # Surface the actual body (truncated) so we can diagnose
                    # 403/429/530 responses from each endpoint.
                    try:
                        body_preview = (await resp.text())[:300]
                    except Exception:
                        body_preview = "<unreadable>"
                    logger.warning(
                        f"[MOMENTUM] {label} HTTP {status} — body: {body_preview!r}"
                    )
                    continue   # try next endpoint
                data = await resp.json()
                if isinstance(data, list):
                    logger.debug(f"[MOMENTUM] {label} returned {len(data)} coin(s)")
                    return data
                if isinstance(data, dict):
                    coins = data.get("coins") or data.get("data") or []
                    logger.debug(f"[MOMENTUM] {label} returned {len(coins)} coin(s) (dict)")
                    return coins
                logger.warning(
                    f"[MOMENTUM] {label} returned unexpected payload type: {type(data).__name__}"
                )
                continue
        except Exception as exc:
            logger.warning(
                f"[MOMENTUM] {label} fetch exception ({type(exc).__name__}): {exc}"
            )
            continue
    # All endpoints failed
    logger.warning("[MOMENTUM] all pump.fun endpoints failed — cycle will be empty")
    return []


def _bonding_pct_from_mcap(mcap_usd: float) -> float:
    """User-spec formula: mcap / graduation_mc * 100 → rough bonding %."""
    if mcap_usd <= 0 or GRADUATION_MC_USD <= 0:
        return 0.0
    return (mcap_usd / GRADUATION_MC_USD) * 100.0


# --- DexScreener candidate-feed helpers -------------------------------

_DEX_BASE = "https://api.dexscreener.com"


async def _dex_get_json(session: aiohttp.ClientSession, path: str):
    """Shared GET wrapper with status/body logging on failure. Returns
    parsed JSON on 200, None otherwise."""
    url = f"{_DEX_BASE}{path}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                try:
                    preview = (await resp.text())[:240]
                except Exception:
                    preview = "<unreadable>"
                logger.warning(
                    f"[MOMENTUM] dexscreener {path} HTTP {resp.status} "
                    f"— body: {preview!r}"
                )
                return None
            return await resp.json()
    except Exception as exc:
        logger.warning(
            f"[MOMENTUM] dexscreener {path} exception "
            f"({type(exc).__name__}): {exc}"
        )
        return None


async def _dex_fetch_candidate_mints(session: aiohttp.ClientSession) -> list[str]:
    """Merge candidates from:
      1. /token-profiles/latest/v1
      2. /token-boosts/latest/v1
      3. /latest/dex/search?q=solana&sort=volume  (top-volume Solana pairs)

    Filter to chainId='solana', dedupe by mint, cap at MOMENTUM_CANDIDATES_MAX.
    Returns a list of mint addresses (strings) in first-seen order.
    """
    seen: list[str] = []
    seen_set: set[str] = set()

    def _add(mint: str | None):
        if not mint or mint in seen_set:
            return
        seen_set.add(mint)
        seen.append(mint)

    # 1. Token profiles (trending-ish)
    profiles = await _dex_get_json(session, "/token-profiles/latest/v1") or []
    if isinstance(profiles, dict):
        profiles = profiles.get("tokenProfiles") or []
    for p in profiles if isinstance(profiles, list) else []:
        if (p.get("chainId") or "").lower() != "solana":
            continue
        _add(p.get("tokenAddress"))

    # 2. Token boosts (paid promotion signal)
    boosts = await _dex_get_json(session, "/token-boosts/latest/v1") or []
    if isinstance(boosts, dict):
        boosts = boosts.get("tokenProfiles") or boosts.get("tokenBoosts") or []
    for b in boosts if isinstance(boosts, list) else []:
        if (b.get("chainId") or "").lower() != "solana":
            continue
        _add(b.get("tokenAddress"))

    # 3. Search by volume on Solana
    search = await _dex_get_json(session, "/latest/dex/search?q=solana&sort=volume") or {}
    pairs  = search.get("pairs") if isinstance(search, dict) else None
    for pr in pairs or []:
        if (pr.get("chainId") or "").lower() != "solana":
            continue
        mint = (pr.get("baseToken") or {}).get("address")
        _add(mint)
        if len(seen) >= MOMENTUM_CANDIDATES_MAX:
            break

    if len(seen) > MOMENTUM_CANDIDATES_MAX:
        seen = seen[:MOMENTUM_CANDIDATES_MAX]
    return seen


# --- Tier classification + TA signal helpers --------------------------

def _fmt_age_hours(hours: float) -> str:
    """Return a short human age: '45m', '2h 10m', '3d 4h'."""
    if hours < 1:
        return f"{int(hours * 60)}m"
    if hours < 24:
        h = int(hours)
        m = int((hours - h) * 60)
        return f"{h}h {m:02d}m"
    d = int(hours // 24)
    h = int(hours - d * 24)
    return f"{d}d {h}h"


def _pair_age_hours(pair: dict) -> float | None:
    """Compute age in hours from pairCreatedAt (ms epoch). None if missing."""
    created_ms = pair.get("pairCreatedAt")
    if not created_ms:
        return None
    try:
        return max(0.0, (time.time() - float(created_ms) / 1000.0) / 3600.0)
    except Exception:
        return None


def _pair_floats(pair: dict) -> dict:
    """Flatten the fields we care about into a single dict of floats/ints."""
    liq    = (pair.get("liquidity")   or {})
    vol    = (pair.get("volume")      or {})
    pc     = (pair.get("priceChange") or {})
    txns   = (pair.get("txns")        or {})
    def _t(key: str) -> dict:
        return txns.get(key) or {}
    return {
        "mcap":     float(pair.get("marketCap") or pair.get("fdv") or 0),
        "liq_usd":  float(liq.get("usd") or 0),
        "vol_m5":   float(vol.get("m5")  or 0),
        "vol_h1":   float(vol.get("h1")  or 0),
        "vol_h6":   float(vol.get("h6")  or 0),
        "vol_h24":  float(vol.get("h24") or 0),
        "pc_m5":    float(pc.get("m5")   or 0),
        "pc_h1":    float(pc.get("h1")   or 0),
        "pc_h6":    float(pc.get("h6")   or 0),
        "pc_h24":   float(pc.get("h24")  or 0),
        "buys_m5":  int(_t("m5").get("buys")    or 0),
        "sells_m5": int(_t("m5").get("sells")   or 0),
        "buys_h1":  int(_t("h1").get("buys")    or 0),
        "sells_h1": int(_t("h1").get("sells")   or 0),
        "buys_h6":  int(_t("h6").get("buys")    or 0),
        "sells_h6": int(_t("h6").get("sells")   or 0),
    }


def _tier1_check(m: dict) -> tuple[bool, str]:
    """Fresh grads (<3h): mcap $20k-$100k, 5m vol ≥ $8k, buys>sells (m5),
    liquidity ≥ $7k. No TA signal required beyond the hard filters."""
    if not (20_000 <= m["mcap"] <= 100_000):
        return False, f"mcap ${m['mcap']:,.0f} outside $20k-$100k"
    if m["vol_m5"] < 8_000:
        return False, f"5m vol ${m['vol_m5']:,.0f} < $8k"
    if m["buys_m5"] <= m["sells_m5"]:
        return False, f"m5 buys {m['buys_m5']} ≤ sells {m['sells_m5']}"
    if m["liq_usd"] < 7_000:
        return False, f"liq ${m['liq_usd']:,.0f} < $7k"
    return True, (
        f"fresh grad — buys {m['buys_m5']}>{m['sells_m5']}, "
        f"5m vol ${m['vol_m5']:,.0f}, liq ${m['liq_usd']:,.0f}"
    )


def _tier2_check(m: dict) -> tuple[bool, str]:
    """Reawakening (3h-7d): mcap $20k-$200k, liq ≥ $10k, volume divergence
    filter (m5 ≥ 3× h1/12), AND at least one of three TA signals."""
    if not (20_000 <= m["mcap"] <= 200_000):
        return False, f"mcap ${m['mcap']:,.0f} outside $20k-$200k"
    if m["liq_usd"] < 10_000:
        return False, f"liq ${m['liq_usd']:,.0f} < $10k"
    # Volume divergence filter (3× hourly-average m5 floor)
    hourly_avg_m5 = m["vol_h1"] / 12.0
    if m["vol_m5"] < 3.0 * hourly_avg_m5:
        return False, (
            f"5m vol ${m['vol_m5']:,.0f} < 3× avg 5m "
            f"${hourly_avg_m5:,.0f} (= ${3 * hourly_avg_m5:,.0f})"
        )

    # TA signals — need at least ONE
    signals: list[str] = []

    # Support holding: compressed 1h, starting to move up 5m
    if -5.0 <= m["pc_h1"] <= 5.0 and m["pc_m5"] >= 2.0:
        signals.append(
            f"support holding ({m['pc_h1']:+.1f}%/1h compressed, "
            f"{m['pc_m5']:+.1f}%/5m starting)"
        )
    # Oversold bounce: big 24h drop, recovering 5m with volume
    if m["pc_h24"] <= -30.0 and m["pc_m5"] >= 3.0 and m["vol_m5"] >= 10_000:
        signals.append(
            f"oversold bounce ({m['pc_h24']:+.0f}%/24h, "
            f"{m['pc_m5']:+.1f}%/5m, 5m vol ${m['vol_m5']:,.0f})"
        )
    # Volume divergence (TA): flat 1h but 5m vol ≥ 4× hourly avg
    if -3.0 <= m["pc_h1"] <= 3.0 and m["vol_m5"] >= 4.0 * hourly_avg_m5:
        signals.append(
            f"volume divergence ({m['pc_h1']:+.1f}%/1h flat, "
            f"5m vol ${m['vol_m5']:,.0f} ≥ 4× avg)"
        )
    if not signals:
        return False, "no tier2 TA signal matched"
    return True, "reawakening — " + "; ".join(signals)


def _tier3_check(m: dict) -> tuple[bool, str]:
    """Second leg (7d-30d): mcap $50k-$500k, liq ≥ $15k, at least one of
    three TA signals (recovery / breakout / accumulation)."""
    if not (50_000 <= m["mcap"] <= 500_000):
        return False, f"mcap ${m['mcap']:,.0f} outside $50k-$500k"
    if m["liq_usd"] < 15_000:
        return False, f"liq ${m['liq_usd']:,.0f} < $15k"

    signals: list[str] = []
    # Recovery setup: big 24h drop, recovering 6h with hourly vol
    if m["pc_h24"] <= -40.0 and m["pc_h6"] >= 10.0 and m["vol_h1"] >= 20_000:
        signals.append(
            f"recovery setup ({m['pc_h24']:+.0f}%/24h, {m['pc_h6']:+.0f}%/6h, "
            f"1h vol ${m['vol_h1']:,.0f})"
        )
    # Breakout: strong 6h move, buys dominating 1h
    if (m["pc_h6"] >= 20.0 and m["pc_h24"] >= -10.0
            and m["buys_h1"] > m["sells_h1"] * 1.5):
        signals.append(
            f"breakout ({m['pc_h6']:+.0f}%/6h, {m['pc_h24']:+.0f}%/24h, "
            f"1h buys {m['buys_h1']}>{int(m['sells_h1']*1.5)})"
        )
    # Accumulation: sideways 24h, high volume, buys dominating 6h
    if (-10.0 <= m["pc_h24"] <= 10.0 and m["vol_h24"] >= 50_000
            and m["buys_h6"] > m["sells_h6"]):
        signals.append(
            f"accumulation ({m['pc_h24']:+.1f}%/24h sideways, "
            f"24h vol ${m['vol_h24']:,.0f}, 6h buys {m['buys_h6']}>{m['sells_h6']})"
        )
    if not signals:
        return False, "no tier3 TA signal matched"
    return True, "second leg — " + "; ".join(signals)


def _classify_and_check(pair: dict) -> tuple[int | None, float | None, str]:
    """Classify a pair into tier 1/2/3 by age, run that tier's filter +
    TA checks, and return (tier_or_None, age_hours, reason_or_signal_desc).
    When a tier is unmatched, tier is None and the second element is the
    reason the checks failed (for logging only)."""
    age_hours = _pair_age_hours(pair)
    if age_hours is None:
        return None, None, "no pairCreatedAt"
    m = _pair_floats(pair)
    if age_hours < MOMENTUM_TIER1_MAX_AGE_HOURS:
        ok, reason = _tier1_check(m)
        return (1 if ok else None), age_hours, reason
    if age_hours < MOMENTUM_TIER2_MAX_AGE_HOURS:
        ok, reason = _tier2_check(m)
        return (2 if ok else None), age_hours, reason
    if age_hours < MOMENTUM_TIER3_MAX_AGE_HOURS:
        ok, reason = _tier3_check(m)
        return (3 if ok else None), age_hours, reason
    return None, age_hours, f"age {_fmt_age_hours(age_hours)} > 30d"


def _tier_buy_threshold(tier: int) -> int:
    """Per-tier Claude buy threshold."""
    return {
        1: MOMENTUM_TIER1_BUY_SCORE,
        2: MOMENTUM_TIER2_BUY_SCORE,
        3: MOMENTUM_TIER3_BUY_SCORE,
    }.get(tier, 68)


async def momentum_scanner_loop(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
) -> None:
    """Every MOMENTUM_SCAN_INTERVAL_SEC seconds: pull trending + boosts +
    top-volume candidates from DexScreener, fetch full pair data per mint,
    classify into Tier 1/2/3 by age, apply tier filters + TA checks, Claude
    score survivors, and act (buy / watch / silent skip)."""
    rpc_url = _rpc_url
    logger.info(
        "[MOMENTUM] DexScreener scanner started — interval "
        f"{MOMENTUM_SCAN_INTERVAL_SEC}s, tiers 1/2/3, thresholds "
        f"{MOMENTUM_TIER1_BUY_SCORE}/{MOMENTUM_TIER2_BUY_SCORE}/"
        f"{MOMENTUM_TIER3_BUY_SCORE}"
    )

    while True:
        try:
            # Prune alert-cooldown dict (4-hour window)
            _mom_cutoff = time.time() - MOMENTUM_ALERT_COOLDOWN_SEC
            for _m in list(_momentum_alerted.keys()):
                if _momentum_alerted[_m] < _mom_cutoff:
                    del _momentum_alerted[_m]

            mints = await _dex_fetch_candidate_mints(session)
            logger.info(
                f"[MOMENTUM] pulled {len(mints)} unique Solana mint(s) "
                f"from DexScreener (profiles/boosts/search)"
            )

            for mint in mints:
                try:
                    await _momentum_process_coin(session, mint, wallet_pubkey, rpc_url)
                except Exception as exc:
                    logger.warning(f"[MOMENTUM] per-coin error ({mint[:8]}): {exc}")
                # Throttle per-mint pair fetches to stay friendly with DS.
                await asyncio.sleep(MOMENTUM_PER_COIN_SLEEP_SEC)
        except Exception as exc:
            logger.error(f"[MOMENTUM] scan cycle crashed: {exc}", exc_info=True)

        await asyncio.sleep(MOMENTUM_SCAN_INTERVAL_SEC)


async def _momentum_process_coin(
    session: aiohttp.ClientSession,
    mint: str,
    wallet_pubkey: str,
    rpc_url: str,
) -> None:
    """Evaluate one candidate mint through the DexScreener three-tier
    pipeline. Isolated from the main loop so exceptions per-coin don't
    abort the cycle."""
    if not mint:
        return

    # Count every candidate we attempt to evaluate. "scanned" further down
    # only fires after Claude scoring, which most coins never reach — this
    # gives the hourly brief an accurate denominator.
    _momentum_events.append((time.time(), "candidate"))

    # --- Cheap static dedup checks ------------------------------------
    if mint in open_positions:
        return
    if mint in _token_blacklist and _token_blacklist[mint] > time.time():
        return
    if mint in _traded_tokens:
        logger.info(f"[MOMENTUM] SKIP — {mint[:8]} already traded and exited, not rebuying")
        return
    if mint in _momentum_alerted:
        return  # 4-hour cooldown still active

    # --- Fetch full pair data -----------------------------------------
    pair = await fetch_dexscreener(session, mint)
    if pair is None:
        logger.debug(f"[MOMENTUM] {mint[:8]} DexScreener pair unavailable — skip")
        return

    base   = pair.get("baseToken") or {}
    name   = base.get("name")   or "?"
    symbol = base.get("symbol") or "?"
    m      = _pair_floats(pair)

    # Skip graduated pump.fun tokens (noise — handled elsewhere)
    if mint.endswith("pump") and m["mcap"] > MOMENTUM_PUMP_SKIP_MCAP_USD:
        logger.info(
            f"[MOMENTUM] skip {symbol} ({mint[:8]}) — graduated pump token, "
            f"mcap ${m['mcap']:,.0f} > ${MOMENTUM_PUMP_SKIP_MCAP_USD:,} cap"
        )
        return

    tier, age_hours, reason = _classify_and_check(pair)
    if tier is None:
        _age_str = _fmt_age_hours(age_hours) if age_hours is not None else "?"
        logger.info(
            f"[MOMENTUM] skip {symbol} ({mint[:8]}) — age {_age_str}, {reason}"
        )
        return

    age_str = _fmt_age_hours(age_hours)
    ta_desc = reason   # signal description from the tier check
    buy_threshold   = _tier_buy_threshold(tier)
    watch_threshold = buy_threshold - MOMENTUM_WATCH_DELTA

    context_note = f"Tier {tier} ({age_str}) — {ta_desc}"

    # --- Claude scoring ------------------------------------------------
    try:
        claude_score, _bullets = await get_claude_score(
            mint,
            dex_pair=pair,
            prebond_progress=None,   # DexScreener flow — no bonding curve context
            context_note=context_note,
            pump_data=None,
        )
    except Exception as exc:
        logger.warning(
            f"[MOMENTUM] Claude scoring failed for {mint[:8]}: {exc} — default 70"
        )
        claude_score = 70

    # Stamp cooldown AFTER we've decided to score so skipped coins can be
    # re-picked on later cycles if tier/TA changes.
    _momentum_alerted[mint] = time.time()
    _momentum_events.append((time.time(), "scanned"))

    header = f"TIER {tier}</b> — {name} ({symbol}"   # closed in alert template
    body = (
        f"CA: <code>{mint}</code>\n"
        f"MCap: ${m['mcap']:,.0f} | Age: {age_str}\n"
        f"Signal: {ta_desc}\n"
        f"5m Vol: ${m['vol_m5']:,.0f} | Liq: ${m['liq_usd']:,.0f}\n"
        f"Score: {claude_score}/100"
    )

    # --- Auto-buy path ------------------------------------------------
    if claude_score >= buy_threshold:
        logger.info(
            f"[MOMENTUM] BUY tier{tier} {symbol} ({mint[:8]}) — "
            f"score {claude_score}, age {age_str}, mcap ${m['mcap']:,.0f}, "
            f"signal: {ta_desc}"
        )
        sol_balance = get_sol_balance(rpc_url, wallet_pubkey)
        if sol_balance < LOW_BALANCE_SOL:
            logger.warning(
                f"[MOMENTUM] LOW BALANCE {sol_balance:.4f} SOL — skipping {mint[:8]}"
            )
            send_telegram(
                f"⚠️ <b>MOMENTUM BUY SKIPPED — TIER {tier}</b> — low balance\n"
                f"{name} ({symbol})\n{body}"
            )
            return

        # Honeypot guard — check the mint's freeze authority before committing.
        if not await _honeypot_guard(session, rpc_url, mint,
                                     symbol=symbol,
                                     source_label="momentum_scanner"):
            return

        send_telegram(
            f"⚡ <b>MOMENTUM BUY — TIER {tier}</b> — {name} ({symbol})\n"
            f"{body}\n→ AUTO-BUY triggered"
        )

        buy_sol = BUY_AMOUNT_SOL
        swap_sig, swap_msg = await execute_buy_routed(
            session, mint, buy_sol, wallet_pubkey, m["mcap"]
        )
        if not swap_sig:
            logger.error(f"[MOMENTUM] buy failed for {mint[:8]}: {swap_msg}")
            _apex_log_error(mint, "momentum_scanner", "momentum_buy_failed",
                            {"msg": swap_msg, "score": claude_score, "tier": tier})
            send_telegram(
                f"⚠️ <b>MOMENTUM BUY FAILED</b> — {symbol}\n"
                f"Reason: {swap_msg}"
            )
            return

        try:
            token_units = await get_spl_token_balance(session, mint, wallet_pubkey)
        except Exception:
            token_units = 0

        open_positions[mint] = {
            "entry_time":         time.time(),
            "entry_sol":          buy_sol,
            "original_entry_sol": buy_sol,
            "tp1_received_sol":   0.0,
            "peak_sol":           buy_sol,
            "amount_tokens":      token_units,
            "whale":              "momentum_scanner",
            "buy_sol":            buy_sol,
            "claude_score":       claude_score,
            "min_target_hit":     False,
            "alerted_25pct_down": False,
            "source":             "momentum_scanner",
            "mc_entry":           m["mcap"],
            "token_label":        f"{symbol} ({mint[:8]})",
            "token_symbol":       symbol,
            # Post-graduation tokens are by definition "graduated" — trailing
            # stop uses this flag to pick 20% (pre-grad) vs 25% (graduated).
            "was_pregrad":        False,
            # Tier / signal metadata for the daily report + insights
            "momentum_score":     claude_score,
            "momentum_tier":      tier,
            "momentum_signal":    ta_desc,
            "age_hours_at_entry": round(age_hours, 2),
            "liq_at_entry":       m["liq_usd"],
            "vol_m5_at_entry":    m["vol_m5"],
        }
        _save_positions()
        _stats["trades_executed"] += 1
        _momentum_events.append((time.time(), "bought"))
        _apex_log_signal("momentum_scanner", "bought", {
            "mint":          mint,
            "symbol":        symbol,
            "tier":          tier,
            "signal":        ta_desc,
            "mcap":          m["mcap"],
            "liq_usd":       m["liq_usd"],
            "vol_m5":        m["vol_m5"],
            "age_hours":     round(age_hours, 2),
            "claude_score":  claude_score,
            "buy_sol":       buy_sol,
        })
        logger.info(
            f"[MOMENTUM] Position opened — tier{tier} {symbol} | "
            f"{token_units:,} tokens | entry {buy_sol:.4f} SOL | score {claude_score}"
        )
        asyncio.create_task(emergency_dump_check(session, mint, wallet_pubkey))

    # --- Watch-only path ----------------------------------------------
    elif claude_score >= watch_threshold:
        logger.info(
            f"[MOMENTUM] WATCH tier{tier} {symbol} ({mint[:8]}) — "
            f"score {claude_score}, signal: {ta_desc}"
        )
        send_telegram(
            f"👁 <b>MOMENTUM WATCH — TIER {tier}</b> — {name} ({symbol})\n"
            f"{body}\n→ Watching only"
        )
        _log_momentum_watch({
            "ts":             time.time(),
            "mint":           mint,
            "symbol":         symbol,
            "name":           name,
            "tier":           tier,
            "signal":         ta_desc,
            "mcap":           m["mcap"],
            "liq_usd":        m["liq_usd"],
            "vol_m5":         m["vol_m5"],
            "age_hours":      round(age_hours, 2),
            "claude_score":   claude_score,
        })
        _momentum_events.append((time.time(), "watched"))
        _apex_log_signal("momentum_scanner", "watched", {
            "mint":         mint,
            "symbol":       symbol,
            "tier":         tier,
            "signal":       ta_desc,
            "mcap":         m["mcap"],
            "liq_usd":      m["liq_usd"],
            "vol_m5":       m["vol_m5"],
            "age_hours":    round(age_hours, 2),
            "claude_score": claude_score,
        })

    # --- Low-score skip (logged so the user can see what's being filtered) ---
    else:
        logger.info(
            f"[MOMENTUM] skip tier{tier} {symbol} ({mint[:8]}) — "
            f"score {claude_score} < watch threshold {watch_threshold} "
            f"(buy {buy_threshold}, signal: {ta_desc})"
        )
        _momentum_events.append((time.time(), "low_score"))
        _apex_log_signal("momentum_scanner", "low_score", {
            "mint":         mint,
            "symbol":       symbol,
            "tier":         tier,
            "signal":       ta_desc,
            "mcap":         m["mcap"],
            "liq_usd":      m["liq_usd"],
            "vol_m5":       m["vol_m5"],
            "age_hours":    round(age_hours, 2),
            "claude_score": claude_score,
        })


# --- Claude analyst ---------------------------------------------------

AEST_OFFSET_SEC = 10 * 3600   # AEST is UTC+10 year-round (no DST)


async def _claude_complete(prompt: str, max_tokens: int, label: str) -> str | None:
    """Thin wrapper around the anthropic async client used by all three
    analyst paths (hourly brief, 6-hour check, /analyse). Returns Claude's
    text on success, None on any failure. Never crashes callers."""
    api_key = os.getenv("CLAUDE_API_KEY", "")
    if not api_key:
        logger.warning(f"[ANALYST:{label}] CLAUDE_API_KEY not set — skipping")
        return None
    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (resp.content[0].text if resp.content else "").strip()
        return text or None
    except Exception as exc:
        logger.error(f"[ANALYST:{label}] Claude call failed: {exc}")
        return None


def _analyst_send_to_all(body: str, label: str) -> None:
    """Sanitize + chunk + send Claude output to both chat IDs via send_telegram."""
    safe   = _sanitize_tg_html(body)
    chunks = _chunk_html_for_telegram(safe, max_len=4000)
    total  = len(chunks)
    sent   = 0
    for i, chunk in enumerate(chunks, 1):
        if total > 1 and i > 1:
            chunk = f"<i>(part {i}/{total})</i>\n\n{chunk}"
        if send_telegram(chunk):
            sent += 1
    logger.info(f"[ANALYST:{label}] sent {sent}/{total} chunk(s)")


async def _analyst_send_to_chat(
    session: aiohttp.ClientSession,
    chat_id: str,
    body: str,
    label: str,
) -> None:
    """Sanitize + chunk + send Claude output to a single chat (for /analyse)."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return
    safe   = _sanitize_tg_html(body)
    chunks = _chunk_html_for_telegram(safe, max_len=4000)
    total  = len(chunks)
    base_url = f"https://api.telegram.org/bot{token}"
    for i, chunk in enumerate(chunks, 1):
        if total > 1 and i > 1:
            chunk = f"<i>(part {i}/{total})</i>\n\n{chunk}"
        try:
            async with session.post(
                f"{base_url}/sendMessage",
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                r.raise_for_status()
        except Exception as exc:
            logger.warning(f"[ANALYST:{label}] reply to {chat_id} chunk {i}/{total} failed: {exc}")


# --- Data gathers ---------------------------------------------------------

async def _pumpfun_market_snapshot(session: aiohttp.ClientSession) -> dict:
    """DEPRECATED — pump.fun frontend-api has been chronically 530'ing.
    Kept only for backward-compat callers. The hourly brief now uses
    _dexscreener_market_snapshot (below)."""
    coins = await _fetch_pumpfun_latest(session)
    if not coins:
        return {}
    mcaps    = []
    bondings = []
    for c in coins:
        m = float(c.get("usd_market_cap") or 0)
        if m > 0:
            mcaps.append(m)
            bondings.append(_bonding_pct_from_mcap(m))
    if not mcaps:
        return {"ts": time.time(), "coin_count": len(coins),
                "avg_bonding_pct": 0.0, "avg_mcap_usd": 0.0}
    return {
        "ts":              time.time(),
        "coin_count":      len(coins),
        "avg_bonding_pct": round(sum(bondings) / len(bondings), 1),
        "avg_mcap_usd":    round(sum(mcaps) / len(mcaps), 0),
    }


async def _dexscreener_market_snapshot(session: aiohttp.ClientSession) -> dict:
    """Aggregate Solana market state from DexScreener's top-volume search.
    Single API call, returns inline pair data — no per-mint fan-out needed.
    Fields: coin_count, avg_mcap_usd, median_mcap_usd, avg_vol_h24_usd.
    Returns {} on fetch failure (callers fail-open)."""
    data = await _dex_get_json(session, "/latest/dex/search?q=solana&sort=volume")
    pairs = (data or {}).get("pairs") if isinstance(data, dict) else None
    if not pairs:
        return {}
    sol_pairs = [p for p in pairs if (p.get("chainId") or "").lower() == "solana"]
    mcaps: list[float] = []
    vols:  list[float] = []
    for p in sol_pairs:
        mc  = float(p.get("fdv") or p.get("marketCap") or 0)
        v24 = float((p.get("volume") or {}).get("h24") or 0)
        if mc > 0:
            mcaps.append(mc)
        if v24 > 0:
            vols.append(v24)
    if not mcaps:
        return {"ts": time.time(), "coin_count": len(sol_pairs),
                "avg_mcap_usd": 0.0, "avg_vol_h24_usd": 0.0,
                "median_mcap_usd": 0.0, "source": "dexscreener_top_volume"}
    return {
        "ts":              time.time(),
        "coin_count":      len(sol_pairs),
        "avg_mcap_usd":    round(sum(mcaps) / len(mcaps), 0),
        "median_mcap_usd": round(sorted(mcaps)[len(mcaps) // 2], 0),
        "avg_vol_h24_usd": round(sum(vols) / len(vols), 0) if vols else 0.0,
        "source":          "dexscreener_top_volume",
    }


def _whale_activity_window(window_sec: int) -> dict[str, list[str]]:
    """Return {whale_name: [mint, mint, …]} of buys inside the last window_sec.
    Preserves chronological order, deduped per whale."""
    cutoff = time.time() - window_sec
    out: dict[str, list[str]] = {}
    for whale_name, events in _whale_activity.items():
        mints: list[str] = []
        for (mint, ts) in events:
            if ts >= cutoff and mint not in mints:
                mints.append(mint)
        if mints:
            out[whale_name] = mints
    return out


def _circled_not_bought(window_sec: int, max_items: int = 3) -> list[dict]:
    """Mints whales have bought in the window that apex is NOT currently
    holding. Ranked by number of unique whales. Useful as 'potential setup'."""
    cutoff = time.time() - window_sec
    per_mint: dict[str, set] = {}
    for whale_name, events in _whale_activity.items():
        for (mint, ts) in events:
            if ts >= cutoff:
                per_mint.setdefault(mint, set()).add(whale_name)
    ranked = sorted(per_mint.items(), key=lambda kv: -len(kv[1]))
    out: list[dict] = []
    for mint, whales in ranked:
        if mint in open_positions:
            continue
        out.append({"mint": mint, "whales": sorted(whales), "whale_count": len(whales)})
        if len(out) >= max_items:
            break
    return out


def _momentum_window_stats(window_sec: int) -> dict:
    """Counts of candidate/scanned/watched/bought/low_score in window_sec.
    candidate = total mints evaluated; scanned = subset that passed hard
    filters and got Claude-scored. The brief uses both for an honest
    'X scanned, Y reached scoring, Z bought' breakdown."""
    cutoff = time.time() - window_sec
    counts = {"candidate": 0, "scanned": 0, "watched": 0, "bought": 0, "low_score": 0}
    for (ts, ev) in _momentum_events:
        if ts >= cutoff and ev in counts:
            counts[ev] += 1
    return counts


def _prune_momentum_events(max_age_sec: int = 86_400) -> None:
    """Drop events older than max_age_sec (default 24h)."""
    global _momentum_events
    cutoff = time.time() - max_age_sec
    _momentum_events = [(ts, ev) for (ts, ev) in _momentum_events if ts >= cutoff]


def _open_positions_summary() -> dict:
    """Lightweight summary — no live quotes. Used by hourly brief only."""
    by_source: dict[str, int] = {}
    for pos in open_positions.values():
        src = pos.get("source") or pos.get("whale") or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
    return {
        "total":     len(open_positions),
        "by_source": by_source,
    }


async def _open_positions_with_live_pnl(session: aiohttp.ClientSession) -> list[dict]:
    """For /analyse: fetch live sell quotes for every open position and compute
    real PnL vs original entry cost (accounts for TP1 partial proceeds).
    One quote per position; up to ~10s for 10 positions."""
    out: list[dict] = []
    for mint, pos in list(open_positions.items()):
        entry_sol = float(pos.get("original_entry_sol") or pos.get("entry_sol") or 0)
        tp1_received = float(pos.get("tp1_received_sol") or 0.0)
        amount_tokens = int(pos.get("amount_tokens") or 0)
        symbol = pos.get("token_symbol") or pos.get("token_label") or mint[:8]
        source = pos.get("source") or pos.get("whale") or "unknown"
        whale_name = pos.get("whale_name") or pos.get("whale") or "—"
        elapsed_min = (time.time() - float(pos.get("entry_time") or time.time())) / 60

        current_sol: float = 0.0
        pnl_pct: float | None = None
        pnl_sol: float | None = None
        if amount_tokens > 0:
            try:
                q = await get_sell_quote(session, mint, amount_tokens)
                if q:
                    current_sol = int(q.get("outAmount", 0)) / 1_000_000_000
            except Exception as exc:
                logger.debug(f"[ANALYST] sell quote failed for {mint[:8]}: {exc}")

        if entry_sol > 0:
            total_proceeds = tp1_received + current_sol
            pnl_sol = round(total_proceeds - entry_sol, 6)
            pnl_pct = round((total_proceeds / entry_sol - 1) * 100, 1)

        out.append({
            "symbol":           symbol,
            "mint":             mint,
            "source":           source,
            "whale_name":       whale_name,
            "entry_sol":        round(entry_sol, 6),
            "tp1_received_sol": round(tp1_received, 6),
            "current_sol":      round(current_sol, 6),
            "pnl_sol":          pnl_sol,
            "pnl_pct":          pnl_pct,
            "elapsed_min":      round(elapsed_min, 1),
        })
    return out


def _wallet_form_last_24h() -> dict:
    """Per-wallet win-rate + PnL over the last 24h of trade_log entries."""
    cutoff = time.time() - 86_400
    rows = [t for t in _load_trade_log() if float(t.get("exit_time") or 0) >= cutoff]
    form: dict[str, dict] = {}
    for t in rows:
        w = t.get("whale_name") or "unknown"
        f = form.setdefault(w, {"trades": 0, "wins": 0, "total_pnl_sol": 0.0})
        f["trades"]        += 1
        if float(t.get("pnl_sol") or 0) >= 0:
            f["wins"]       += 1
        f["total_pnl_sol"] += float(t.get("pnl_sol") or 0)
    for f in form.values():
        f["win_rate"]      = round((f["wins"] / f["trades"]) * 100, 1) if f["trades"] else 0.0
        f["total_pnl_sol"] = round(f["total_pnl_sol"], 4)
    return form


def _pregrad_vs_graduated_24h() -> dict:
    """Split last-24h trades by pre-grad entry vs already-graduated entry."""
    cutoff = time.time() - 86_400
    rows = [t for t in _load_trade_log() if float(t.get("exit_time") or 0) >= cutoff]
    pre   = [t for t in rows if t.get("was_pregrad")]
    grad  = [t for t in rows if not t.get("was_pregrad")]
    def _summ(lst: list[dict]) -> dict:
        if not lst:
            return {"trades": 0, "wins": 0, "win_rate": 0.0, "total_pnl_sol": 0.0}
        wins = sum(1 for t in lst if float(t.get("pnl_sol") or 0) >= 0)
        pnl  = sum(float(t.get("pnl_sol") or 0) for t in lst)
        return {
            "trades":        len(lst),
            "wins":          wins,
            "win_rate":      round(wins / len(lst) * 100, 1),
            "total_pnl_sol": round(pnl, 4),
        }
    return {"pre_grad": _summ(pre), "graduated": _summ(grad)}


def _market_risk_signal(cur: dict, prev: dict) -> tuple[str, str]:
    """Classify hour-over-hour market state as risk-on / neutral / risk-off
    from the DexScreener snapshot. Returns (emoji_label, short_reason)."""
    if not cur:
        return ("🟡 Neutral", "DexScreener data unavailable")
    if not prev:
        return ("🟡 Neutral", "no prior snapshot yet (first brief this run)")
    d_mcap = cur.get("avg_mcap_usd", 0)    - prev.get("avg_mcap_usd", 0)
    d_vol  = cur.get("avg_vol_h24_usd", 0) - prev.get("avg_vol_h24_usd", 0)
    d_ct   = cur.get("coin_count", 0)      - prev.get("coin_count", 0)
    # Both avg mcap AND avg 24h volume rising → risk-on; both falling → risk-off.
    if d_mcap > 0 and d_vol > 0:
        return ("🟢 Risk-on", f"avg mcap +${d_mcap:,.0f}, 24h vol +${d_vol:,.0f}")
    if d_mcap < 0 and d_vol < 0:
        return ("🔴 Risk-off", f"avg mcap -${abs(d_mcap):,.0f}, 24h vol -${abs(d_vol):,.0f}")
    return ("🟡 Neutral", f"Δmcap {d_mcap:+,.0f}, Δvol {d_vol:+,.0f}, Δcoins {d_ct:+d}")


# --- Hourly brief ----------------------------------------------------------

async def _send_hourly_brief(session: aiohttp.ClientSession) -> None:
    """Gather last-hour data, ask Claude for the market brief, send to
    both chat IDs. Never crashes the caller."""
    global _analyst_prev_snapshot
    cur_snapshot: dict = {}
    try:
        cur_snapshot = await _dexscreener_market_snapshot(session)
        whale_hr     = _whale_activity_window(3600)
        mom_raw      = _momentum_window_stats(3600)
        circled      = _circled_not_bought(3600, max_items=3)
        open_summary = _open_positions_summary()
        risk_emoji, risk_reason = _market_risk_signal(cur_snapshot, _analyst_prev_snapshot)

        market_conditions = {
            "data_source":   "dexscreener_top_volume",
            "current":       cur_snapshot or None,
            "previous_hour": _analyst_prev_snapshot or None,
            "risk":          f"{risk_emoji} ({risk_reason})",
        }
        # Restructure for the prompt: separate "candidate" (total evaluated)
        # from "scanned" (passed hard filters → Claude-scored). When nothing
        # passed filters the brief should show that explicitly, not "0 scanned".
        passed_filters = mom_raw["scanned"]   # legacy key; semantics: post-Claude
        mom_stats = {
            "candidates_evaluated":         mom_raw["candidate"],
            "passed_filters_to_claude":     passed_filters,
            "bought":                       mom_raw["bought"],
            "watched":                      mom_raw["watched"],
            "scored_below_watch_threshold": mom_raw["low_score"],
            "summary":                      (
                f"{mom_raw['candidate']} candidates evaluated, "
                f"{passed_filters} reached Claude scoring, "
                f"{mom_raw['bought']} bought, "
                f"{mom_raw['watched']} watched"
                + (" — none met entry criteria"
                   if mom_raw["candidate"] > 0 and mom_raw["bought"] == 0
                   else "")
            ),
        }
        circled_payload = [
            {"mint": c["mint"], "whales": c["whales"], "whale_count": c["whale_count"]}
            for c in circled
        ]

        aest_str = time.strftime("%H:%M", time.gmtime(time.time() + AEST_OFFSET_SEC))

        prompt = (
            "You are a Solana memecoin trading analyst. Write a concise hourly "
            "market brief for a copy-trading bot. Be direct, use emojis, max 300 "
            "words. No bare HTML tags like <word>. Market data comes from "
            "DexScreener (top-volume Solana pairs). Do NOT mention pump.fun "
            "as a data source — we do not use it for the market overview.\n\n"
            f"Data:\n"
            f"- Whale activity last hour: {json.dumps(whale_hr)}\n"
            f"- Market conditions: {json.dumps(market_conditions)}\n"
            f"- Momentum scanner (last 1h): {json.dumps(mom_stats)}\n"
            f"- Coins whales circled but bot didn't buy: {json.dumps(circled_payload)}\n"
            f"- Open positions: {json.dumps(open_summary)}\n\n"
            "Format:\n"
            f"🧠 APEX HOURLY BRIEF — {aest_str} AEST\n\n"
            f"Market: {risk_emoji} (reason)\n\n"
            "Whale activity: [1-2 sentences]\n"
            "Momentum: [1 sentence using the summary string verbatim or "
            "rephrased — never report '0 scanned' if candidates_evaluated > 0]\n\n"
            "👀 Worth watching: [2-3 coins or 'Nothing notable']\n\n"
            "[1 sentence overall market read]"
        )

        body = await _claude_complete(prompt, max_tokens=800, label="hourly")
        if not body:
            logger.warning("[ANALYST:hourly] no response from Claude — skipping")
        else:
            # Persist the brief as a text file: reports/briefs/YYYY-MM-DD-HH.txt
            _now_gm = time.gmtime()
            _brief_path = os.path.join(
                APEX_DATA_REPORTS_BRIEFS,
                f"{time.strftime('%Y-%m-%d-%H', _now_gm)}.txt",
            )
            _apex_data_save_text(_brief_path, body)
            _analyst_send_to_all(body, label="hourly")
    except Exception as exc:
        logger.error(f"[ANALYST:hourly] brief crashed: {exc}", exc_info=True)
    finally:
        # Persist the snapshot for next hour's delta regardless of Claude outcome
        if cur_snapshot:
            _analyst_prev_snapshot = cur_snapshot


# --- 6-hour strategy check -------------------------------------------------

async def _send_strategy_check() -> None:
    """Every 6h (00/06/12/18 UTC) have Claude review 24h performance and
    suggest a specific adjustment. Never crashes the caller."""
    try:
        trades_24h = [
            t for t in _load_trade_log()
            if float(t.get("exit_time") or 0) >= time.time() - 86_400
        ]
        overall    = _trades_basic_stats(trades_24h)
        momentum   = _momentum_report_stats(trades_24h, days=1)
        wallets    = _wallet_form_last_24h()
        grad_split = _pregrad_vs_graduated_24h()

        momentum_settings = {
            "data_source":         "dexscreener",
            "scan_interval_sec":   MOMENTUM_SCAN_INTERVAL_SEC,
            "alert_cooldown_sec":  MOMENTUM_ALERT_COOLDOWN_SEC,
            "watch_delta":         MOMENTUM_WATCH_DELTA,
            "tier1": {
                "max_age_hours":   MOMENTUM_TIER1_MAX_AGE_HOURS,
                "buy_score":       MOMENTUM_TIER1_BUY_SCORE,
                "watch_score":     MOMENTUM_TIER1_BUY_SCORE - MOMENTUM_WATCH_DELTA,
                "mcap_range":      "$30k-$100k",
                "min_vol_m5":      8_000,
                "min_liq_usd":     7_000,
                "rule":            "buys>sells (m5) required",
            },
            "tier2": {
                "max_age_hours":   MOMENTUM_TIER2_MAX_AGE_HOURS,
                "buy_score":       MOMENTUM_TIER2_BUY_SCORE,
                "watch_score":     MOMENTUM_TIER2_BUY_SCORE - MOMENTUM_WATCH_DELTA,
                "mcap_range":      "$20k-$200k",
                "min_liq_usd":     10_000,
                "vol_divergence":  "m5 ≥ 3× (h1 / 12)",
                "ta_signals":      ["support_holding", "oversold_bounce", "volume_divergence_4x"],
            },
            "tier3": {
                "max_age_hours":   MOMENTUM_TIER3_MAX_AGE_HOURS,
                "buy_score":       MOMENTUM_TIER3_BUY_SCORE,
                "watch_score":     MOMENTUM_TIER3_BUY_SCORE - MOMENTUM_WATCH_DELTA,
                "mcap_range":      "$50k-$500k",
                "min_liq_usd":     15_000,
                "ta_signals":      ["recovery_setup", "breakout", "accumulation"],
            },
        }

        strategy_data = {
            "overall_24h":        overall,
            "wallet_form_24h":    wallets,
            "pregrad_vs_graduated_24h": grad_split,
            "momentum_24h":       momentum,
            "momentum_settings":  momentum_settings,
        }

        prompt = (
            "You are a trading strategy analyst. Review the last 24 hours of bot "
            "performance and give a concise strategy update. Max 400 words. No "
            "bare HTML tags.\n\n"
            f"Data: {json.dumps(strategy_data)}\n\n"
            "Format:\n"
            "📊 <b>6-HOUR STRATEGY CHECK</b>\n\n"
            "🐋 Wallet form: [which wallets hot/cold]\n"
            "⚡ Momentum scanner: [is it working, threshold ok?]\n"
            "📈 Entry conditions: [pre-grad vs graduated performing better?]\n"
            "⚠️ Concerns: [anything worth flagging]\n"
            "💡 Suggestion: [one specific adjustment to consider]"
        )

        body = await _claude_complete(prompt, max_tokens=1200, label="6hour")
        if not body:
            logger.warning("[ANALYST:6hour] no response from Claude — skipping")
            return
        _analyst_send_to_all(body, label="6hour")
    except Exception as exc:
        logger.error(f"[ANALYST:6hour] strategy check crashed: {exc}", exc_info=True)


# --- /analyse command ------------------------------------------------------

async def _send_state_analysis(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
    reply_chat_id: str,
) -> None:
    """Build and send the current-state analysis to a single chat (the one
    that sent /analyse). Uses live sell quotes for PnL so this is slower than
    the hourly brief — caller should have acked first."""
    try:
        positions_live = await _open_positions_with_live_pnl(session)
        recent_whales  = _whale_activity_window(2 * 3600)   # 2h
        mom_last_scan  = _momentum_window_stats(2 * 3600)
        wallet_sol     = get_sol_balance(_rpc_url, wallet_pubkey) if _rpc_url else 0.0
        aest_str       = time.strftime("%H:%M", time.gmtime(time.time() + AEST_OFFSET_SEC))

        prompt = (
            "You are a trading analyst. Give a current state analysis for a "
            "copy-trading bot. Be concise, max 350 words. No bare HTML tags.\n\n"
            f"Current state:\n"
            f"- Open positions: {json.dumps(positions_live)}\n"
            f"- Recent whale activity: {json.dumps(recent_whales)}\n"
            f"- Wallet balance: {wallet_sol:.4f} SOL\n"
            f"- Last momentum scan: {json.dumps(mom_last_scan)}\n\n"
            "Format:\n"
            f"🔍 <b>CURRENT STATE ANALYSIS</b> — {aest_str}\n\n"
            f"💼 Positions ({len(positions_live)} open):\n"
            "[list each with current PnL]\n\n"
            "🐋 Whale activity (last 2h):\n"
            "[summary]\n\n"
            "⚡ Momentum:\n"
            "[summary]\n\n"
            "🎯 Outlook: [2-3 sentence overall read]"
        )

        body = await _claude_complete(prompt, max_tokens=1000, label="analyse")
        if not body:
            await _analyst_send_to_chat(
                session, reply_chat_id,
                "⚠️ /analyse: Claude unavailable — try again in a minute.",
                label="analyse",
            )
            return
        await _analyst_send_to_chat(session, reply_chat_id, body, label="analyse")
    except Exception as exc:
        logger.error(f"[ANALYST:analyse] state analysis crashed: {exc}", exc_info=True)
        await _analyst_send_to_chat(
            session, reply_chat_id,
            f"⚠️ /analyse failed: {type(exc).__name__}",
            label="analyse",
        )


# --- Hourly loop -----------------------------------------------------------

# --- Deep insights (/insights + weekly deep-dive) -------------------------

def _apex_read_all_json(subdir: str) -> list[dict]:
    """Load every *.json file in a daily-bucketed subdir, concatenated."""
    if not os.path.isdir(subdir):
        return []
    out: list[dict] = []
    try:
        for fname in sorted(os.listdir(subdir)):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(subdir, fname)
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    out.extend(data)
            except Exception as exc:
                logger.warning(f"[INSIGHTS] failed reading {path}: {exc}")
    except Exception as exc:
        logger.warning(f"[INSIGHTS] scandir {subdir} failed: {exc}")
    return out


def _insights_build_summary() -> dict:
    """Read ALL persistent data from ~/apex-data/ and reduce it to an
    aggregate summary dict suitable for a single Claude prompt. We don't
    dump raw rows — a year of trading would blow the context window. We
    send counts, rates, top/bottom extracts, and per-wallet / per-source /
    per-week rollups instead."""
    trades  = _apex_read_all_json(APEX_DATA_TRADES)
    signals = _apex_read_all_json(APEX_DATA_SIGNALS)
    errors  = _apex_read_all_json(APEX_DATA_ERRORS)
    whales  = _apex_read_all_json(APEX_DATA_WHALES)

    summary: dict = {
        "counts": {
            "trades":         len(trades),
            "signals":        len(signals),
            "errors":         len(errors),
            "whale_activity": len(whales),
        },
    }

    # ---- Trade aggregates ----------------------------------------------
    if trades:
        pnl_vals   = [float(t.get("pnl_sol") or 0) for t in trades]
        wins       = sum(1 for p in pnl_vals if p >= 0)
        losses     = len(pnl_vals) - wins
        total_pnl  = sum(pnl_vals)
        first_ts   = min(float(t.get("exit_time") or t.get("entry_time") or 0) for t in trades)
        last_ts    = max(float(t.get("exit_time") or t.get("entry_time") or 0) for t in trades)

        # Per-wallet rollup (risk-adjusted = mean / stdev)
        by_wallet: dict[str, dict] = {}
        for t in trades:
            w = t.get("whale_name") or "unknown"
            b = by_wallet.setdefault(w, {"trades": 0, "wins": 0, "pnl_vals": []})
            b["trades"] += 1
            p = float(t.get("pnl_sol") or 0)
            if p >= 0: b["wins"] += 1
            b["pnl_vals"].append(p)
        for w, b in by_wallet.items():
            vals = b.pop("pnl_vals")
            b["total_pnl_sol"]  = round(sum(vals), 4)
            b["avg_pnl_sol"]    = round(sum(vals) / len(vals), 4) if vals else 0.0
            b["win_rate"]       = round(b["wins"] / b["trades"] * 100, 1) if b["trades"] else 0.0
            if len(vals) >= 2:
                mean   = sum(vals) / len(vals)
                stdev  = (sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5
                b["risk_adj"]   = round(mean / stdev, 3) if stdev > 0 else None
            else:
                b["risk_adj"]   = None
            b["best_pnl_sol"]   = round(max(vals), 4) if vals else 0.0
            b["worst_pnl_sol"]  = round(min(vals), 4) if vals else 0.0

        # Per-source rollup
        by_source: dict[str, dict] = {}
        for t in trades:
            src = t.get("source") or "unknown"
            if src == "whale":
                src = "whale_copy"
            b = by_source.setdefault(src, {"trades": 0, "wins": 0, "total_pnl_sol": 0.0})
            b["trades"] += 1
            p = float(t.get("pnl_sol") or 0)
            if p >= 0: b["wins"] += 1
            b["total_pnl_sol"] += p
        for b in by_source.values():
            b["win_rate"]      = round(b["wins"] / b["trades"] * 100, 1) if b["trades"] else 0.0
            b["total_pnl_sol"] = round(b["total_pnl_sol"], 4)

        # Per-exit-reason rollup
        by_reason: dict[str, dict] = {}
        for t in trades:
            r = t.get("exit_reason") or "unknown"
            b = by_reason.setdefault(r, {"count": 0, "total_pnl_sol": 0.0})
            b["count"] += 1
            b["total_pnl_sol"] += float(t.get("pnl_sol") or 0)
        for b in by_reason.values():
            b["total_pnl_sol"] = round(b["total_pnl_sol"], 4)

        # Weekly trend (ISO week) for "is win rate improving?"
        by_week: dict[str, dict] = {}
        for t in trades:
            ts = float(t.get("exit_time") or t.get("entry_time") or 0)
            if ts <= 0:
                continue
            iso_year, iso_week, _ = time.gmtime(ts).tm_year, 0, 0
            # time.gmtime doesn't give iso week directly; use strftime
            week_key = time.strftime("%G-W%V", time.gmtime(ts))
            b = by_week.setdefault(week_key, {"trades": 0, "wins": 0, "total_pnl_sol": 0.0})
            b["trades"] += 1
            p = float(t.get("pnl_sol") or 0)
            if p >= 0: b["wins"] += 1
            b["total_pnl_sol"] += p
        for b in by_week.values():
            b["win_rate"]      = round(b["wins"] / b["trades"] * 100, 1) if b["trades"] else 0.0
            b["total_pnl_sol"] = round(b["total_pnl_sol"], 4)

        # Top 5 / bottom 5 individual trades
        sorted_by_pnl = sorted(trades, key=lambda t: float(t.get("pnl_sol") or 0), reverse=True)
        def _extract(t: dict) -> dict:
            return {
                "symbol":  t.get("token_symbol"),
                "whale":   t.get("whale_name"),
                "source":  t.get("source"),
                "pnl_sol": round(float(t.get("pnl_sol") or 0), 4),
                "pnl_pct": round(float(t.get("pnl_pct") or 0), 1),
                "exit_reason": t.get("exit_reason"),
            }
        top5    = [_extract(t) for t in sorted_by_pnl[:5]]
        bottom5 = [_extract(t) for t in sorted_by_pnl[-5:] if float(t.get("pnl_sol") or 0) < 0]

        summary["trades"] = {
            "total":        len(trades),
            "wins":         wins,
            "losses":       losses,
            "win_rate":     round(wins / len(trades) * 100, 1),
            "total_pnl_sol": round(total_pnl, 4),
            "first_ts_iso": time.strftime("%Y-%m-%d", time.gmtime(first_ts)) if first_ts else None,
            "last_ts_iso":  time.strftime("%Y-%m-%d", time.gmtime(last_ts))  if last_ts  else None,
            "by_wallet":    by_wallet,
            "by_source":    by_source,
            "by_reason":    by_reason,
            "by_week":      by_week,
            "top5":         top5,
            "bottom5":      bottom5,
        }

    # ---- Error patterns ------------------------------------------------
    if errors:
        by_err_reason: dict[str, int] = {}
        for e in errors:
            r = e.get("reason") or "unknown"
            by_err_reason[r] = by_err_reason.get(r, 0) + 1
        summary["errors"] = {
            "total":           len(errors),
            "by_reason":       dict(sorted(by_err_reason.items(), key=lambda kv: -kv[1])[:10]),
            "most_common":     max(by_err_reason.items(), key=lambda kv: kv[1])[0] if by_err_reason else None,
        }

    # ---- Momentum scanner hit rate ------------------------------------
    if signals:
        mom = [s for s in signals if s.get("source") == "momentum_scanner"]
        if mom:
            by_action: dict[str, int] = {}
            scores: list[int] = []
            win_scores: list[int] = []
            loss_scores: list[int] = []
            for s in mom:
                by_action[s.get("action", "unknown")] = by_action.get(s.get("action", "unknown"), 0) + 1
                if s.get("action") == "bought":
                    sc = int(s.get("claude_score") or 0)
                    if sc: scores.append(sc)
            bought = by_action.get("bought", 0)
            watched = by_action.get("watched", 0)
            total_mom = sum(by_action.values())
            summary["momentum"] = {
                "total_processed":   total_mom,
                "by_action":         by_action,
                "bought":            bought,
                "watched":           watched,
                "hit_rate_vs_scanned": round(bought / total_mom * 100, 1) if total_mom else 0.0,
                "avg_buy_score":     round(sum(scores) / len(scores), 1) if scores else None,
            }

    # ---- Whale activity hit rate --------------------------------------
    if whales:
        by_whale: dict[str, dict] = {}
        skip_reasons: dict[str, int] = {}
        for w in whales:
            name = w.get("whale_name") or "unknown"
            b = by_whale.setdefault(name, {"signals": 0, "apex_bought": 0})
            b["signals"] += 1
            if w.get("apex_bought"):
                b["apex_bought"] += 1
            elif w.get("reason_if_skipped"):
                skip_reasons[w["reason_if_skipped"]] = skip_reasons.get(w["reason_if_skipped"], 0) + 1
        for b in by_whale.values():
            b["bought_rate"] = round(b["apex_bought"] / b["signals"] * 100, 1) if b["signals"] else 0.0
        summary["whale_activity"] = {
            "total_signals":  len(whales),
            "by_whale":       by_whale,
            "skip_reasons":   dict(sorted(skip_reasons.items(), key=lambda kv: -kv[1])[:10]),
        }

    return summary


async def _send_insights(
    session: aiohttp.ClientSession,
    reply_chat_id: str | None,
    triggered_by: str = "/insights",
    weekly: bool = False,
) -> None:
    """Build the full-history summary, ask Claude (sonnet-4-6) for a deep
    insights report, save the output to analysis/, and send to Telegram.
    If reply_chat_id is None → broadcast to both chat IDs; else reply to
    that single chat only (the /insights command path)."""
    try:
        summary = _insights_build_summary()
        logger.info(
            f"[INSIGHTS] built summary — trades={summary.get('counts', {}).get('trades', 0)}, "
            f"errors={summary.get('counts', {}).get('errors', 0)}, "
            f"signals={summary.get('counts', {}).get('signals', 0)}, "
            f"weekly={weekly}, triggered_by={triggered_by}"
        )

        framing = (
            "This is a WEEKLY DEEP-DIVE — reference week-over-week changes prominently. "
            if weekly else
            ""
        )
        prompt = (
            "You are a trading strategy analyst with access to complete historical "
            "data from a Solana memecoin copy-trading bot. Analyse ALL the data "
            "provided and give a comprehensive insights report. Be specific, "
            "reference actual numbers and trade names. No bare HTML tags like "
            "<word>. " + framing + "\n\n"
            f"Complete historical data:\n{json.dumps(summary)}\n\n"
            "Provide:\n"
            "1. OVERALL PERFORMANCE — total trades, win rate trend over time (is it "
            "improving? reference by_week), PnL trajectory\n"
            "2. WALLET RANKINGS — rank all wallets by risk-adjusted returns, not "
            "just raw PnL. Use by_wallet.risk_adj where available.\n"
            "3. WHAT IS ACTUALLY WORKING — specific conditions, entry points, exit "
            "strategies that have produced wins (cite top5)\n"
            "4. WHAT IS FAILING — specific patterns in losses (cite bottom5 + "
            "by_reason + errors.most_common)\n"
            "5. STRATEGY EVOLUTION — how performance changed since the strategy "
            "updates (mirror sell → trailing stop; bigboy disabled 2026-04-19; "
            "momentum velocity 3% → 4% → 2.5%). Look at by_week around those dates.\n"
            "6. MOMENTUM SCANNER ASSESSMENT — real opportunities or noise? Use "
            "momentum.hit_rate_vs_scanned + avg_buy_score + compare momentum "
            "by_source PnL to whale_copy by_source PnL.\n"
            "7. 5 SPECIFIC ACTIONABLE IMPROVEMENTS — ranked by expected impact\n\n"
            "Format for Telegram with emojis and <b>bold</b> headers. Max 800 words."
        )

        body = await _claude_complete(prompt, max_tokens=4000, label="insights")
        if not body:
            err = "⚠️ Insights: Claude unavailable — try again shortly."
            if reply_chat_id:
                await _analyst_send_to_chat(session, reply_chat_id, err, label="insights")
            else:
                _analyst_send_to_all(err, label="insights")
            return

        # For sonnet-4-6 output, still sanitize defensively and route through chunker.
        sanitised = _sanitize_tg_html(body)

        # Persist to disk
        stamp = time.strftime("%Y-%m-%d-%H", time.gmtime())
        if weekly:
            header = "📅 <b>WEEKLY DEEP-DIVE</b>\n\n"
            save_path = os.path.join(
                APEX_DATA_ANALYSIS_WEEKLY,
                f"{time.strftime('%Y-%m-%d', time.gmtime())}.txt",
            )
        else:
            header = ""
            save_path = os.path.join(
                APEX_DATA_ANALYSIS,
                f"{stamp}-insights.txt",
            )
        _apex_data_save_text(save_path, body)

        final = header + sanitised
        if reply_chat_id:
            await _analyst_send_to_chat(session, reply_chat_id, final, label="insights")
        else:
            _analyst_send_to_all(final, label="insights")
    except Exception as exc:
        logger.error(f"[INSIGHTS] crashed: {exc}", exc_info=True)
        if reply_chat_id:
            try:
                await _analyst_send_to_chat(
                    session, reply_chat_id,
                    f"⚠️ /insights failed: {type(exc).__name__}",
                    label="insights",
                )
            except Exception:
                pass


async def hourly_analyst_loop(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
) -> None:
    """Every top-of-hour: fire an analyst brief (respecting an
    ANALYST_BRIEF_INTERVAL_HOURS rate limit so a restart doesn't
    double-fire), and at 00/06/12/18 UTC also fire the 6-hour strategy
    check (with its own 5.5h rate limit)."""
    global _analyst_last_brief, _analyst_last_strategy_check
    logger.info(
        f"[ANALYST] loop started — brief every {ANALYST_BRIEF_INTERVAL_HOURS}h, "
        f"strategy check at 00/06/12/18 UTC, aligning to next top-of-hour"
    )
    # Align to next top-of-hour first
    t  = time.gmtime()
    secs_to_next_hour = 3600 - (t.tm_min * 60 + t.tm_sec)
    await asyncio.sleep(secs_to_next_hour)

    while True:
        try:
            _prune_momentum_events()
            now = time.time()

            # Market brief — fires every ANALYST_BRIEF_INTERVAL_HOURS hours.
            # Loop still wakes every hour so the 6-hour strategy check can
            # land on its UTC slots.
            if now - _analyst_last_brief >= ANALYST_BRIEF_INTERVAL_HOURS * 3600:
                await _send_hourly_brief(session)
                _analyst_last_brief = now
            else:
                logger.info(
                    "[ANALYST:hourly] skipping brief — "
                    f"{(now - _analyst_last_brief) / 60:.1f}m since last brief "
                    f"(rate limit {ANALYST_BRIEF_INTERVAL_HOURS}h)"
                )

            # 6-hour strategy check — at 00/06/12/18 UTC with ≥ 5.5h guard
            hour_utc = time.gmtime(now).tm_hour
            if hour_utc in (0, 6, 12, 18) and now - _analyst_last_strategy_check >= 5.5 * 3600:
                await _send_strategy_check()
                _analyst_last_strategy_check = now
        except Exception as exc:
            logger.error(f"[ANALYST] loop tick crashed: {exc}", exc_info=True)

        # Sleep until next top-of-hour
        t  = time.gmtime()
        secs_to_next_hour = 3600 - (t.tm_min * 60 + t.tm_sec)
        await asyncio.sleep(secs_to_next_hour)


# --- Wallet Lore tracker ---------------------------------------------------
# Observation-only wallet intelligence. Polls a manually-curated address pool
# (state/lore_wallets.json), detects buys/sells via parsed-tx pre/post token
# balance diff, attempts buy→sell PnL matching, and persists rolling stats to
# state/wallet_lore.json. /walletlore command runs Claude analysis on demand.

def _load_lore_wallets_input() -> dict[str, str]:
    """Load address → alias map from lore_wallets.json. Empty dict if missing."""
    try:
        with open(LORE_WALLETS_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _empty_lore_entry(addr: str, alias: str) -> dict:
    now = time.time()
    return {
        "address":               addr,
        "alias":                 alias,
        "first_seen":            now,
        "last_active":           0.0,
        "last_checked_sig":      "",
        "active":                True,
        "total_trades":          0,
        "wins":                  0,
        "losses":                0,
        "win_rate":              0.0,
        "total_pnl_sol":         0.0,
        "avg_position_size_sol": 0.0,
        "avg_hold_time_mins":    0.0,
        "avg_win_pct":           0.0,
        "avg_loss_pct":          0.0,
        "best_trade":            {"token": "", "pnl_pct": 0.0, "date": ""},
        "worst_trade":           {"token": "", "pnl_pct": 0.0, "date": ""},
        "mcap_preference":       {"under_10k": 0, "10k_50k": 0, "50k_200k": 0, "over_200k": 0},
        "avg_entry_mcap":        0.0,
        "style":                 "unknown",
        "active_hours_utc":      [],
        "days_since_last_trade": 0,
        "recent_trades":         [],
        # Internal-only: open positions kept here so a sell can be matched to
        # an earlier buy and realised PnL recorded. Not part of the public
        # schema — leading underscore signals "implementation detail".
        "_open_positions":       {},
    }


def _load_wallet_lore() -> dict[str, dict]:
    try:
        with open(WALLET_LORE_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_wallet_lore() -> None:
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        tmp = WALLET_LORE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_WALLET_LORE, f, indent=2)
        os.replace(tmp, WALLET_LORE_FILE)
    except Exception as exc:
        logger.warning(f"[LORE] save failed: {exc}")


def _ensure_lore_entry(addr: str, alias: str) -> dict:
    if addr not in _WALLET_LORE:
        _WALLET_LORE[addr] = _empty_lore_entry(addr, alias)
    else:
        _WALLET_LORE[addr]["alias"] = alias  # operator may rename
    return _WALLET_LORE[addr]


def _detect_lore_token_change(tx: dict, wallet: str) -> tuple[str | None, float, float]:
    """Look at parsed tx pre/post balances for this wallet. Return:
        (mint, token_delta, sol_flow)
    where token_delta > 0 means tokens received (buy candidate),
          token_delta < 0 means tokens sent     (sell candidate),
          sol_flow is the wallet's lamport delta in SOL (signed).
    Returns (None, 0, 0) if not a clean swap."""
    if not tx or (tx.get("meta") or {}).get("err"):
        return None, 0.0, 0.0
    meta = tx.get("meta") or {}
    pre  = meta.get("preTokenBalances")  or []
    post = meta.get("postTokenBalances") or []

    def _amount(b: dict) -> float:
        ui = b.get("uiTokenAmount") or {}
        v  = ui.get("uiAmount")
        if v is not None:
            return float(v)
        raw = int(ui.get("amount") or 0)
        dec = int(ui.get("decimals") or 0)
        return raw / (10 ** dec) if dec > 0 else float(raw)

    def _collect(balances: list) -> dict[str, float]:
        out: dict[str, float] = {}
        for b in balances:
            if b.get("owner") != wallet:
                continue
            mint = b.get("mint")
            if not mint or mint in (SOL_MINT, WSOL_MINT):
                continue
            out[mint] = out.get(mint, 0.0) + _amount(b)
        return out

    pre_amt  = _collect(pre)
    post_amt = _collect(post)
    mints = set(pre_amt) | set(post_amt)
    biggest_mint  = None
    biggest_delta = 0.0
    for m in mints:
        delta = post_amt.get(m, 0.0) - pre_amt.get(m, 0.0)
        if abs(delta) > abs(biggest_delta):
            biggest_mint, biggest_delta = m, delta
    if biggest_mint is None or biggest_delta == 0:
        return None, 0.0, 0.0

    # SOL flow for this wallet: pre/post lamport balance at its accountKey index.
    accounts = (tx.get("transaction") or {}).get("message", {}).get("accountKeys") or []
    pre_sol  = meta.get("preBalances")  or []
    post_sol = meta.get("postBalances") or []
    sol_flow = 0.0
    for i, k in enumerate(accounts):
        addr = k if isinstance(k, str) else (k.get("pubkey") if isinstance(k, dict) else None)
        if addr == wallet and i < len(pre_sol) and i < len(post_sol):
            sol_flow = (post_sol[i] - pre_sol[i]) / 1_000_000_000
            break

    return biggest_mint, biggest_delta, sol_flow


def _mcap_bucket(mc: float) -> str:
    if mc < 10_000:  return "under_10k"
    if mc < 50_000:  return "10k_50k"
    if mc < 200_000: return "50k_200k"
    return "over_200k"


def _classify_lore_style(lore: dict) -> str:
    """Heuristic 1-2 word label combining quality (win-rate) + behavior
    (entry mcap or hold time). Caps at 2 words to avoid degenerate stacks
    like 'sharp scalper sniper'. Returns 'unknown' until 3 trades."""
    if lore["total_trades"] < 3:
        return "unknown"
    quality = ""
    if lore["win_rate"] >= 0.55:
        quality = "sharp"
    elif lore["win_rate"] < 0.30:
        quality = "degen"

    if lore["avg_entry_mcap"] > 0 and lore["avg_entry_mcap"] < 10_000:
        behavior = "sniper"
    else:
        h = lore["avg_hold_time_mins"]
        if   h < 5:    behavior = "scalper"
        elif h <= 30:  behavior = "trader"
        else:          behavior = "conviction"
    return f"{quality} {behavior}".strip()


def _record_lore_close(lore: dict, mint: str, buy_sol: float, sell_sol: float,
                       buy_ts: float, sell_ts: float, buy_mcap: float) -> None:
    """Update aggregates after a matched buy → full sell."""
    pnl_sol  = sell_sol - buy_sol
    pnl_pct  = (sell_sol / buy_sol - 1) * 100 if buy_sol > 0 else 0.0
    hold_min = (sell_ts - buy_ts) / 60.0

    lore["total_trades"] += 1
    if pnl_sol >= 0:
        lore["wins"] += 1
    else:
        lore["losses"] += 1
    n = lore["total_trades"]
    lore["win_rate"]      = round(lore["wins"] / n, 4) if n else 0.0
    lore["total_pnl_sol"] = round(lore["total_pnl_sol"] + pnl_sol, 6)

    # Running averages (exact, not exponential)
    lore["avg_position_size_sol"] = round(
        ((lore["avg_position_size_sol"] * (n - 1)) + buy_sol) / n, 6)
    lore["avg_hold_time_mins"]    = round(
        ((lore["avg_hold_time_mins"]    * (n - 1)) + hold_min) / n, 2)
    if buy_mcap > 0:
        lore["avg_entry_mcap"] = round(
            ((lore["avg_entry_mcap"] * (n - 1)) + buy_mcap) / n, 0)
        bucket = _mcap_bucket(buy_mcap)
        lore["mcap_preference"][bucket] = lore["mcap_preference"].get(bucket, 0) + 1

    if pnl_pct >= 0:
        wn = lore["wins"]
        if wn > 0:
            lore["avg_win_pct"] = round(
                ((lore["avg_win_pct"] * (wn - 1)) + pnl_pct) / wn, 2)
    else:
        ln = lore["losses"]
        if ln > 0:
            lore["avg_loss_pct"] = round(
                ((lore["avg_loss_pct"] * (ln - 1)) + pnl_pct) / ln, 2)

    if pnl_pct > lore["best_trade"].get("pnl_pct", 0):
        lore["best_trade"] = {
            "token": mint, "pnl_pct": round(pnl_pct, 2),
            "date":  time.strftime("%Y-%m-%d", time.gmtime(sell_ts)),
        }
    if (lore["worst_trade"].get("token") == "" or
            pnl_pct < lore["worst_trade"].get("pnl_pct", 0)):
        lore["worst_trade"] = {
            "token": mint, "pnl_pct": round(pnl_pct, 2),
            "date":  time.strftime("%Y-%m-%d", time.gmtime(sell_ts)),
        }

    hr = time.gmtime(buy_ts).tm_hour
    if hr not in lore["active_hours_utc"]:
        lore["active_hours_utc"] = sorted(set(lore["active_hours_utc"] + [hr]))

    lore["recent_trades"].append({
        "token":     mint,
        "buy_ts":    buy_ts,
        "sell_ts":   sell_ts,
        "buy_sol":   round(buy_sol, 6),
        "sell_sol":  round(sell_sol, 6),
        "pnl_sol":   round(pnl_sol, 6),
        "pnl_pct":   round(pnl_pct, 2),
        "hold_mins": round(hold_min, 2),
        "buy_mcap":  buy_mcap,
    })
    lore["recent_trades"] = lore["recent_trades"][-LORE_RECENT_TRADES_KEEP:]
    lore["style"] = _classify_lore_style(lore)


async def _process_lore_signature(session: aiohttp.ClientSession, rpc_url: str,
                                  addr: str, sig: str, lore: dict) -> bool:
    """Examine one signature, classify as buy/sell/neither, update lore.
    Returns True if a trade event was recorded."""
    if not sig:
        return False
    tx = get_transaction(rpc_url, sig)
    if tx is None:
        return False
    mint, token_delta, sol_flow = _detect_lore_token_change(tx, addr)
    if mint is None or token_delta == 0:
        return False

    block_time = int(tx.get("blockTime") or time.time())
    is_buy  = token_delta > 0 and sol_flow < 0   # received tokens, spent SOL
    is_sell = token_delta < 0 and sol_flow > 0   # sent tokens, received SOL
    if not (is_buy or is_sell):
        return False

    open_pos = lore.setdefault("_open_positions", {})

    if is_buy:
        buy_mcap = 0.0
        try:
            pair = await fetch_dexscreener(session, mint)
            if pair:
                buy_mcap = float((pair.get("marketCap") or pair.get("fdv") or 0))
        except Exception:
            pass
        existing = open_pos.get(mint)
        if existing:
            existing["buy_sol"]  = round(existing["buy_sol"] + abs(sol_flow), 6)
            existing["buy_mcap"] = existing.get("buy_mcap") or buy_mcap
        else:
            open_pos[mint] = {
                "buy_sol":  round(abs(sol_flow), 6),
                "buy_ts":   block_time,
                "buy_mcap": buy_mcap,
            }
        lore["last_active"] = block_time
        return True

    # is_sell
    existing = open_pos.pop(mint, None)
    if existing is None:
        # Unmatched sell — record without faking a PnL number
        lore["recent_trades"].append({
            "token":    mint,
            "buy_ts":   None,
            "sell_ts":  block_time,
            "sell_sol": round(sol_flow, 6),
            "pnl_sol":  None,
            "pnl_pct":  None,
            "note":     "unmatched_sell",
        })
        lore["recent_trades"] = lore["recent_trades"][-LORE_RECENT_TRADES_KEEP:]
    else:
        _record_lore_close(
            lore, mint,
            buy_sol  = existing["buy_sol"],
            sell_sol = sol_flow,
            buy_ts   = existing["buy_ts"],
            sell_ts  = block_time,
            buy_mcap = existing.get("buy_mcap") or 0,
        )
    lore["last_active"] = block_time
    return True


def _initial_lore_activity_scan(rpc_url: str) -> tuple[int, int, int]:
    """One-shot at startup: ensure a lore record exists for each LORE_WALLETS
    entry, fetch latest sig to set last_active, mark active vs inactive based
    on LORE_INACTIVE_DAYS. Returns (total, active, inactive)."""
    cutoff = time.time() - LORE_INACTIVE_DAYS * 86_400
    active = inactive = 0
    for addr, alias in LORE_WALLETS.items():
        lore = _ensure_lore_entry(addr, alias)
        sigs = get_recent_signatures(rpc_url, addr, limit=1)
        if not sigs:
            lore["active"] = False
            inactive += 1
            continue
        bt = int(sigs[0].get("blockTime") or 0)
        lore["last_active"]           = bt
        lore["last_checked_sig"]      = sigs[0].get("signature") or ""
        lore["days_since_last_trade"] = max(0, int((time.time() - bt) / 86_400)) if bt else 999
        if bt >= cutoff:
            lore["active"] = True
            active += 1
        else:
            lore["active"] = False
            inactive += 1
    _save_wallet_lore()
    return len(LORE_WALLETS), active, inactive


async def lore_wallet_poll_loop(session: aiohttp.ClientSession, rpc_url: str) -> None:
    """Poll active LORE_WALLETS every LORE_POLL_INTERVAL_SEC, staggered by
    LORE_PER_WALLET_SLEEP_SEC to spread RPC load. Inactive wallets are
    re-checked once per cycle (cheap getSignatures limit=1) so they
    auto-reactivate on new activity."""
    if not LORE_WALLETS:
        logger.info("[LORE] no wallets configured — poller idle (add addresses to state/lore_wallets.json)")
        while True:
            await asyncio.sleep(3600)

    total, active, inactive = _initial_lore_activity_scan(rpc_url)
    logger.info(
        f"[LORE] loaded {total} wallets, {active} active "
        f"(traded in last {LORE_INACTIVE_DAYS}d), {inactive} inactive (skipped)"
    )

    while True:
        cycle_start = time.time()
        active_addrs = [a for a, lore in _WALLET_LORE.items() if lore.get("active")]
        # Dynamic stagger so a full cycle fits inside LORE_POLL_INTERVAL_SEC
        # when possible. Floor protects RPC at very high wallet counts.
        stagger = max(
            LORE_MIN_STAGGER_SEC,
            LORE_POLL_INTERVAL_SEC / max(1, len(active_addrs)),
        )
        new_txns = 0

        # Active poll: full sig fan-out per wallet
        for addr in active_addrs:
            lore = _WALLET_LORE.get(addr)
            if lore is None:
                continue
            try:
                sigs = get_recent_signatures(rpc_url, addr, limit=10)
                if sigs:
                    last_sig = lore.get("last_checked_sig") or ""
                    new_sigs: list[dict] = []
                    for s in sigs:
                        if s.get("signature") == last_sig:
                            break
                        new_sigs.append(s)
                    # Process oldest-first so buy→sell sequencing matches
                    for s in reversed(new_sigs):
                        if await _process_lore_signature(
                                session, rpc_url, addr, s.get("signature", ""), lore):
                            new_txns += 1
                    if new_sigs:
                        lore["last_checked_sig"] = sigs[0].get("signature") or ""
            except Exception as exc:
                logger.warning(f"[LORE] poll error {addr[:8]}: {exc}")
            await asyncio.sleep(stagger)

        # Cheap reactivation pass: one sig fetch per inactive wallet
        for addr, lore in _WALLET_LORE.items():
            if lore.get("active"):
                continue
            try:
                sigs = get_recent_signatures(rpc_url, addr, limit=1)
                if not sigs:
                    continue
                bt = int(sigs[0].get("blockTime") or 0)
                if bt and (time.time() - bt) < LORE_INACTIVE_DAYS * 86_400:
                    lore["active"]      = True
                    lore["last_active"] = bt
                    logger.info(
                        f"[LORE] reactivated {addr[:8]} "
                        f"(alias={lore.get('alias')}) — recent activity detected"
                    )
            except Exception:
                pass
            await asyncio.sleep(stagger)

        _save_wallet_lore()
        logger.info(
            f"[LORE] polled {len(active_addrs)} active wallets, {new_txns} new txns detected"
        )

        elapsed = time.time() - cycle_start
        if elapsed < LORE_POLL_INTERVAL_SEC:
            await asyncio.sleep(LORE_POLL_INTERVAL_SEC - elapsed)


def _walletlore_summary_for_claude() -> str:
    """Compact line-per-wallet snapshot for Claude. Filters out wallets with
    fewer than LORE_MIN_TRADES_FOR_REPORT closed trades."""
    rows = []
    for addr, lore in _WALLET_LORE.items():
        if lore["total_trades"] < LORE_MIN_TRADES_FOR_REPORT:
            continue
        n  = lore["total_trades"]
        ra = (lore["total_pnl_sol"] / n) if n else 0  # crude risk-adj: avg PnL/trade
        rows.append((ra, addr, lore))
    rows.sort(key=lambda r: r[0], reverse=True)
    if not rows:
        return "(no wallets with ≥5 closed trades yet — keep collecting data)"
    lines = []
    for ra, addr, lore in rows[:50]:
        alias = lore.get("alias") or "?"
        lines.append(
            f"- {alias} ({addr[:8]}): trades={lore['total_trades']} "
            f"win_rate={lore['win_rate']:.0%} pnl={lore['total_pnl_sol']:+.2f}SOL "
            f"avg_size={lore['avg_position_size_sol']:.3f} "
            f"avg_hold={lore['avg_hold_time_mins']:.1f}m "
            f"avg_entry_mcap=${lore['avg_entry_mcap']:,.0f} style={lore['style']} "
            f"best={lore['best_trade'].get('pnl_pct', 0):+.0f}% "
            f"worst={lore['worst_trade'].get('pnl_pct', 0):+.0f}% "
            f"avg_win={lore['avg_win_pct']:+.1f}% avg_loss={lore['avg_loss_pct']:+.1f}%"
        )
    return "\n".join(lines)


async def _send_walletlore_report(triggered_by: str = "/walletlore",
                                   reply_chat_id: str | None = None,
                                   header: str | None = None) -> None:
    """Build the wallet intelligence report via Claude Sonnet 4.6 and post it
    to Telegram. Persists a copy to apex-data/analysis/walletlore-YYYY-MM-DD.txt."""
    summary = _walletlore_summary_for_claude()
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[LORE] ANTHROPIC_API_KEY missing — cannot run /walletlore")
        send_telegram("⚠️ /walletlore failed — ANTHROPIC_API_KEY missing")
        return

    prompt = (
        "You are a trading analyst reviewing Solana memecoin trader wallets. "
        "Based on the data provided give a comprehensive intelligence report. "
        "No bare HTML tags.\n\n"
        f"Wallet data: {summary}\n\n"
        "Provide:\n"
        "1. TOP 10 WALLETS — ranked by risk-adjusted returns with trading style\n"
        "2. RECOMMENDED FOR APEX — which wallets to add to copy-trading and why\n"
        "3. WALLETS TO AVOID — worst performers with explanation\n"
        "4. TRADING PATTERNS — what strategies work across best wallets\n"
        "5. MARKET INSIGHTS — what these wallets reveal about current conditions\n\n"
        "Format for Telegram with emojis. Max 600 words."
    )

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if hasattr(b, "text"))
    except Exception as exc:
        logger.error(f"[LORE] Claude call failed: {exc}", exc_info=True)
        send_telegram(f"⚠️ /walletlore Claude call failed: {type(exc).__name__}")
        return

    full = (header + "\n\n" if header else "") + text

    try:
        os.makedirs(APEX_DATA_ANALYSIS, exist_ok=True)
        fname = os.path.join(APEX_DATA_ANALYSIS, f"walletlore-{time.strftime('%Y-%m-%d')}.txt")
        with open(fname, "w") as f:
            f.write(full)
    except Exception as exc:
        logger.warning(f"[LORE] failed to persist walletlore file: {exc}")

    # send_telegram broadcasts to TELEGRAM_CHAT_IDS — both numbers in the env.
    send_telegram(full)
    logger.info(f"[LORE] walletlore report sent (triggered by {triggered_by})")


async def lore_weekly_loop() -> None:
    """Sleep until next Sunday 23:00 UTC, fire walletlore report, repeat.
    Sundays sit one day after the existing Saturday 23:00 weekly insights
    deep-dive — no Claude-call collision."""
    while True:
        now = time.gmtime()
        # tm_wday: Mon=0 .. Sat=5 .. Sun=6
        days_ahead = (6 - now.tm_wday) % 7
        if days_ahead == 0 and (now.tm_hour > 23 or
                                (now.tm_hour == 23 and now.tm_min > 0)):
            days_ahead = 7
        secs = (days_ahead * 86_400
                + (23 - now.tm_hour) * 3600
                - now.tm_min * 60
                - now.tm_sec)
        if secs <= 0:
            secs = 60
        logger.info(
            f"[LORE] weekly walletlore in "
            f"{secs // 3600}h {(secs % 3600) // 60}m"
        )
        await asyncio.sleep(secs)
        try:
            await _send_walletlore_report(
                triggered_by="weekly_auto",
                header="📚 <b>WEEKLY WALLET LORE REPORT</b>",
            )
        except Exception as exc:
            logger.error(f"[LORE] weekly report crashed: {exc}", exc_info=True)
        # Cushion so we don't immediately re-fire from rounding
        await asyncio.sleep(300)


# --- External signal API ---------------------------------------------------
# handle_cto_signal() is called directly by DexAlert scanner, and
# cto_queue_loop() drains signals dropped into state/cto_queue.json by
# external monitors (e.g. cto_radar_monitor.py).

CTO_QUEUE_FILE        = os.path.join(_STATE_DIR, "cto_queue.json")
CTO_QUEUE_POLL_SEC    = 30


def _cto_queue_read() -> list[dict]:
    try:
        with open(CTO_QUEUE_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    except Exception as exc:
        logger.warning(f"[CTO QUEUE] read failed: {exc}")
        return []


def _cto_queue_write(queue: list[dict]) -> None:
    try:
        os.makedirs(os.path.dirname(CTO_QUEUE_FILE), exist_ok=True)
        tmp = CTO_QUEUE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(queue, f, indent=2)
        os.replace(tmp, CTO_QUEUE_FILE)
    except Exception as exc:
        logger.warning(f"[CTO QUEUE] write failed: {exc}")


async def cto_queue_loop(
    session: aiohttp.ClientSession,
    rpc_url: str,
    wallet_pubkey: str,
) -> None:
    """Poll state/cto_queue.json every 30s, fire handle_cto_signal for each
    pending entry, then remove it. Entries are dicts with mint/name/symbol.
    Empty-queue heartbeat fires once every 5 minutes so the log shows the
    poller is alive without spamming a line every 30s."""
    logger.info(f"[CTO QUEUE] poller started — file={CTO_QUEUE_FILE} interval={CTO_QUEUE_POLL_SEC}s")
    _last_empty_heartbeat = 0.0
    while True:
        try:
            queue = _cto_queue_read()
            if queue:
                # Take a snapshot, clear the queue first so cto_radar can keep
                # appending while we process.
                pending = list(queue)
                _cto_queue_write([])
                logger.info(f"[CTO QUEUE] processing {len(pending)} pending signal(s)")
                for entry in pending:
                    mint   = entry.get("mint")   or ""
                    name   = entry.get("name")   or "?"
                    symbol = entry.get("symbol") or "?"
                    src    = entry.get("source") or "queue"
                    if not mint:
                        logger.warning(f"[CTO QUEUE] entry missing mint, dropping: {entry}")
                        continue
                    logger.info(
                        f"[CTO QUEUE] processing {symbol} ({mint[:8]}) from queue "
                        f"(source={src}, mcap=${entry.get('mcap', 0):,.0f})"
                    )
                    try:
                        await handle_cto_signal(
                            session=session,
                            token_mint=mint,
                            token_name=name,
                            token_symbol=symbol,
                            rpc_url=rpc_url,
                            wallet_pubkey=wallet_pubkey,
                        )
                    except Exception as exc:
                        logger.error(f"[CTO QUEUE] handle_cto_signal({symbol}) crashed: {exc}", exc_info=True)
            else:
                # Empty-queue heartbeat — log every 5 min, not every 30s
                if time.time() - _last_empty_heartbeat >= 300:
                    logger.info("[CTO QUEUE] checked queue — empty")
                    _last_empty_heartbeat = time.time()
        except Exception as exc:
            logger.error(f"[CTO QUEUE] loop iteration crashed: {exc}", exc_info=True)
        await asyncio.sleep(CTO_QUEUE_POLL_SEC)


# Window to wait before firing the CTO post-entry review (seconds).
CTO_REVIEW_WAIT_SEC      = 15 * 60
# Auto-hold threshold — price/mcap up at least this much → keep position.
CTO_REVIEW_AUTO_HOLD_PCT = 10.0
# Auto-dump threshold — price/mcap down at least this much → force sell.
CTO_REVIEW_AUTO_DUMP_PCT = -20.0


async def _cto_review_claude_decision(
    symbol:       str,
    entry_mcap:   float,
    current_mcap: float,
    pct_change:   float,
    vol_m5:       float,
    buys:         int,
    sells:        int,
    liq:          float,
) -> tuple[str, str]:
    """Call Claude haiku to decide HOLD or SELL for a CTO review in the
    flat/unclear band. Returns (decision, reason). decision is 'HOLD' or 'SELL'.
    Fail-open → returns ('HOLD', 'claude unavailable — default hold')."""
    api_key = os.getenv("CLAUDE_API_KEY", "")
    if not api_key:
        return "HOLD", "claude api key missing — default hold"

    prompt = (
        "You are reviewing a CTO position 15 minutes after entry. Decide HOLD or SELL.\n"
        f"Token: {symbol} | Entry mcap: ${entry_mcap:,.0f} | Current mcap: ${current_mcap:,.0f}\n"
        f"Price change: {pct_change:+.1f}%\n"
        f"5m volume: ${vol_m5:,.0f} | Buys: {buys} | Sells: {sells}\n"
        f"Liquidity: ${liq:,.0f}\n\n"
        "Reply with exactly: HOLD or SELL\n"
        "Then one line explaining why (max 15 words)"
    )

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp   = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        raw   = resp.content[0].text.strip()
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if not lines:
            return "HOLD", "empty claude response — default hold"
        verdict = lines[0].upper().strip().strip(".:,")
        reason  = lines[1] if len(lines) > 1 else "no reason given"
        if verdict.startswith("SELL"):
            return "SELL", reason
        return "HOLD", reason
    except Exception as exc:
        logger.warning(f"[CTO REVIEW] Claude call failed for {symbol}: {exc} — default hold")
        return "HOLD", "claude call failed — default hold"


async def cto_review_task(token_mint: str, token_symbol: str, wallet_pubkey: str) -> None:
    """Background review task spawned ~15 min after a CTO entry.

    Flow:
      1. Sleep until entry_time + CTO_REVIEW_WAIT_SEC (handles restart-rehydrated
         tasks — remaining delay is computed from entry_time).
      2. Fetch DexScreener pair data.
      3. Up >= +10% → HOLD (trailing stop carries from here).
         Down <= -20% → AUTO SELL (cto_no_momentum_dump).
         Anywhere else → ask Claude haiku.
      4. Mark pos.cto_review_pending = False with the decision + pct, persist.
    """
    try:
        pos = open_positions.get(token_mint)
        if not pos:
            return
        entry_time = float(pos.get("entry_time") or time.time())
        delay = max(0.0, (entry_time + CTO_REVIEW_WAIT_SEC) - time.time())
        if delay > 0:
            await asyncio.sleep(delay)

        pos = open_positions.get(token_mint)
        if not pos:
            logger.info(f"[CTO REVIEW] {token_symbol} already closed before review — skipping")
            return
        if not pos.get("cto_review_pending"):
            return

        entry_mcap = float(pos.get("mc_entry") or 0)
        entry_sol  = float(pos.get("entry_sol") or 0)

        async with aiohttp.ClientSession() as rev_session:
            dex_pair = await fetch_dexscreener(rev_session, token_mint)
            if dex_pair is None:
                logger.warning(f"[CTO REVIEW] {token_symbol} no DexScreener pair — defaulting to HOLD")
                pos["cto_review_pending"]  = False
                pos["cto_review_decision"] = "hold"
                pos["cto_review_pct"]      = None
                _save_positions()
                _apex_log_signal("cto_signal", "review_decision", {
                    "token_mint": token_mint, "symbol": token_symbol,
                    "decision": "hold", "reason": "no_dex_pair",
                    "pct": None,
                })
                return

            current_mcap = float(dex_pair.get("marketCap") or dex_pair.get("fdv") or 0)
            vol_m5       = float((dex_pair.get("volume") or {}).get("m5") or 0)
            tx_m5        = (dex_pair.get("txns") or {}).get("m5") or {}
            buys         = int(tx_m5.get("buys") or 0)
            sells        = int(tx_m5.get("sells") or 0)
            liq          = float((dex_pair.get("liquidity") or {}).get("usd") or 0)

            if entry_mcap > 0 and current_mcap > 0:
                pct = (current_mcap - entry_mcap) / entry_mcap * 100.0
            else:
                pct = 0.0

            pos["cto_review_pct"] = round(pct, 2)

            # --- Branch 1: AUTO HOLD (up >= 10%) ---
            if pct >= CTO_REVIEW_AUTO_HOLD_PCT:
                pos["cto_review_pending"]  = False
                pos["cto_review_decision"] = "hold"
                _save_positions()
                logger.info(
                    f"[CTO REVIEW] {token_symbol}: HOLDING — up {pct:+.1f}%, trailing stop active"
                )
                send_telegram(
                    f"\U0001f916 <b>CTO REVIEW</b> — {token_symbol} — <b>HOLDING</b>\n"
                    f"Price: {pct:+.1f}% | Vol 5m: ${vol_m5:,.0f}\n"
                    f"Reason: auto-hold (up ≥ {CTO_REVIEW_AUTO_HOLD_PCT:.0f}%)"
                )
                _apex_log_signal("cto_signal", "review_decision", {
                    "token_mint": token_mint, "symbol": token_symbol,
                    "decision": "hold", "reason": f"auto_up_{pct:.1f}pct",
                    "pct": round(pct, 2), "current_mcap": current_mcap,
                    "entry_mcap": entry_mcap, "vol_m5": vol_m5,
                    "buys_m5": buys, "sells_m5": sells, "liq": liq,
                })
                return

            # --- Branch 2: AUTO DUMP (down >= 20%) ---
            if pct <= CTO_REVIEW_AUTO_DUMP_PCT:
                logger.info(
                    f"[CTO REVIEW] {token_symbol}: AUTO SELL — down {pct:.1f}%, no momentum"
                )
                await _cto_review_execute_sell(
                    rev_session, token_mint, token_symbol, wallet_pubkey,
                    pos, pct, current_mcap, entry_mcap,
                    exit_reason="cto_no_momentum_dump",
                    claude_reason=f"auto_dump_{pct:.1f}pct",
                )
                return

            # --- Branch 3: FLAT — defer to Claude haiku ---
            decision, claude_reason = await _cto_review_claude_decision(
                token_symbol, entry_mcap, current_mcap, pct,
                vol_m5, buys, sells, liq,
            )
            logger.info(
                f"[CTO REVIEW] {token_symbol}: Claude says {decision} — {claude_reason}"
            )

            if decision == "HOLD":
                pos["cto_review_pending"]  = False
                pos["cto_review_decision"] = "hold"
                _save_positions()
                send_telegram(
                    f"\U0001f916 <b>CTO REVIEW</b> — {token_symbol} — <b>HOLDING</b>\n"
                    f"Price: {pct:+.1f}% | Vol 5m: ${vol_m5:,.0f}\n"
                    f"Claude: {claude_reason}"
                )
                _apex_log_signal("cto_signal", "review_decision", {
                    "token_mint": token_mint, "symbol": token_symbol,
                    "decision": "hold", "reason": claude_reason,
                    "pct": round(pct, 2), "current_mcap": current_mcap,
                    "entry_mcap": entry_mcap, "vol_m5": vol_m5,
                    "buys_m5": buys, "sells_m5": sells, "liq": liq,
                })
                return

            # Claude says SELL
            await _cto_review_execute_sell(
                rev_session, token_mint, token_symbol, wallet_pubkey,
                pos, pct, current_mcap, entry_mcap,
                exit_reason="cto_review_sell",
                claude_reason=claude_reason,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(f"[CTO REVIEW] {token_symbol} review task crashed: {exc}", exc_info=True)


async def _cto_review_execute_sell(
    session:       aiohttp.ClientSession,
    token_mint:    str,
    token_symbol:  str,
    wallet_pubkey: str,
    pos:           dict,
    pct:           float,
    current_mcap:  float,
    entry_mcap:    float,
    exit_reason:   str,
    claude_reason: str,
) -> None:
    """Execute the full-position sell for a CTO review decision of SELL."""
    amount_tokens = int(pos.get("amount_tokens") or 0)
    entry_sol     = float(pos.get("entry_sol") or 0)

    if entry_mcap and current_mcap:
        expected_sol = entry_sol * (current_mcap / entry_mcap)
    else:
        expected_sol = entry_sol

    sell_sig, sell_msg = await execute_sell_routed(
        session, token_mint, amount_tokens, wallet_pubkey, current_mcap or 0.0,
    )
    if not sell_sig:
        logger.error(f"[CTO REVIEW] {token_symbol} sell failed: {sell_msg}")
        send_telegram(
            f"\u26a0\ufe0f <b>CTO REVIEW SELL FAILED</b> — {token_symbol}\n"
            f"Reason: {sell_msg}\n"
            f"Position remains open — manual review needed."
        )
        pos["cto_review_pending"]  = False
        pos["cto_review_decision"] = "sell_failed"
        _save_positions()
        _apex_log_signal("cto_signal", "review_decision", {
            "token_mint": token_mint, "symbol": token_symbol,
            "decision": "sell_failed", "reason": sell_msg,
            "pct": round(pct, 2),
        })
        return

    logger.info(f"[BUY/SELL SIG] {token_mint[:8]} {exit_reason} sig={sell_sig}")

    pos["cto_review_decision"] = "sell"
    pos["cto_review_pending"]  = False
    pos["cto_review_pct"]      = round(pct, 2)
    _log_trade(pos, exit_reason, expected_sol, token_mint)
    del open_positions[token_mint]
    _save_positions()
    _mark_token_traded(token_mint)

    send_telegram(
        f"\U0001f916 <b>CTO REVIEW</b> — {token_symbol} — <b>SELLING</b>\n"
        f"Price: {pct:+.1f}% | Reason: {claude_reason}\n"
        f"Exit: no momentum detected"
    )
    _apex_log_signal("cto_signal", "review_decision", {
        "token_mint": token_mint, "symbol": token_symbol,
        "decision": "sell", "reason": claude_reason,
        "exit_reason": exit_reason,
        "pct": round(pct, 2), "current_mcap": current_mcap,
        "entry_mcap": entry_mcap,
    })


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

    # Step 1 — Duplicate check (WARN-level so silent drops are visible in logs)
    if token_mint in open_positions:
        logger.warning(
            f"{PREFIX} SKIP — {token_symbol} ({token_mint[:8]}) already in "
            f"open_positions — not re-entering"
        )
        return
    if token_mint in _token_blacklist and _token_blacklist[token_mint] > time.time():
        _bl_remain_min = (_token_blacklist[token_mint] - time.time()) / 60
        logger.warning(
            f"{PREFIX} SKIP — {token_symbol} ({token_mint[:8]}) blacklisted "
            f"(expires in {_bl_remain_min:.0f}min)"
        )
        return
    if token_mint in _traded_tokens:
        logger.warning(
            f"{PREFIX} SKIP — {token_symbol} ({token_mint[:8]}) is in "
            f"_traded_tokens (previously traded and exited, never-rebuy guard "
            f"is on). Use /cleartrades to allow rebuys."
        )
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

    # Honeypot guard — skip freeze-authority mints before any live buy.
    if not await _honeypot_guard(session, rpc_url, token_mint,
                                 symbol=token_symbol,
                                 source_label="cto_signal"):
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
        _apex_log_error(token_mint, "cto_signal", "cto_buy_failed", {"msg": swap_msg})
        send_telegram(
            f"⚠️ {PREFIX} <b>TX FAILED</b> — {token_symbol}\n"
            f"CA: <code>{token_mint}</code>\n"
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
            "entry_time":         time.time(),
            "entry_sol":          entry_sol,
            "original_entry_sol": entry_sol,   # never mutated — real PnL baseline
            "tp1_received_sol":   0.0,          # accumulates partial-exit proceeds
            "peak_sol":           entry_sol,
            "amount_tokens":      token_units,
            "whale":              "cto_signal",
            "buy_sol":            buy_sol,
            "claude_score":       claude_score,
            "min_target_hit":     False,
            "alerted_25pct_down": False,
            "source":             "cto_signal",
            "mc_entry":           mc_entry,
            "token_label":        token_label,
            "cto_review_pending": True,
            "cto_review_decision": None,
            "cto_review_pct":     None,
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
        logger.error(f"[BUY/SELL SIG] {token_mint[:8]} cto_position_save_failed sig={swap_sig}")
        _apex_log_error(token_mint, "cto_signal", "cto_position_save_failed",
                        {"msg": str(exc), "sig": swap_sig})
        send_telegram(
            f"🚨 <b>POSITION SAVE FAILED</b>\n"
            f"Token: {token_label}\n"
            f"CA: <code>{token_mint}</code>\n"
            f"Manual intervention needed!"
        )
    _stats["trades_executed"] += 1
    asyncio.create_task(emergency_dump_check(session, token_mint, wallet_pubkey))
    asyncio.create_task(
        cto_review_task(token_mint, token_symbol, wallet_pubkey),
        name=f"cto-review-{token_symbol}",
    )

    # Step 8 — Telegram alert with sell buttons
    _cto_mc_str = _fmt_usd(mc_entry) if mc_entry else "\u2014"

    logger.info(f"[BUY/SELL SIG] {token_mint[:8]} cto_signal_buy sig={swap_sig}")
    _cto_msg = (
        f"\U0001f3af {PREFIX} <b>Bought {token_symbol}</b>\n"
        f"CA: <code>{token_mint}</code>\n"
        f"Size: {buy_sol} SOL\n"
        f"MC Entry: {_cto_mc_str}\n"
        f"Claude: {claude_score}/100\n"
        f"Source: DexAlert verified CTO"
    )
    send_telegram_with_buttons(_cto_msg, _make_position_buttons(token_mint))


if __name__ == "__main__":
    asyncio.run(run())
