"""cto_dry_consumer.py — standalone CTO Radar dry-run measurement tool.

7-day trial that simulates trades on every CTO Radar Telegram alert
that passes the cto_radar_monitor.py filter set. NO real money. NO
Jupiter. NO RPC calls. NO private key reads. NO Telegram alerts sent.

Purpose: measure whether CTO Radar alerts are alpha or noise so the
operator can decide at end-of-week whether to wire them into
whale_sniper live trading. All measurement state lands in
~/apex-bot/state/cto_dry_results.json.

ARCHITECTURE

  Telethon listener on channel -1003360535374 (the same channel
  cto_radar_monitor.py uses to feed whale_sniper). Own session file
  at ~/dex-alert-bot/state/cto_dry_consumer so this listener does
  NOT collide with cto-radar's running session.

  When an alert passes the entry filter (mcap < $50k, vol_1hr >
  $10k, pct_1hr > -10% — mirrors cto_radar_monitor.py exactly), a
  simulated 0.025 SOL position is opened and tracked via
  DexScreener polling every 30 seconds.

  Sell ladder (all percentages of ORIGINAL bag, never of remaining):
    - At 2x entry_mcap → sell 50% of original, set last_sell_mcap
    - Every time mcap rises to 1.5x last_sell_mcap → sell 15% of
      original, update last_sell_mcap
    - Final leg caps at remaining bag (so total sold = 100%)
    - HARD STOP: any time mcap drops to 0.5x entry → close ALL
      remaining bag immediately, regardless of ladder state
    - Drawdowns between ladder steps are ignored unless they cross
      the hard stop. Only NEW highs above last_sell_mcap trigger
      ladder sells — never sell on a retrace.

  No time stop, no trailing stop, no manual close. Position runs
  until ladder exhausted OR hard stop hit OR token delisted.

  P&L (linear model, stake=0.025 SOL split into 100 units at entry):
    units_sold per leg = pct_of_original × 100
    sol_realized       = units_sold × (current_mcap / entry_mcap) × 0.00025
    final_pnl_sol      = sum(sol_realized) − 0.025

PROHIBITIONS (enforced by construction — these things are simply
absent from this file):

  - No imports from whale_sniper / cto_radar_monitor / anywhere in
    the apex-bot tree (other than .env via python-dotenv)
  - No WALLET_PRIVATE_KEY reads, no Jupiter, no Helius, no RPC
  - No send_telegram — silent dry run

USAGE

  venv/bin/python cto_dry_consumer.py                  # listen + simulate
  venv/bin/python cto_dry_consumer.py --check-connection
      Connects to Telegram, verifies channel access, exits without
      registering an event handler. No risk of a simulated trade
      landing during the check.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from typing import Any

import aiohttp
from dotenv import load_dotenv
from telethon import TelegramClient, events


# ── Config ─────────────────────────────────────────────────────────────────────

# Load secrets from ~/apex-bot/.env — same file whale_sniper and
# cto_radar_monitor read. We DO NOT read WALLET_PRIVATE_KEY.
_APEX_DIR = os.path.expanduser("~/apex-bot")
load_dotenv(os.path.join(_APEX_DIR, ".env"))

# Telethon — values mirror cto_radar_monitor.py except SESSION_PATH
# is distinct so the two consumers don't share a session file.
API_ID            = 38707762
API_HASH          = os.environ["TELEGRAM_API_HASH"]
PHONE             = os.environ["TELEGRAM_PHONE"]
SESSION_PATH      = os.path.expanduser("~/dex-alert-bot/state/cto_dry_consumer")
CTO_RADAR_CHANNEL = -1003360535374

# Result store
RESULTS_FILE = os.path.join(_APEX_DIR, "state", "cto_dry_results.json")

# Entry filters — exact mirror of cto_radar_monitor.py so we measure
# the same alert population the live consumer would act on. If those
# filters change, change them here too (manually — no shared import).
MIN_VOL_1HR_USD = 10_000
MAX_MCAP_USD    = 50_000
MIN_PCT_1HR     = -10.0

# Dry-run trade simulation
DRY_STAKE_SOL          = 0.025
DRY_UNITS_PER_POSITION = 100.0
SOL_PER_UNIT_AT_ENTRY  = DRY_STAKE_SOL / DRY_UNITS_PER_POSITION   # 0.00025

# Sell ladder — all percentages are fractions of the ORIGINAL bag,
# never of the remaining bag.
TP1_MULT                = 2.0    # First sell when mcap >= 2x entry
LADDER_MULT             = 1.5    # Subsequent sells when mcap >= 1.5x last_sell
TP1_PCT_OF_ORIGINAL     = 0.50   # First sell: 50% of original
LADDER_PCT_OF_ORIGINAL  = 0.15   # Each subsequent: 15% of original
HARD_STOP_MULT          = 0.5    # Close ALL remaining at 0.5x entry

# Polling
POLL_INTERVAL_SEC          = 30
DEXSCREENER_URL            = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
DEXSCREENER_RETRY_DELAY_S  = 5
PRICE_PATH_CAP             = 1000   # ≈8.3 hours of 30s ticks

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("cto-dry")
# Quiet down Telethon's chatty handshake / heartbeat logs
logging.getLogger("telethon").setLevel(logging.WARNING)


# ── Message parsing ───────────────────────────────────────────────────────────
# Mirror of cto_radar_monitor.py — duplicated to keep this consumer
# fully standalone (the spec forbids modifying any existing file, so
# we cannot extract a shared module).

_RE_CA       = re.compile(r"CA[:\s]*([1-9A-HJ-NP-Za-km-z]{32,44})", re.IGNORECASE)
_RE_MCAP     = re.compile(r"Market\s*Cap[:\s]*\$?\s*([\d,.]+)\s*([KMB]?)", re.IGNORECASE)
_RE_VOL_1HR  = re.compile(r"1\s*hr?[:\s]*\$\s*([\d,.]+)\s*([KMB]?)", re.IGNORECASE)
_RE_PCT_1HR  = re.compile(r"1\s*hr?[:\s]*([+\-]?\d+(?:\.\d+)?)\s*%")
_RE_NAME_SYM = re.compile(r"([^\n()]+?)\s*\(([^)\s]+)\)")


def _parse_amount(num_str: str, suffix: str) -> float:
    val = float(num_str.replace(",", ""))
    mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix.upper(), 1)
    return val * mult


def parse_radar_message(text: str) -> dict[str, Any] | None:
    """Extract CTO Radar fields. Returns None if essential fields are
    missing or the message isn't a Solana CTO alert."""
    if not text:
        return None
    if "SOLANA" not in text.upper():
        return None
    ca_m = _RE_CA.search(text)
    if not ca_m:
        return None
    ca = ca_m.group(1)
    if not (len(ca) == 44 or ca.endswith("pump")):
        return None

    mcap = vol_1hr = None
    pct_1hr = 0.0

    if (m := _RE_MCAP.search(text)):
        mcap = _parse_amount(m.group(1), m.group(2))
    for m in _RE_VOL_1HR.finditer(text):
        vol_1hr = _parse_amount(m.group(1), m.group(2))
        break
    if (m := _RE_PCT_1HR.search(text)):
        pct_1hr = float(m.group(1))

    name = symbol = "?"
    for line in text.splitlines()[:5]:
        line = line.strip("*_ ").strip()
        if not line:
            continue
        if (m := _RE_NAME_SYM.search(line)):
            name, symbol = m.group(1).strip(), m.group(2).strip()
            break

    return {
        "ca": ca, "name": name, "symbol": symbol,
        "mcap": mcap, "vol_1hr": vol_1hr, "pct_1hr": pct_1hr,
    }


def passes_filters(parsed: dict[str, Any]) -> tuple[bool, str]:
    """Mirror of cto_radar_monitor.py's filter — duplicated, not
    imported, so this consumer is fully standalone."""
    if parsed.get("mcap") is None:
        return False, "mcap missing"
    if parsed["mcap"] >= MAX_MCAP_USD:
        return False, f"mcap ${parsed['mcap']:,.0f} >= ${MAX_MCAP_USD:,}"
    if parsed.get("vol_1hr") is None:
        return False, "vol_1hr missing"
    if parsed["vol_1hr"] <= MIN_VOL_1HR_USD:
        return False, f"vol_1hr ${parsed['vol_1hr']:,.0f} <= ${MIN_VOL_1HR_USD:,}"
    if parsed.get("pct_1hr", 0.0) < MIN_PCT_1HR:
        return False, f"pct_1hr {parsed['pct_1hr']:+.0f}% < {MIN_PCT_1HR:+.0f}% (freefall)"
    return True, "ok"


# ── State persistence ─────────────────────────────────────────────────────────

_RESULTS_LOCK = asyncio.Lock()


def _load_results() -> list:
    if not os.path.exists(RESULTS_FILE):
        return []
    try:
        with open(RESULTS_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"failed to load {RESULTS_FILE}: {e} — starting empty")
        return []


async def _save_results(results: list) -> None:
    """Atomic write under a single async lock so concurrent per-position
    monitors don't race each other or shred the file mid-write."""
    async with _RESULTS_LOCK:
        try:
            os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
            tmp = RESULTS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(results, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, RESULTS_FILE)
        except OSError as e:
            logger.error(f"file write error: {e}")


def _has_open_position(results: list, mint: str) -> bool:
    """Dedup gate: True iff there's already an OPEN position for this
    mint. Closed positions don't count — a re-alert of a token whose
    prior measurement closed is fair game for a new measurement."""
    return any(p.get("mint") == mint and p.get("status") == "OPEN" for p in results)


# ── Position lifecycle ────────────────────────────────────────────────────────

def _open_position(parsed: dict[str, Any]) -> dict:
    """Build a fresh OPEN position record from a parsed alert."""
    now = time.time()
    entry_mcap = float(parsed["mcap"])
    return {
        "mint":               parsed["ca"],
        "symbol":             parsed["symbol"],
        "name":               parsed["name"],
        "entry_mcap_usd":     entry_mcap,
        "entry_ts":           now,
        "bag_remaining_pct":  1.0,
        "last_sell_mcap_usd": entry_mcap,    # baseline for ladder math
        "sells":              [],
        "status":             "OPEN",
        "peak_mcap_usd":      entry_mcap,
        "price_path":         [],
        "final_pnl_sol":      None,
    }


def _compute_sol_realized(pct_of_original: float, current_mcap: float,
                          entry_mcap: float) -> float:
    """Linear-value model: stake split into 100 units at entry. Each
    unit's SOL value scales with mcap ratio."""
    units_sold = pct_of_original * DRY_UNITS_PER_POSITION
    return units_sold * (current_mcap / entry_mcap) * SOL_PER_UNIT_AT_ENTRY


def _record_sell(pos: dict, current_mcap: float, pct_of_original: float) -> float:
    """Append a sell leg to the position, mutate bag_remaining_pct and
    last_sell_mcap_usd. Returns the sol_realized for log context."""
    sol = _compute_sol_realized(pct_of_original, current_mcap, pos["entry_mcap_usd"])
    pos["sells"].append({
        "ts":                   time.time(),
        "mcap_usd":             current_mcap,
        "pct_of_original_sold": pct_of_original,
        "sol_realized":         sol,
    })
    pos["bag_remaining_pct"] = max(0.0, pos["bag_remaining_pct"] - pct_of_original)
    pos["last_sell_mcap_usd"] = current_mcap
    return sol


def _close_position(pos: dict, status: str) -> None:
    pos["status"] = status
    realized = sum(s["sol_realized"] for s in pos["sells"])
    pos["final_pnl_sol"] = realized - DRY_STAKE_SOL


def _process_tick(pos: dict, current_mcap: float) -> bool:
    """One DexScreener tick. Returns True if state mutated (the caller
    persists results on True). Order of checks:
      1. update peak
      2. append to price_path (capped)
      3. HARD STOP — always wins, closes everything
      4. Ladder step — only on a NEW high above the relevant threshold
    """
    # 1. Peak tracking
    if current_mcap > pos["peak_mcap_usd"]:
        pos["peak_mcap_usd"] = current_mcap

    # 2. Append to price_path with rolling cap
    pos["price_path"].append({"ts": time.time(), "mcap_usd": current_mcap})
    if len(pos["price_path"]) > PRICE_PATH_CAP:
        # Keep the most recent PRICE_PATH_CAP entries — older path is
        # dropped on the floor. Spec: "capped at 1000 entries".
        pos["price_path"] = pos["price_path"][-PRICE_PATH_CAP:]

    # 3. Hard stop — beats every ladder consideration. Closes all
    # remaining bag at the current mcap.
    if current_mcap <= pos["entry_mcap_usd"] * HARD_STOP_MULT:
        remaining = pos["bag_remaining_pct"]
        if remaining > 0:
            sol = _record_sell(pos, current_mcap, remaining)
            logger.info(
                f"[{pos['symbol']}] HARD STOP — closing remaining {remaining*100:.1f}% "
                f"at mcap ${current_mcap:,.0f} (entry ${pos['entry_mcap_usd']:,.0f}, "
                f"{HARD_STOP_MULT}x stop) → realized {sol:.6f} SOL"
            )
        _close_position(pos, "CLOSED_STOP")
        logger.info(
            f"[{pos['symbol']}] CLOSED_STOP — final PnL {pos['final_pnl_sol']:+.6f} SOL"
        )
        return True

    # 4. Ladder — only fires on new highs above the threshold. We never
    # sell on a retrace; last_sell_mcap_usd only moves up.
    if pos["bag_remaining_pct"] <= 0:
        # All sold via ladder already — should have been CLOSED_LADDER
        # by the prior _record_sell call. Defensive.
        return True   # tick still appended to price_path

    if not pos["sells"]:
        # First sell: 2x entry
        threshold = pos["entry_mcap_usd"] * TP1_MULT
        if current_mcap >= threshold:
            pct = min(TP1_PCT_OF_ORIGINAL, pos["bag_remaining_pct"])
            sol = _record_sell(pos, current_mcap, pct)
            logger.info(
                f"[{pos['symbol']}] TP1 — sold {pct*100:.0f}% at mcap ${current_mcap:,.0f} "
                f"({current_mcap / pos['entry_mcap_usd']:.2f}x entry) → "
                f"realized {sol:.6f} SOL, bag remaining {pos['bag_remaining_pct']*100:.0f}%"
            )
            if pos["bag_remaining_pct"] <= 0:
                _close_position(pos, "CLOSED_LADDER")
                logger.info(
                    f"[{pos['symbol']}] CLOSED_LADDER — "
                    f"final PnL {pos['final_pnl_sol']:+.6f} SOL"
                )
            return True
    else:
        # Subsequent ladder step: 1.5x last_sell_mcap
        threshold = pos["last_sell_mcap_usd"] * LADDER_MULT
        if current_mcap >= threshold:
            pct = min(LADDER_PCT_OF_ORIGINAL, pos["bag_remaining_pct"])
            sol = _record_sell(pos, current_mcap, pct)
            logger.info(
                f"[{pos['symbol']}] LADDER — sold {pct*100:.0f}% at mcap ${current_mcap:,.0f} "
                f"({current_mcap / pos['entry_mcap_usd']:.2f}x entry, "
                f"{LADDER_MULT}x prior sell) → realized {sol:.6f} SOL, "
                f"bag remaining {pos['bag_remaining_pct']*100:.0f}%"
            )
            if pos["bag_remaining_pct"] <= 0:
                _close_position(pos, "CLOSED_LADDER")
                logger.info(
                    f"[{pos['symbol']}] CLOSED_LADDER — "
                    f"final PnL {pos['final_pnl_sol']:+.6f} SOL"
                )
            return True

    # No ladder / stop trigger this tick — state still changed via the
    # price_path append + possible peak update.
    return True


# ── DexScreener polling ───────────────────────────────────────────────────────

class _DelistedError(Exception):
    """DexScreener responded with no pairs / no mcap — treat as delisted."""


class _NetworkError(Exception):
    """Could not reach DexScreener after retry — skip this tick."""


async def _fetch_mcap(session: aiohttp.ClientSession, mint: str) -> float:
    """Returns current mcap. Raises _DelistedError when DexScreener
    responds with no pairs, _NetworkError when the call fails twice
    (one initial + one retry after 5s)."""
    url = DEXSCREENER_URL.format(mint=mint)
    last_net_exc: Exception | None = None
    for attempt in range(2):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    if not pairs:
                        raise _DelistedError("dexscreener returned no pairs")
                    p = pairs[0]
                    mc = p.get("marketCap") or p.get("fdv")
                    if mc is None:
                        raise _DelistedError("pair has neither marketCap nor fdv")
                    return float(mc)
                last_net_exc = _NetworkError(f"http {resp.status}")
        except _DelistedError:
            raise   # don't retry — token isn't coming back
        except Exception as e:
            last_net_exc = _NetworkError(f"{type(e).__name__}: {e}")
        if attempt == 0:
            await asyncio.sleep(DEXSCREENER_RETRY_DELAY_S)
    raise last_net_exc or _NetworkError("unknown failure")


async def _monitor_position(pos: dict, results: list) -> None:
    """Per-position polling loop. Lives until the position is closed
    (CLOSED_LADDER / CLOSED_STOP / CLOSED_DELISTED). One aiohttp
    session per position to avoid sharing state across monitors.
    """
    async with aiohttp.ClientSession() as session:
        while pos["status"] == "OPEN":
            await asyncio.sleep(POLL_INTERVAL_SEC)
            try:
                mcap = await _fetch_mcap(session, pos["mint"])
            except _DelistedError as e:
                # Pull last known mcap from price_path if available, else
                # fall back to peak_mcap_usd (which is ≥ entry).
                last_known = (
                    pos["price_path"][-1]["mcap_usd"]
                    if pos["price_path"]
                    else pos["peak_mcap_usd"]
                )
                logger.info(
                    f"[{pos['symbol']}] DELISTED — {e}; closing at last known "
                    f"mcap ${last_known:,.0f}"
                )
                remaining = pos["bag_remaining_pct"]
                if remaining > 0:
                    _record_sell(pos, last_known, remaining)
                _close_position(pos, "CLOSED_DELISTED")
                logger.info(
                    f"[{pos['symbol']}] CLOSED_DELISTED — "
                    f"final PnL {pos['final_pnl_sol']:+.6f} SOL"
                )
                await _save_results(results)
                return
            except _NetworkError:
                # Two attempts failed — skip this tick, no state change,
                # try again next interval. Do not log per-tick noise.
                continue

            changed = _process_tick(pos, mcap)
            if changed:
                await _save_results(results)


# ── Telegram alert handler ────────────────────────────────────────────────────

async def _handle_alert(text: str, results: list, position_tasks: list) -> None:
    parsed = parse_radar_message(text)
    if parsed is None:
        logger.debug("alert: did not parse / not solana — skip")
        return

    sym = parsed.get("symbol", "?")
    ca  = parsed.get("ca", "?")
    logger.info(f"alert received: {sym} ({ca[:8]})")

    ok, reason = passes_filters(parsed)
    if not ok:
        logger.info(f"filter SKIP {sym} ({ca[:8]}) — {reason}")
        return

    if _has_open_position(results, ca):
        logger.info(f"dedup SKIP {sym} ({ca[:8]}) — position already open for this mint")
        return

    pos = _open_position(parsed)
    results.append(pos)
    logger.info(
        f"OPENED dry position {sym} ({ca[:8]}) at mcap ${pos['entry_mcap_usd']:,.0f} "
        f"— stake {DRY_STAKE_SOL} SOL simulated"
    )
    await _save_results(results)
    task = asyncio.create_task(
        _monitor_position(pos, results), name=f"monitor-{ca[:8]}"
    )
    position_tasks.append(task)


# ── Entry points ──────────────────────────────────────────────────────────────

async def _run_listener() -> None:
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(RESULTS_FILE),  exist_ok=True)

    # Ensure results file exists so _load_results can do a clean read.
    if not os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "w") as f:
            json.dump([], f)

    results = _load_results()
    position_tasks: list[asyncio.Task] = []

    # Restart safety: re-attach a monitor to any positions still OPEN
    # in the persisted state. The position's price_path resumes from
    # where it left off; the ladder threshold (last_sell_mcap_usd) is
    # preserved so we don't double-sell.
    resumed = 0
    for pos in results:
        if pos.get("status") == "OPEN":
            position_tasks.append(asyncio.create_task(
                _monitor_position(pos, results),
                name=f"resume-{pos['mint'][:8]}",
            ))
            resumed += 1

    logger.info(
        f"cto_dry_consumer starting — {len(results)} historical record(s), "
        f"{resumed} OPEN position(s) resumed"
    )

    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.start(phone=PHONE)
    me = await client.get_me()
    logger.info(f"telethon signed in as {me.username or me.first_name} (id={me.id})")
    logger.info(f"listening to channel {CTO_RADAR_CHANNEL}")

    @client.on(events.NewMessage(chats=CTO_RADAR_CHANNEL))
    async def handler(event: Any) -> None:
        try:
            await _handle_alert(
                event.message.message or "", results, position_tasks
            )
        except Exception as exc:
            logger.error(f"alert handler error: {exc}", exc_info=True)

    try:
        await client.run_until_disconnected()
    finally:
        # Cancel position monitors so the asyncio loop tears down
        # cleanly. State already persisted — next startup resumes.
        for task in position_tasks:
            task.cancel()


async def _check_connection() -> None:
    """Connect to Telegram, verify the CTO Radar channel is readable,
    then disconnect. NO event handler is registered, so even if the
    channel emits an alert during this call, NO position will be
    opened and NO write will touch cto_dry_results.json.
    """
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.start(phone=PHONE)
    me = await client.get_me()
    logger.info(f"telethon connected as {me.username or me.first_name} (id={me.id})")
    try:
        chan = await client.get_entity(CTO_RADAR_CHANNEL)
        title = getattr(chan, "title", str(CTO_RADAR_CHANNEL))
        logger.info(f"channel access confirmed: {title}")
    except Exception as e:
        logger.error(f"channel access FAILED: {type(e).__name__}: {e}")
    finally:
        await client.disconnect()
        logger.info("check-connection complete — no event handler registered, no simulated trade")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "CTO Radar dry-run measurement tool. Standalone Telethon "
            "listener that simulates 0.025 SOL trades on filtered alerts. "
            "No real trading."
        )
    )
    p.add_argument(
        "--check-connection",
        action="store_true",
        help=(
            "Connect to Telegram + verify channel access + exit. NO event "
            "handler registered → no risk of a simulated trade landing "
            "during the check. Use this to confirm Telethon auth + channel "
            "membership before running the full listener."
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        if args.check_connection:
            asyncio.run(_check_connection())
        else:
            asyncio.run(_run_listener())
    except KeyboardInterrupt:
        sys.exit(0)
