"""
APEX Market Radar
Scans DexScreener for new Solana tokens and uses Claude AI for analysis.
"""

from __future__ import annotations

import os
import asyncio
import logging
import time
import aiohttp
import anthropic
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("market_radar")

# --- Config -----------------------------------------------------------

JUPITER_API      = "https://lite-api.jup.ag/swap/v1"
DEXSCREENER_API  = "https://api.dexscreener.com/latest/dex/tokens"
SOLANA_CHAIN     = "solana"

# Bug fix #2: include pumpswap alongside raydium and pump
ALLOWED_DEX_IDS  = {"raydium", "pump", "pumpswap", "pumpfun", "meteora", "meteoradlmm", "meteoradbc", "orca", "whirlpool", "launchlab"}

DRY_RUN          = os.getenv("DRY_RUN", "True").lower() == "true"
BUY_AMOUNT_SOL   = float(os.getenv("BUY_AMOUNT_SOL", "0.1"))
MAX_SLIPPAGE_BPS = int(os.getenv("MAX_SLIPPAGE_BPS", "300"))
SOL_MINT         = "So11111111111111111111111111111111111111112"

# Rough filter thresholds — liquidity is time-gated (peak vs off-peak UTC)
LIQUIDITY_PEAK_USD    = 30_000   # UTC 12:00–22:00 (busy market hours)
LIQUIDITY_OFFPEAK_USD = 20_000   # UTC 22:00–12:00 (quiet overnight hours)
PEAK_START_UTC        = 12
PEAK_END_UTC          = 22
MAX_AGE_MINUTES       = 120
MIN_VOLUME_5M_USD     = 500


def _min_liquidity_usd() -> int:
    """Return the active liquidity threshold based on current UTC hour."""
    hour = time.gmtime().tm_hour
    if PEAK_START_UTC <= hour < PEAK_END_UTC:
        return LIQUIDITY_PEAK_USD
    return LIQUIDITY_OFFPEAK_USD

claude_client = anthropic.Anthropic(api_key=os.getenv("CLAUDE_API_KEY"))

# --- Helius rate tracker ----------------------------------------------

HELIUS_DAILY_WARN_LIMIT = 26_000   # ~800k credits/month ÷ 30 days
_helius_calls: int = 0
_helius_day_start: float = time.time()


def _track_helius_call() -> None:
    """Increment the Helius call counter and warn via Telegram if over daily limit."""
    global _helius_calls, _helius_day_start
    now = time.time()
    # Reset counter at midnight (86400s elapsed)
    if now - _helius_day_start >= 86_400:
        _helius_calls = 0
        _helius_day_start = now
    _helius_calls += 1
    if _helius_calls == HELIUS_DAILY_WARN_LIMIT:
        msg = (
            f"⚠️ <b>APEX market_radar</b> — Helius daily limit reached\n"
            f"Made {_helius_calls:,} RPC calls today (≈800k credits/month threshold).\n"
            f"Consider reducing scan frequency."
        )
        logger.warning(f"Helius daily call limit hit: {_helius_calls:,}")
        send_telegram(msg)


# --- DexScreener ------------------------------------------------------

async def fetch_new_tokens(session: aiohttp.ClientSession) -> list[dict]:
    """Poll DexScreener for recently-listed Solana tokens."""
    url = "https://api.dexscreener.com/token-profiles/latest/v1"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            profiles = await resp.json()
    except Exception as e:
        logger.error(f"DexScreener fetch failed: {e}")
        return []

    solana_tokens = [p for p in profiles if p.get("chainId") == SOLANA_CHAIN]
    logger.info(f"DexScreener returned {len(solana_tokens)} Solana token profiles")
    return solana_tokens


async def fetch_token_pairs(session: aiohttp.ClientSession, token_address: str) -> list[dict]:
    """Get pair data for a token address."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("pairs") or []
    except Exception as e:
        logger.warning(f"Pair fetch failed for {token_address}: {e}")
        return []


# --- Filters ----------------------------------------------------------

def passes_basic_filters(pair: dict) -> tuple[bool, str]:
    """
    Return (True, '') if the pair passes basic filters, else (False, reason).
    """
    # Bug fix #2: require known DEX
    dex_id = pair.get("dexId", "")
    if dex_id not in ALLOWED_DEX_IDS:
        return False, f"dexId '{dex_id}' not in allowed list"

    # Liquidity check — threshold varies by UTC hour (peak vs off-peak)
    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    min_liq = _min_liquidity_usd()
    if liquidity < min_liq:
        return False, f"liquidity ${liquidity:.0f} below ${min_liq:,} threshold"

    # Age check
    pair_created_at = pair.get("pairCreatedAt")
    if pair_created_at:
        age_minutes = (time.time() * 1000 - pair_created_at) / 60_000
        if age_minutes > MAX_AGE_MINUTES:
            return False, f"pair age {age_minutes:.0f}m exceeds {MAX_AGE_MINUTES}m"

    # Volume check
    volume_5m = (pair.get("volume") or {}).get("m5", 0) or 0
    if volume_5m < MIN_VOLUME_5M_USD:
        return False, f"5m volume ${volume_5m:.0f} below ${MIN_VOLUME_5M_USD}"

    return True, ""


def get_holder_count(token_address: str, rpc_url: str) -> int | None:
    """
    Fetch holder count via Helius RPC getTokenLargestAccounts.
    Returns None on failure.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [token_address],
    }
    try:
        _track_helius_call()
        resp = requests.post(rpc_url, json=payload, timeout=5)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        accounts = (result.get("value") or [])
        return len(accounts)
    except Exception as e:
        logger.warning(f"holder count fetch failed for {token_address}: {e}")
        return None


def passes_holder_filter(holder_count: int | None, max_holders: int = 500) -> bool:
    """
    Bug fix #3: fail-open — if holder_count is None (unknown), allow the trade.
    Original fail-closed:  if holder_count > 0 and holder_count < max_holders
    Fixed fail-open:       unknown count is treated as passing
    """
    if holder_count is None:
        logger.warning("holder count unknown — failing open (allowing trade)")
        return True
    return holder_count < max_holders


# --- Pre-Claude filter ------------------------------------------------

# Cache: token_address -> timestamp of last Claude analysis
seen_tokens: dict[str, float] = {}

PREFILTER_MIN_MCAP    = 10_000      # $10k minimum FDV / market cap
PREFILTER_MAX_MCAP    = 2_000_000   # $2M maximum FDV / market cap
PREFILTER_MIN_VOL_H1  = 5_000       # $5k minimum 1h volume
PREFILTER_RECHECK_SEC = 7_200       # 2 hours before re-analysing same token


def passes_pre_claude_filter(pair: dict) -> tuple[bool, str]:
    """
    Cheap checks run BEFORE calling Claude to avoid wasting API credits.
    Returns (True, '') if the token should be sent to Claude, else (False, reason).
    """
    token_address = pair.get("baseToken", {}).get("address", "")

    # --- Seen-token cache: skip if analysed within the last 2 hours ---
    last_seen = seen_tokens.get(token_address)
    if last_seen and (time.time() - last_seen) < PREFILTER_RECHECK_SEC:
        age_min = int((time.time() - last_seen) / 60)
        return False, f"already analysed {age_min}m ago"

    # --- FDV / market cap range ---
    fdv = pair.get("fdv") or 0
    if fdv < PREFILTER_MIN_MCAP:
        return False, f"FDV ${fdv:,.0f} below ${PREFILTER_MIN_MCAP:,}"
    if fdv > PREFILTER_MAX_MCAP:
        return False, f"FDV ${fdv:,.0f} above ${PREFILTER_MAX_MCAP:,}"

    # --- 1h volume ---
    vol_h1 = (pair.get("volume") or {}).get("h1", 0) or 0
    if vol_h1 < PREFILTER_MIN_VOL_H1:
        return False, f"1h volume ${vol_h1:,.0f} below ${PREFILTER_MIN_VOL_H1:,}"

    return True, ""


# --- Claude AI analysis -----------------------------------------------

def analyze_token_with_claude(pair: dict, holder_count: int | None) -> dict:
    """
    Ask Claude to score the token on a 1-10 scale and provide reasoning.
    Returns dict with keys: score (int), reasoning (str), recommendation (str),
    deciding_factor (str), age_minutes (float | None).
    """
    token_name    = pair.get("baseToken", {}).get("name", "Unknown")
    token_symbol  = pair.get("baseToken", {}).get("symbol", "?")
    token_address = pair.get("baseToken", {}).get("address", "")
    dex_id        = pair.get("dexId", "")
    price_usd     = pair.get("priceUsd", "unknown")
    liquidity_usd = (pair.get("liquidity") or {}).get("usd", 0) or 0
    volume_5m     = (pair.get("volume") or {}).get("m5", 0) or 0
    volume_h1     = (pair.get("volume") or {}).get("h1", 0) or 0
    price_chg_m5  = (pair.get("priceChange") or {}).get("m5", 0) or 0
    price_chg_m15 = (pair.get("priceChange") or {}).get("m15", 0) or 0
    price_chg_h1  = (pair.get("priceChange") or {}).get("h1", 0) or 0
    fdv           = pair.get("fdv", 0) or 0

    # Volume/Liquidity ratio — primary momentum signal
    vl_ratio = round(volume_5m / liquidity_usd, 2) if liquidity_usd > 0 else 0

    # Token age in minutes (Change C)
    pair_created_at = pair.get("pairCreatedAt")
    age_minutes = round((time.time() * 1000 - pair_created_at) / 60_000, 1) if pair_created_at else None
    age_str = f"{age_minutes}m" if age_minutes is not None else "unknown"

    prompt = f"""You are a Solana meme coin risk analyst. Evaluate this newly listed token for a short-term trade (flip within 30 minutes).

Token: {token_name} ({token_symbol})
Address: {token_address}
DEX: {dex_id}
Price: ${price_usd}
Liquidity: ${liquidity_usd:,.0f}
5m Volume: ${volume_5m:,.0f} | Vol/Liquidity ratio: {vl_ratio}x
1h Volume: ${volume_h1:,.0f}
Price Change — 5m: {price_chg_m5}% | 15m: {price_chg_m15}% | 1h: {price_chg_h1}%
FDV: ${fdv:,.0f}
Token Age: {age_str}
Holder Count: {holder_count if holder_count is not None else "unknown"}

SCORING RULES (apply in order):
1. Vol/Liquidity ratio >10x = strong momentum signal, weight this MOST HEAVILY (+2 pts)
2. Price gains sustained across 5m, 15m, and 1h = trend confirmation (+2 pts)
3. FDV/Liquidity ratio <5x = healthy token structure (+1 pt)
4. If Holder Count is "unknown": do NOT penalise — score on the other factors only
5. Flat or declining volume across timeframes = strong negative signal (-2 pts)
6. Price already dumping (negative 5m AND negative 15m) = score 1-2, avoid

Score 1-10 for short-term trade potential (7+ = BUY, <7 = SKIP).

Respond with JSON only:
{{"score": <int 1-10>, "reasoning": "<one sentence>", "recommendation": "BUY" | "SKIP", "deciding_factor": "<volume_momentum|price_trend|fdv_ratio|holder_distribution|liquidity>"}}"""

    try:
        message = claude_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        import json
        text = message.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = json.loads(text.strip())
        result["age_minutes"] = age_minutes
        return result
    except Exception as e:
        logger.error(f"Claude analysis failed: {e}")
        return {"score": 0, "reasoning": "analysis failed", "recommendation": "SKIP", "age_minutes": age_minutes}


# --- Jupiter swap -----------------------------------------------------

async def get_jupiter_quote(
    session: aiohttp.ClientSession,
    output_mint: str,
    amount_lamports: int,
) -> dict | None:
    """Get a swap quote from Jupiter lite API."""
    params = {
        "inputMint":        SOL_MINT,
        "outputMint":       output_mint,
        "amount":           str(amount_lamports),
        "slippageBps":      str(MAX_SLIPPAGE_BPS),
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


async def execute_swap(
    session: aiohttp.ClientSession,
    quote: dict,
    wallet_pubkey: str,
) -> str | None:
    """
    POST to Jupiter /swap. In DRY_RUN mode, logs the intent and returns a fake sig.
    Returns transaction signature or None on failure.
    """
    if DRY_RUN:
        logger.info(f"[DRY RUN] Would swap {quote.get('inAmount')} lamports → {quote.get('outAmount')} tokens")
        return "DRY_RUN_SIG"

    payload = {
        "quoteResponse":        quote,
        "userPublicKey":        wallet_pubkey,
        "wrapAndUnwrapSol":     True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": "auto",
    }
    url = f"{JUPITER_API}/swap"
    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get("txid")
        except Exception as e:
            logger.error(f"Jupiter swap attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
    return None


# --- Telegram ---------------------------------------------------------

def send_telegram(message: str) -> None:
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# --- Skip reason classifier -------------------------------------------

def _skip_reason(reason: str) -> str:
    """Map a filter rejection string to a structured SKIP_REASON tag."""
    r = reason.lower()
    if "liquidity"      in r: return "SKIP_REASON:liquidity"
    if "age"            in r or "exceeds" in r: return "SKIP_REASON:age"
    if "volume"         in r: return "SKIP_REASON:volume"
    if "dexid"          in r or "dex"   in r: return "SKIP_REASON:dex"
    if "fdv"            in r: return "SKIP_REASON:fdv"
    if "already"        in r: return "SKIP_REASON:duplicate"
    if "holder"         in r: return "SKIP_REASON:holders"
    return "SKIP_REASON:other"


# --- Main loop --------------------------------------------------------

async def run():
    rpc_url    = os.getenv("SOLANA_RPC", "")
    wallet_key = os.getenv("WALLET_PRIVATE_KEY", "")
    seen_pairs: set[str] = set()

    logger.info(f"Market Radar starting — DRY_RUN={DRY_RUN}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                utc_hour = time.gmtime().tm_hour
                logger.info(
                    f"Scan cycle | UTC {utc_hour:02d}:xx | "
                    f"MIN_LIQUIDITY=${_min_liquidity_usd():,}"
                )
                profiles = await fetch_new_tokens(session)

                for profile in profiles:
                    token_address = profile.get("tokenAddress", "")
                    if not token_address:
                        continue

                    pairs = await fetch_token_pairs(session, token_address)

                    for pair in pairs:
                        pair_address = pair.get("pairAddress", "")
                        if not pair_address or pair_address in seen_pairs:
                            continue
                        seen_pairs.add(pair_address)

                        symbol = pair.get("baseToken", {}).get("symbol", "?")

                        # ── 1. Cheap early-out: token already analysed this cycle ──────
                        # Must run BEFORE passes_basic_filters so that a second low-liq
                        # pair of an already-scored token never produces a misleading
                        # SKIP_REASON:liquidity log line after a SCORED line.
                        if token_address in seen_tokens and \
                                (time.time() - seen_tokens[token_address]) < PREFILTER_RECHECK_SEC:
                            continue

                        # ── 2. Cheap structural filters (DEX, liquidity, age, volume) ──
                        ok, reason = passes_basic_filters(pair)
                        if not ok:
                            logger.info(f"SKIP | {symbol} | {_skip_reason(reason)} | {reason}")
                            continue

                        # ── 3. Cheap pre-Claude filters (FDV range, 1h vol) ────────────
                        # Runs BEFORE the expensive Helius call so bad FDV/volume tokens
                        # never consume an RPC credit.
                        ok_pre, pre_reason = passes_pre_claude_filter(pair)
                        if not ok_pre:
                            logger.info(f"SKIP | {symbol} | {_skip_reason(pre_reason)} | {pre_reason}")
                            continue

                        # ── 4. Expensive Helius RPC — holder count ─────────────────────
                        holder_count = get_holder_count(token_address, rpc_url) if rpc_url else None

                        if not passes_holder_filter(holder_count):
                            logger.info(f"SKIP | {symbol} | SKIP_REASON:holders | too many holders ({holder_count})")
                            continue

                        # ── 5. Expensive Claude API call ───────────────────────────────
                        # Stamp first so concurrent pair iterations don't double-analyse.
                        seen_tokens[token_address] = time.time()
                        analysis = analyze_token_with_claude(pair, holder_count)
                        score    = analysis.get("score", 0)
                        rec      = analysis.get("recommendation", "SKIP")
                        age_min  = analysis.get("age_minutes")
                        age_tag  = f"age={age_min}m" if age_min is not None else "age=unknown"
                        factor   = analysis.get("deciding_factor", "unknown")

                        logger.info(
                            f"SCORED | {symbol} | score={score}/10 | {rec} | {age_tag} | "
                            f"DECIDING_FACTOR:{factor} | {analysis.get('reasoning','')}"
                        )

                        if rec == "BUY" and score >= 7:
                            amount_lamports = int(BUY_AMOUNT_SOL * 1_000_000_000)
                            quote = await get_jupiter_quote(session, token_address, amount_lamports)
                            if quote:
                                sig = await execute_swap(session, quote, wallet_key)
                                msg = (
                                    f"🟢 <b>APEX BUY</b> [{symbol}]\n"
                                    f"Score: {score}/10\n"
                                    f"{analysis.get('reasoning','')}\n"
                                    f"Sig: <code>{sig}</code>"
                                )
                                logger.info(msg)
                                send_telegram(msg)

            except Exception as e:
                logger.error(f"Radar loop error: {e}", exc_info=True)

            await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(run())
