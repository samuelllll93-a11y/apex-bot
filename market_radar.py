"""
APEX Market Radar
Scans DexScreener for new Solana tokens and uses Claude AI for analysis.
"""

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
ALLOWED_DEX_IDS  = {"raydium", "pump", "pumpswap"}

DRY_RUN          = os.getenv("DRY_RUN", "True").lower() == "true"
BUY_AMOUNT_SOL   = float(os.getenv("BUY_AMOUNT_SOL", "0.1"))
MAX_SLIPPAGE_BPS = int(os.getenv("MAX_SLIPPAGE_BPS", "300"))
SOL_MINT         = "So11111111111111111111111111111111111111112"

# Rough filter thresholds
MIN_LIQUIDITY_USD = 5_000
MAX_AGE_MINUTES   = 60
MIN_VOLUME_5M_USD = 500

claude_client = anthropic.Anthropic(api_key=os.getenv("CLAUDE_API_KEY"))


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

    # Liquidity check
    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    if liquidity < MIN_LIQUIDITY_USD:
        return False, f"liquidity ${liquidity:.0f} below ${MIN_LIQUIDITY_USD}"

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


# --- Claude AI analysis -----------------------------------------------

def analyze_token_with_claude(pair: dict, holder_count: int | None) -> dict:
    """
    Ask Claude to score the token on a 1-10 scale and provide reasoning.
    Returns dict with keys: score (int), reasoning (str), recommendation (str).
    """
    token_name    = pair.get("baseToken", {}).get("name", "Unknown")
    token_symbol  = pair.get("baseToken", {}).get("symbol", "?")
    token_address = pair.get("baseToken", {}).get("address", "")
    dex_id        = pair.get("dexId", "")
    price_usd     = pair.get("priceUsd", "unknown")
    liquidity_usd = (pair.get("liquidity") or {}).get("usd", 0)
    volume_5m     = (pair.get("volume") or {}).get("m5", 0)
    volume_h1     = (pair.get("volume") or {}).get("h1", 0)
    price_change  = (pair.get("priceChange") or {}).get("m5", 0)
    fdv           = pair.get("fdv", 0)

    prompt = f"""You are a Solana meme coin risk analyst. Evaluate this newly listed token for a short-term trade (flip within 30 minutes).

Token: {token_name} ({token_symbol})
Address: {token_address}
DEX: {dex_id}
Price: ${price_usd}
Liquidity: ${liquidity_usd:,.0f}
5m Volume: ${volume_5m:,.0f}
1h Volume: ${volume_h1:,.0f}
5m Price Change: {price_change}%
FDV: ${fdv:,.0f}
Holder Count: {holder_count if holder_count is not None else "unknown"}

Score this token 1-10 for short-term trade potential (10 = strong buy, 1 = avoid).
Consider: liquidity depth, volume momentum, FDV vs liquidity ratio, holder distribution red flags.

Respond with JSON only:
{{"score": <int 1-10>, "reasoning": "<one sentence>", "recommendation": "BUY" | "SKIP"}}"""

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
        return json.loads(text.strip())
    except Exception as e:
        logger.error(f"Claude analysis failed: {e}")
        return {"score": 0, "reasoning": "analysis failed", "recommendation": "SKIP"}


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
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        logger.error(f"Jupiter quote failed: {e}")
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
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("txid")
    except Exception as e:
        logger.error(f"Jupiter swap failed: {e}")
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


# --- Main loop --------------------------------------------------------

async def run():
    rpc_url    = os.getenv("SOLANA_RPC", "")
    wallet_key = os.getenv("WALLET_PRIVATE_KEY", "")
    seen_pairs: set[str] = set()

    logger.info(f"Market Radar starting — DRY_RUN={DRY_RUN}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
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

                        ok, reason = passes_basic_filters(pair)
                        if not ok:
                            logger.debug(f"Skipping {token_address[:8]} — {reason}")
                            continue

                        holder_count = get_holder_count(token_address, rpc_url) if rpc_url else None

                        if not passes_holder_filter(holder_count):
                            logger.info(f"Skipping {token_address[:8]} — too many holders ({holder_count})")
                            continue

                        analysis = analyze_token_with_claude(pair, holder_count)
                        score    = analysis.get("score", 0)
                        rec      = analysis.get("recommendation", "SKIP")

                        symbol = pair.get("baseToken", {}).get("symbol", "?")
                        logger.info(
                            f"{symbol} | score={score}/10 | {rec} | "
                            f"{analysis.get('reasoning','')}"
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

            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(run())
