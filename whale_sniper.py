"""
APEX Whale Sniper
Monitors 4 whale wallets on Solana and mirrors their token buys via Jupiter.
"""

import os
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
MAX_SLIPPAGE_BPS = int(os.getenv("MAX_SLIPPAGE_BPS", "300"))

POLL_INTERVAL_SEC = 12   # Helius free tier: ~40 req/s

# Whale wallets to track
WHALE_WALLETS: dict[str, str] = {
    "peace":  "7b88jCzsirGfLmFMyr7BXbCaDGTtuq8oDTWusqWvLv38",
    "crispy": "EdbNfzVJjVZFsz1awBezeJpBaySLsckoZyPyaucy3g2R",
    "mannos": "CAmNcBJ82xr1tzXrwZ6tZKwEFs26TG8kT6dJeR1bxjW9",
    "early":  "Bv2BAw5UmKxv5SBMWYKqpsh6eXKNGM2RKxJGpGPk5vmb",
}

# Track the last seen signature per wallet to detect new txns
last_seen_sig: dict[str, str | None] = {name: None for name in WHALE_WALLETS}


# --- RPC helpers ------------------------------------------------------

def rpc_post(rpc_url: str, method: str, params: list) -> dict:
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
    if DRY_RUN:
        logger.info(
            f"[DRY RUN] Would swap {quote.get('inAmount')} lamports → "
            f"{quote.get('outAmount')} tokens ({quote.get('outputMint','?')[:8]})"
        )
        return "DRY_RUN_SIG"

    payload = {
        "quoteResponse":             quote,
        "userPublicKey":             wallet_pubkey,
        "wrapAndUnwrapSol":          True,
        "dynamicComputeUnitLimit":   True,
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

        amount_lamports = int(BUY_AMOUNT_SOL * 1_000_000_000)
        quote = await get_jupiter_quote(session, token_mint, amount_lamports)
        if not quote:
            logger.warning(f"[{name}] No Jupiter quote for {token_mint[:8]}")
            continue

        swap_sig = await execute_swap(session, quote, wallet_pubkey)

        msg = (
            f"🐋 <b>APEX WHALE COPY</b> [{name.upper()}]\n"
            f"Token: <code>{token_mint}</code>\n"
            f"Amount: {BUY_AMOUNT_SOL} SOL\n"
            f"Whale sig: <code>{sig}</code>\n"
            f"Our sig: <code>{swap_sig}</code>"
        )
        logger.info(msg)
        send_telegram(msg)


async def run():
    rpc_url    = os.getenv("SOLANA_RPC", "")
    wallet_key = os.getenv("WALLET_PRIVATE_KEY", "")

    if not rpc_url:
        logger.error("SOLANA_RPC not set — exiting")
        return

    logger.info(f"Whale Sniper starting — DRY_RUN={DRY_RUN}")
    logger.info(f"Tracking {len(WHALE_WALLETS)} whales: {', '.join(WHALE_WALLETS)}")

    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [
                poll_whale(session, name, wallet, rpc_url, wallet_key)
                for name, wallet in WHALE_WALLETS.items()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(run())
