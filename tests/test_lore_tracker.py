"""Tests for lore_tracker.py — observation-only wallet performance monitor.

Covers (per phase 4 spec):
  - Polling logic (mocked Helius responses end-to-end via _extract_trade
    + _apply_trade pipeline)
  - Trade reconstruction (buy → sell pair gives correct PnL)
  - Group aggregation (multi-wallet group sums correctly)
  - Dormant detection (14d threshold respected, activity timestamp
    updates on all txns — the phase-2 flag-2 fix)
  - Top-3 ranking (correct sort order, ties, ≤3 cap)
  - Empty data path (zero activity → "No qualifying activity")
  - Claude prompt construction (JSON output requested, includes 3-day
    group data) and response parsing (with code-fence stripping)
  - Pluralization (1 trade vs 2 trades)
  - Restart-safe scheduler (date persistence + double-send prevention)
"""
import json
import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lore_tracker as lt


# --- Fake-txn builders -------------------------------------------------
# Helius getTransaction with jsonParsed encoding returns this shape:
#   { "blockTime": <int>,
#     "meta":      { "err": None, "fee": <int>, "preBalances": [...],
#                    "postBalances": [...], "preTokenBalances": [...],
#                    "postTokenBalances": [...] },
#     "transaction": { "message": { "accountKeys": [{"pubkey": "..."}] } }
#   }

WALLET   = "WaLLeT1111111111111111111111111111111111111"
OTHER    = "OtHeR111111111111111111111111111111111111111"
TOKEN_A  = "TokenA11111111111111111111111111111111111111"
TOKEN_B  = "TokenB22222222222222222222222222222222222222"
LAMPORTS = lt.LAMPORTS_PER_SOL


def _make_txn(
    *,
    sol_pre_lamports: int,
    sol_post_lamports: int,
    wallet_is_fee_payer: bool = True,
    fee_lamports: int = 5_000,
    token_pre: list[dict] | None = None,
    token_post: list[dict] | None = None,
    err=None,
    blocktime: int | None = None,
) -> dict:
    """Build a minimal Helius getTransaction response. The wallet
    sits at accountKeys[0] when wallet_is_fee_payer=True so the
    fee-add-back path in _wallet_sol_delta is exercised."""
    if wallet_is_fee_payer:
        keys = [{"pubkey": WALLET}, {"pubkey": OTHER}]
        pre_balances  = [sol_pre_lamports,  1_000_000_000]
        post_balances = [sol_post_lamports, 1_000_000_000]
    else:
        keys = [{"pubkey": OTHER}, {"pubkey": WALLET}]
        pre_balances  = [1_000_000_000, sol_pre_lamports]
        post_balances = [1_000_000_000, sol_post_lamports]
    return {
        "blockTime": blocktime if blocktime is not None else int(time.time()),
        "meta": {
            "err": err,
            "fee": fee_lamports,
            "preBalances":       pre_balances,
            "postBalances":      post_balances,
            "preTokenBalances":  token_pre or [],
            "postTokenBalances": token_post or [],
        },
        "transaction": {"message": {"accountKeys": keys}},
    }


def _tok_balance(mint: str, owner: str, ui_amount: float, idx: int = 1) -> dict:
    return {
        "accountIndex": idx,
        "mint":         mint,
        "owner":        owner,
        "uiTokenAmount": {"uiAmount": ui_amount, "amount": "0", "decimals": 9},
    }


# --- _wallet_sol_delta -------------------------------------------------

def test_sol_delta_fee_payer_includes_fee_addback():
    """Wallet is fee payer (accountKeys[0]). lamport delta is
    -100M but actual swap-side movement was -100M + fee. Helper
    should add the fee back."""
    tx = _make_txn(
        sol_pre_lamports=2 * LAMPORTS,
        sol_post_lamports=2 * LAMPORTS - 100_000_000,
        wallet_is_fee_payer=True,
        fee_lamports=5_000,
    )
    delta_sol = lt._wallet_sol_delta(tx, WALLET)
    # delta = (-100_000_000 + 5_000) / 1e9 = -0.099995
    assert abs(delta_sol - (-0.099995)) < 1e-9


def test_sol_delta_non_fee_payer_no_addback():
    """Wallet is NOT fee payer (accountKeys[1]). No fee
    adjustment needed."""
    tx = _make_txn(
        sol_pre_lamports=2 * LAMPORTS,
        sol_post_lamports=2 * LAMPORTS + 100_000_000,
        wallet_is_fee_payer=False,
    )
    delta_sol = lt._wallet_sol_delta(tx, WALLET)
    assert abs(delta_sol - 0.1) < 1e-9


def test_sol_delta_wallet_not_in_keys():
    """Wallet not present in accountKeys → 0 delta (don't crash)."""
    tx = _make_txn(sol_pre_lamports=0, sol_post_lamports=0)
    delta_sol = lt._wallet_sol_delta(tx, "UnknownWallet11111111111111111111111111111")
    assert delta_sol == 0.0


# --- _wallet_token_deltas ----------------------------------------------

def test_token_delta_buy_new_account():
    """Buy: no pre balance (newly opened account), post = 1000."""
    tx = _make_txn(
        sol_pre_lamports=0, sol_post_lamports=0,
        token_pre=[],
        token_post=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
    )
    deltas = lt._wallet_token_deltas(tx, WALLET)
    assert deltas == {TOKEN_A: 1000.0}


def test_token_delta_sell_full_close():
    """Full sell: pre = 1000, post = 0 (or no entry)."""
    tx = _make_txn(
        sol_pre_lamports=0, sol_post_lamports=0,
        token_pre=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
        token_post=[],
    )
    deltas = lt._wallet_token_deltas(tx, WALLET)
    assert deltas == {TOKEN_A: -1000.0}


def test_token_delta_ignores_other_owners():
    """Token balance entries owned by other wallets must not appear
    in our delta — otherwise routing accounts would contaminate."""
    tx = _make_txn(
        sol_pre_lamports=0, sol_post_lamports=0,
        token_pre=[_tok_balance(TOKEN_A, OTHER, 500.0)],
        token_post=[_tok_balance(TOKEN_A, OTHER, 500.0)],
    )
    deltas = lt._wallet_token_deltas(tx, WALLET)
    assert deltas == {}


def test_token_delta_handles_none_uiAmount():
    """uiAmount can be None when an account holds exactly zero —
    treat as 0, don't crash."""
    tx = _make_txn(
        sol_pre_lamports=0, sol_post_lamports=0,
        token_pre=[{"accountIndex": 1, "mint": TOKEN_A, "owner": WALLET,
                    "uiTokenAmount": {"uiAmount": None}}],
        token_post=[_tok_balance(TOKEN_A, WALLET, 500.0)],
    )
    deltas = lt._wallet_token_deltas(tx, WALLET)
    assert deltas == {TOKEN_A: 500.0}


# --- _extract_trade ----------------------------------------------------

def test_extract_failed_txn_returns_none():
    """meta.err present → txn failed → no trade record."""
    tx = _make_txn(
        sol_pre_lamports=LAMPORTS, sol_post_lamports=LAMPORTS,
        err={"InstructionError": [0, "Custom-17"]},
    )
    assert lt._extract_trade(tx, WALLET, "sig", 150.0) is None


def test_extract_buy_sol_in_token_out():
    """Wallet spends 1 SOL, receives 1000 of TOKEN_A → BUY."""
    fee = 5_000
    tx = _make_txn(
        sol_pre_lamports=2 * LAMPORTS,
        sol_post_lamports=2 * LAMPORTS - LAMPORTS,    # -1 SOL gross
        fee_lamports=fee,
        token_pre=[],
        token_post=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
    )
    trade = lt._extract_trade(tx, WALLET, "sig123", 150.0)
    assert trade is not None
    assert trade["kind"]        == "buy"
    assert trade["token_mint"]  == TOKEN_A
    assert trade["token_units"] == 1000.0
    # sol_value = abs(-1 + fee_addback) — for fee payer, the addback
    # makes the delta slightly less negative. Just assert it's
    # close to 1 SOL.
    assert abs(trade["sol_value"] - 0.999995) < 1e-6


def test_extract_sell_token_out_sol_in():
    """Wallet sells TOKEN_A for SOL → SELL."""
    tx = _make_txn(
        sol_pre_lamports=LAMPORTS,
        sol_post_lamports=LAMPORTS + 2 * LAMPORTS,   # +2 SOL
        token_pre=[_tok_balance(TOKEN_A, WALLET, 500.0)],
        token_post=[],
    )
    trade = lt._extract_trade(tx, WALLET, "sig456", 150.0)
    assert trade is not None
    assert trade["kind"]        == "sell"
    assert trade["token_mint"]  == TOKEN_A
    assert trade["token_units"] == 500.0
    assert abs(trade["sol_value"] - 2.000005) < 1e-6


def test_extract_usdc_buy_normalised_via_rate():
    """Buy paid for in USDC, not SOL. USDC delta is normalised by
    the SOL/USD rate so cost basis is in SOL units."""
    sol_usd_rate = 150.0
    tx = _make_txn(
        sol_pre_lamports=LAMPORTS, sol_post_lamports=LAMPORTS,  # no SOL move
        fee_lamports=0,    # isolate USDC math from the fee-addback path
        token_pre=[_tok_balance(lt.USDC_MINT, WALLET, 300.0)],
        token_post=[
            _tok_balance(lt.USDC_MINT, WALLET, 0.0),
            _tok_balance(TOKEN_A,      WALLET, 1000.0),
        ],
    )
    trade = lt._extract_trade(tx, WALLET, "sig789", sol_usd_rate)
    # USDC delta = -300; -300 / 150 = -2 SOL equivalent.
    # token_delta = +1000 TOKEN_A.
    assert trade is not None
    assert trade["kind"]      == "buy"
    assert abs(trade["sol_value"] - 2.0) < 1e-6


def test_extract_multi_token_rotation_returns_none():
    """Wallet rotates TOKEN_A → TOKEN_B (no quote-currency change).
    Out of scope for SOL-balance-delta PnL → skip."""
    tx = _make_txn(
        sol_pre_lamports=LAMPORTS, sol_post_lamports=LAMPORTS,
        token_pre=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
        token_post=[_tok_balance(TOKEN_B, WALLET, 500.0)],
    )
    assert lt._extract_trade(tx, WALLET, "sig", 150.0) is None


def test_extract_pure_transfer_returns_none():
    """Wallet sends SOL with no token movement — not a trade."""
    tx = _make_txn(
        sol_pre_lamports=2 * LAMPORTS,
        sol_post_lamports=LAMPORTS,
    )
    assert lt._extract_trade(tx, WALLET, "sig", 150.0) is None


# --- _apply_trade ------------------------------------------------------

def _trade(kind, mint, units, sol_value, ts=1_700_000_000.0,
           wallet=WALLET, sig="sig"):
    return {
        "kind": kind, "wallet": wallet, "token_mint": mint,
        "token_units": units, "sol_value": sol_value,
        "timestamp": ts, "signature": sig,
    }


def test_apply_buy_creates_new_position():
    op, ct = {}, []
    rec = lt._apply_trade(_trade("buy", TOKEN_A, 1000, 0.5), op, ct)
    assert WALLET in op and TOKEN_A in op[WALLET]
    pos = op[WALLET][TOKEN_A]
    assert pos["cost_basis_sol"] == 0.5
    assert pos["tokens_held"]    == 1000
    assert pos["tokens_at_open"] == 1000
    assert pos["buys_count"]     == 1
    assert ct == []           # no closed-trade record for a buy
    assert rec is pos         # buys return the affected position


def test_apply_buy_averages_existing_position():
    op = {WALLET: {TOKEN_A: {
        "cost_basis_sol": 0.5, "tokens_held": 1000.0, "tokens_at_open": 1000.0,
        "opened_at": 1.0, "last_buy_at": 1.0, "buys_count": 1,
    }}}
    ct = []
    lt._apply_trade(_trade("buy", TOKEN_A, 500, 0.3, ts=2.0), op, ct)
    pos = op[WALLET][TOKEN_A]
    assert pos["cost_basis_sol"] == 0.8
    assert pos["tokens_held"]    == 1500.0
    assert pos["buys_count"]     == 2
    assert pos["last_buy_at"]    == 2.0


def test_apply_sell_unknown_position_returns_none():
    op, ct = {}, []
    rec = lt._apply_trade(_trade("sell", TOKEN_A, 500, 1.0), op, ct)
    assert rec is None
    assert ct == []
    assert op == {}


def test_apply_sell_full_close_realises_correct_pnl():
    op = {WALLET: {TOKEN_A: {
        "cost_basis_sol": 0.5, "tokens_held": 1000.0, "tokens_at_open": 1000.0,
        "opened_at": 1_000.0, "last_buy_at": 1_000.0, "buys_count": 1,
    }}}
    ct = []
    rec = lt._apply_trade(
        _trade("sell", TOKEN_A, 1000, 1.5, ts=1_060.0), op, ct,
    )
    assert rec is not None
    assert rec["fraction_sold"] == 1.0
    assert rec["entry_sol"]     == 0.5    # full cost basis realised
    assert rec["exit_sol"]      == 1.5
    assert rec["pnl_sol"]       == 1.0
    assert abs(rec["pnl_pct"] - 200.0) < 1e-6
    assert rec["hold_minutes"]  == 1.0    # (1060-1000)/60
    assert WALLET not in op               # position fully closed


def test_apply_sell_partial_close_reduces_basis_proportionally():
    op = {WALLET: {TOKEN_A: {
        "cost_basis_sol": 1.0, "tokens_held": 1000.0, "tokens_at_open": 1000.0,
        "opened_at": 1.0, "last_buy_at": 1.0, "buys_count": 1,
    }}}
    ct = []
    rec = lt._apply_trade(
        _trade("sell", TOKEN_A, 500, 0.8, ts=10.0), op, ct,
    )
    assert rec["fraction_sold"] == 0.5
    assert rec["entry_sol"]     == 0.5    # half the cost basis
    assert rec["pnl_sol"]       == 0.3    # 0.8 - 0.5
    # Position still open, basis reduced
    pos = op[WALLET][TOKEN_A]
    assert pos["tokens_held"]    == 500.0
    assert pos["cost_basis_sol"] == 0.5


def test_apply_sell_dust_remainder_closes_position():
    """If post-sell tokens_held drops below LORE_DUST_FRACTION of
    the original, the position is fully removed (not left to
    linger at near-zero)."""
    op = {WALLET: {TOKEN_A: {
        "cost_basis_sol": 1.0, "tokens_held": 1000.0, "tokens_at_open": 1000.0,
        "opened_at": 1.0, "last_buy_at": 1.0, "buys_count": 1,
    }}}
    ct = []
    lt._apply_trade(
        _trade("sell", TOKEN_A, 999.9, 0.99), op, ct,
    )
    # 0.1 tokens remain, 0.1/1000 = 0.0001 < 0.001 dust threshold
    assert WALLET not in op


# --- Buy → Sell pair (integration via _extract + _apply) ---------------

def test_buy_then_sell_pair_yields_expected_pnl():
    """End-to-end: classify a buy, classify a sell of the same
    mint, apply both. PnL on the sell should reflect the buy's
    cost basis."""
    op, ct = {}, []
    # Buy: spend 1 SOL, receive 1000 TOKEN_A
    buy_tx = _make_txn(
        sol_pre_lamports=2 * LAMPORTS,
        sol_post_lamports=2 * LAMPORTS - LAMPORTS,
        fee_lamports=0,    # zero fee so cost basis ≈ 1 SOL exactly
        token_pre=[],
        token_post=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
        blocktime=1_700_000_000,
    )
    buy = lt._extract_trade(buy_tx, WALLET, "buy_sig", 150.0)
    lt._apply_trade(buy, op, ct)

    # Sell: dump 1000 TOKEN_A for 1.5 SOL
    sell_tx = _make_txn(
        sol_pre_lamports=LAMPORTS,
        sol_post_lamports=LAMPORTS + int(1.5 * LAMPORTS),
        fee_lamports=0,
        token_pre=[_tok_balance(TOKEN_A, WALLET, 1000.0)],
        token_post=[],
        blocktime=1_700_000_300,    # 5 min later
    )
    sell = lt._extract_trade(sell_tx, WALLET, "sell_sig", 150.0)
    rec = lt._apply_trade(sell, op, ct)
    assert rec is not None
    assert abs(rec["pnl_sol"] - 0.5) < 1e-6        # 1.5 - 1.0
    assert abs(rec["pnl_pct"] - 50.0) < 1e-3
    assert rec["hold_minutes"] == 5.0


# --- _prune_closed_trades ----------------------------------------------

def test_prune_drops_old_entries():
    now = time.time()
    trades = [
        {"exit_time": now - 86400,                      "pnl_sol": 1.0},   # 1d ago
        {"exit_time": now - 15 * 86400,                 "pnl_sol": 0.5},   # 15d ago
        {"exit_time": now - (lt.LORE_METRICS_WINDOW_DAYS + 1) * 86400, "pnl_sol": -0.2},   # >30d
    ]
    n = lt._prune_closed_trades(trades)
    assert n == 1
    assert len(trades) == 2


def test_prune_zero_when_all_recent():
    now = time.time()
    trades = [{"exit_time": now - 100, "pnl_sol": 1.0}]
    assert lt._prune_closed_trades(trades) == 0
    assert len(trades) == 1


# --- _recompute_wallet_metrics -----------------------------------------

def test_recompute_metrics_from_trades():
    closed_trades = [
        {"wallet": WALLET, "pnl_sol": 0.3, "hold_minutes": 5,  "exit_time": 100.0},
        {"wallet": WALLET, "pnl_sol": -0.1, "hold_minutes": 15, "exit_time": 200.0},
        {"wallet": WALLET, "pnl_sol": 0.8, "hold_minutes": 10, "exit_time": 300.0},
        {"wallet": OTHER,  "pnl_sol": 99.0, "hold_minutes": 1,  "exit_time": 50.0},
    ]
    metrics = {WALLET: {"last_processed_sig": "sig123", "last_activity_ts": 250.0}}
    lt._recompute_wallet_metrics(WALLET, metrics, closed_trades)
    m = metrics[WALLET]
    assert m["trade_count_30d"]    == 3
    assert m["win_count_30d"]      == 2
    assert m["win_rate_30d"]       == round(2/3, 4)
    assert abs(m["total_pnl_sol_30d"] - 1.0) < 1e-9
    assert m["avg_hold_min"]       == 10.0
    # last_activity_ts max of (existing 250.0, max trade exit 300.0)
    assert m["last_activity_ts"]   == 300.0
    # last_processed_sig must be preserved
    assert m["last_processed_sig"] == "sig123"


# --- Dormant detection -------------------------------------------------

def test_dormant_true_when_old_activity():
    metrics = {WALLET: {"last_activity_ts": time.time() - 20 * 86_400}}
    assert lt._is_wallet_dormant(WALLET, metrics) is True


def test_dormant_false_when_recent_activity():
    metrics = {WALLET: {"last_activity_ts": time.time() - 60}}
    assert lt._is_wallet_dormant(WALLET, metrics) is False


def test_dormant_false_when_no_activity_yet():
    """Wallet with no recorded activity is NOT dormant — needs
    active polling to discover its first trade. Otherwise newly
    added wallets would never get the polling cadence they need."""
    assert lt._is_wallet_dormant(WALLET, {}) is False
    assert lt._is_wallet_dormant(WALLET, {WALLET: {}}) is False
    assert lt._is_wallet_dormant(WALLET, {WALLET: {"last_activity_ts": 0}}) is False


# --- Group aggregation -------------------------------------------------

def _wallets(*items):
    """Build a wallets dict from (addr, name, group, emoji) tuples."""
    return {addr: {"name": n, "group": g, "emoji": e} for addr, n, g, e in items}


def test_wallets_by_group_indexes_correctly():
    w = _wallets(
        ("addr1", "n1", "groupA", "😀"),
        ("addr2", "n2", "groupA", "😀"),
        ("addr3", "n3", "groupB", "🤠"),
    )
    by = lt._wallets_by_group(w)
    assert sorted(by["groupA"]) == ["addr1", "addr2"]
    assert by["groupB"]         == ["addr3"]


def test_group_emoji_picks_first_non_empty():
    w = _wallets(
        ("a", "x", "g1", ""),
        ("b", "y", "g1", "🤠"),
        ("c", "z", "g1", "🎩"),
    )
    assert lt._group_emoji("g1", w) == "🤠"


def test_group_emoji_returns_empty_when_none():
    w = _wallets(("a", "x", "g1", ""), ("b", "y", "g1", ""))
    assert lt._group_emoji("g1", w) == ""


def test_aggregate_group_metrics_sums_multi_wallet_group():
    w = _wallets(
        ("a1", "n", "peace", "🤠"),
        ("a2", "n", "peace", "🤠"),
        ("a3", "n", "solo",  "🦊"),
    )
    metrics = {
        "a1": {"trade_count_30d": 4, "win_count_30d": 3, "total_pnl_sol_30d": 0.5, "last_activity_ts": time.time() - 60},
        "a2": {"trade_count_30d": 6, "win_count_30d": 2, "total_pnl_sol_30d": -0.2, "last_activity_ts": time.time() - 30 * 86_400},
        "a3": {"trade_count_30d": 1, "win_count_30d": 1, "total_pnl_sol_30d": 0.1, "last_activity_ts": time.time() - 30},
    }
    out = lt._aggregate_group_metrics(w, metrics)
    peace = out["peace"]
    assert peace["trade_count"]          == 10
    assert peace["win_count"]            == 5
    assert peace["win_rate"]             == 0.5
    assert abs(peace["pnl_sol"] - 0.3) < 1e-9
    assert peace["active_member_count"]  == 1
    assert peace["dormant_member_count"] == 1
    assert peace["is_active_group"]      is True


# --- Top-3 ranking -----------------------------------------------------

def test_top3_filters_to_24h_window():
    now = time.time()
    w = _wallets(("a", "n", "g1", "😀"))
    trades = [
        {"wallet": "a", "exit_time": now - 30 * 60,    "pnl_sol": 1.0},   # in
        {"wallet": "a", "exit_time": now - 25 * 3600,  "pnl_sol": 99.0},  # out (>24h)
    ]
    top3 = lt._top3_pnl_24h(w, trades)
    assert len(top3) == 1
    assert top3[0]["pnl_sol"] == 1.0


def test_top3_sorts_by_pnl_desc():
    now = time.time()
    w = _wallets(
        ("a", "n", "lo",  "1"),
        ("b", "n", "mid", "2"),
        ("c", "n", "hi",  "3"),
    )
    trades = [
        {"wallet": "a", "exit_time": now, "pnl_sol":  0.5},
        {"wallet": "b", "exit_time": now, "pnl_sol":  1.0},
        {"wallet": "c", "exit_time": now, "pnl_sol":  3.0},
    ]
    top3 = lt._top3_pnl_24h(w, trades)
    assert [g["group"] for g in top3] == ["hi", "mid", "lo"]


def test_top3_caps_at_three():
    now = time.time()
    w = _wallets(
        ("a", "n", "g1", ""), ("b", "n", "g2", ""),
        ("c", "n", "g3", ""), ("d", "n", "g4", ""),
    )
    trades = [{"wallet": k, "exit_time": now, "pnl_sol": float(i)}
              for i, k in enumerate("abcd", 1)]
    assert len(lt._top3_pnl_24h(w, trades)) == 3


def test_top3_empty_when_no_activity():
    assert lt._top3_pnl_24h({}, []) == []


# --- Message formatting + pluralization --------------------------------

def test_format_top3_singular_and_plural():
    top3 = [
        {"group": "alpha", "pnl_sol": 1.234, "trade_count": 1, "win_count": 1, "win_rate": 1.0, "emoji": "🤠"},
        {"group": "beta",  "pnl_sol": 0.5,   "trade_count": 2, "win_count": 1, "win_rate": 0.5, "emoji": ""},
    ]
    msg = lt._format_top3_message(top3)
    assert "1 trade," in msg          # singular
    assert "2 trades," in msg         # plural
    assert "100% WR" in msg
    assert "50% WR" in msg
    assert "🤠" in msg                # emoji rendered
    assert "+1.234 SOL" in msg
    assert "Perth" in msg             # timestamp suffix


def test_format_top3_empty_message():
    msg = lt._format_top3_message([])
    assert "No qualifying activity in last 24h." in msg
    assert "Perth" in msg


def test_format_top3_negative_pnl_no_plus_sign():
    top3 = [{"group": "g", "pnl_sol": -0.5, "trade_count": 3,
             "win_count": 1, "win_rate": 0.333, "emoji": ""}]
    msg = lt._format_top3_message(top3)
    assert "-0.500 SOL" in msg
    # no "+" prefix on negatives
    assert "+-0.500" not in msg


# --- Atomic write + load helpers ---------------------------------------

def test_atomic_write_creates_directory_and_file():
    with tempfile.TemporaryDirectory() as d:
        target = os.path.join(d, "nested", "deep", "file.json")
        lt._atomic_write_json(target, {"a": 1, "b": [2, 3]})
        assert os.path.exists(target)
        with open(target) as f:
            assert json.load(f) == {"a": 1, "b": [2, 3]}


def test_load_json_or_default_when_missing():
    assert lt._load_json_or("/does/not/exist.json", {"fallback": True}) == {"fallback": True}
    assert lt._load_json_or("/does/not/exist.json", []) == []


def test_load_json_or_handles_corrupt_file():
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write("not valid json {{{")
        path = f.name
    try:
        # Should return default, not raise
        assert lt._load_json_or(path, {"sentinel": 1}) == {"sentinel": 1}
    finally:
        os.unlink(path)


# --- 3-day aggregate (Claude input) ------------------------------------

def test_aggregate_3day_filters_to_window():
    now = time.time()
    w = _wallets(("addr", "n", "groupA", "🤠"))
    metrics = {"addr": {"last_activity_ts": now - 60}}
    trades = [
        {"wallet": "addr", "exit_time": now - 60,              "pnl_sol":  1.0, "hold_minutes": 5},   # in
        {"wallet": "addr", "exit_time": now - 4 * 86_400,      "pnl_sol":  9.0, "hold_minutes": 5},   # out (>3d)
    ]
    out = lt._aggregate_3day_group_data(w, metrics, trades)
    assert out["window_days"] == 3
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["group"]     == "groupA"
    assert g["trades_3d"] == 1
    assert abs(g["total_pnl_sol_3d"] - 1.0) < 1e-9


def test_aggregate_3day_excludes_inactive_groups():
    """Groups with zero trades in window are omitted from output —
    saves Claude tokens and focuses analysis on real signal."""
    now = time.time()
    w = _wallets(
        ("active",   "n", "groupA", ""),
        ("inactive", "n", "groupB", ""),
    )
    trades = [{"wallet": "active", "exit_time": now, "pnl_sol": 1.0, "hold_minutes": 1}]
    out = lt._aggregate_3day_group_data(w, {}, trades)
    assert len(out["groups"])      == 1
    assert out["groups"][0]["group"] == "groupA"


# --- Claude prompt construction ----------------------------------------

def test_build_lore_prompt_returns_system_and_user():
    data = {"window_days": 3, "as_of_perth": "2026-05-16 09:00",
            "groups": [{"group": "alpha", "trades_3d": 5}]}
    system, user = lt._build_lore_analysis_prompt(data)
    assert isinstance(system, str) and system.strip()
    assert isinstance(user,   str) and user.strip()


def test_build_lore_prompt_demands_strict_json():
    data = {"window_days": 3, "as_of_perth": "x", "groups": []}
    _, user = lt._build_lore_analysis_prompt(data)
    low = user.lower()
    # The prompt MUST request strict JSON and forbid prose/fences
    assert "strict json" in low
    assert "no markdown" in low or "no code fences" in low
    # The schema must mention all three required output keys
    assert "promote" in low
    assert "watch"   in low
    assert "demote"  in low


def test_build_lore_prompt_embeds_input_data():
    data = {"window_days": 3, "as_of_perth": "2026-05-16 09:00",
            "groups": [{"group": "marker_group_xyz", "trades_3d": 99}]}
    _, user = lt._build_lore_analysis_prompt(data)
    assert "marker_group_xyz" in user
    assert "2026-05-16 09:00" in user


# --- Claude response parsing -------------------------------------------

def test_parse_clean_json():
    raw = '{"summary": "ok", "promote": [{"group": "a", "reasoning": "r"}], "watch": [], "demote": []}'
    parsed = lt._parse_lore_analysis_response(raw)
    assert parsed["summary"]    == "ok"
    assert parsed["promote"]    == [{"group": "a", "reasoning": "r"}]
    assert parsed["watch"]      == []
    assert parsed["demote"]     == []


def test_parse_strips_json_code_fence():
    raw = '```json\n{"summary": "x", "promote": [], "watch": [], "demote": []}\n```'
    parsed = lt._parse_lore_analysis_response(raw)
    assert parsed is not None
    assert parsed["summary"] == "x"


def test_parse_strips_bare_code_fence():
    raw = '```\n{"summary": "y", "promote": [], "watch": [], "demote": []}\n```'
    parsed = lt._parse_lore_analysis_response(raw)
    assert parsed is not None
    assert parsed["summary"] == "y"


def test_parse_invalid_json_returns_none():
    assert lt._parse_lore_analysis_response("not json")            is None
    assert lt._parse_lore_analysis_response("{ broken")            is None
    assert lt._parse_lore_analysis_response("")                    is None


def test_parse_non_object_returns_none():
    """JSON array, string, etc. are valid JSON but not the expected
    object shape — reject so callers don't blow up later."""
    assert lt._parse_lore_analysis_response('[1, 2, 3]') is None
    assert lt._parse_lore_analysis_response('"just a string"') is None


def test_parse_defaults_missing_arrays():
    """If Claude omits a list field, default to [] so the message
    formatter doesn't have to defensive-check."""
    raw = '{"summary": "ok"}'
    parsed = lt._parse_lore_analysis_response(raw)
    assert parsed["promote"] == []
    assert parsed["watch"]   == []
    assert parsed["demote"]  == []


# --- _format_lore_analysis_message -------------------------------------

def test_format_analysis_renders_all_sections():
    parsed = {
        "summary": "Three-day window saw mixed signal.",
        "promote": [{"group": "alpha", "reasoning": "high WR, consistent PnL"}],
        "watch":   [{"group": "beta",  "reasoning": "declining"}],
        "demote":  [{"group": "gamma", "reasoning": "negative PnL"}],
    }
    w = _wallets(
        ("a1", "n", "alpha", "🤠"),
        ("b1", "n", "beta",  ""),
        ("c1", "n", "gamma", "🎯"),
    )
    msg = lt._format_lore_analysis_message(parsed, w)
    assert "LORE ANALYSIS" in msg
    assert "Three-day window saw mixed signal." in msg
    assert "PROMOTE" in msg and "alpha" in msg and "🤠" in msg
    assert "WATCH"   in msg and "beta"  in msg
    assert "DEMOTE"  in msg and "gamma" in msg and "🎯" in msg
    assert "Perth" in msg


def test_format_analysis_omits_empty_sections():
    parsed = {
        "summary": "Quiet week.",
        "promote": [{"group": "alpha", "reasoning": "r"}],
        "watch":   [],
        "demote":  [],
    }
    w = _wallets(("a", "n", "alpha", "🤠"))
    msg = lt._format_lore_analysis_message(parsed, w)
    assert "PROMOTE" in msg
    assert "WATCH"   not in msg
    assert "DEMOTE"  not in msg


def test_format_analysis_handles_missing_summary():
    parsed = {"summary": "", "promote": [{"group": "g", "reasoning": "r"}], "watch": [], "demote": []}
    w = _wallets(("a", "n", "g", ""))
    msg = lt._format_lore_analysis_message(parsed, w)
    # Summary line absent but PROMOTE still rendered
    assert "PROMOTE" in msg
    assert "g — r" in msg


# --- Config sanity -----------------------------------------------------

def test_config_invariants():
    """Pin the key constants so an accidental config edit can't
    silently destroy ranking, scheduling, or rate-limiting."""
    assert lt.LORE_POLL_INTERVAL_ACTIVE_MIN  == 15
    assert lt.LORE_POLL_INTERVAL_DORMANT_MIN == 60
    assert lt.LORE_DORMANT_THRESHOLD_DAYS    == 14
    assert lt.LORE_METRICS_WINDOW_DAYS       == 30
    assert lt.LORE_RPC_GAP_SEC               > 0
    assert lt.LORE_RPC_GAP_SEC               < 1   # not absurdly slow
    assert lt.LORE_COLD_START_SIG_LIMIT      >= 1
    assert lt.LORE_TOP3_PERTH_HOUR           == 9
    assert lt.LORE_ANALYSIS_PERTH_HOUR       == 9
    assert lt.LORE_ANALYSIS_WEEKDAYS         == {0, 3, 6}   # Mon, Thu, Sun
    assert lt.LORE_CLAUDE_MODEL              == "claude-haiku-4-5"
    assert lt.SOL_MINT  == "So11111111111111111111111111111111111111112"
    assert lt.USDC_MINT == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    assert lt.USDT_MINT == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
