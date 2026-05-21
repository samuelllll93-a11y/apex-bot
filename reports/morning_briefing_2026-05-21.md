# Morning Briefing — 2026-05-21

Generated at 02:xx AWST by overnight analysis agent.

---

## TL;DR

30 calibration trades covering a ~19-hour window (2026-05-14 09:00 → 2026-05-15 04:17) show an overall win rate of 46.7%. The 75–80% confidence band is the only one hitting ≥85% win rate but n=7 — well below the 30-trade minimum needed to set a new threshold with confidence. No MIN_CONFIDENCE change can be statistically justified yet; this branch proposes a conservative floor raise from WHALE_MIN_SCORE=50 to 65 based on 0/3 wins below that level. POSITION_SIZE_PCT remains unchanged. Whale copy win rates for both crispy and mannos are below 50% in this window, concentrated entirely in the <$50K mcap tier; mannos is particularly weak at 27%.

---

## Win Rate by Confidence Band

| Band | n | Wins | Losses | Win Rate | Notes |
|------|---|------|--------|----------|-------|
| < 65% | 3 | 0 | 3 | 0.0% | Below all tracked bands; all losses |
| 65–70% | 7 | 2 | 5 | 28.6% | **LOW CONFIDENCE IN ESTIMATE** (n < 20) |
| 70–75% | 6 | 2 | 4 | 33.3% | **LOW CONFIDENCE IN ESTIMATE** (n < 20) |
| 75–80% | 7 | 6 | 1 | 85.7% | **LOW CONFIDENCE IN ESTIMATE** (n < 20) |
| 80%+ | 7 | 4 | 3 | 57.1% | **LOW CONFIDENCE IN ESTIMATE** (n < 20) |
| **All ≥65** | **27** | **14** | **13** | **51.9%** | |
| **All trades** | **30** | **14** | **16** | **46.7%** | Breakeven treated as loss |

Note: All 30 trades in the 75–80% band had a confidence value of exactly 77. The total absence of 75, 76, 78, or 79 is suspicious and may indicate a Claude prompt artefact or quantisation in the scoring function — worth investigating before raising the threshold to 77.

---

## Recommended MIN_CONFIDENCE

**No threshold achieves ≥85% win rate with n≥30 above it in the current dataset.** This is the correct outcome: 30 trades total is not enough to statistically anchor a new floor, and data collection should continue.

However, a **conservative incremental change is proposed**: raise `WHALE_MIN_SCORE` from **50 → 65**.

Justification:
- All 3 trades below 65% confidence were losses (0/3 win rate, 0.0%).
- The 65–70% band is also weak (28.6%), but with n=7 it does not cleanly separate from noise, so moving all the way to 70 is premature.
- Raising to 65 reduces daily churn from marginally-scored signals and removes the definitive zero-value region, without making an aggressive bet on the noisy 75+ zone.
- Revisit in 14 days with ≥150 additional trades; if the 75–80% band continues to outperform at n≥20, consider a second raise to 75.

---

## Recommended POSITION_SIZE_PCT

**No change.** Overall win rate (46.7%) does not support scaling up position size. The 75–80% band's sample size is too small to trust for a sizing decision. Recommend revisiting once ≥50 trades land in that band.

---

## Whale Behaviour Intelligence

### Data window

Log files parsed: `whale_sniper.log.2026-05-11`, `whale_sniper.log.2026-05-12` (where actual executions occurred). Files from 2026-05-13 through 2026-05-17 and the current `whale_sniper.log` contain BUY signals for crispy/mannos but no executed trades — the bot was in LOW BALANCE or skip state for those days. Actual trade dataset is therefore **n=7 for crispy** and **n=11 for mannos**, both from 2026-05-11 to 2026-05-12 only.

No prior `whale_patterns.json` snapshot exists — this is the first run.

---

### crispy (7 executed trades)

**Entry market cap**

All 6 trades with known MC entry were under $50K (one trade, `domi`, entered before the log window):

| Tier | Count |
|------|-------|
| < $50K | 6 |
| $50K–$250K | 0 |
| $250K–$1M | 0 |
| $1M–$5M | 0 |
| > $5M | 0 |

Entry MCs: $5.3K, $5.5K, $8.0K, $15.8K, $16.1K, $18.8K. Crispy is a deep micro-cap buyer — all entries well inside the PumpFun pre-graduation zone.

**Token characteristics**

All tokens have a `pump` suffix in their contract address, indicating PumpFun bonding-curve tokens. Holder count, top-10 concentration, and bonded/unbonded status are **not logged** — field absent from logs.

**Hold duration (executed trades with known entry/exit)**

| Outcome | Hold times | Median |
|---------|------------|--------|
| Winners (n=3) | 1m, 6m, 23m | **6 min** |
| Losers (n=4) | 0m, 0m, 51m, 112m | **25.5 min** |

Winners are exited quickly via trailing stop or a fast manual sell. Losers linger — the 112-minute `CZM` loss (−100%) is the clearest example.

**Exit patterns**

| Exit reason | Count |
|-------------|-------|
| trailing_stop | 1 |
| whale_full_exit | 3 |
| manual_sell | 3 |

**Win rate by market cap tier**

| Tier | Wins | Total | Win Rate |
|------|------|-------|----------|
| < $50K | 3 | 6 | **50%** |
| All other tiers | — | 0 | N/A |

---

### mannos (11 executed trades)

**Entry market cap**

All 11 trades had known MC entries, all under $50K:

| Tier | Count |
|------|-------|
| < $50K | 11 |
| $50K–$250K | 0 |
| $250K–$1M | 0 |
| $1M–$5M | 0 |
| > $5M | 0 |

Entry MCs: $4.6K, $6.8K, $11.0K, $11.1K, $11.5K, $12.6K, $13.7K, $16.8K, $21.2K, $24.4K, $28.6K. More spread than crispy, but firmly in the <$50K zone — several near the $25K–$30K upper boundary.

**Token characteristics**

Same as crispy: all PumpFun bonding-curve tokens. Holder count, top-10 concentration, and bonded status are **not logged**.

**Hold duration**

| Outcome | Hold times | Median |
|---------|------------|--------|
| Winners (n=3) | 0m, 7m, 51m | **7 min** |
| Losers (n=8) | 1m, 1m, 1m, 3m, 4m, 17m, 49m, 247m | **3.5 min** |

Interestingly, losers are exited *faster* on average than winners — suggesting the exit logic (whale_full_exit, hard sell floor) is firing quickly on bad entries rather than holding through a drawdown. The 247-minute `SCOOBYJEW` outlier was a manual sell at −40.6%.

**Exit patterns**

| Exit reason | Count |
|-------------|-------|
| trailing_stop | 2 |
| whale_full_exit | 3 |
| manual_sell | 6 |

Manual sells dominate mannos exits. 6 of 11 trades were closed manually — worth checking whether those manual interventions helped or hurt vs. letting the bot manage them.

**Win rate by market cap tier**

| Tier | Wins | Total | Win Rate |
|------|------|-------|----------|
| < $50K | 3 | 11 | **27.3%** |
| All other tiers | — | 0 | N/A |

---

### Cross-whale summary

Mannos wins are concentrated at lower MC entries ($6.8K, $11.1K, $11.5K) — both trailing-stop winners hit at sub-$12K. Losing mannos entries span a wider range up to $28.6K. Crispy's two clearest wins (`Grok` $5.5K, `LeMeme` $8.0K) were also sub-$10K. Tentative pattern: entries below ~$12K show better outcomes for both whales, but sample sizes are too small to formalise this as a filter.

**Drift vs previous snapshot:** N/A — no prior `whale_patterns.json` exists.

---

## Caveats

1. **Confidence calibration window is short.** All 30 trades occurred over ~19 hours (2026-05-14 09:00 – 2026-05-15 04:17). This is a single market session and may not be representative.
2. **All 75–80% confidence trades had exactly confidence=77.** Suspicious quantisation — investigate whether the Claude scoring prompt produces a bimodal distribution.
3. **80%+ band underperforms 75–80%.** The model appears overconfident at high scores (57.1% vs 85.7%). This is unintuitive and warrants prompt inspection.
4. **Whale trade count is very low.** n=7 (crispy) and n=11 (mannos) reflect a two-day active window; the bot was in LOW BALANCE / skip state from 2026-05-13 onwards.
5. **No token trait fields in logs.** Holder count, top-10 concentration, and bonded/unbonded status are not captured in whale_sniper.log. The whale behaviour analysis is limited to what is available.
6. **Manual sells dominate mannos exits** (6/11). Their impact on outcome is unclear without knowing the counterfactual (what the bot would have done).
7. **Breakeven trades counted as losses** per task spec (conservative).

---

## Proposed Diff

```diff
--- a/whale_sniper.py
+++ b/whale_sniper.py
@@ -188,7 +188,7 @@ DIP_SNIPER_MIN_SCORE     = 65     # minimum Claude score to enter a dip
-WHALE_MIN_SCORE          = 50     # minimum Claude score to enter a whale copy
+WHALE_MIN_SCORE          = 65     # minimum Claude score to enter a whale copy
```

**Rationale:** All 3 observed trades with confidence < 65 resulted in losses (0.0% win rate). Raising the floor to 65 eliminates this zero-value region. The 65–70 band still shows poor results (28.6%, n=7) — a second raise to 70 or 75 should be considered after accumulating ≥50 additional trades per band.

**POSITION_SIZE_PCT:** No change. `PREBOND_POS_SIZE_PCT` remains at `0.02` (2%). Overall win rate does not support scaling up.
