"""Compare the final scores of two streaming-scorer runs on the same games (L13, lake/scores/live_game).

Used to compare the provisional baselines (L12) with workstream B's baselines (L10): run A = job 29064
(baselines_provisional), run B = the same 60,000 October games scored against lake/baselines.

Run inside a Slurm job (cluster/compare_scores.sbatch), from the repo root:
    python src/streaming/compare_scores.py --config config/cluster.yaml \
        --run-a 29064-nopacing-c1 --run-b <job>-nopacing-c1 \
        --out docs/validation_results/scorer_baselines_comparison.csv

Two scopes, one CSV row each, overall and per time class:
  - player_game: one score per (game_id, player_id), the grain of L13
  - game: one score per game = the higher of its two players' risk_score (null when both are null)
Metrics on the rows present in both runs: Pearson and Spearman correlation of risk_score (rows scored
in both), mean and max absolute difference, risk tier changes, and the overlap of the top 1%.
Risk tiers: unscored (risk_score null), normal (< threshold), high (>= threshold); the threshold is the
dashboard's high_risk_threshold (config/serving.yaml, 1.0). Top 1%: the k = ceil(1% of the rows scored
in run A) highest scores of each run (ties broken by key); overlap = shared rows / k.
Only aggregates are written; no ids.
"""

import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.common.config import build_arg_parser, load_config
from src.common.logging_setup import setup_logging

STEP = "compare_scores"
KEY = ["game_id", "player_id"]
TOP_SHARE = 0.01
log = logging.getLogger(STEP)


def read_run(live_dir: str, run_id: str) -> pd.DataFrame:
    """Final scores of one run: key, time class, risk score and the baselines folder it used."""
    path = Path(live_dir) / f"run_id={run_id}"
    if not path.is_dir():
        raise SystemExit(f"no scores for run {run_id}: {path} does not exist")
    df = pd.read_parquet(path, columns=KEY + ["time_class", "risk_score", "baselines"])
    if df.duplicated(KEY).any():
        raise SystemExit(f"run {run_id}: duplicate (game_id, player_id) rows")
    return df


def tier(score: pd.Series, threshold: float) -> pd.Series:
    """Risk tier of each score: unscored (null), normal (< threshold), high (>= threshold)."""
    return pd.Series(np.select([score.isna(), score >= threshold], ["unscored", "high"], "normal"),
                     index=score.index)


def top_keys(df: pd.DataFrame, col: str, key: list[str], k: int) -> set:
    """Keys of the k highest scores (ties broken by key, so the selection is deterministic)."""
    top = df[df[col].notna()].sort_values([col] + key, ascending=[False] + [True] * len(key)).head(k)
    return set(map(tuple, top[key].to_numpy()))


def compare(a: pd.DataFrame, b: pd.DataFrame, key: list[str], threshold: float) -> dict:
    """Metrics for one scope and time class; a and b have key, time_class and risk_score."""
    m = a.merge(b, on=key, how="inner", suffixes=("_a", "_b"))
    both = m[m["risk_score_a"].notna() & m["risk_score_b"].notna()]
    diff = (both["risk_score_b"] - both["risk_score_a"]).abs()
    ta, tb = tier(m["risk_score_a"], threshold), tier(m["risk_score_b"], threshold)
    changed = ta != tb
    scored_a = int(a["risk_score"].notna().sum())
    k = math.ceil(TOP_SHARE * scored_a) if scored_a else 0
    overlap = len(top_keys(a, "risk_score", key, k) & top_keys(b, "risk_score", key, k)) if k else 0
    return {
        "rows_a": len(a), "rows_b": len(b), "rows_matched": len(m),
        "scored_a": scored_a, "scored_b": int(b["risk_score"].notna().sum()), "scored_both": len(both),
        "scored_only_a": int((m["risk_score_a"].notna() & m["risk_score_b"].isna()).sum()),
        "scored_only_b": int((m["risk_score_a"].isna() & m["risk_score_b"].notna()).sum()),
        "pearson_r": both["risk_score_a"].corr(both["risk_score_b"]) if len(both) > 1 else None,
        "spearman_r": (both["risk_score_a"].rank().corr(both["risk_score_b"].rank())  # = Spearman, no scipy
                       if len(both) > 1 else None),
        "mean_score_a": both["risk_score_a"].mean(), "mean_score_b": both["risk_score_b"].mean(),
        "mean_abs_diff": diff.mean(), "p99_abs_diff": diff.quantile(0.99) if len(diff) else None,
        "max_abs_diff": diff.max(),
        "high_a": int((ta == "high").sum()), "high_b": int((tb == "high").sum()),
        "tier_changes": int(changed.sum()), "tier_changes_pct": 100 * changed.mean() if len(m) else None,
        "normal_to_high": int(((ta == "normal") & (tb == "high")).sum()),
        "high_to_normal": int(((ta == "high") & (tb == "normal")).sum()),
        "unscored_changes": int((changed & ((ta == "unscored") | (tb == "unscored"))).sum()),
        "top1pct_k": k, "top1pct_overlap": overlap, "top1pct_overlap_pct": 100 * overlap / k if k else None,
    }


def game_level(df: pd.DataFrame) -> pd.DataFrame:
    """One row per game: the higher of the two players' risk scores (max skips nulls)."""
    return df.groupby(["game_id", "time_class"], as_index=False)["risk_score"].max()


def main() -> None:
    parser = build_arg_parser("Compare the final scores of two streaming-scorer runs on the same games")
    parser.add_argument("--run-a", required=True, help="reference run_id (e.g. provisional baselines)")
    parser.add_argument("--run-b", required=True, help="run_id to compare with it (e.g. final baselines)")
    parser.add_argument("--out", required=True, help="output CSV")
    parser.add_argument("--serving-config", default="config/serving.yaml",
                        help="config with dashboard.high_risk_threshold (default config/serving.yaml)")
    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    with open(args.serving_config) as fh:
        threshold = float(yaml.safe_load(fh)["dashboard"]["high_risk_threshold"])

    live = cfg["paths"]["live_scores_dir"]
    a, b = read_run(live, args.run_a), read_run(live, args.run_b)
    base_a, base_b = ",".join(sorted(a["baselines"].unique())), ",".join(sorted(b["baselines"].unique()))
    log.info("run A %s (%s): %d rows; run B %s (%s): %d rows; high risk >= %g",
             args.run_a, base_a, len(a), args.run_b, base_b, len(b), threshold)
    if set(map(tuple, a[KEY].to_numpy())) != set(map(tuple, b[KEY].to_numpy())):
        log.warning("the runs do not cover the same (game, player) rows: metrics use the rows in both")

    rows = []
    for scope, fa, fb, key in [("player_game", a, b, KEY), ("game", game_level(a), game_level(b), ["game_id"])]:
        for tc in ["all"] + sorted(fa["time_class"].unique()):
            sa = fa if tc == "all" else fa[fa["time_class"] == tc]
            sb = fb if tc == "all" else fb[fb["time_class"] == tc]
            r = {"scope": scope, "time_class": tc, **compare(sa, sb, key, threshold)}
            rows.append(r)
            log.info("%s %s: r=%.4f spearman=%.4f tier changes %d (%.2f%%), top-1%% overlap %d/%d",
                     scope, tc, r["pearson_r"] or float("nan"), r["spearman_r"] or float("nan"),
                     r["tier_changes"], r["tier_changes_pct"] or 0, r["top1pct_overlap"], r["top1pct_k"])
    out = pd.DataFrame(rows)
    out.insert(0, "run_a", args.run_a)
    out.insert(1, "baselines_a", base_a)
    out.insert(2, "run_b", args.run_b)
    out.insert(3, "baselines_b", base_b)
    out["high_risk_threshold"] = threshold
    out.round(6).to_csv(args.out, index=False)
    log.info("written %s (%d rows)", args.out, len(out))


if __name__ == "__main__":
    main()
