"""Workstream C, step 4: label statistics for the report (CSV files in docs/validation_results/).

Run from the repo root with the venv active, after build_labels_spark.py:
    PYTHONPATH=. spark-submit --driver-memory 3g src/ingestion/label_stats.py --config config/local.yaml \
        [--part1-csv data/processed/player_labels.csv]

Writes aggregates only (no player_id, no username), one file per table, named with the config's
`env` (local / cluster) so laptop tests never overwrite cluster results:
  labels_summary_<env>.csv              per sample_source and in total: sampled, labelled, label counts,
                                        positive share (of positive + negative) with a 95% Wilson interval
  labels_enrichment_<env>.csv           high_anomaly vs random: positive shares, ratio, odds ratio,
                                        Fisher exact test (two-sided), positives expected with no effect
  labels_by_band_time_class_<env>.csv   label counts per sample_source x time_class x rating band; one row
                                        per (player, time class) in lake/anomaly/player, so a player who
                                        plays several time classes counts once in each
  labels_spark_vs_part1_<env>.csv       only with --part1-csv: this Spark port vs the Part 1 SQL labels
                                        on every player of restricted/identity (mismatches per column)
"""

import csv
import logging
import math
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import REPO_ROOT, build_arg_parser, load_config
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark
from src.ingestion.build_labels_spark import apply_label_rules, read_profiles

STEP = "label_stats"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "docs" / "validation_results"
COMPARED_COLUMNS = ["label", "tos_violation", "disabled", "found", "account_created_at", "label_fetched_at"]

log = logging.getLogger(STEP)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    """95% Wilson score interval for a proportion k/n, in percent (better than p ± z·se when k is small)."""
    if n == 0:
        return None, None
    p, d = k / n, 1 + z * z / n
    centre, half = (p + z * z / (2 * n)) / d, z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return round(100 * (centre - half), 3), round(100 * (centre + half), 3)


def fisher_log10_p(a: int, b: int, c: int, d: int) -> float:
    """log10 of the two-sided Fisher exact p-value for the 2x2 table [[a, b], [c, d]].

    Summed in log space (log-sum-exp): with large tables p can be far below the smallest float
    (~1e-308), and exp() would round it to 0, which is not a valid p-value.
    """
    n1, total, k_total = a + b, a + b + c + d, a + c
    log_comb = lambda n, k: math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)
    log_pmf = lambda k: log_comb(k_total, k) + log_comb(total - k_total, n1 - k) - log_comb(total, n1)
    observed = log_pmf(a)
    support = range(max(0, n1 - (total - k_total)), min(k_total, n1) + 1)
    terms = [lp for lp in map(log_pmf, support) if lp <= observed + 1e-7]
    top = max(terms)
    log_p = top + math.log(sum(math.exp(lp - top) for lp in terms))
    return min(0.0, log_p / math.log(10))


def band_label(band_id: int) -> str:
    """Same bands as ref.rating_bands: <800, 200-point bands 800–2799, 2800+."""
    if band_id == 1:
        return "<800"
    if band_id == 12:
        return "2800+"
    low = 800 + 200 * (band_id - 2)
    return f"{low}-{low + 199}"


def label_counts(df: DataFrame, *keys: str) -> DataFrame:
    return df.groupBy(*keys).agg(
        F.count("*").alias("labelled"),
        F.sum((F.col("label") == "positive").cast("int")).alias("positive"),
        F.sum((F.col("label") == "negative").cast("int")).alias("negative"),
        F.sum(((F.col("label") == "unknown") & F.col("found")).cast("int")).alias("unknown_disabled"),
        F.sum((~F.col("found")).cast("int")).alias("unknown_not_found"),
        # Formatted in Spark (UTC session time zone): collected datetimes would show in the local time zone
        F.date_format(F.min("label_fetched_at"), "yyyy-MM-dd HH:mm:ss").alias("fetched_from"),
        F.date_format(F.max("label_fetched_at"), "yyyy-MM-dd HH:mm:ss").alias("fetched_to"),
    )


def with_share(row: dict[str, Any]) -> dict[str, Any]:
    """Positive share among positive + negative (unknown is excluded from training and evaluation)."""
    known = row["positive"] + row["negative"]
    row["positive_pct"] = round(100 * row["positive"] / known, 3) if known else None
    row["positive_pct_ci_low"], row["positive_pct_ci_high"] = wilson(row["positive"], known)
    return row


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log.info("wrote %s (%d rows)", path, len(rows))


def summary_rows(sample: DataFrame, labels: DataFrame, population: int) -> list[dict[str, Any]]:
    """`eligible_population`: players in lake/anomaly/player, the population both parts are drawn from."""
    sampled = {r["sample_source"]: r["n"] for r in sample.groupBy("sample_source").agg(F.count("*").alias("n")).collect()}
    per_part = {r["sample_source"]: r.asDict() for r in label_counts(labels, "sample_source").collect()}
    total = label_counts(labels.withColumn("sample_source", F.lit("all")), "sample_source").first().asDict()
    rows = []
    for part in sorted(per_part) + ["all"]:
        row = per_part[part] if part != "all" else total
        row = {"sample_source": part, "eligible_population": population,
               "sampled": sampled.get(part, sum(sampled.values())), **{
            k: v for k, v in row.items() if k != "sample_source"}}
        rows.append(with_share(row))
    return rows


def enrichment_row(summary: list[dict[str, Any]]) -> dict[str, Any] | None:
    """2x2 table: (high_anomaly, random) x (positive, negative)."""
    by_part = {r["sample_source"]: r for r in summary}
    if not {"high_anomaly", "random"} <= by_part.keys():
        return None
    hi, rnd = by_part["high_anomaly"], by_part["random"]
    a, b, c, d = hi["positive"], hi["negative"], rnd["positive"], rnd["negative"]
    n_hi, k_total, total = a + b, a + c, a + b + c + d
    return {
        "high_anomaly_positive": a, "high_anomaly_known": n_hi, "high_anomaly_positive_pct": hi["positive_pct"],
        "random_positive": c, "random_known": c + d, "random_positive_pct": rnd["positive_pct"],
        "ratio_of_shares": round(hi["positive_pct"] / rnd["positive_pct"], 3) if rnd["positive_pct"] else None,
        "odds_ratio": round(a * d / (b * c), 3) if b and c else None,
        "expected_high_anomaly_positive_if_no_effect": round(k_total * n_hi / total, 1),
        # p itself underflows to 0.0 below ~1e-308; log10_p is always exact enough to report
        "fisher_p_two_sided": float(f"{10 ** fisher_log10_p(a, b, c, d):.4g}"),
        "fisher_log10_p_two_sided": round(fisher_log10_p(a, b, c, d), 2),
    }


def band_time_class_rows(labels: DataFrame, anomaly: DataFrame) -> list[dict[str, Any]]:
    joined = labels.join(anomaly.select("player_id", "time_class", "band_id"), "player_id")
    rows = []
    for r in label_counts(joined, "sample_source", "time_class", "band_id").orderBy(
            "sample_source", "time_class", "band_id").collect():
        row = r.asDict()
        row = {"sample_source": row.pop("sample_source"), "time_class": row.pop("time_class"),
               "band_id": row["band_id"], "band_label": band_label(row.pop("band_id")), **row}
        row.pop("fetched_from"), row.pop("fetched_to")
        rows.append(with_share(row))
    return rows


def part1_rows(spark: Any, cfg: dict[str, Any], part1_csv: str) -> list[dict[str, Any]]:
    """Recompute labels for every identity player with the Spark rules and compare with the Part 1 CSV."""
    lake = cfg["paths"]["lake_dir"]
    identity = spark.read.parquet(f"{lake}/restricted/identity").select("player_id", F.lower("username").alias("username"))
    spark_labels = apply_label_rules(identity.join(read_profiles(spark, cfg["paths"]["profiles_dir"]), "username"))
    sql_labels = (spark.read.option("header", True).csv(part1_csv)
                  .select("player_id", "label", F.col("tos_violation").cast("boolean"), F.col("disabled").cast("boolean"),
                          F.col("found").cast("boolean"), F.to_timestamp("account_created_at").alias("account_created_at"),
                          F.to_timestamp("label_fetched_at").alias("label_fetched_at")))
    s = spark_labels.select("player_id", *[F.col(c).alias(f"{c}_spark") for c in COMPARED_COLUMNS]).withColumn(
        "account_created_at_spark", F.date_trunc("second", "account_created_at_spark"))  # Part 1 kept whole seconds
    q = sql_labels.select("player_id", *[F.col(c).alias(f"{c}_sql") for c in COMPARED_COLUMNS])
    m = s.join(q, "player_id", "full_outer")
    aggs = [F.count(F.col("label_sql")).alias("players_part1_sql"), F.count(F.col("label_spark")).alias("players_spark"),
            F.sum((F.col("label_sql").isNull() | F.col("label_spark").isNull()).cast("int")).alias("players_in_one_only")]
    aggs += [F.sum((~F.col(f"{c}_sql").eqNullSafe(F.col(f"{c}_spark"))).cast("int")).alias(f"mismatches_{c}")
             for c in COMPARED_COLUMNS]
    result = m.agg(*aggs).first().asDict()
    return [{"metric": k, "value": v} for k, v in result.items()]


def main() -> None:
    parser = build_arg_parser("Label statistics (CSV) for the report")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--part1-csv", help="Part 1 player_labels.csv: also write the Spark vs Part 1 comparison")
    args = parser.parse_args()
    cfg = load_config(args.config, args.months)
    setup_logging(STEP, cfg["paths"]["log_dir"])
    spark = get_spark(cfg, STEP)
    lake, env, out_dir = cfg["paths"]["lake_dir"], cfg.get("env", "local"), Path(args.output_dir)

    with log_duration(log, STEP):
        sample = spark.read.parquet(f"{lake}/labels_sample")
        labels = spark.read.parquet(f"{lake}/labels")
        anomaly = spark.read.parquet(f"{lake}/anomaly/player")
        summary = summary_rows(sample, labels, anomaly.select("player_id").distinct().count())
        write_csv(out_dir / f"labels_summary_{env}.csv", summary)
        for r in summary:
            log.info("%s: sampled=%d labelled=%d positive=%d negative=%d unknown=%d positive_pct=%s [%s, %s]",
                     r["sample_source"], r["sampled"], r["labelled"], r["positive"], r["negative"],
                     r["unknown_disabled"] + r["unknown_not_found"], r["positive_pct"],
                     r["positive_pct_ci_low"], r["positive_pct_ci_high"])

        enrichment = enrichment_row(summary)
        if enrichment:
            write_csv(out_dir / f"labels_enrichment_{env}.csv", [enrichment])
            log.info("enrichment high_anomaly vs random: ratio %s, odds ratio %s, Fisher p=%.3g (log10 p = %s)",
                     enrichment["ratio_of_shares"], enrichment["odds_ratio"], enrichment["fisher_p_two_sided"],
                     enrichment["fisher_log10_p_two_sided"])
        else:
            log.warning("no high_anomaly and random parts both present: enrichment file not written")

        write_csv(out_dir / f"labels_by_band_time_class_{env}.csv", band_time_class_rows(labels, anomaly))

        if args.part1_csv:
            rows = part1_rows(spark, cfg, args.part1_csv)
            write_csv(out_dir / f"labels_spark_vs_part1_{env}.csv", rows)
            bad = {r["metric"]: r["value"] for r in rows if r["metric"].startswith(("mismatches", "players_in")) and r["value"]}
            (log.error if bad else log.info)("Spark vs Part 1: %s", bad or "identical on every column")
    spark.stop()


if __name__ == "__main__":
    main()
