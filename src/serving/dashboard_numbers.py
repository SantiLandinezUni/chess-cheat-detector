"""Numbers quoted in the report's dashboard chapter, computed from the serving database (workstream E2).

Writes docs/validation_results/dashboard_{summary,by_time_class,by_band,score_terms,timeline}.csv for one
scorer run, with the high-risk threshold of serving.settings (the same one the dashboard uses).

Usage:
    python -m src.serving.dashboard_numbers --config config/serving.yaml [--run-id <id>]
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

LOG = logging.getLogger("dashboard_numbers")
OUT_DIR = Path("docs/validation_results")
TERMS = ("z_move_time_cv", "z_share_moves_under_2s", "z_move_time_cv_low_clock")

SUMMARY = """
WITH s AS (SELECT * FROM serving.live_scores WHERE run_id = :run),
     t AS (SELECT high_risk_threshold AS t FROM serving.settings),
     p AS (SELECT player_id, COUNT(*) FILTER (WHERE risk_score >= (SELECT t FROM t)) AS high
           FROM s GROUP BY player_id)
SELECT COUNT(DISTINCT game_key)                                     AS games,
       COUNT(*)                                                     AS player_games,
       COUNT(DISTINCT player_id)                                    AS players,
       COUNT(risk_score)                                            AS scored,
       COUNT(*) - COUNT(risk_score)                                 AS not_scored,
       COUNT(*) FILTER (WHERE risk_score >= (SELECT t FROM t))      AS high_risk,
       ROUND(percentile_cont(0.5)  WITHIN GROUP (ORDER BY risk_score)::numeric, 3) AS median,
       ROUND(percentile_cont(0.9)  WITHIN GROUP (ORDER BY risk_score)::numeric, 3) AS p90,
       ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY risk_score)::numeric, 3) AS p99,
       ROUND(MAX(risk_score)::numeric, 3)                           AS max,
       COUNT(DISTINCT game_key) FILTER (WHERE risk_score >= (SELECT t FROM t)) AS games_any_high,
       (SELECT COUNT(*) FROM p WHERE high >= 1)                     AS players_high_1plus,
       (SELECT COUNT(*) FROM p WHERE high >= 2)                     AS players_high_2plus
FROM s"""

BY_GROUP = """
SELECT {col}, COUNT(*) AS player_games,
       ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY risk_score)::numeric, 3) AS median,
       ROUND(100.0 * AVG((risk_score >= (SELECT high_risk_threshold FROM serving.settings))::int), 3)
           AS high_pct
FROM serving.live_scores WHERE run_id = :run AND risk_score IS NOT NULL
GROUP BY {col} ORDER BY {col}"""

TERM = """
SELECT ROUND(AVG(LEAST(5, GREATEST(0, -{z})))::numeric, 2) AS mean_term_all,
       ROUND((AVG(LEAST(5, GREATEST(0, -{z})))
              FILTER (WHERE risk_score >= (SELECT high_risk_threshold FROM serving.settings)))::numeric, 2)
           AS mean_term_high_risk
FROM serving.live_scores WHERE run_id = :run AND {z} IS NOT NULL"""

TIMELINE = """
WITH t AS (SELECT high_risk_threshold AS t FROM serving.settings),
     r AS (SELECT * FROM serving.running_scores WHERE run_id = :run),
     f AS (SELECT DISTINCT ON (game_key, color) game_key, color, score, ply
           FROM r ORDER BY game_key, color, ply DESC),
     c AS (SELECT game_key, color, MIN(ply) AS first_ply FROM r
           WHERE score >= (SELECT t FROM t) GROUP BY game_key, color)
SELECT (SELECT COUNT(DISTINCT game_key) FROM r)                                  AS games,
       (SELECT COUNT(*) FROM r)                                                  AS rows,
       COUNT(*)                                                                  AS player_timelines,
       COUNT(c.first_ply)                                                        AS ever_high,
       COUNT(*) FILTER (WHERE f.score >= (SELECT t FROM t))                      AS final_high,
       COUNT(*) FILTER (WHERE c.first_ply IS NOT NULL
                        AND COALESCE(f.score, 0) < (SELECT t FROM t))            AS high_then_dropped,
       ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY c.first_ply::float / f.ply)
              FILTER (WHERE f.score >= (SELECT t FROM t)))::numeric, 2)          AS median_first_high_share_of_game
FROM f LEFT JOIN c USING (game_key, color)"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/serving.yaml")
    parser.add_argument("--run-id", help="scorer run (default: the only run in serving.live_scores)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = yaml.safe_load(Path(args.config).read_text())
    engine = create_engine(os.environ.get("SERVING_DB_URL", cfg["postgres"]["url"]))

    with engine.connect() as conn:
        run = args.run_id
        if run is None:
            runs = [r[0] for r in conn.execute(text("SELECT DISTINCT run_id FROM serving.live_scores"))]
            if len(runs) != 1:
                raise SystemExit(f"{len(runs)} runs in serving.live_scores, pass --run-id")
            run = runs[0]
        read = lambda sql: pd.read_sql(text(sql), conn, params={"run": run})  # noqa: E731
        tables = {
            "dashboard_summary": read(SUMMARY),
            "dashboard_by_time_class": read(BY_GROUP.format(col="time_class")),
            "dashboard_by_band": read(BY_GROUP.format(col="band_id")),
            "dashboard_score_terms": pd.concat(
                [read(TERM.format(z=z)).assign(term=z) for z in TERMS]).set_index("term"),
            "dashboard_timeline": read(TIMELINE),
        }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(OUT_DIR / f"{name}.csv", index=name == "dashboard_score_terms")
        LOG.info("%s.csv: %d rows (run %s)", name, len(df), run)


if __name__ == "__main__":
    main()
