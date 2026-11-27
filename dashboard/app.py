"""Chess Cheat Detector dashboard (Part 2, workstream E2).

Reads only the PostgreSQL ``serving`` schema. Shows hashed player IDs and hashed game keys,
never usernames or Lichess game IDs. Every chart is aggregated in SQL, so only small result
sets leave the database and the full day (about 6M player-games) runs on a laptop.

Run:
    streamlit run dashboard/app.py                                  # config/serving.yaml
    streamlit run dashboard/app.py -- --config config/other.yaml    # or SERVING_CONFIG=...
SERVING_DB_URL overrides the database URL in the config.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[1]


def config_path() -> Path:
    """--config after "--" on the streamlit command line, else SERVING_CONFIG, else the default."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=os.environ.get("SERVING_CONFIG", "config/serving.yaml"))
    path = Path(parser.parse_known_args(sys.argv[1:])[0].config)
    return path if path.is_absolute() else ROOT / path


CFG = yaml.safe_load(config_path().read_text())
THRESHOLD = float(CFG["dashboard"]["high_risk_threshold"])
TOP_N = int(CFG["dashboard"]["top_n_games"])
ACCENT, NEUTRAL, LIGHT = "#B4232A", "#4A4A4A", "#9A9A9A"
TIME_CLASS_ORDER = ["ultrabullet", "bullet", "blitz", "rapid", "classical"]
FEATURES = {"share_moves_under_2s": "Share of moves under 2 s",
            "move_time_cv": "Move-time variation (CV)",
            "move_time_mean_s": "Mean move time (s)"}
HIST_BINS = 60

st.set_page_config(page_title="Chess Cheat Detector", layout="wide")


@st.cache_resource
def engine():
    return create_engine(os.environ.get("SERVING_DB_URL", CFG["postgres"]["url"]))


@st.cache_data(ttl=600, show_spinner="Querying the serving database…")
def query(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=600)
def serving_tables() -> set[str]:
    return set(inspect(engine()).get_table_names(schema="serving"))


def style(fig, height=360):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10),
                      plot_bgcolor="white", paper_bgcolor="white", font=dict(size=13))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#E5E5E5")
    return fig


# ---------------------------------------------------------------- sidebar filters
st.sidebar.title("Filters")
runs = query("SELECT DISTINCT run_id FROM serving.live_scores ORDER BY run_id")
if runs.empty:
    st.error("serving.live_scores is empty. Run: python -m src.serving.load_serving")
    st.stop()
run_id = st.sidebar.selectbox("Scorer run", runs["run_id"], index=len(runs) - 1)
classes = st.sidebar.multiselect("Time class", TIME_CLASS_ORDER, default=TIME_CLASS_ORDER)
bands = query("SELECT band_id, band_label FROM serving.rating_bands ORDER BY band_id")
band_order = list(bands["band_label"])
band_range = st.sidebar.select_slider("Rating band", options=band_order,
                                      value=(band_order[0], band_order[-1]))
lo = int(bands.loc[bands.band_label == band_range[0], "band_id"].iloc[0])
hi = int(bands.loc[bands.band_label == band_range[1], "band_id"].iloc[0])
min_conf = st.sidebar.slider("Minimum confidence", 0.0, 0.9, 0.0, 0.05,
                             help="confidence = timed moves / (timed moves + 20)")
st.sidebar.caption(f"High risk = risk score ≥ {THRESHOLD:g}. Scores rank games for review; "
                   "they are not a probability and not a verdict.")

FILTER = ("s.run_id = :run AND s.time_class = ANY(:classes) AND s.band_id BETWEEN :lo AND :hi "
          "AND s.confidence >= :conf")
P = dict(run=run_id, classes=classes or TIME_CLASS_ORDER, lo=lo, hi=hi, conf=min_conf, t=THRESHOLD)
FROM = "FROM serving.live_scores s JOIN serving.rating_bands b USING (band_id)"

st.title("Chess Cheat Detector")
meta = query("SELECT DISTINCT rule_version, baselines FROM serving.live_scores WHERE run_id = :run",
             run=run_id)
st.caption(f"Run **{run_id}** · scoring rule **{', '.join(meta.rule_version)}** · baselines "
           f"**{', '.join(meta.baselines)}** · hashed player IDs and game keys only")

tab_overview, tab_baselines, tab_games, tab_timeline, tab_players, tab_model = st.tabs(
    ["Overview", "Behaviour by band", "High-risk games", "Suspicion timeline", "Players",
     "Anomaly & model"])

# ---------------------------------------------------------------- overview
with tab_overview:
    k = query(f"""
        SELECT COUNT(DISTINCT s.game_key) AS games, COUNT(*) AS player_games,
               COUNT(DISTINCT s.player_id) AS players, COUNT(s.risk_score) AS scored,
               COUNT(*) FILTER (WHERE s.risk_score >= :t) AS high
        {FROM} WHERE {FILTER}""", **P).iloc[0]
    c = st.columns(5)
    c[0].metric("Games", f"{k.games:,}")
    c[1].metric("Player-games", f"{k.player_games:,}")
    c[2].metric("Players", f"{k.players:,}")
    c[3].metric("Scored", f"{k.scored / max(k.player_games, 1):.1%}",
                help="Not scored = no feature had enough timed moves")
    c[4].metric(f"High risk (≥ {THRESHOLD:g})", f"{k.high:,}")

    left, right = st.columns(2)
    hist = query(f"""
        SELECT LEAST(width_bucket(s.risk_score, 0, 5, {HIST_BINS}), {HIST_BINS}) AS bucket,
               COUNT(*) AS n
        {FROM} WHERE {FILTER} AND s.risk_score IS NOT NULL GROUP BY 1 ORDER BY 1""", **P)
    hist["risk_score"] = (hist.bucket - 0.5) * 5 / HIST_BINS
    fig = px.bar(hist, x="risk_score", y="n", color_discrete_sequence=[NEUTRAL],
                 labels={"risk_score": "Risk score (0–5)", "n": "Player-games"},
                 title="Distribution of risk scores")
    fig.update_traces(width=5 / HIST_BINS)
    fig.add_vline(x=THRESHOLD, line_dash="dash", line_color=ACCENT,
                  annotation_text="high risk", annotation_position="top right")
    left.plotly_chart(style(fig), use_container_width=True)

    by_class = query(f"""
        SELECT s.time_class, COUNT(*) AS player_games,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY s.risk_score) AS median,
               100.0 * AVG((s.risk_score >= :t)::int) AS high_share
        {FROM} WHERE {FILTER} AND s.risk_score IS NOT NULL GROUP BY s.time_class""", **P)
    by_class["order"] = by_class.time_class.map(
        {t: i for i, t in enumerate(TIME_CLASS_ORDER)}).fillna(99)
    by_class = by_class.sort_values("order").drop(columns="order")
    fig = px.bar(by_class, x="time_class", y="high_share", color_discrete_sequence=[ACCENT],
                 text=by_class.high_share.map(lambda v: f"{v:.2f}%"),
                 labels={"time_class": "", "high_share": "% of player-games high risk"},
                 title="Share of high-risk player-games by time class")
    right.plotly_chart(style(fig), use_container_width=True)
    st.dataframe(by_class.rename(columns={"time_class": "Time class", "player_games": "Player-games",
                                          "median": "Median score", "high_share": "High risk (%)"})
                 .round(3), hide_index=True, use_container_width=True,
                 column_config={"Player-games": st.column_config.NumberColumn(format="%d")})

# ---------------------------------------------------------------- behaviour by band
with tab_baselines:
    if "baselines" in serving_tables():
        st.subheader("Population baselines (workstream B)")
        st.dataframe(query("SELECT * FROM serving.baselines LIMIT 2000"), use_container_width=True)
    st.subheader("Observed timing behaviour by rating band")
    st.caption("Medians over the filtered scorer export. Normal play changes strongly with "
               "rating, which is why every score is computed against the player's own band.")
    feat = st.selectbox("Feature", list(FEATURES), format_func=FEATURES.get)  # whitelisted name
    band = query(f"""
        SELECT s.band_id, b.band_label, s.time_class,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY s.{feat}) AS value, COUNT(s.{feat}) AS n
        {FROM} WHERE {FILTER} GROUP BY 1, 2, 3 HAVING COUNT(s.{feat}) >= 30
        ORDER BY s.band_id""", **P)
    fig = px.line(band, x="band_label", y="value", color="time_class", markers=True,
                  category_orders={"time_class": TIME_CLASS_ORDER, "band_label": band_order},
                  labels={"band_label": "Rating band", "value": "Median", "time_class": ""},
                  title="Median per band (cells with at least 30 player-games)")
    st.plotly_chart(style(fig, 420), use_container_width=True)

    hb = query(f"""
        SELECT s.band_id, b.band_label, 100.0 * AVG((s.risk_score >= :t)::int) AS high,
               COUNT(*) AS n
        {FROM} WHERE {FILTER} AND s.risk_score IS NOT NULL GROUP BY 1, 2 ORDER BY 1""", **P)
    fig = px.bar(hb, x="band_label", y="high", color_discrete_sequence=[ACCENT],
                 category_orders={"band_label": band_order},
                 labels={"band_label": "Rating band", "high": "% high risk"},
                 title="Share of high-risk player-games by rating band")
    st.plotly_chart(style(fig), use_container_width=True)

# ---------------------------------------------------------------- high-risk games
with tab_games:
    # At most two rows per game, so the top 2N player-games always contain the top N games.
    top = query(f"""
        SELECT s.game_key, s.player_id, s.time_class, b.band_label, s.n_timed_moves,
               s.risk_score, s.confidence, s.z_move_time_cv, s.z_share_moves_under_2s,
               s.z_move_time_cv_low_clock
        {FROM} WHERE {FILTER} AND s.risk_score IS NOT NULL
        ORDER BY s.risk_score DESC LIMIT :n""", **P, n=2 * TOP_N)
    games = top.drop_duplicates("game_key").head(TOP_N).reset_index(drop=True)
    games.insert(0, "Rank", games.index + 1)
    st.subheader(f"Top {len(games)} games by risk score")
    st.caption("Each game is shown by a hashed key, never its Lichess ID. z-scores compare the "
               "player with the baseline of their band and time class; negative values are the "
               "suspicious direction.")
    st.dataframe(games.rename(columns={
        "game_key": "Game key", "player_id": "Player (hashed)", "time_class": "Time class",
        "band_label": "Band", "n_timed_moves": "Timed moves", "risk_score": "Risk score",
        "confidence": "Confidence", "z_move_time_cv": "z: timing variation",
        "z_share_moves_under_2s": "z: fast moves", "z_move_time_cv_low_clock": "z: variation, low clock"})
        .round(2), hide_index=True, use_container_width=True, height=420)

    if not games.empty:
        pick = st.selectbox("Inspect a game", games["game_key"])
        g = query("""SELECT s.*, b.band_label FROM serving.live_scores s
                     JOIN serving.rating_bands b USING (band_id)
                     WHERE s.run_id = :run AND s.game_key = :g ORDER BY s.color DESC""",
                  run=run_id, g=pick)
        for _, row in g.iterrows():
            side = "White" if row.color == "w" else "Black"
            if pd.isna(row.risk_score):
                st.markdown(f"**{side}** · player `{row.player_id}` · not scored")
                continue
            st.markdown(f"**{side}** · player `{row.player_id}` · band {row.band_label} · "
                        f"{row.n_timed_moves} timed moves · risk score **{row.risk_score:.2f}** "
                        f"(confidence {row.confidence:.2f})")
            terms = pd.DataFrame({
                "feature": ["Timing variation", "Fast moves", "Variation under low clock"],
                "term": [min(5, max(0, -v)) if pd.notna(v) else None for v in
                         (row.z_move_time_cv, row.z_share_moves_under_2s, row.z_move_time_cv_low_clock)]})
            fig = px.bar(terms.dropna(), x="term", y="feature", orientation="h", range_x=[0, 5],
                         color_discrete_sequence=[ACCENT], labels={"term": "Score term (0–5)", "feature": ""})
            st.plotly_chart(style(fig, 180), use_container_width=True)

# ---------------------------------------------------------------- suspicion timeline
with tab_timeline:
    st.subheader("Move-by-move suspicion timeline")
    st.caption("How the scorer's running risk score develops while the game is played. Early "
               "values rest on few timed moves (low confidence); the score settles as evidence "
               "builds up. Available for the sample of games the scorer logged move by move.")
    if "running_scores" not in serving_tables():
        st.info("No running scores loaded yet (running_scores in config/serving.yaml).")
    else:
        truns = query("SELECT DISTINCT run_id FROM serving.running_scores ORDER BY run_id")
        if truns.empty:
            st.info("serving.running_scores is empty.")
        else:
            opts = list(truns.run_id)
            trun = st.selectbox("Run", opts,
                                index=opts.index(run_id) if run_id in opts else len(opts) - 1)
            # Logged games ordered by final score, so the demo opens on the clearest case.
            glist = query("""
                SELECT r.game_key, MAX(r.ply) AS plies, MAX(l.risk_score) AS final_score,
                       MIN(l.time_class) AS time_class
                FROM serving.running_scores r
                LEFT JOIN serving.live_scores l ON l.run_id = r.run_id AND l.game_key = r.game_key
                WHERE r.run_id = :run GROUP BY r.game_key
                ORDER BY MAX(l.risk_score) DESC NULLS LAST, r.game_key""", run=trun)
            label = {r.game_key: f"{r.game_key} · {r.time_class or '?'} · final "
                     f"{'–' if pd.isna(r.final_score) else f'{r.final_score:.2f}'} · {r.plies} plies"
                     for r in glist.itertuples()}
            c1, c2 = st.columns([3, 1])
            pick = c1.selectbox(f"Game ({len(glist):,} logged, highest final score first)",
                                glist.game_key, format_func=label.get)
            typed = c2.text_input("…or paste a game key", max_chars=16).strip().lower()
            if typed:
                pick = typed
            ts = query("""SELECT ply, color, score FROM serving.running_scores
                          WHERE run_id = :run AND game_key = :g ORDER BY ply""", run=trun, g=pick)
            if ts.empty:
                st.warning("No running scores for that game key in this run.")
            else:
                ts["side"] = ts.color.map({"w": "White", "b": "Black"}).fillna("Game")
                fig = px.line(ts, x="ply", y="score", color="side", markers=len(ts) <= 120,
                              color_discrete_map={"White": NEUTRAL, "Black": ACCENT, "Game": ACCENT},
                              labels={"ply": "Ply (half-move)", "score": "Running risk score (0–5)",
                                      "side": ""},
                              title=f"Running risk score · game {pick}")
                fig.add_hline(y=THRESHOLD, line_dash="dash", line_color=LIGHT,
                              annotation_text="high risk", annotation_position="top left")
                fig.update_yaxes(range=[0, 5 if ts.score.max() > 2.5 else 2.5])
                st.plotly_chart(style(fig, 420), use_container_width=True)
                first = ts[ts.score >= THRESHOLD].groupby("side").ply.min()
                final = ts.groupby("side").score.last()
                for col, (side, v) in zip(st.columns(len(final)), final.items()):
                    col.metric(f"{side}: final running score", f"{v:.2f}")
                    col.caption(f"First reached {THRESHOLD:g} at ply {first[side]}"
                                if side in first else f"Never reached {THRESHOLD:g}")

# ---------------------------------------------------------------- players
with tab_players:
    st.subheader("Players flagged in more than one game")
    st.caption("A single high score can be chance; repeated high scores across games are a "
               "stronger reason for review.")
    per_player = f"""
        SELECT s.player_id, COUNT(*) AS games,
               COUNT(*) FILTER (WHERE s.risk_score >= :t) AS high_games,
               AVG(s.risk_score) AS mean_score, MAX(s.risk_score) AS max_score
        {FROM} WHERE {FILTER} AND s.risk_score IS NOT NULL GROUP BY s.player_id"""
    pk = query(f"""SELECT COUNT(*) FILTER (WHERE high_games >= 1) AS h1,
                          COUNT(*) FILTER (WHERE high_games >= 2) AS h2,
                          COUNT(*) FILTER (WHERE games >= 5) AS g5
                   FROM ({per_player}) p""", **P).iloc[0]
    c = st.columns(3)
    c[0].metric("Players with ≥ 1 high-risk game", f"{pk.h1:,}")
    c[1].metric("Players with ≥ 2 high-risk games", f"{pk.h2:,}")
    c[2].metric("Players with ≥ 5 games", f"{pk.g5:,}")
    pl = query(f"""SELECT * FROM ({per_player}) p WHERE high_games >= 2
                   ORDER BY high_games DESC, mean_score DESC LIMIT 500""", **P)
    st.dataframe(pl.rename(columns={"player_id": "Player (hashed)", "games": "Games",
                                    "high_games": "High-risk games", "mean_score": "Mean score",
                                    "max_score": "Max score"}).round(2),
                 hide_index=True, use_container_width=True)
    if len(pl) == 500:
        st.caption("Showing the first 500.")

# ---------------------------------------------------------------- anomaly & model
with tab_model:
    tables = serving_tables()
    if "player_anomaly" in tables:
        st.subheader("Unlabelled anomaly scores (workstream B)")
        st.dataframe(query("SELECT * FROM serving.player_anomaly LIMIT 500"), use_container_width=True)
    if "labels_summary" in tables:
        st.subheader("Labels at scale (workstream C)")
        st.dataframe(query("SELECT * FROM serving.labels_summary"), use_container_width=True)
    if "model_results" in tables:
        st.subheader("Model evaluation (workstream D)")
        st.dataframe(query("SELECT * FROM serving.model_results"), use_container_width=True)
    if not tables & {"player_anomaly", "labels_summary", "model_results"}:
        st.info("Anomaly scores, labels and model results appear here once their tables are "
                "loaded into the serving schema (python -m src.serving.load_serving).")
