"""Chess Cheat Detector dashboard (Part 2, workstream E2).

Reads only the PostgreSQL ``serving`` schema. Shows hashed player IDs and anonymous game keys,
never usernames or Lichess game IDs.

Run:
    streamlit run dashboard/app.py
(set SERVING_DB_URL, or edit config/serving.yaml)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "serving.yaml").read_text())
THRESHOLD = float(CFG["dashboard"]["high_risk_threshold"])
TOP_N = int(CFG["dashboard"]["top_n_games"])
ACCENT, NEUTRAL = "#B4232A", "#4A4A4A"
TIME_CLASS_ORDER = ["ultrabullet", "bullet", "blitz", "rapid", "classical"]

st.set_page_config(page_title="Chess Cheat Detector", layout="wide")


@st.cache_resource
def engine():
    return create_engine(os.environ.get("SERVING_DB_URL", CFG["postgres"]["url"]))


@st.cache_data(ttl=600)
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
runs = query("SELECT run_id, COUNT(*) AS n FROM serving.live_scores GROUP BY run_id ORDER BY run_id")
if runs.empty:
    st.error("serving.live_scores is empty. Run: python -m src.serving.load_serving")
    st.stop()
run_id = st.sidebar.selectbox("Scorer run", runs["run_id"])
classes = st.sidebar.multiselect("Time class", TIME_CLASS_ORDER, default=TIME_CLASS_ORDER)
bands = query("SELECT band_id, band_label FROM serving.rating_bands ORDER BY band_id")
band_range = st.sidebar.select_slider("Rating band", options=list(bands["band_label"]),
                                      value=(bands["band_label"].iloc[0], bands["band_label"].iloc[-1]))
lo = int(bands.loc[bands.band_label == band_range[0], "band_id"].iloc[0])
hi = int(bands.loc[bands.band_label == band_range[1], "band_id"].iloc[0])
min_conf = st.sidebar.slider("Minimum confidence", 0.0, 0.9, 0.0, 0.05,
                             help="confidence = timed moves / (timed moves + 20)")
st.sidebar.caption(f"High risk = risk score ≥ {THRESHOLD:g}. Scores rank games for review; "
                   "they are not a probability and not a verdict.")

FILTER = ("s.run_id = :run AND s.time_class = ANY(:classes) AND s.band_id BETWEEN :lo AND :hi "
          "AND s.confidence >= :conf")
P = dict(run=run_id, classes=classes or TIME_CLASS_ORDER, lo=lo, hi=hi, conf=min_conf)

scores = query(f"""
    SELECT s.game_key, s.player_id, s.color, s.time_class, s.band_id, b.band_label, s.elo,
           s.n_timed_moves, s.move_time_mean_s, s.move_time_cv, s.share_moves_under_2s,
           s.move_time_cv_low_clock, s.z_move_time_cv, s.z_share_moves_under_2s,
           s.z_move_time_cv_low_clock, s.n_score_features, s.confidence, s.risk_score,
           s.rule_version, s.baselines
    FROM serving.live_scores s JOIN serving.rating_bands b USING (band_id)
    WHERE {FILTER}""", **P)

st.title("Chess Cheat Detector")
meta = scores[["rule_version", "baselines"]].drop_duplicates()
st.caption(f"Run **{run_id}** · scoring rule **{', '.join(meta.rule_version)}** · baselines "
           f"**{', '.join(meta.baselines)}** · hashed player IDs and anonymous game keys only")

tab_overview, tab_baselines, tab_games, tab_players, tab_model = st.tabs(
    ["Overview", "Behaviour by band", "High-risk games", "Players", "Anomaly & model"])

# ---------------------------------------------------------------- overview
with tab_overview:
    scored = scores.dropna(subset=["risk_score"])
    c = st.columns(5)
    c[0].metric("Games", f"{scores.game_key.nunique():,}")
    c[1].metric("Player-games", f"{len(scores):,}")
    c[2].metric("Players", f"{scores.player_id.nunique():,}")
    c[3].metric("Scored", f"{len(scored) / max(len(scores), 1):.1%}",
                help="Not scored = no feature had enough timed moves")
    c[4].metric(f"High risk (≥ {THRESHOLD:g})", f"{(scored.risk_score >= THRESHOLD).sum():,}")

    left, right = st.columns(2)
    fig = px.histogram(scored, x="risk_score", nbins=60, color_discrete_sequence=[NEUTRAL],
                       labels={"risk_score": "Risk score (0–5)"}, title="Distribution of risk scores")
    fig.add_vline(x=THRESHOLD, line_dash="dash", line_color=ACCENT,
                  annotation_text="high risk", annotation_position="top right")
    left.plotly_chart(style(fig), use_container_width=True)

    by_class = (scored.groupby("time_class")
                .agg(player_games=("risk_score", "size"),
                     median=("risk_score", "median"),
                     high_share=("risk_score", lambda x: (x >= THRESHOLD).mean() * 100))
                .reindex([t for t in TIME_CLASS_ORDER if t in scored.time_class.unique()])
                .reset_index())
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
        st.dataframe(query("SELECT * FROM serving.baselines"), use_container_width=True)
    st.subheader("Observed timing behaviour by rating band")
    st.caption("Medians over the filtered scorer export. Normal play changes strongly with "
               "rating, which is why every score is computed against the player's own band.")
    feat = st.selectbox("Feature", {
        "share_moves_under_2s": "Share of moves under 2 s",
        "move_time_cv": "Move-time variation (CV)",
        "move_time_mean_s": "Mean move time (s)",
    }.keys(), format_func=lambda k: {"share_moves_under_2s": "Share of moves under 2 s",
                                     "move_time_cv": "Move-time variation (CV)",
                                     "move_time_mean_s": "Mean move time (s)"}[k])
    band = (scores.groupby(["band_id", "band_label", "time_class"])
            .agg(value=(feat, "median"), n=(feat, "size")).reset_index())
    band = band[band.n >= 30].sort_values("band_id")
    fig = px.line(band, x="band_label", y="value", color="time_class", markers=True,
                  category_orders={"time_class": TIME_CLASS_ORDER,
                                   "band_label": list(bands["band_label"])},
                  labels={"band_label": "Rating band", "value": "Median", "time_class": ""},
                  title="Median per band (cells with at least 30 player-games)")
    st.plotly_chart(style(fig, 420), use_container_width=True)

    hb = (scores.dropna(subset=["risk_score"]).groupby(["band_id", "band_label"])
          .agg(high=("risk_score", lambda x: (x >= THRESHOLD).mean() * 100), n=("risk_score", "size"))
          .reset_index().sort_values("band_id"))
    fig = px.bar(hb, x="band_label", y="high", color_discrete_sequence=[ACCENT],
                 category_orders={"band_label": list(bands["band_label"])},
                 labels={"band_label": "Rating band", "high": "% high risk"},
                 title="Share of high-risk player-games by rating band")
    st.plotly_chart(style(fig), use_container_width=True)

# ---------------------------------------------------------------- high-risk games
with tab_games:
    games = (scores.dropna(subset=["risk_score"])
             .sort_values("risk_score", ascending=False)
             .drop_duplicates("game_key").head(TOP_N).reset_index(drop=True))
    games.insert(0, "Rank", games.index + 1)
    st.subheader(f"Top {len(games)} games by risk score")
    st.caption("Each game is shown by an anonymous key (a keyed hash), never its Lichess ID. "
               "z-scores compare the player with the baseline of their band and time class; "
               "negative values are the suspicious direction.")
    show = games[["Rank", "game_key", "player_id", "time_class", "band_label", "n_timed_moves",
                  "risk_score", "confidence", "z_move_time_cv", "z_share_moves_under_2s",
                  "z_move_time_cv_low_clock"]].rename(columns={
        "game_key": "Game key", "player_id": "Player (hashed)", "time_class": "Time class",
        "band_label": "Band", "n_timed_moves": "Timed moves", "risk_score": "Risk score",
        "confidence": "Confidence", "z_move_time_cv": "z: timing variation",
        "z_share_moves_under_2s": "z: fast moves", "z_move_time_cv_low_clock": "z: variation, low clock"})
    st.dataframe(show.round(2), hide_index=True, use_container_width=True, height=420)

    if not games.empty:
        pick = st.selectbox("Inspect a game", games["game_key"])
        g = scores[scores.game_key == pick]
        for _, row in g.iterrows():
            st.markdown(f"**{'White' if row.color == 'w' else 'Black'}** · player `{row.player_id}` · "
                        f"band {row.band_label} · {row.n_timed_moves} timed moves · "
                        f"risk score **{row.risk_score:.2f}** (confidence {row.confidence:.2f})"
                        if pd.notna(row.risk_score) else
                        f"**{'White' if row.color == 'w' else 'Black'}** · player `{row.player_id}` · not scored")
            terms = pd.DataFrame({
                "feature": ["Timing variation", "Fast moves", "Variation under low clock"],
                "term": [min(5, max(0, -v)) if pd.notna(v) else None for v in
                         (row.z_move_time_cv, row.z_share_moves_under_2s, row.z_move_time_cv_low_clock)]})
            fig = px.bar(terms.dropna(), x="term", y="feature", orientation="h", range_x=[0, 5],
                         color_discrete_sequence=[ACCENT], labels={"term": "Score term (0–5)", "feature": ""})
            st.plotly_chart(style(fig, 180), use_container_width=True)

# ---------------------------------------------------------------- players
with tab_players:
    st.subheader("Players flagged in more than one game")
    st.caption("A single high score can be chance; repeated high scores across games are a "
               "stronger reason for review.")
    pl = (scores.dropna(subset=["risk_score"]).groupby("player_id")
          .agg(games=("risk_score", "size"),
               high_games=("risk_score", lambda x: (x >= THRESHOLD).sum()),
               mean_score=("risk_score", "mean"), max_score=("risk_score", "max"))
          .reset_index())
    c = st.columns(3)
    c[0].metric("Players with ≥ 1 high-risk game", f"{(pl.high_games >= 1).sum():,}")
    c[1].metric("Players with ≥ 2 high-risk games", f"{(pl.high_games >= 2).sum():,}")
    c[2].metric("Players with ≥ 5 games", f"{(pl.games >= 5).sum():,}")
    st.dataframe(pl[pl.high_games >= 2].sort_values(["high_games", "mean_score"], ascending=False)
                 .rename(columns={"player_id": "Player (hashed)", "games": "Games",
                                  "high_games": "High-risk games", "mean_score": "Mean score",
                                  "max_score": "Max score"}).round(2),
                 hide_index=True, use_container_width=True)

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
