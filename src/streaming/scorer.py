"""Streaming scorer (Part 2, E1): a running cheating-risk score per player per game.

Reads topic lichess.moves (data dictionary §8) and keeps, for each player of each open game, running
timing statistics: number of moves, move-time mean and coefficient of variation (cv), share of fast
moves, and behaviour when the clock is low. Each player is mapped to a rating band from the message's
`elo` (ref.rating_bands) and compared with the baseline of that band x `time_class`
(paths.scorer_baselines_dir: rows with level "game" of lake/baselines, L10; or of the provisional
lake/baselines_provisional, L12, which has the same layout; correspondence cells are ignored).
Every --emit-every moves of a player, and for both players at the end of the game, it sends a score
message to topic lichess.scores (data dictionary §9). Final scores also go to a small Parquet table for
the dashboard, paths.live_scores_dir/run_id=<run>/part-<name>.parquet (L13; hashed player_id only).

Run inside a Slurm job with a broker (cluster/kafka_job.sh exports KAFKA_BOOTSTRAP):
    python src/streaming/scorer.py --config config/cluster.yaml --run-id test
Several scorers share the work as one consumer group (--group): key = game_id keeps all the moves of a
game on one partition, so each game is scored by exactly one member. src/streaming/scorer_group.py
starts N members and records the group's latency and throughput.

End of a game: the move with is_last = true. Fallbacks: a game with no move for --game-timeout seconds
(end_reason "idle"), and every game still open when the scorer stops ("shutdown").

Offsets and rebalances (group members; fix for job 32798, where members never committed and a member
that received the partitions of the others re-read the whole topic from offset 0):
  - After every processed batch the member commits, per partition, its position (offset after the last
    processed message), so whoever gets a partition next continues from there and nothing is read twice.
  - The scorer keeps state (the running statistics of open games), so moving a partition also moves its
    state. When partitions are taken away (rebalance, or this member leaves early: SIGUSR1 or
    --leave-after), the member writes a snapshot of every open game of those partitions to topic
    lichess.scorer_state (same partition number, key = game_id; data dictionary §9), then commits the
    position with metadata "state:<first>:<end>" = the snapshots' offset range. The next owner reads that
    range when it gets the partition, restores the games and continues them from the next move: each game
    gets exactly one final score, identical to the one without a rebalance (the rule is deterministic and
    the restored floats are exact), and no move is read twice.
  - SIGTERM / SIGINT (the group stops all members together, or the time limit) stop with "final": open games
    get their final score ("shutdown"), the positions are committed and the Parquet part is written.
  - A partition lost without a handoff (session timeout) loses its open games' state; the next owner then
    sees games from the middle (counted as games_joined_late) and the member exits with an error.
Partitions are assigned with cooperative-sticky, so a member leaving moves only its own partitions.

Scoring rule (RULE_VERSION; documented in data dictionary §9; workstream D's model can replace it):
  1. Running statistics of the player in this game, with the definitions of L8 (only moves with
     move_time >= 0): move_time_cv, share_moves_under_2s, and move_time_cv_low_clock (low clock =
     clock after the move < 10% of base_seconds).
  2. Robust z-score of each against the band x time class baseline, as in L11:
     z = (x - p50) / ((p75 - p25) / 1.349), using sd when the IQR is 0. No z when the baseline cell has
     fewer than MIN_CELL_GAMES games, or the player has fewer than MIN_TIMED timed moves
     (MIN_LOW_CLOCK timed low-clock moves for move_time_cv_low_clock).
  3. Only the suspicious direction counts (low values for all three: very even timing, even under
     time pressure, and few instant moves): term = min(5, max(0, -z)).
  4. raw = mean of the available terms (0-5); confidence = n_timed / (n_timed + CONFIDENCE_K);
     risk_score = raw x confidence (0-5). Running values of a partial game are compared with
     full-game baselines, so scores built on few moves are discounted.
The score is a heuristic ranking ("high risk"), not a probability and not a label.

Latency: for every score triggered by a move (periodic and end-of-game), emit time minus the move
message's `timestamp` (producer send time; same node, same clock). Throughput: moves consumed per
second between the first and the last message.
"""

import json
import logging
import math
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition

from src.common.config import build_arg_parser, load_config, parquet_codec
from src.common.logging_setup import setup_logging
from src.streaming.common import TOPIC_SCORER_STATE, TOPIC_SCORES, default_bootstrap, default_topic
from src.streaming.count_consumer import latency_stats

STEP = "scorer"
RULE_VERSION = "timing-z-v1"
BASELINE_LEVEL = "game"
FAST_MOVE_S = 2.0        # "very fast" move, as share_moves_under_2s in L8
LOW_CLOCK_SHARE = 0.10   # low clock: clock after the move < 10% of base_seconds, as in L8
MIN_CELL_GAMES = 30      # baseline cells with fewer games give no z
MIN_TIMED = 10           # timed moves before move_time_cv / share_moves_under_2s are scored
MIN_LOW_CLOCK = 5        # timed low-clock moves before move_time_cv_low_clock is scored
CONFIDENCE_K = 20        # confidence = n_timed / (n_timed + K): 0.33 at 10 moves, 0.67 at 40
Z_CAP = 5.0
IQR_TO_SD = 1.349
SCORED = ("move_time_cv", "share_moves_under_2s", "move_time_cv_low_clock")  # low = suspicious
EXCLUDED_TIME_CLASSES = ("correspondence",)  # not scored: outside the baseline population (L12, §9)
BASELINE_COLUMNS = ["level", "band_id", "time_class", "feature", "n", "sd", "p25", "p50", "p75"]
BATCH = 10_000
SWEEP_EVERY_S = 5.0
DEFAULT_EMIT_EVERY = 10

log = logging.getLogger(STEP)

# Value of a lichess.scores message, in this order (data dictionary §9)
SCORE_FIELDS = (
    "game_id", "player_id", "color", "time_class", "elo", "band_id", "ply", "n_moves", "n_timed_moves",
    "move_time_mean_s", "move_time_cv", "share_moves_under_2s", "share_low_clock", "n_low_clock_moves",
    "move_time_cv_low_clock", "z_move_time_cv", "z_share_moves_under_2s", "z_move_time_cv_low_clock",
    "n_score_features", "confidence", "risk_score", "is_final", "end_reason", "rule_version",
    "move_timestamp", "timestamp",
)
SCORE_VALUES = SCORE_FIELDS[SCORE_FIELDS.index("n_moves"):SCORE_FIELDS.index("is_final")]  # from score_player

# Final per-game scores (Parquet, L13); run_id is the folder name (hive partition), not a column
FINAL_SCHEMA = pa.schema([
    ("game_id", pa.string()), ("player_id", pa.string()), ("color", pa.string()),
    ("time_class", pa.string()), ("elo", pa.int16()), ("band_id", pa.int16()), ("n_plies", pa.int16()),
    ("n_moves", pa.int32()), ("n_timed_moves", pa.int32()),
    ("move_time_mean_s", pa.float32()), ("move_time_cv", pa.float32()),
    ("share_moves_under_2s", pa.float32()), ("share_low_clock", pa.float32()),
    ("n_low_clock_moves", pa.int32()), ("move_time_cv_low_clock", pa.float32()),
    ("z_move_time_cv", pa.float32()), ("z_share_moves_under_2s", pa.float32()),
    ("z_move_time_cv_low_clock", pa.float32()), ("n_score_features", pa.int8()),
    ("confidence", pa.float32()), ("risk_score", pa.float32()), ("end_reason", pa.string()),
    ("rule_version", pa.string()), ("baselines", pa.string()), ("scored_at", pa.timestamp("ms", tz="UTC")),
])

Baselines = dict[tuple[int, str, str], tuple[float, float]]  # (band_id, time_class, feature) -> (p50, scale)
CommitPoint = tuple[int, int]  # (partition, position = offset after the last processed message)
STATE_PREFIX = "state:"        # commit metadata of a handoff: "state:<first offset>:<end offset>" in lichess.scorer_state


def state_metadata(first: int, end: int) -> str:
    return f"{STATE_PREFIX}{first}:{end}"


def parse_state_metadata(metadata: str | None) -> tuple[int, int] | None:
    """(first, end) offsets of the handoff snapshots in lichess.scorer_state, or None."""
    if not metadata or not metadata.startswith(STATE_PREFIX):
        return None
    first, end = metadata[len(STATE_PREFIX):].split(":")
    return int(first), int(end)


def band_of(elo: int | None) -> int | None:
    """Rating band of ref.rating_bands: 1 = <800, 2..11 = 200-point bands from 800, 12 = 2800+."""
    if elo is None:
        return None
    if elo < 800:
        return 1
    if elo >= 2800:
        return 12
    return (elo - 800) // 200 + 2


def load_baselines(path: str) -> Baselines:
    """Center (p50) and robust scale per (band, time class, scored feature) from a lake/baselines-style table.

    Reads L10 (lake/baselines, workstream B) or L12 (lake/baselines_provisional): same columns and
    level-"game" rows (mapping in data dictionary §9). L10 also has correspondence cells (L8 keeps those
    games); they are dropped so the population matches L12 and the replay producer: no BOT games, no
    0-move games (both tables), no correspondence.
    """
    df = pd.read_parquet(path, columns=BASELINE_COLUMNS)
    df = df[(df["level"] == BASELINE_LEVEL) & df["feature"].isin(SCORED)
            & ~df["time_class"].isin(EXCLUDED_TIME_CLASSES) & (df["n"] >= MIN_CELL_GAMES)]
    out: Baselines = {}
    for r in df.itertuples(index=False):
        iqr_scale = (r.p75 - r.p25) / IQR_TO_SD if pd.notna(r.p75) and pd.notna(r.p25) else 0.0
        scale = iqr_scale if iqr_scale > 0 else (r.sd if pd.notna(r.sd) and r.sd > 0 else None)
        if scale is not None and pd.notna(r.p50):
            out[(int(r.band_id), r.time_class, r.feature)] = (float(r.p50), float(scale))
    return out


class Running:
    """Welford running mean and variance (sample sd, as stddev_samp)."""
    __slots__ = ("n", "mean", "m2")

    def __init__(self) -> None:
        self.n, self.mean, self.m2 = 0, 0.0, 0.0

    def add(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    def cv(self) -> float | None:
        """sd / mean; None with fewer than 2 values or a mean of 0 (as L8)."""
        if self.n < 2 or self.mean <= 0:
            return None
        return math.sqrt(max(self.m2, 0.0) / (self.n - 1)) / self.mean

    def state(self) -> list:
        return [self.n, self.mean, self.m2]

    @classmethod
    def from_state(cls, st: list) -> "Running":
        r = cls()
        r.n, r.mean, r.m2 = int(st[0]), float(st[1]), float(st[2])
        return r


class PlayerState:
    """Running statistics of one player in one game."""
    __slots__ = ("player_id", "color", "elo", "band_id", "n_moves", "n_fast", "n_clock", "n_low", "mt", "mt_low")

    def __init__(self, player_id: str, color: str, elo: int | None) -> None:
        self.player_id, self.color, self.elo, self.band_id = player_id, color, elo, band_of(elo)
        self.n_moves = self.n_fast = self.n_clock = self.n_low = 0
        self.mt, self.mt_low = Running(), Running()

    def update(self, move_time: float | None, clock: float | None, base: int | None) -> None:
        self.n_moves += 1
        timed = move_time is not None and move_time >= 0
        if timed:
            self.mt.add(move_time)
            self.n_fast += move_time < FAST_MOVE_S
        if clock is not None and base is not None:  # L8: "low" is null without a clock or base
            self.n_clock += 1
            if clock < LOW_CLOCK_SHARE * base:
                self.n_low += 1
                if timed:
                    self.mt_low.add(move_time)

    def features(self) -> dict[str, Any]:
        return {
            "n_moves": self.n_moves,
            "n_timed_moves": self.mt.n,
            "move_time_mean_s": self.mt.mean if self.mt.n else None,
            "move_time_cv": self.mt.cv(),
            "share_moves_under_2s": self.n_fast / self.mt.n if self.mt.n else None,
            "share_low_clock": self.n_low / self.n_clock if self.n_clock else None,
            "n_low_clock_moves": self.mt_low.n,
            "move_time_cv_low_clock": self.mt_low.cv(),
        }

    def state(self) -> dict[str, Any]:
        """Snapshot for a handoff (JSON; floats round-trip exactly)."""
        return {"player_id": self.player_id, "color": self.color, "elo": self.elo, "n_moves": self.n_moves,
                "n_fast": self.n_fast, "n_clock": self.n_clock, "n_low": self.n_low,
                "mt": self.mt.state(), "mt_low": self.mt_low.state()}

    @classmethod
    def from_state(cls, st: dict[str, Any]) -> "PlayerState":
        p = cls(st["player_id"], st["color"], st["elo"])
        p.n_moves, p.n_fast, p.n_clock, p.n_low = st["n_moves"], st["n_fast"], st["n_clock"], st["n_low"]
        p.mt, p.mt_low = Running.from_state(st["mt"]), Running.from_state(st["mt_low"])
        return p


class GameState:
    __slots__ = ("time_class", "partition", "players", "last_ply", "last_ts", "last_seen")

    def __init__(self, time_class: str, partition: int) -> None:
        self.time_class, self.partition = time_class, partition
        self.players: dict[str, PlayerState] = {}
        self.last_ply = 0
        self.last_ts = 0
        self.last_seen = 0.0


def score_player(p: PlayerState, time_class: str, baselines: Baselines) -> dict[str, Any]:
    """Features, z-scores and risk score of one player (the scoring rule in the module docstring)."""
    f = p.features()
    enough = {"move_time_cv": p.mt.n >= MIN_TIMED, "share_moves_under_2s": p.mt.n >= MIN_TIMED,
              "move_time_cv_low_clock": p.mt_low.n >= MIN_LOW_CLOCK}
    terms = []
    for feat in SCORED:
        cell = baselines.get((p.band_id, time_class, feat)) if p.band_id is not None else None
        x = f[feat]
        z = (x - cell[0]) / cell[1] if cell is not None and x is not None and enough[feat] else None
        f[f"z_{feat}"] = z
        if z is not None:
            terms.append(min(Z_CAP, max(0.0, -z)))
    confidence = p.mt.n / (p.mt.n + CONFIDENCE_K)
    f["n_score_features"] = len(terms)
    f["confidence"] = confidence
    f["risk_score"] = sum(terms) / len(terms) * confidence if terms else None
    return f


def _r(v: Any) -> Any:
    """Round floats for the JSON message."""
    return round(v, 4) if isinstance(v, float) else v


class Scorer:
    """Game states, score emission, the final rows and the commit points. One instance per consumer process.

    Kafka-free (apart from the score producer), so the handoff can be tested without a broker: the consumer
    callbacks call assign() / release(), the poll loop calls on_move() and due_commits().
    """

    def __init__(self, baselines: Baselines, baselines_name: str, emit_every: int,
                 producer: Producer, scores_topic: str) -> None:
        self.baselines, self.baselines_name = baselines, baselines_name
        self.emit_every, self.producer, self.scores_topic = emit_every, producer, scores_topic
        self.games: dict[str, GameState] = {}
        self.finished: dict[str, float] = {}  # game_id -> monotonic time it ended (late messages are dropped)
        self.final_rows: list[dict[str, Any]] = []
        self.latency_ms: list[int] = []       # emit time - triggering move's timestamp
        self.ack_latency_ms: list[int] = []   # broker ack of the score - triggering move's timestamp
        self.counts: Counter = Counter()
        self.owned: set[int] = set()          # partitions assigned to this member
        self.pos: dict[int, int] = {}         # partition -> offset after the last processed message
        self.open_by_part: dict[int, set[str]] = {}  # partition -> its open games
        self.committed: dict[int, int] = {}   # partition -> position last sent in a commit
        self.dirty: set[int] = set()          # partitions whose position changed since the last commit

    def assign(self, partition: int, start: int | None = None, snapshots: list[dict[str, Any]] | None = None,
               now: float = 0.0) -> None:
        """Start owning a partition: start = committed position (None if nothing committed yet),
        snapshots = the open games the previous owner handed over (restored as they were)."""
        self.owned.add(partition)
        self.dirty.discard(partition)
        if start is None:
            self.pos.pop(partition, None)
            self.committed.pop(partition, None)
        else:
            self.pos[partition] = self.committed[partition] = start
        for st in snapshots or []:
            gid = st["game_id"]
            if gid in self.games:
                self.counts["snapshot_conflicts"] += 1
                continue
            g = self.games[gid] = GameState(st["time_class"], partition)
            g.last_ply, g.last_ts, g.last_seen = st["last_ply"], st["last_ts"], now
            g.players = {ps["color"]: PlayerState.from_state(ps) for ps in st["players"]}
            self.open_by_part.setdefault(partition, set()).add(gid)
            self.counts["games_restored"] += 1

    def snapshot(self, gid: str) -> dict[str, Any]:
        g = self.games[gid]
        return {"game_id": gid, "time_class": g.time_class, "last_ply": g.last_ply, "last_ts": g.last_ts,
                "players": [p.state() for p in g.players.values()]}

    def on_move(self, v: dict[str, Any], partition: int, offset: int, now: float) -> None:
        """Update the game with one lichess.moves value (at partition/offset); emit scores when due."""
        self.pos[partition] = offset + 1
        self.dirty.add(partition)
        gid = v["game_id"]
        if gid in self.finished:
            self.counts["late_messages"] += 1
            return
        g = self.games.get(gid)
        if g is None:
            g = self.games[gid] = GameState(v["time_class"], partition)
            self.open_by_part.setdefault(partition, set()).add(gid)
            self.counts["games"] += 1
            if v["ply"] > 1:  # first seen mid-game: its earlier moves were never scored here (no snapshot)
                self.counts["games_joined_late"] += 1
        ply = v["ply"]
        if ply <= g.last_ply:  # redelivered or out of order: already counted
            self.counts["duplicates"] += 1
            return
        g.last_ply, g.last_ts, g.last_seen = ply, v["timestamp"], now
        color = v["color"]
        p = g.players.get(color)
        if p is None:
            p = g.players[color] = PlayerState(v["player_id"], color, v["elo"])
        p.update(v["move_time"], v["clock"], v["base_seconds"])

        if v["is_last"]:
            self.finish(gid, "last_move", from_move=True)
        elif p.n_moves % self.emit_every == 0:
            self.emit(gid, g, p, is_final=False, end_reason=None, from_move=True)

    def emit(self, gid: str, g: GameState, p: PlayerState, is_final: bool, end_reason: str | None,
             from_move: bool) -> dict[str, Any]:
        """Score one player and send the score message (key = game_id)."""
        s = score_player(p, g.time_class, self.baselines)
        now_ms = time.time_ns() // 1_000_000
        value = {"game_id": gid, "player_id": p.player_id, "color": p.color, "time_class": g.time_class,
                 "elo": p.elo, "band_id": p.band_id, "ply": g.last_ply,
                 **{k: _r(s[k]) for k in SCORE_VALUES},
                 "is_final": is_final, "end_reason": end_reason, "rule_version": RULE_VERSION,
                 "move_timestamp": g.last_ts, "timestamp": now_ms}
        trigger_ts = g.last_ts if from_move else None

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                self.counts["scores_failed"] += 1
                if self.counts["scores_failed"] <= 5:
                    log.error("score delivery failed: %s", err)
                return
            self.counts["scores_delivered"] += 1
            if trigger_ts is not None:
                self.ack_latency_ms.append(time.time_ns() // 1_000_000 - trigger_ts)

        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self.producer.produce(self.scores_topic, key=gid.encode(), value=payload,
                                      timestamp=now_ms, on_delivery=on_delivery)
                break
            except BufferError:
                self.producer.poll(0.05)
        self.counts["scores_emitted"] += 1
        if from_move:
            self.latency_ms.append(now_ms - g.last_ts)
        return s

    def finish(self, gid: str, reason: str, from_move: bool = False) -> None:
        """Final score of both players: score message + Parquet row; forget the game."""
        g = self.games.pop(gid)
        self.open_by_part[g.partition].discard(gid)
        self.finished[gid] = time.monotonic()
        scored_at = datetime.now(timezone.utc)
        for p in g.players.values():
            s = self.emit(gid, g, p, is_final=True, end_reason=reason, from_move=from_move)
            self.final_rows.append({
                "game_id": gid, "player_id": p.player_id, "color": p.color, "time_class": g.time_class,
                "elo": p.elo, "band_id": p.band_id, "n_plies": g.last_ply,
                **{k: s[k] for k in SCORE_VALUES},
                "end_reason": reason, "rule_version": RULE_VERSION, "baselines": self.baselines_name,
                "scored_at": scored_at,
            })

    def sweep(self, now: float, game_timeout: float) -> None:
        """Close games idle for game_timeout seconds; forget old finished ids."""
        for gid in [gid for gid, g in self.games.items() if now - g.last_seen >= game_timeout]:
            self.finish(gid, "idle")
        for gid in [gid for gid, t in self.finished.items() if now - t >= 10 * game_timeout]:
            del self.finished[gid]

    def finish_all(self, reason: str) -> None:
        for gid in list(self.games):
            self.finish(gid, reason)

    def due_commits(self) -> list[CommitPoint]:
        """Positions that changed since the last commit (called after every batch)."""
        out = []
        for p in self.dirty:
            if p in self.pos and self.committed.get(p) != self.pos[p]:
                self.committed[p] = self.pos[p]
                out.append((p, self.pos[p]))
        self.dirty.clear()
        return out

    def commit_failed(self, partition: int) -> None:
        """An async commit failed: send this partition's position again with the next batch."""
        self.committed.pop(partition, None)
        self.dirty.add(partition)

    def release(self, partitions: list[int], lost: bool = False
                ) -> tuple[list[CommitPoint], dict[int, list[dict[str, Any]]]]:
        """Hand partitions over to their next owner: returns the positions to commit and, per partition,
        the snapshots of its open games (which this member then forgets, without a final score).
        lost=True: no commit and no snapshot (the state of those games is lost)."""
        points: list[CommitPoint] = []
        snaps: dict[int, list[dict[str, Any]]] = {}
        for p in partitions:
            gids = sorted(self.open_by_part.pop(p, set()))
            if not lost:
                snaps[p] = [self.snapshot(gid) for gid in gids]
                if p in self.pos:
                    points.append((p, self.pos[p]))
            for gid in gids:
                del self.games[gid]
            self.counts["lost_open_games" if lost else "handed_over_open_games"] += len(gids)
            self.owned.discard(p)
            self.dirty.discard(p)
            self.pos.pop(p, None)
            self.committed.pop(p, None)
        return points, snaps

    def final_games(self) -> dict[str, int]:
        """Games with a final score, by end_reason."""
        reasons = {r["game_id"]: r["end_reason"] for r in self.final_rows}
        c = Counter(reasons.values())
        return {r: c[r] for r in ("last_move", "idle", "shutdown")}


def write_final(rows: list[dict[str, Any]], out_dir: Path, name: str, codec: str = "zstd") -> Path | None:
    """Final scores of this member to <out_dir>/part-<name>.parquet (overwrites a rerun's file)."""
    if not rows:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"part-{name}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=FINAL_SCHEMA), path, compression=codec)
    return path


def to_commit(topic: str, points: list[CommitPoint], metadata: dict[int, str] | None = None) -> list[TopicPartition]:
    """Commit positions as TopicPartitions (metadata: handoff snapshot range per partition)."""
    md = metadata or {}
    return [TopicPartition(topic, p, position, metadata=md.get(p, "")) for p, position in points]


def commit_now(consumer: Consumer, topic: str, scorer: Scorer, points: list[CommitPoint],
               metadata: dict[int, str] | None = None) -> bool:
    """Synchronous commit (handoff and shutdown); an error is logged and counted, never raised."""
    if not points:
        return True
    try:
        consumer.commit(offsets=to_commit(topic, points, metadata), asynchronous=False)
        scorer.counts["commits"] += 1
        return True
    except KafkaException as exc:
        scorer.counts["commit_errors"] += 1
        log.error("commit of %s failed: %s", points, exc)
        return False


def hand_over(consumer: Consumer, producer: Producer, scorer: Scorer, topic: str, state_topic: str,
              partitions: list[int]) -> None:
    """Release partitions: snapshots of their open games to state_topic (same partition number), wait until
    the broker has them, then commit the positions with the snapshots' offset range as metadata."""
    points, snaps = scorer.release(partitions)
    sent: dict[int, list[int]] = {}
    failed = []

    def on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            failed.append(err)
        else:
            sent.setdefault(msg.partition(), []).append(msg.offset())

    n = 0
    for p, games in snaps.items():
        for st in games:
            payload = json.dumps(st, separators=(",", ":")).encode()
            while True:
                try:
                    producer.produce(state_topic, key=st["game_id"].encode(), value=payload, partition=p,
                                     on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)
            n += 1
    remaining = producer.flush(60)  # snapshots and the scores already emitted for these games
    if failed or remaining:
        scorer.counts["snapshot_errors"] += len(failed) + remaining
        log.error("%d snapshots not delivered (%s): the next owner will miss those games' earlier moves",
                  len(failed) + remaining, failed[:3])
    metadata = {p: state_metadata(min(o), max(o) + 1) for p, o in sent.items()}
    ok = commit_now(consumer, topic, scorer, points, metadata)
    scorer.counts["snapshots_sent"] += n
    if n:
        log.info("handed over partitions %s: %d open games -> %s %s, commit %s%s", sorted(partitions), n,
                 state_topic, metadata, points, "" if ok else " FAILED")
    else:
        log.info("released partitions %s (no open game), commit %s%s", sorted(partitions), points,
                 "" if ok else " FAILED")


def read_snapshots(bootstrap: str, state_topic: str, partition: int, first: int, end: int) -> list[dict[str, Any]]:
    """The snapshots a previous owner wrote to state_topic[partition] at offsets [first, end)."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{STEP}-state-{time.time_ns()}",
                  "enable.auto.commit": False})
    out: list[dict[str, Any]] = []
    try:
        c.assign([TopicPartition(state_topic, partition, first)])
        deadline = time.monotonic() + 60
        while len(out) < end - first and time.monotonic() < deadline:
            m = c.poll(1.0)
            if m is None:
                continue
            if m.error():
                raise KafkaException(m.error())
            if first <= m.offset() < end:
                out.append(json.loads(m.value()))
            if m.offset() >= end - 1:
                break
    finally:
        c.close()
    if len(out) != end - first:
        raise KafkaException(f"read {len(out)} of {end - first} snapshots of {state_topic}[{partition}]")
    return out


def make_scorer_consumer(bootstrap: str, topic: str, state_topic: str, group: str | None,
                         ready_file: str | None, client_id: str, scorer: Scorer, producer: Producer) -> Consumer:
    """Consumer of lichess.moves wired to the scorer's offsets and handoff.

    With a group: subscribed with cooperative-sticky assignment (a member leaving moves only its own
    partitions). On assignment it reads the committed positions and restores the snapshots named in their
    metadata; on revocation it hands the partitions over (hand_over). Without a group: every partition from
    the first offset, no commits. The ready file is touched once the consumer has partitions."""
    def on_commit(err: Any, partitions: list[TopicPartition]) -> None:
        if err is not None:
            scorer.counts["commit_errors"] += 1
            log.warning("async commit failed (%s): sent again with the next batch", err)
            for tp in partitions:
                scorer.commit_failed(tp.partition)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group or f"{STEP}-{time.time_ns()}",
        "enable.auto.commit": False,  # the scorer commits its positions itself, after every batch
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        "client.id": client_id,
        "on_commit": on_commit,
    })
    meta = consumer.list_topics(topic, timeout=30)
    if topic not in meta.topics or meta.topics[topic].error is not None:
        raise KafkaException(f"topic {topic} not found")

    def mark_ready() -> None:
        if ready_file and scorer.owned:
            Path(ready_file).touch()

    if not group:
        partitions = sorted(meta.topics[topic].partitions)
        for p in partitions:
            scorer.assign(p)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        log.info("assigned %d partitions of %s", len(partitions), topic)
        mark_ready()
        return consumer

    def on_assign(c: Consumer, parts: list[TopicPartition]) -> None:
        if not parts:
            return
        try:
            committed = c.committed(parts, timeout=30)
        except KafkaException as exc:
            scorer.counts["assign_errors"] += 1
            log.error("could not read the committed offsets of %s: %s", [p.partition for p in parts], exc)
            committed = [TopicPartition(topic, p.partition) for p in parts]
        info = {}
        for tp in committed:
            start = tp.offset if tp.offset >= 0 else None
            snaps: list[dict[str, Any]] = []
            rng = parse_state_metadata(tp.metadata)
            if rng:
                try:
                    snaps = read_snapshots(bootstrap, state_topic, tp.partition, *rng)
                except KafkaException as exc:  # those games then continue from the middle (games_joined_late)
                    scorer.counts["assign_errors"] += 1
                    log.error("could not restore the snapshots of partition %d: %s", tp.partition, exc)
            scorer.assign(tp.partition, start, snaps, time.monotonic())
            if start is not None:
                info[tp.partition] = f"from {start}, {len(snaps)} games restored"
        log.info("group %s: assigned partitions %s (%s)", group, sorted(tp.partition for tp in committed),
                 info or "no committed offsets: from the beginning")
        mark_ready()

    def on_revoke(c: Consumer, parts: list[TopicPartition]) -> None:
        mine = [p.partition for p in parts if p.partition in scorer.owned]
        if mine:
            hand_over(c, producer, scorer, topic, state_topic, mine)

    def on_lost(_c: Consumer, parts: list[TopicPartition]) -> None:
        mine = [p.partition for p in parts if p.partition in scorer.owned]
        scorer.release(mine, lost=True)
        scorer.counts["lost_partitions"] += len(mine)
        log.error("group %s: lost partitions %s without a handoff: the state of their open games is lost",
                  group, sorted(mine))

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    log.info("subscribed to %s as a member of group %s (cooperative-sticky)", topic, group)
    return consumer


def run(consumer: Consumer, scorer: Scorer, producer: Producer, topic: str, state_topic: str,
        use_commits: bool, idle_timeout: float, first_timeout: float, game_timeout: float, interval: float,
        leave_after: int | None) -> dict[str, Any]:
    """Poll and score, committing after every batch, until stopped; then shut down (no consumer close).

    Stop "final" (SIGTERM/SIGINT, idle or first-message timeout): open games get a final "shutdown" score
    and the positions are committed. Stop "handoff" (SIGUSR1 or --leave-after messages): the partitions are
    handed over to the rest of the group (hand_over). Timeouts of 0 are disabled."""
    stop: dict[str, str | None] = {"mode": None}

    def on_signal(signum: int, _frame: Any) -> None:
        mode = "handoff" if signum == signal.SIGUSR1 else "final"
        log.info("received signal %d: stopping (%s)", signum, mode)
        stop["mode"] = stop["mode"] or mode

    for s in (signal.SIGTERM, signal.SIGINT, signal.SIGUSR1):
        signal.signal(s, on_signal)

    total = bad = errors = 0
    t_start = time.monotonic()
    t_first = t_last = None
    first_ms = last_ms = None
    next_sweep = t_start + SWEEP_EVERY_S
    next_report = t_start + interval
    while stop["mode"] is None:
        first = consumer.poll(0.2)
        msgs = [first] + consumer.consume(num_messages=BATCH, timeout=0) if first is not None else []
        now = time.monotonic()
        for m in msgs:
            if m.error():
                errors += 1
                log.error("consumer error: %s", m.error())
                continue
            if m.partition() not in scorer.owned:  # fetched before a revocation: the next owner reads it
                scorer.counts["not_owned_skipped"] += 1
                continue
            total += 1
            try:
                scorer.on_move(json.loads(m.value()), m.partition(), m.offset(), now)
            except (ValueError, KeyError, TypeError) as exc:
                bad += 1
                if bad <= 5:
                    log.error("bad lichess.moves value (%s): %r", exc, m.value()[:200])
        producer.poll(0)
        if msgs:
            now_ms = time.time_ns() // 1_000_000
            t_first = t_first or now
            first_ms = first_ms or now_ms
            t_last, last_ms = now, now_ms
        if now >= next_sweep:
            scorer.sweep(now, game_timeout)
            next_sweep = now + SWEEP_EVERY_S
        if use_commits:
            points = scorer.due_commits()
            if points:
                consumer.commit(offsets=to_commit(topic, points), asynchronous=True)
        if now >= next_report:
            log.info("t=%6.1fs moves=%d open_games=%d scores=%d finals=%d partitions=%s",
                     now - t_start, total, len(scorer.games), scorer.counts["scores_emitted"],
                     len(scorer.final_rows), sorted(scorer.owned))
            next_report = now + interval
        if leave_after and total >= leave_after:
            log.info("processed %d messages (--leave-after %d): leaving the group", total, leave_after)
            stop["mode"] = "handoff"
        elif idle_timeout > 0 and t_last is not None and now - t_last >= idle_timeout:
            log.info("no messages for %.0f s: stopping", idle_timeout)
            break
        elif first_timeout > 0 and t_first is None and now - t_start >= first_timeout:
            log.error("no message within %.0f s", first_timeout)
            break

    mode = stop["mode"] or "final"
    if mode == "handoff" and use_commits:
        hand_over(consumer, producer, scorer, topic, state_topic, sorted(scorer.owned))
    else:
        mode = "final"
        scorer.finish_all("shutdown")
        if use_commits:  # every position, also those an async commit may still be sending
            commit_now(consumer, topic, scorer, [(q, scorer.pos[q]) for q in sorted(scorer.owned) if q in scorer.pos])
    remaining = producer.flush(60)
    active_s = (t_last - t_first) if t_first is not None and t_last > t_first else None
    c = scorer.counts
    return {"messages": total, "errors": errors, "bad_values": bad, "duplicates": c["duplicates"],
            "late_messages": c["late_messages"], "games": c["games"], "stop_mode": mode,
            "active_s": round(active_s, 3) if active_s else None,
            "msg_s": round(total / active_s, 1) if active_s else None,
            "first_msg_ms": first_ms, "last_msg_ms": last_ms,
            "scores_emitted": c["scores_emitted"], "scores_delivered": c["scores_delivered"],
            "scores_failed": c["scores_failed"], "scores_undelivered": remaining,
            "final_scores": len(scorer.final_rows), "final_games": scorer.final_games(),
            "handed_over_open_games": c["handed_over_open_games"], "snapshots_sent": c["snapshots_sent"],
            "games_restored": c["games_restored"], "games_joined_late": c["games_joined_late"],
            "not_owned_skipped": c["not_owned_skipped"], "commits": c["commits"],
            "commit_errors": c["commit_errors"], "assign_errors": c["assign_errors"],
            "snapshot_errors": c["snapshot_errors"], "snapshot_conflicts": c["snapshot_conflicts"],
            "lost_partitions": c["lost_partitions"], "lost_open_games": c["lost_open_games"],
            "score_latency": latency_stats(np.asarray(scorer.latency_ms, dtype=np.int64)),
            "ack_latency": latency_stats(np.asarray(scorer.ack_latency_ms, dtype=np.int64))}


def main() -> None:
    parser = build_arg_parser("Score lichess.moves move by move and send the scores to lichess.scores")
    parser.add_argument("--bootstrap", default=default_bootstrap(), help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP)")
    parser.add_argument("--topic", default=default_topic(), help="input topic (default: $KAFKA_TOPIC or lichess.moves)")
    parser.add_argument("--scores-topic", default=TOPIC_SCORES, help=f"output topic (default {TOPIC_SCORES})")
    parser.add_argument("--state-topic", default=TOPIC_SCORER_STATE,
                        help=f"handoff snapshots of open games, group mode (default {TOPIC_SCORER_STATE})")
    parser.add_argument("--baselines", help="baselines folder (default: paths.scorer_baselines_dir of the config)")
    parser.add_argument("--emit-every", type=int, default=DEFAULT_EMIT_EVERY,
                        help=f"send a score every N moves of a player (default {DEFAULT_EMIT_EVERY})")
    parser.add_argument("--run-id", default=os.getenv("SLURM_JOB_ID", "local"),
                        help="folder of the final scores under paths.live_scores_dir (default: Slurm job id)")
    parser.add_argument("--group", help="join this consumer group instead of reading every partition")
    parser.add_argument("--name", default="0", help="member name: log file and Parquet part names (default 0)")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between progress lines (default 10)")
    parser.add_argument("--idle-timeout", type=float, default=30.0,
                        help="stop after this many seconds without messages; 0 = never, the group stops it "
                             "(default 30)")
    parser.add_argument("--first-timeout", type=float, default=900.0,
                        help="give up if no message arrives within this many seconds; 0 = never (default 900)")
    parser.add_argument("--game-timeout", type=float, default=600.0,
                        help="close a game with no move for this many seconds (default 600)")
    parser.add_argument("--leave-after", type=int,
                        help="rebalance test: leave the group (hand the partitions over) after N messages")
    parser.add_argument("--summary-out", help="write the run summary to this JSON file")
    parser.add_argument("--latency-out", help="save score and ack latencies (ms) to this .npz file")
    parser.add_argument("--ready-file", help="touch once partitions are assigned (default: $KAFKA_CONSUMER_READY)")
    args = parser.parse_args()
    if not args.bootstrap:
        parser.error("no broker: pass --bootstrap or run inside cluster/kafka_job.sh (sets KAFKA_BOOTSTRAP)")
    if args.emit_every <= 0:
        parser.error("--emit-every must be > 0")
    if args.leave_after is not None and (args.leave_after <= 0 or not args.group):
        parser.error("--leave-after needs --group and a value > 0")

    cfg = load_config(args.config)
    setup_logging(f"{STEP}-{args.name}", cfg["paths"]["log_dir"])
    baselines_dir = args.baselines or cfg["paths"]["scorer_baselines_dir"]
    baselines = load_baselines(baselines_dir)
    if not baselines:
        log.error("no usable level-%s baselines in %s", BASELINE_LEVEL, baselines_dir)
        sys.exit(1)
    out_dir = Path(cfg["paths"]["live_scores_dir"]) / f"run_id={args.run_id}"
    log.info("topic=%s -> %s bootstrap=%s group=%s emit_every=%d baselines=%s (%d cells x features) out=%s",
             args.topic, args.scores_topic, args.bootstrap, args.group, args.emit_every, baselines_dir,
             len(baselines), out_dir)

    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": f"{STEP}-{args.name}",
                         "enable.idempotence": True, "acks": "all", "linger.ms": 5,
                         "compression.type": "lz4", "partitioner": "murmur2_random"})
    for t in [args.scores_topic] + ([args.state_topic] if args.group else []):
        try:
            meta = producer.list_topics(t, timeout=30)
            if meta.topics[t].error is not None:
                raise KafkaException(meta.topics[t].error)
        except KafkaException as exc:
            log.error("topic %s not available at %s: %s", t, args.bootstrap, exc)
            sys.exit(1)
    scorer = Scorer(baselines, Path(baselines_dir).name, args.emit_every, producer, args.scores_topic)

    consumer = make_scorer_consumer(args.bootstrap, args.topic, args.state_topic, args.group,
                                    args.ready_file or os.getenv("KAFKA_CONSUMER_READY"),
                                    f"{STEP}-{args.name}", scorer, producer)
    try:
        summary = run(consumer, scorer, producer, args.topic, args.state_topic, bool(args.group),
                      args.idle_timeout, args.first_timeout, args.game_timeout, args.interval, args.leave_after)
    finally:
        consumer.close()  # leaves the group; any revocation here finds nothing left to hand over

    path = write_final(scorer.final_rows, out_dir, args.name, parquet_codec(cfg))
    log.info("final scores: %d rows -> %s", len(scorer.final_rows), path)
    if args.latency_out:
        np.savez(args.latency_out, score=np.asarray(scorer.latency_ms, dtype=np.int64),
                 ack=np.asarray(scorer.ack_latency_ms, dtype=np.int64))
    summary.update(baselines=baselines_dir, emit_every=args.emit_every, run_id=args.run_id, final_path=str(path),
                   final_scores=len(scorer.final_rows), final_games=scorer.final_games())
    log.info("summary: %s", json.dumps(summary))
    if args.summary_out:
        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if (summary["messages"] == 0 or summary["errors"] or summary["bad_values"] or summary["scores_failed"]
            or summary["scores_undelivered"] or summary["assign_errors"] or summary["lost_partitions"]
            or summary["snapshot_errors"] or summary["snapshot_conflicts"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
