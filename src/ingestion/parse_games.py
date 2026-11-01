"""Build step 3: parse Lichess PGN into the Parquet lake (games, moves, quarantine, restricted/identity).

Run from the repo root with the venv active:
    PYTHONPATH=. spark-submit --driver-memory 3g src/ingestion/parse_games.py --config config/local.yaml

Every month in the config is parsed in one run. games and moves are overwritten per
(source_month, time_class) partition, quarantine per source_month, and identity is rebuilt
from all configured months, so rerunning never duplicates rows.
"""

import io
import re
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

import chess
import chess.pgn
from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import build_arg_parser, load_config, pgn_path
from src.common.ids import load_hash_key, make_player_id
from src.common.logging_setup import log_duration, setup_logging
from src.common.spark import get_spark

STEP = "parse_games"
RECORD_DELIMITER = "\n\n[Event "
RESULTS = ("1-0", "0-1", "1/2-1/2", "*")
REQUIRED_HEADERS = ("Event", "Site", "White", "Black", "Result", "UTCDate", "UTCTime",
                    "WhiteElo", "BlackElo", "TimeControl", "Termination")
EXCERPT_CHARS = 500
GAME_ID_RE = re.compile(r"^[A-Za-z0-9]{8}$")
SITE_RE = re.compile(r'^\[Site "[^"]*/([A-Za-z0-9]+)"\]', re.MULTILINE)
PLAYER_RE = re.compile(r'^\[(?:White|Black) "([^"]*)"', re.MULTILINE)

# Column order and types follow docs/data_dictionary.md (§4.1, §4.2, L3, L4)
GAMES_SCHEMA = StructType([
    StructField("game_id", StringType(), False),
    StructField("source_month", StringType(), False),
    StructField("event", StringType(), False),
    StructField("is_tournament", BooleanType(), False),
    StructField("time_control", StringType(), False),
    StructField("base_seconds", IntegerType(), True),
    StructField("increment_seconds", IntegerType(), True),
    StructField("time_class", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("white_id", StringType(), False),
    StructField("black_id", StringType(), False),
    StructField("white_elo", ShortType(), False),
    StructField("black_elo", ShortType(), False),
    StructField("white_rating_diff", ShortType(), True),
    StructField("black_rating_diff", ShortType(), True),
    StructField("white_title", StringType(), True),
    StructField("black_title", StringType(), True),
    StructField("has_bot", BooleanType(), False),
    StructField("result", StringType(), False),
    StructField("termination", StringType(), False),
    StructField("eco", StringType(), True),
    StructField("opening", StringType(), True),
    StructField("n_plies", ShortType(), False),
    StructField("has_clock", BooleanType(), False),
    StructField("has_eval", BooleanType(), False),
    StructField("ingested_at", TimestampType(), False),
])

MOVES_SCHEMA = StructType([
    StructField("game_id", StringType(), False),
    StructField("ply", ShortType(), False),
    StructField("color", StringType(), False),
    StructField("player_id", StringType(), False),
    StructField("san", StringType(), False),
    StructField("uci", StringType(), False),
    StructField("clock_after_s", FloatType(), True),
    StructField("move_time_s", FloatType(), True),
    StructField("eval_cp", IntegerType(), True),
    StructField("eval_mate", ShortType(), True),
    StructField("source_month", StringType(), False),
    StructField("time_class", StringType(), False),
])

QUARANTINE_SCHEMA = StructType([
    StructField("source_month", StringType(), False),
    StructField("game_id", StringType(), True),
    StructField("error_stage", StringType(), False),
    StructField("error_code", StringType(), False),
    StructField("error_detail", StringType(), True),
    StructField("raw_excerpt", StringType(), True),
    StructField("quarantined_at", TimestampType(), False),
])

IDENTITY_SCHEMA = StructType([
    StructField("player_id", StringType(), False),
    StructField("username", StringType(), False),
    StructField("first_seen_month", StringType(), False),
])


class Rejected(Exception):
    """A game that fails parsing or a record-level hard check; it goes to quarantine."""

    def __init__(self, stage: str, code: str, detail: str) -> None:
        super().__init__(detail)
        self.stage, self.code, self.detail = stage, code, detail


# ---------- derived fields ----------

def time_class_for(base: int, inc: int) -> str:
    """Time class from the estimated duration base + 40 x increment (data dictionary §6)."""
    est = base + 40 * inc
    if est < 30:
        return "ultrabullet"
    if est < 180:
        return "bullet"
    if est < 480:
        return "blitz"
    if est < 1500:
        return "rapid"
    return "classical"


def parse_time_control(tc: str) -> tuple[int | None, int | None, str]:
    """'180+2' -> (180, 2, 'blitz'); '-' -> (None, None, 'correspondence')."""
    if tc == "-":
        return None, None, "correspondence"
    try:
        base, inc = (int(x) for x in tc.split("+"))
    except ValueError:
        raise Rejected("parse", "invalid_time_control", f"TimeControl {tc!r}") from None
    return base, inc, time_class_for(base, inc)


def _optional_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "", "?", "-") else None
    except ValueError:
        return None


def _optional_str(value: str | None) -> str | None:
    return value if value not in (None, "", "?", "-") else None


# ---------- one record ----------

def _is_complete(text: str) -> bool:
    """A full record has header lines, a blank line, then move text ending with the result token."""
    _, sep, movetext = text.strip().partition("\n\n")
    tokens = movetext.split()
    return bool(sep) and bool(tokens) and tokens[-1] in RESULTS


def _excerpt(text: str) -> str:
    """First characters of the record with the [White ...] and [Black ...] lines removed."""
    lines = (ln for ln in text.splitlines() if not ln.startswith(("[White ", "[Black ")))
    return "\n".join(lines)[:EXCERPT_CHARS]


def _scrub(detail: str, text: str) -> str:
    """Remove player names from an error message before it is stored."""
    for name in PLAYER_RE.findall(text):
        if name:
            detail = detail.replace(name, "<player>")
    return detail[:EXCERPT_CHARS]


def _parse_game(text: str, month: str, key: bytes, run_ts: datetime) -> tuple[tuple, list[tuple], list[tuple]]:
    """Return (games row, moves rows, identity rows) or raise Rejected."""
    if not _is_complete(text):
        raise Rejected("parse", "truncated_record", "record does not end with a result token")

    game = chess.pgn.read_game(io.StringIO(text))
    if game is None:
        raise Rejected("parse", "unparseable_pgn", "no game found in record")
    if game.errors:
        raise Rejected("parse", "unparseable_pgn", str(game.errors[0]))

    h = game.headers
    for tag in REQUIRED_HEADERS:
        if _optional_str(h.get(tag)) is None and not (tag == "TimeControl" and h.get(tag) == "-"):
            raise Rejected("parse", "missing_site" if tag == "Site" else "missing_field", f"missing header {tag}")

    result = h["Result"]
    if result not in RESULTS:
        raise Rejected("dq", "invalid_result", f"Result {result!r}")

    game_id = h["Site"].rstrip("/").rsplit("/", 1)[-1]
    if not GAME_ID_RE.match(game_id):
        raise Rejected("parse", "invalid_game_id", f"Site {h['Site']!r}")

    base, inc, time_class = parse_time_control(h["TimeControl"])
    try:
        started_at = datetime.strptime(f"{h['UTCDate']} {h['UTCTime']}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        raise Rejected("parse", "invalid_date", f"UTCDate/UTCTime {h['UTCDate']!r} {h['UTCTime']!r}") from None
    white_elo, black_elo = _optional_int(h["WhiteElo"]), _optional_int(h["BlackElo"])
    if white_elo is None or black_elo is None:
        raise Rejected("parse", "invalid_elo", f"WhiteElo {h['WhiteElo']!r}, BlackElo {h['BlackElo']!r}")

    white_id, black_id = make_player_id(h["White"], key), make_player_id(h["Black"], key)
    if white_id == black_id:
        raise Rejected("dq", "same_player", "white_id equals black_id")
    player_ids = {chess.WHITE: white_id, chess.BLACK: black_id}

    # Moves: walk the mainline with one board so SAN is computed in O(n)
    moves: list[tuple] = []
    board = game.board()
    last_clock = {chess.WHITE: base, chess.BLACK: base}  # first move of each side uses base_seconds
    for ply, node in enumerate(game.mainline(), start=1):
        color = board.turn
        san = board.san(node.move)
        board.push(node.move)
        clock = node.clock()
        prev = last_clock[color]
        move_time = prev - clock + inc if None not in (clock, prev, inc) else None
        last_clock[color] = clock
        pov = node.eval()
        eval_cp = pov.white().score() if pov is not None else None
        eval_mate = pov.white().mate() if pov is not None else None
        moves.append((game_id, ply, "w" if color == chess.WHITE else "b", player_ids[color], san,
                      node.move.uci(), clock, move_time, eval_cp, eval_mate, month, time_class))

    white_title, black_title = _optional_str(h.get("WhiteTitle")), _optional_str(h.get("BlackTitle"))
    event = h["Event"]
    games_row = (
        game_id, month, event, "tournament" in event.lower() or "swiss" in event.lower(),
        h["TimeControl"], base, inc, time_class, started_at,
        white_id, black_id, white_elo, black_elo,
        _optional_int(h.get("WhiteRatingDiff")), _optional_int(h.get("BlackRatingDiff")),
        white_title, black_title, "BOT" in (white_title, black_title),
        result, h["Termination"], _optional_str(h.get("ECO")), _optional_str(h.get("Opening")),
        len(moves),
        len(moves) > 0 and all(m[6] is not None for m in moves),
        any(m[8] is not None or m[9] is not None for m in moves),
        run_ts,
    )
    identity = [(white_id, h["White"], month), (black_id, h["Black"], month)]
    return games_row, moves, identity


def parse_record(text: str, month: str, key: bytes, run_ts: datetime) -> tuple[str, Any]:
    """('ok', (game, moves, identity)) or ('bad', quarantine row). Never raises."""
    try:
        return "ok", _parse_game(text, month, key, run_ts)
    except Rejected as e:
        stage, code, detail = e.stage, e.code, e.detail
    except Exception as e:  # anything unexpected still must not crash the job
        stage, code, detail = "parse", "unparseable_pgn", f"{type(e).__name__}: {e}"
    site = SITE_RE.search(text)
    return "bad", (month, site.group(1) if site else None, stage, code,
                   _scrub(detail, text), _excerpt(text), run_ts)


def parse_partition(records: Iterable[tuple[int, str]], month: str, key: bytes,
                    run_ts: datetime) -> Iterator[tuple[str, Any]]:
    """Restore the '[Event ' prefix the delimiter consumed and parse each record."""
    for offset, text in records:
        if offset != 0:  # only the first record of the file keeps its prefix
            text = "[Event " + text
        yield parse_record(text, month, key, run_ts)


# ---------- Spark ----------

def read_and_parse(spark: SparkSession, cfg: dict[str, Any], key: bytes, run_ts: datetime) -> RDD:
    """One RDD of tagged parse results for all configured months."""
    rdds = []
    for month in cfg["months"]:
        raw = spark.sparkContext.newAPIHadoopFile(
            pgn_path(cfg, month),
            "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "org.apache.hadoop.io.LongWritable",
            "org.apache.hadoop.io.Text",
            conf={"textinputformat.record.delimiter": RECORD_DELIMITER},
        )
        rdds.append(raw.mapPartitions(lambda recs, m=month: parse_partition(recs, m, key, run_ts)))
    return spark.sparkContext.union(rdds)


def write_parquet(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """Overwrite only the partitions present in df (dynamic partition overwrite)."""
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


def main() -> None:
    args = build_arg_parser("Parse Lichess PGN into the Parquet lake").parse_args()
    cfg = load_config(args.config)
    log = setup_logging(STEP, cfg["paths"]["log_dir"])
    lake = cfg["paths"]["lake_dir"]
    key = load_hash_key()
    run_ts = datetime.now(timezone.utc)
    log.info("months=%s input=%s lake=%s", cfg["months"], cfg["input"]["pgn_pattern"], lake)

    spark = get_spark(cfg, STEP)
    with log_duration(log, STEP):
        with log_duration(log, "read and parse"):
            # DISK_ONLY: parse once, reuse for the four outputs without holding it in RAM
            parsed = read_and_parse(spark, cfg, key, run_ts).persist(StorageLevel.DISK_ONLY)
            kinds = parsed.map(lambda r: r[0]).countByValue()
            log.info("input records=%d parsed games=%d quarantined=%d",
                     sum(kinds.values()), kinds.get("ok", 0), kinds.get("bad", 0))
        ok = parsed.filter(lambda r: r[0] == "ok").map(lambda r: r[1])
        bad = parsed.filter(lambda r: r[0] == "bad").map(lambda r: r[1])

        with log_duration(log, "write games"):
            write_parquet(spark.createDataFrame(ok.map(lambda g: g[0]), GAMES_SCHEMA),
                          f"{lake}/games", ["source_month", "time_class"])
            log.info("games rows=%d", kinds.get("ok", 0))

        with log_duration(log, "write moves"):
            write_parquet(spark.createDataFrame(ok.flatMap(lambda g: g[1]), MOVES_SCHEMA),
                          f"{lake}/moves", ["source_month", "time_class"])
            log.info("moves rows=%d", ok.map(lambda g: len(g[1])).sum())

        with log_duration(log, "write quarantine"):
            write_parquet(spark.createDataFrame(bad, QUARANTINE_SCHEMA),
                          f"{lake}/quarantine", ["source_month"])
            for code, n in sorted(bad.map(lambda q: q[3]).countByValue().items()):
                log.info("quarantined %s=%d", code, n)

        with log_duration(log, "write restricted/identity"):
            identity = (spark.createDataFrame(ok.flatMap(lambda g: g[2]), IDENTITY_SCHEMA)
                        .groupBy("player_id")
                        .agg(F.min("username").alias("username"),
                             F.min("first_seen_month").alias("first_seen_month")))
            write_parquet(identity, f"{lake}/restricted/identity", [])
            log.info("identity rows=%d", spark.read.parquet(f"{lake}/restricted/identity").count())

        parsed.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
