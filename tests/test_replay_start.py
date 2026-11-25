"""replay_producer.py --start is UTC, whatever the node's time zone (the nodes run on CET/CEST).

Run from the repo root on the login node (no Spark, no broker): PYTHONPATH=. pytest tests
"""

import calendar
import time
from datetime import datetime, timezone

import pytest
from pyspark.sql import types as spark_types

from src.streaming.replay_producer import cache_file, parse_start_utc

UTC_MIDNIGHT = calendar.timegm((2026, 10, 22, 0, 0, 0))  # 2026-10-22T00:00:00Z, epoch seconds


@pytest.fixture
def madrid_tz(monkeypatch: pytest.MonkeyPatch):
    """Run the test with the compute nodes' local zone (Europe/Madrid, UTC+2 on 22 Oct 2026)."""
    monkeypatch.setenv("TZ", "Europe/Madrid")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def spark_literal_ms(dt: datetime, monkeypatch: pytest.MonkeyPatch) -> int:
    """Epoch ms PySpark sends to the JVM for F.lit(dt), via its own DatetimeConverter (Java class stubbed)."""
    class FakeTimestamp:
        def __init__(self, ms: int) -> None:
            self.ms = ms

        def setNanos(self, _nanos: int) -> None:  # noqa: N802 (java.sql.Timestamp API)
            pass

    monkeypatch.setattr(spark_types, "JavaClass", lambda _name, _gateway: FakeTimestamp)
    return spark_types.DatetimeConverter().convert(dt, None).ms


def test_start_without_offset_is_utc(madrid_tz, monkeypatch):
    start = parse_start_utc("2026-10-22T00:00")
    assert start == datetime(2026, 10, 22, tzinfo=timezone.utc)
    assert spark_literal_ms(start, monkeypatch) == UTC_MIDNIGHT * 1000


def test_naive_start_was_local_time(madrid_tz, monkeypatch):
    """The old behaviour (datetime.fromisoformat): the window started 2 h early in Madrid summer time."""
    naive = datetime.fromisoformat("2026-10-22T00:00")
    assert spark_literal_ms(naive, monkeypatch) == (UTC_MIDNIGHT - 2 * 3600) * 1000


def test_start_with_offset_is_converted(madrid_tz, monkeypatch):
    start = parse_start_utc("2026-10-22T02:00+02:00")
    assert start == datetime(2026, 10, 22, tzinfo=timezone.utc)
    assert start.utcoffset().total_seconds() == 0
    assert spark_literal_ms(start, monkeypatch) == UTC_MIDNIGHT * 1000


def test_cache_tag_is_utc():
    """Same window, same cache file; the Z keeps caches selected with the old local-time rule out."""
    tcs = ["blitz", "bullet"]
    a = cache_file("c", "2026-10", 100, parse_start_utc("2026-10-22T00:00"), tcs)
    b = cache_file("c", "2026-10", 100, parse_start_utc("2026-10-22T02:00+02:00"), tcs)
    assert a == b
    assert a.name == "replay_v2_2026-10_100g_20261022T000000Z_blitz-bullet.parquet"
    assert cache_file("c", "2026-10", 100, None, tcs).name == "replay_v2_2026-10_100g_monthstart_blitz-bullet.parquet"
