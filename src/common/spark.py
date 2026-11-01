"""SparkSession factory: every step gets its session settings from the config."""

import os
import sys
from typing import Any

from pyspark.sql import SparkSession


def get_spark(cfg: dict[str, Any], app_name: str) -> SparkSession:
    """Create (or reuse) a SparkSession with the project defaults.

    Run steps from the repo root with PYTHONPATH=. so that workers can import `src`.
    """
    # Workers use the same Python as the driver (the .venv), not conda's or the system's
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    s = cfg["spark"]
    spark = (
        SparkSession.builder.appName(app_name)
        .master(s["master"])
        .config("spark.driver.memory", s["driver_memory"])
        .config("spark.sql.shuffle.partitions", s["shuffle_partitions"])
        .config("spark.jars.packages", s["jars_packages"])
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # toPandas() on timestamp columns crashes without Arrow (PySpark 3.5 + pandas 2.x)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
