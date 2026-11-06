"""SparkSession factory: every step gets its session settings from the config."""

import logging
import os
import sys
from typing import Any

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

SHUFFLE_PARTITIONS_PER_CORE = 3  # "auto" shuffle partitions = 3 x Slurm CPUs
DRIVER_MEMORY_SHARE = 0.75       # "auto" driver memory = 75% of the Slurm job memory


def _slurm_cpus() -> int:
    """CPUs of the current Slurm job; refuses to run outside one (e.g. on the login node)."""
    cpus = os.getenv("SLURM_CPUS_PER_TASK")
    if not cpus:
        raise RuntimeError("spark settings 'auto' need a Slurm job (SLURM_CPUS_PER_TASK is not set). "
                           "Never run Spark on the login node: use sbatch or srun.")
    return int(cpus)


def _slurm_driver_memory() -> str:
    """75% of the Slurm job memory, in MB (SLURM_MEM_PER_NODE, or SLURM_MEM_PER_CPU x CPUs)."""
    mem_mb = os.getenv("SLURM_MEM_PER_NODE")
    if not mem_mb and os.getenv("SLURM_MEM_PER_CPU"):
        mem_mb = str(int(os.environ["SLURM_MEM_PER_CPU"]) * _slurm_cpus())
    if not mem_mb:
        raise RuntimeError("spark.driver_memory 'auto' needs a Slurm job with --mem or --mem-per-cpu")
    return f"{int(int(mem_mb) * DRIVER_MEMORY_SHARE)}m"


def resolve_spark_settings(s: dict[str, Any]) -> dict[str, Any]:
    """Replace 'auto' values with ones from the Slurm job: local[N], 75% of --mem, 3 x N partitions."""
    s = dict(s)
    if s["master"] == "auto":
        s["master"] = f"local[{_slurm_cpus()}]"
    if s["driver_memory"] == "auto":
        s["driver_memory"] = _slurm_driver_memory()
    if s["shuffle_partitions"] == "auto":
        s["shuffle_partitions"] = SHUFFLE_PARTITIONS_PER_CORE * _slurm_cpus()
    return s


def get_spark(cfg: dict[str, Any], app_name: str, use_postgres: bool = False) -> SparkSession:
    """Create (or reuse) a SparkSession with the project defaults.

    Only steps that talk to PostgreSQL through Spark JDBC pass use_postgres=True; the others
    start without asking Maven for the JDBC driver. With spark-submit, --packages and
    --driver-memory on the command line take precedence (the JVM is already running).
    Run steps from the repo root with PYTHONPATH=. so that workers can import `src`.
    """
    # Workers use the same Python as the driver (the .venv), not conda's or the system's
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    s = resolve_spark_settings(cfg["spark"])
    log.info("spark: master=%s driver_memory=%s shuffle_partitions=%s jdbc=%s",
             s["master"], s["driver_memory"], s["shuffle_partitions"], use_postgres)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(s["master"])
        .config("spark.driver.memory", s["driver_memory"])
        .config("spark.sql.shuffle.partitions", s["shuffle_partitions"])
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # toPandas() on timestamp columns crashes without Arrow (PySpark 3.5 + pandas 2.x)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # The PostgreSQL JDBC driver writes TIMESTAMP values in the JVM time zone: keep it UTC
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )
    if use_postgres:
        builder = builder.config("spark.jars.packages", s["jars_packages"])
    spark = builder.getOrCreate()
    jvm = spark.sparkContext._jvm
    jvm.java.util.TimeZone.setDefault(jvm.java.util.TimeZone.getTimeZone("UTC"))  # driver JVM is already running
    spark.sparkContext.setLogLevel("WARN")
    return spark
