"""Spark smoke test for a Slurm compute node: 50M generated rows, a group-by, a Parquet round trip.

Run it inside a Slurm allocation, never on the login node:
    sbatch cluster/spark_test.sbatch
    srun -p interactive --cpus-per-task=8 --mem=16G --time=00:30:00 python cluster/spark_test.py

Spark uses local[N] with N = SLURM_CPUS_PER_TASK (Slurm enforces the CPU limit). The driver
memory comes from SPARK_DRIVER_MEM (the sbatch script sets it to 75% of --mem). The output goes
to SPARK_TEST_OUT, default $HOME/chess/test_parquet, and is overwritten on every run.
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

cores = int(os.environ.get("SLURM_CPUS_PER_TASK", "4"))
out = os.environ.get("SPARK_TEST_OUT", os.path.join(os.path.expanduser("~"), "chess", "test_parquet"))

spark = (SparkSession.builder
         .master(f"local[{cores}]")
         .appName("cluster-smoke-test")
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "8g"))
         .config("spark.sql.shuffle.partitions", cores * 2)
         .getOrCreate())

t = time.time()
df = (spark.range(50_000_000)
      .withColumn("rating_band", (F.col("id") % 20) * 100)
      .withColumn("move_time", F.rand(seed=42) * 30))
agg = df.groupBy("rating_band").agg(F.count("*").alias("n"), F.avg("move_time").alias("avg_t"))

agg.write.mode("overwrite").parquet(out)
spark.read.parquet(out).orderBy("rating_band").show(5)
print(f"OK  cores={cores}  rows=50M  time={time.time() - t:.1f}s  out={out}")
spark.stop()
