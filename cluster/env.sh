# Cluster environment for the chess cheat detector. Source it (don't run it) at the start of
# every login shell, srun session and sbatch job:
#     source cluster/env.sh            # from the repo root
# Loads Java 17 and Python 3.11 from the cluster modules, activates the project venv and points
# Spark's scratch space at a folder in your home. Everything lives under $CHESS_HOME
# (default $HOME/chess), so it works for any account; set CHESS_HOME first to use another place.

CHESS_HOME="${CHESS_HOME:-$HOME/chess}"

module load Java/17.0.15 Python/3.11.5-GCCcore-13.2.0

if [[ ! -f "$CHESS_HOME/venv/bin/activate" ]]; then
    echo "No venv at $CHESS_HOME/venv: see cluster/README.md, step 3" >&2
    return 1
fi
source "$CHESS_HOME/venv/bin/activate"

export SPARK_LOCAL_DIRS="$CHESS_HOME/spark-tmp"
mkdir -p "$SPARK_LOCAL_DIRS"
