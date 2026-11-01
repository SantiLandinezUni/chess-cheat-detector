# Local setup (Mac, Apple Silicon, 8 GB)

Each teammate who will run the pipeline does steps 1–4. Step 5 is done once by whoever creates the repo.

## 1. System tools

```bash
brew install openjdk@17 python@3.11 zstd postgresql@16
```

Add Java to your shell (append to `~/.zshrc`, then open a new terminal):

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:/opt/homebrew/opt/postgresql@16/bin:$PATH"
```

Check that it worked: `java -version` should say 17.

## 2. PostgreSQL

```bash
brew services start postgresql@16
createdb chess
psql chess -c "select version();"
```

Homebrew's PostgreSQL uses your Mac username as superuser with no password. That's fine locally; the cluster config will hold real credentials.

## 3. Python environment

From the repo root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` pins PySpark 3.5.3 and `pandas<3` (PySpark 3.5 is built for pandas 2), so everyone gets the same versions.

## 4. Secrets

```bash
cp .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"
```

Paste the printed value as `LICHESS_HASH_KEY` in `.env`. **Everyone in the group must use the same key**, otherwise player IDs won't match across machines. Share it privately (not in GitHub, not in the group chat history if you can avoid it).

## 5. Get the sample data (124,914 games, ~280 MB)

```bash
mkdir -p data/raw
curl -s https://database.lichess.org/standard/lichess_db_standard_rated_2026-09.pgn.zst \
  | zstd -dc | head -n 2500000 > data/raw/lichess_2026-09_sample.pgn
```

If September 2026 isn't on database.lichess.org yet, use `2026-08`. The last game in the file is cut off; the parser quarantines it. With `2026-09` the file has 124,914 game records (including the cut-off one).

## Running a pipeline step

Always from the repo root, with the venv active:

```bash
source .venv/bin/activate
PYTHONPATH=. spark-submit --driver-memory 3g src/ingestion/parse_games.py --config config/local.yaml
```

- `PYTHONPATH=.` lets the driver and the Spark workers import the `src` package.
- `--driver-memory 3g` must be on the command line: once `spark-submit` has started the JVM, the value in `config/local.yaml` can't change it anymore.
- Steps that use Spark JDBC (`src/db/load_postgres.py`) also need the PostgreSQL driver on the command line, for the same reason (the first run downloads it):

```bash
PYTHONPATH=. spark-submit --driver-memory 3g --packages org.postgresql:postgresql:42.7.4 src/db/load_postgres.py --config config/local.yaml
```

## 6. Start Claude Code

Open the repo in VS Code, start Claude Code, use Opus 5.5, and give it this first prompt:

> Read CLAUDE.md and docs/data_dictionary.md fully. Then do step 1 of the build order only (scaffolding). Show me the files, then stop and wait for me before step 2.

Then continue one build step at a time. After each step, run it on the sample, check the log and row counts, and commit before moving on.
