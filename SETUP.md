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
pip install pyspark==3.5.3 python-chess pyyaml python-dotenv requests psycopg2-binary pyarrow pandas matplotlib jupyter
```

## 4. Secrets

```bash
cp .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"
```

Paste the printed value as `LICHESS_HASH_KEY` in `.env`. **Everyone in the group must use the same key**, otherwise player IDs won't match across machines. Share it privately (not in GitHub, not in the group chat history if you can avoid it).

## 5. Get the sample data (~100k games, a few hundred MB)

```bash
mkdir -p data/raw
curl -s https://database.lichess.org/standard/lichess_db_standard_rated_2026-09.pgn.zst \
  | zstd -dc | head -n 2500000 > data/raw/lichess_2026-09_sample.pgn
```

If September 2026 isn't on database.lichess.org yet, use `2026-08`. The last game in the file is cut off; the parser quarantines it.

## 6. Start Claude Code

Open the repo in VS Code, start Claude Code, use Opus 5.5, and give it this first prompt:

> Read CLAUDE.md and docs/data_dictionary.md fully. Then do step 1 of the build order only (scaffolding). Show me the files, then stop and wait for me before step 2.

Then continue one build step at a time. After each step, run it on the sample, check the log and row counts, and commit before moving on.
