"""Pseudonymous player IDs: the only place where usernames are hashed (see CLAUDE.md, Privacy rules)."""

import hashlib
import hmac
import os

from dotenv import load_dotenv

from src.common.config import REPO_ROOT

PLAYER_ID_LENGTH = 16


def load_hash_key() -> bytes:
    """Read LICHESS_HASH_KEY from the environment or .env. Call on the driver, then pass the key to workers."""
    load_dotenv(REPO_ROOT / ".env")
    key = os.getenv("LICHESS_HASH_KEY", "").strip()
    if not key:
        raise RuntimeError("LICHESS_HASH_KEY is not set; see SETUP.md step 4")
    return key.encode("utf-8")


def make_player_id(username: str, key: bytes) -> str:
    """HMAC-SHA256 of the lowercase username, hex digest truncated to 16 characters."""
    digest = hmac.new(key, username.lower().encode("utf-8"), hashlib.sha256).hexdigest()
    return digest[:PLAYER_ID_LENGTH]
