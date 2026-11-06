"""Load a YAML config plus secrets from .env, and the shared --config CLI argument."""

import argparse
import getpass
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_SECTIONS = ("months", "input", "paths", "spark", "postgres", "lichess", "quality")


def _resolve(path: str) -> str:
    """Make a relative local path absolute from the repo root; leave URIs (hdfs://, s3a://) as they are."""
    if "://" in path or os.path.isabs(path):
        return path
    return str(REPO_ROOT / path)


def expand_path(cfg: dict[str, Any], path: str) -> str:
    """Replace {data_dir} with paths.data_dir (if the config has one) and make the path absolute."""
    data_dir = cfg["paths"].get("data_dir")
    if data_dir is not None:
        path = path.replace("{data_dir}", _resolve(data_dir))
    return _resolve(path)


def load_config(config_path: str, months: list[str] | None = None) -> dict[str, Any]:
    """Read the YAML config, resolve paths and add Postgres connection details from the environment.

    `months` (from --months) replaces the config's month list for this run.
    """
    load_dotenv(REPO_ROOT / ".env")
    with open(_resolve(config_path), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    missing = [s for s in REQUIRED_SECTIONS if s not in cfg]
    if missing:
        raise ValueError(f"{config_path} is missing sections: {missing}")
    if months:
        cfg["months"] = months

    cfg["paths"] = {k: expand_path(cfg, v) for k, v in cfg["paths"].items()}
    cfg["input"]["pgn_pattern"] = expand_path(cfg, cfg["input"]["pgn_pattern"])

    pg = cfg["postgres"]
    pg["user"] = os.getenv("PGUSER") or getpass.getuser()
    pg["password"] = os.getenv("PGPASSWORD", "")
    pg["jdbc_url"] = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}"
    return cfg


def pgn_path(cfg: dict[str, Any], month: str) -> str:
    """Input PGN path for one dump month."""
    return cfg["input"]["pgn_pattern"].format(month=month)


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Argument parser with the --config and --months arguments every pipeline step takes."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", required=True, help="path to config/local.yaml or config/cluster.yaml")
    parser.add_argument("--months", type=lambda s: [m.strip() for m in s.split(",") if m.strip()],
                        help="comma-separated months (e.g. 2026-08,2026-09); replaces the config's list")
    return parser
