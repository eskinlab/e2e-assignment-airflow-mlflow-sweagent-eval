"""Build and resolve the configuration for a single evaluation run."""

from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Bundled config name mini-extra resolves against its own package config dir.
# See plan assumptions: the batch script's literal relative path only works
# if mini-swe-agent is cloned *inside* this repo, which the README doesn't ask
# for. Override via the `mswea_config` key in params if that turns out wrong.
DEFAULT_MSWEA_CONFIG = "swebench.yaml"

SUBSET_TO_DATASET = {
    "lite": "princeton-nlp/SWE-bench_Lite",
    "verified": "princeton-nlp/SWE-bench_Verified",
    "full": "princeton-nlp/SWE-bench",
}

_RUN_ID_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _sanitize_run_id(raw: str) -> str:
    return _RUN_ID_SANITIZE_RE.sub("-", raw).strip("-") or "run"


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return None


def resolve_dataset_name(subset: str) -> str:
    try:
        return SUBSET_TO_DATASET[subset]
    except KeyError as exc:
        raise ValueError(
            f"Unknown subset {subset!r}; expected one of {sorted(SUBSET_TO_DATASET)} "
            "or extend SUBSET_TO_DATASET in pipeline/config.py"
        ) from exc


def build_run_config(params: dict[str, Any], fallback_run_id: str) -> dict[str, Any]:
    """Resolve Airflow params into a fully-specified, JSON-serializable run config.

    `fallback_run_id` is used when params["run_id"] is blank (e.g. Airflow's
    own dag_run.run_id, already sanitized by the caller).
    """
    run_id = str(params.get("run_id") or "").strip() or fallback_run_id
    run_id = _sanitize_run_id(run_id)

    subset = str(params["subset"])
    return {
        "run_id": run_id,
        "split": str(params["split"]),
        "subset": subset,
        "dataset_name": resolve_dataset_name(subset),
        "workers": int(params["workers"]),
        "model": str(params.get("model") or "nebius/moonshotai/Kimi-K2.6"),
        "task_slice": str(params.get("task_slice") or "0:3"),
        "cost_limit": float(params.get("cost_limit") if params.get("cost_limit") not in (None, "") else 3.0),
        "mswea_config": str(params.get("mswea_config") or DEFAULT_MSWEA_CONFIG),
        "git_sha": _git_sha(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
