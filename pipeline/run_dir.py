"""Filesystem layout for a single run: runs/<run_id>/..."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

RUNS_ROOT = Path(__file__).resolve().parents[1] / "runs"


def run_dir_for(run_id: str) -> Path:
    return RUNS_ROOT / run_id


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def prepare_run_dir(run_config: dict[str, Any]) -> Path:
    run_dir = run_dir_for(run_config["run_id"])
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "config.json", run_config)
    return run_dir


def write_manifest(
    run_dir: Path,
    run_config: dict[str, Any],
    agent_result: dict[str, Any],
    eval_result: dict[str, Any],
    metrics: dict[str, Any],
    mlflow_info: dict[str, Any],
) -> Path:
    """Point at every file needed to reconstruct this run from the folder alone.

    agent_result/eval_result already store their paths relative to run_dir
    (see agent.py/eval.py), so this just re-shapes them - no path resolution
    happens here, which keeps this immune to cwd/mount differences between
    the steps that produced them and the step that writes this file.
    """
    trajectories_dir = agent_result.get("trajectories_dir")
    log_path = f"{trajectories_dir}/minisweagent.log"
    if not (run_dir / log_path).exists():
        log_path = None

    manifest = {
        "run_id": run_config.get("run_id"),
        "created_at": run_config.get("created_at"),
        "git_sha": run_config.get("git_sha"),
        "config_path": "config.json",
        "run_agent": {
            "preds_path": agent_result.get("preds_path"),
            "output_dir": trajectories_dir,
            "log_path": log_path,
            "returncode": agent_result.get("returncode"),
        },
        "run_eval": {
            "summary_path": eval_result.get("summary_path"),
            "eval_dir": eval_result.get("eval_dir"),
            "returncode": eval_result.get("returncode"),
        },
        "metrics_path": "metrics.json",
        "metrics_summary": metrics,
        "mlflow": mlflow_info,
        "remote_artifact_uri": None,
    }
    manifest_path = run_dir / "manifest.json"
    write_json(manifest_path, manifest)
    return manifest_path


def update_manifest_remote_uri(run_dir: Path, remote_artifact_uri: str) -> Path:
    manifest_path = run_dir / "manifest.json"
    manifest = read_json(manifest_path)
    manifest["remote_artifact_uri"] = remote_artifact_uri
    write_json(manifest_path, manifest)
    return manifest_path
