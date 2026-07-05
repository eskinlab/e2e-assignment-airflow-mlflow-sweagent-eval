"""Log a completed run's params, metrics, and key artifact paths to MLflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import mlflow

DEFAULT_EXPERIMENT_NAME = "swe-bench-agent-eval"


def log_mlflow_run(run_config: dict[str, Any], metrics: dict[str, Any], run_dir: Path,
                    summary_path: str | Path) -> dict[str, Any]:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", DEFAULT_EXPERIMENT_NAME)
    mlflow.set_experiment(experiment_name)

    params = {k: v for k, v in run_config.items() if v is not None}
    numeric_metrics = {k: v for k, v in metrics.items() if isinstance(v, (int, float))}

    with mlflow.start_run(run_name=run_config["run_id"]) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(numeric_metrics)
        mlflow.set_tag("run_dir", str(run_dir.resolve()))
        mlflow.log_artifact(str(run_dir / "config.json"))
        mlflow.log_artifact(str(run_dir / "metrics.json"))
        mlflow.log_artifact(str(Path(summary_path)))

        return {
            "mlflow_run_id": run.info.run_id,
            "mlflow_experiment_id": run.info.experiment_id,
            "mlflow_tracking_uri": mlflow.get_tracking_uri(),
            "mlflow_artifact_uri": run.info.artifact_uri,
        }


def log_remote_artifact_uri(mlflow_run_id: str, remote_artifact_uri: str) -> None:
    """Re-open a completed run to attach the S3 URI produced by the later upload step."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    with mlflow.start_run(run_id=mlflow_run_id):
        mlflow.log_param("remote_artifact_uri", remote_artifact_uri)
