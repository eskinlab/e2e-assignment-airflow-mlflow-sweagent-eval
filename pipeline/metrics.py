"""Turn a SWE-bench evaluation summary into flat, MLflow-friendly metrics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.run_dir import read_json

_COUNT_FIELDS = (
    "total_instances",
    "submitted_instances",
    "completed_instances",
    "resolved_instances",
    "unresolved_instances",
    "empty_patch_instances",
    "error_instances",
)


def collect_metrics(summary_path: str | Path) -> dict[str, Any]:
    summary = read_json(Path(summary_path))

    metrics: dict[str, Any] = {field: summary.get(field, 0) for field in _COUNT_FIELDS}

    submitted = metrics["submitted_instances"]
    metrics["resolve_rate"] = (metrics["resolved_instances"] / submitted) if submitted else 0.0

    return metrics
