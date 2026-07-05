"""Run the SWE-bench harness against agent predictions (the `run-eval` step)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.run_dir import write_json
from pipeline.subprocess_utils import run_logged


def run_swebench_eval(run_config: dict[str, Any], preds_path: str, run_dir: Path) -> dict[str, Any]:
    """`preds_path` is relative to run_dir (as written by run_agent_batch)."""
    eval_dir = run_dir / "run-eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "python",
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        run_config["dataset_name"],
        "--split",
        run_config["split"],
        "--predictions_path",
        str((run_dir / preds_path).resolve()),
        "--max_workers",
        str(run_config["workers"]),
        "--run_id",
        run_config["run_id"],
    ]

    result = run_logged(cmd, eval_dir, cwd=eval_dir)

    summary_path = eval_dir / f"{run_config['model'].replace('/', '__')}.{run_config['run_id']}.json"
    if result.returncode != 0 or not summary_path.exists():
        raise RuntimeError(
            f"swebench.harness.run_evaluation failed (exit {result.returncode}) or did not produce "
            f"the expected summary {summary_path.name}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    eval_result = {
        "summary_path": summary_path.relative_to(run_dir).as_posix(),
        "eval_dir": eval_dir.relative_to(run_dir).as_posix(),
        "returncode": result.returncode,
    }
    write_json(eval_dir / "_result.json", eval_result)
    return eval_result
