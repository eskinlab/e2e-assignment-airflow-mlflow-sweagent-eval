"""Run mini-swe-agent against a slice of SWE-bench (the `run-agent` step)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pipeline.run_dir import write_json
from pipeline.subprocess_utils import run_logged


def run_agent_batch(run_config: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    run_agent_dir = run_dir / "run-agent"
    run_agent_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "mini-extra",
        "swebench",
        "--subset",
        run_config["subset"],
        "--split",
        run_config["split"],
        "--model",
        run_config["model"],
        "--slice",
        run_config["task_slice"],
        "--workers",
        str(run_config["workers"]),
        "--config",
        run_config["mswea_config"],
        "--config",
        f"agent.cost_limit={run_config['cost_limit']}",
        "--environment-class",
        "docker",
        "-o",
        str(run_agent_dir),
    ]

    env = {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}
    result = run_logged(cmd, run_agent_dir, env=env)

    # A non-zero exit is tolerated as long as preds.json exists: mini-extra
    # swebench already handles per-instance failures internally (that's what
    # MSWEA_COST_TRACKING=ignore_errors is for), so a non-zero top-level exit
    # with predictions on disk usually just means some instances errored out.
    # No preds.json at all - regardless of exit code - means nothing ran.
    preds_path = run_agent_dir / "preds.json"
    if not preds_path.exists():
        raise RuntimeError(
            f"mini-extra swebench produced no preds.json (exit {result.returncode}).\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    agent_result = {
        "preds_path": preds_path.relative_to(run_dir).as_posix(),
        "trajectories_dir": run_agent_dir.relative_to(run_dir).as_posix(),
        "returncode": result.returncode,
    }
    write_json(run_agent_dir / "_result.json", agent_result)
    return agent_result
