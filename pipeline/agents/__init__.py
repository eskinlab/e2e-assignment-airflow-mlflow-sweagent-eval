"""Registry of agent-harness adapters for the `run-agent` step.

The rest of the pipeline (`pipeline/eval.py`, `pipeline/metrics.py`,
`pipeline/mlflow_logging.py`, `pipeline/upload.py`, `pipeline/cli.py`) only ever consumes what an
adapter produces, never how it produced it - swapping the harness (mini-swe-agent, or something
else later) while holding the model fixed should not require touching any of those. Every
adapter registered here must satisfy this contract:

- Signature: `run_agent_batch(run_config: dict, run_dir: Path) -> dict`.
- Must write `run_dir/run-agent/preds.json` in the SWE-bench standard shape
  (`instance_id -> {model_name_or_path, instance_id, model_patch}`).
- Must return `{"preds_path": <relative path>, "trajectories_dir": <relative path>,
  "returncode": int}` and persist that same dict via `pipeline.run_dir.write_json` as
  `run-agent/_result.json` - this is what `run_eval`/`summarize` read next.
- Should use `pipeline.subprocess_utils.run_logged` (or equivalent) so `stdout.log`/`stderr.log`
  land next to the rest of that step's output, matching the existing convention.
- Must raise (not silently emit an empty `preds.json`) when the harness produced nothing usable.

Note for future adapters: unlike mini-swe-agent, most other coding agents (Claude Code, Codex,
OpenCode, Cursor, ...) have no built-in "run a slice of SWE-bench" batch command. Wiring one of
those up needs its own loop over dataset instances, per-instance container handling, and patch
extraction (e.g. `git diff`) to assemble `preds.json` - that shared batch-runner glue doesn't
exist yet and isn't provided by this registry alone.

HARNESS_ADAPTERS maps a harness name to its adapter *module path* rather than an already-imported
callable, and `run_agent_batch` below imports that one module lazily, on dispatch. This keeps
adapters isolated from each other: `pipeline.config` imports this module just to validate a
harness name against `HARNESS_ADAPTERS.keys()`, and every pipeline subcommand imports
`pipeline.config` unconditionally - if adapters were imported eagerly here, one harness's broken
or missing optional dependency (e.g. a future Claude Code adapter's SDK package not installed in
this image) would raise ImportError for every run, regardless of which harness it selected.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

HARNESS_ADAPTERS: dict[str, str] = {
    "mini-swe-agent": "pipeline.agents.mini_swe_agent",
}


def run_agent_batch(run_config: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    harness = run_config["harness"]
    try:
        module_path = HARNESS_ADAPTERS[harness]
    except KeyError as exc:
        raise ValueError(
            f"Unknown harness {harness!r}; expected one of {sorted(HARNESS_ADAPTERS)}"
        ) from exc
    adapter_module = importlib.import_module(module_path)
    return adapter_module.run_agent_batch(run_config, run_dir)
