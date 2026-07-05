"""Shared subprocess-running helper for pipeline steps.

Only covers what agent.py and eval.py have in common (run + persist
stdout/stderr next to the step's output); each caller still decides for
itself what counts as success, since that condition differs per step.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


def run_logged(cmd: list[str], out_dir: Path, **kwargs: Any) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    (out_dir / "stdout.log").write_text(result.stdout, encoding="utf-8")
    (out_dir / "stderr.log").write_text(result.stderr, encoding="utf-8")
    return result
