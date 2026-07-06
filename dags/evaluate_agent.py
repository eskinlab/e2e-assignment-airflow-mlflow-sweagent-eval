"""Configurable run-agent -> run-eval -> summarize-and-log -> upload pipeline for
evaluating an agent harness (mini-swe-agent by default; see `pipeline/agents/` for other
registered adapters) on a slice of SWE-bench.

Phase 3: every pipeline step runs as its own container (`DockerOperator`, built from the
project `Dockerfile`) instead of a bare `uv run` subprocess on the Airflow host. This DAG
module itself still only imports `airflow` + stdlib + `docker.types.Mount` - the actual
step logic (mini-swe-agent, swebench, mlflow, boto3) only needs to exist inside the
pipeline image, never in Airflow's own environment.

Docker-outside-of-docker: each task container is a *sibling* of whatever container (or
host process) Airflow itself runs in, launched against the host's Docker daemon via the
mounted `/var/run/docker.sock` - not a nested container. That's why bind-mount sources
below must be paths that resolve on the *host*, not inside Airflow's own container/venv:
`HOST_PROJECT_ROOT` carries that host path explicitly (see docker-compose.yaml /
run-airflow-standalone.sh, both of which set it to the repo's location as the host sees
it). run_eval (SWE-bench harness) and run_agent (`--environment-class docker`) both also
launch their own per-instance containers via `docker.from_env()`, which is the other
reason the docker socket has to be reachable from inside these task containers too.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Mirrors pipeline.config._sanitize_run_id - duplicated here (rather than imported) so
# this DAG file keeps its "stdlib + airflow only" dependency footprint; pipeline/config.py
# re-applies the same regex to whatever run_id it's handed, so sanitizing here too is a
# harmless no-op from its point of view, not a second source of truth.
_RUN_ID_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _sanitize_run_id(raw: str) -> str:
    return _RUN_ID_SANITIZE_RE.sub("-", raw).strip("-") or "run"


CONTAINER_PROJECT_ROOT = "/mlops-assignment"
RUN_DIR_TEMPLATE = f"{CONTAINER_PROJECT_ROOT}/runs/{{{{ ti.xcom_pull(task_ids='resolve_run_id') }}}}"

PIPELINE_IMAGE = os.environ.get("PIPELINE_IMAGE", "mlops-assignment-pipeline:latest")
# Host-side path to this repo checkout, as the Docker daemon itself sees it - see the
# module docstring. Falls back to this file's own on-disk location, which is only correct
# when Airflow runs directly on the same host/VM as the Docker daemon (e.g. the
# `run-airflow-standalone.sh` path), never when Airflow itself runs inside a container.
HOST_PROJECT_ROOT = os.environ.get("HOST_PROJECT_ROOT") or str(Path(__file__).resolve().parents[1])
PIPELINE_DOCKER_NETWORK = os.environ.get("PIPELINE_DOCKER_NETWORK", "bridge")

_ENV_PASSTHROUGH = (
    "NEBIUS_API_KEY",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_EXPERIMENT_NAME",
    "S3_BUCKET",
    "S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
)
COMMON_ENVIRONMENT = {k: os.environ[k] for k in _ENV_PASSTHROUGH if k in os.environ}

_HOST_ROOT = Path(HOST_PROJECT_ROOT)
COMMON_MOUNTS = [
    # Only runs/ (read-write, the actual state that must survive between task containers)
    # and .git (read-only, for pipeline.config's git_sha capture) - NOT the whole project
    # root. Bind-mounting the whole repo over /mlops-assignment would shadow the image's
    # own baked .venv (from `uv sync --locked` at build time) with whatever .venv happens
    # to sit on the host, which may not even be the right OS/architecture - breaking
    # `python` inside the container entirely. Verified: this is exactly what happened
    # before this was narrowed down to just these two paths.
    Mount(source=str(_HOST_ROOT / "runs"), target=f"{CONTAINER_PROJECT_ROOT}/runs", type="bind"),
    Mount(source=str(_HOST_ROOT / ".git"), target=f"{CONTAINER_PROJECT_ROOT}/.git", type="bind", read_only=True),
    Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
]


def _pipeline_step(
    task_id: str,
    command: list[str],
    execution_timeout: timedelta,
    retries: int = 0,
    retry_delay: timedelta | None = None,
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=PIPELINE_IMAGE,
        command=command,
        working_dir=CONTAINER_PROJECT_ROOT,
        mounts=COMMON_MOUNTS,
        environment=COMMON_ENVIRONMENT,
        network_mode=PIPELINE_DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        do_xcom_push=False,
        execution_timeout=execution_timeout,
        retries=retries,
        retry_delay=retry_delay or timedelta(minutes=1),
    )


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string", description="SWE-bench dataset split"),
        "subset": Param(
            "verified",
            type="string",
            enum=["lite", "verified", "full"],
            description="SWE-bench subset",
        ),
        "workers": Param(5, type="integer", minimum=1, description="Parallel workers for agent + eval"),
        # enum mirrors pipeline.agents.HARNESS_ADAPTERS' keys - keep in sync when adding adapters.
        "harness": Param(
            "mini-swe-agent",
            type="string",
            enum=["mini-swe-agent"],
            description="Agent harness to run (see pipeline/agents/ for available adapters)",
        ),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string", description="LiteLLM model id"),
        "task_slice": Param("0:3", type="string", description="Instance slice, e.g. '0:3'"),
        "run_id": Param(
            None,
            type=["string", "null"],
            description="Optional explicit run id; auto-generated if blank",
        ),
        "cost_limit": Param(
            3.0,
            type="number",
            description=(
                "Per-instance USD cost limit. Note: the reference "
                "scripts/mini-swe-bench-batch.sh ran unbounded; this pipeline "
                "applies a safety cap by default."
            ),
        ),
    },
    tags=["swe-bench", "agent-eval"],
)
def evaluate_agent():
    # Both of these are plain Python (no Docker needed) so the run_dir every DockerOperator
    # task below needs can be built from a single templated string (RUN_DIR_TEMPLATE)
    # instead of parsing a container's JSON stdout out of XCom.
    @task
    def resolve_run_id() -> str:
        context = get_current_context()
        params = context["params"]
        raw = str(params.get("run_id") or "").strip() or context["dag_run"].run_id
        return _sanitize_run_id(raw)

    @task
    def build_params_json() -> str:
        context = get_current_context()
        return json.dumps(dict(context["params"]))

    run_id = resolve_run_id()
    params_json = build_params_json()

    # prepare_run has no retries: it only writes local config from Airflow params, so a
    # failure is a real bug (bad params, disk issue), not something a retry fixes.
    prepare_run = _pipeline_step(
        "prepare_run",
        [
            "prepare-run",
            "--params-json",
            "{{ ti.xcom_pull(task_ids='build_params_json') }}",
            "--fallback-run-id",
            "{{ ti.xcom_pull(task_ids='resolve_run_id') }}",
        ],
        execution_timeout=timedelta(minutes=5),
    )

    # run_agent/run_eval: 1 retry with a long delay. The one real run so far failed on a
    # cold Docker image pull exceeding mini-swe-agent's container-start timeout (see
    # REPORT.md) - a transient infra condition a retry actually resolves once the image
    # is cached, not a logic error worth failing the whole DAG over.
    run_agent = _pipeline_step(
        "run_agent",
        ["run-agent", "--run-dir", RUN_DIR_TEMPLATE],
        execution_timeout=timedelta(hours=4),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    run_eval = _pipeline_step(
        "run_eval",
        ["run-eval", "--run-dir", RUN_DIR_TEMPLATE],
        execution_timeout=timedelta(hours=2),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # summarize_and_log/upload_artifacts: 2 retries, short delay - their expected failure
    # mode is a network/API blip against MLflow or S3, not a logic error.
    summarize_and_log = _pipeline_step(
        "summarize_and_log",
        ["summarize", "--run-dir", RUN_DIR_TEMPLATE],
        execution_timeout=timedelta(minutes=15),
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    upload_artifacts = _pipeline_step(
        "upload_artifacts",
        ["upload", "--run-dir", RUN_DIR_TEMPLATE],
        execution_timeout=timedelta(minutes=15),
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    [run_id, params_json] >> prepare_run >> run_agent >> run_eval >> summarize_and_log >> upload_artifacts


evaluate_agent()
