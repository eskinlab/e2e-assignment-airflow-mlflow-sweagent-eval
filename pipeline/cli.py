"""CLI entrypoint the Airflow DAG shells out to via `uv run python -m pipeline.cli <step> ...`.

Each step (other than prepare-run) takes only --run-dir and reads whatever it
needs from files already written into that run directory, so the run
directory stays the single source of truth and XCom payloads stay tiny.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.agent import run_agent_batch
from pipeline.config import build_run_config
from pipeline.eval import run_swebench_eval
from pipeline.metrics import collect_metrics
from pipeline.mlflow_logging import log_mlflow_run, log_remote_artifact_uri
from pipeline.run_dir import prepare_run_dir, read_json, update_manifest_remote_uri, write_json, write_manifest
from pipeline.upload import upload_artifacts


def _cmd_prepare_run(args: argparse.Namespace) -> dict:
    params = json.loads(args.params_json)
    run_config = build_run_config(params, fallback_run_id=args.fallback_run_id)
    run_dir = prepare_run_dir(run_config)
    return {"run_id": run_config["run_id"], "run_dir": str(run_dir)}


def _cmd_run_agent(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    run_config = read_json(run_dir / "config.json")
    agent_result = run_agent_batch(run_config, run_dir)
    return {"run_id": run_config["run_id"], "run_dir": str(run_dir), **agent_result}


def _cmd_run_eval(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    run_config = read_json(run_dir / "config.json")
    agent_result = read_json(run_dir / "run-agent" / "_result.json")
    eval_result = run_swebench_eval(run_config, agent_result["preds_path"], run_dir)
    return {"run_id": run_config["run_id"], "run_dir": str(run_dir), **eval_result}


def _cmd_summarize(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    run_config = read_json(run_dir / "config.json")
    agent_result = read_json(run_dir / "run-agent" / "_result.json")
    eval_result = read_json(run_dir / "run-eval" / "_result.json")

    summary_path = run_dir / eval_result["summary_path"]
    metrics = collect_metrics(summary_path)
    write_json(run_dir / "metrics.json", metrics)

    mlflow_info = log_mlflow_run(run_config, metrics, run_dir, summary_path)
    manifest_path = write_manifest(run_dir, run_config, agent_result, eval_result, metrics, mlflow_info)

    return {
        "run_id": run_config["run_id"],
        "run_dir": str(run_dir),
        "metrics": metrics,
        "manifest_path": str(manifest_path),
        **mlflow_info,
    }


def _cmd_upload(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    run_config = read_json(run_dir / "config.json")
    manifest = read_json(run_dir / "manifest.json")

    upload_result = upload_artifacts(run_config, run_dir)
    remote_artifact_uri = upload_result["remote_artifact_uri"]

    log_remote_artifact_uri(manifest["mlflow"]["mlflow_run_id"], remote_artifact_uri)
    update_manifest_remote_uri(run_dir, remote_artifact_uri)
    # The copy just uploaded above still has remote_artifact_uri: null (it was read off
    # disk before update_manifest_remote_uri patched it) - re-upload it alone so the
    # remote copy is self-describing too.
    upload_artifacts(run_config, run_dir, only=["manifest.json"])

    return {"run_id": run_config["run_id"], "run_dir": str(run_dir), **upload_result}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline.cli")
    subparsers = parser.add_subparsers(dest="step", required=True)

    prepare_run = subparsers.add_parser("prepare-run")
    prepare_run.add_argument("--params-json", required=True)
    prepare_run.add_argument("--fallback-run-id", required=True)
    prepare_run.set_defaults(func=_cmd_prepare_run)

    run_agent = subparsers.add_parser("run-agent")
    run_agent.add_argument("--run-dir", required=True)
    run_agent.set_defaults(func=_cmd_run_agent)

    run_eval = subparsers.add_parser("run-eval")
    run_eval.add_argument("--run-dir", required=True)
    run_eval.set_defaults(func=_cmd_run_eval)

    summarize = subparsers.add_parser("summarize")
    summarize.add_argument("--run-dir", required=True)
    summarize.set_defaults(func=_cmd_summarize)

    upload = subparsers.add_parser("upload")
    upload.add_argument("--run-dir", required=True)
    upload.set_defaults(func=_cmd_upload)

    args = parser.parse_args(argv)
    result = args.func(args)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
