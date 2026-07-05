# Testing

Phase 1 (configurable DAG) below; Phase 3 (DockerOperator / docker-compose / S3 upload) at
the bottom.

Local dev setup for this repo: Airflow runs inside WSL2 Ubuntu (not native Windows —
Airflow is POSIX-only), Docker Desktop with WSL integration enabled for that distro.

## Prerequisites

- WSL2 Ubuntu distro, repo checked out under `/mnt/d/...` (or wherever it lives on the
  Windows filesystem) so both WSL and Windows tools can read `runs/`.
- Docker Desktop → Settings → Resources → WSL Integration → enabled for the Ubuntu distro,
  then **Apply & Restart**.
- `.env` in the project root with `NEBIUS_API_KEY` set.

## 1. Start Airflow

From a fresh WSL terminal (fresh login session, so group membership like `docker` is
picked up):

```bash
cd /mnt/d/.../mlops-assignment-e2e-ml-pipeline
bash run-airflow-standalone.sh
```

If `uv` hangs on `Resolving dependencies...` and eventually fails with
`Failed to fetch: https://pypi.org/simple/...` / connection timeout, and `apache-airflow`
has already been resolved once before (check with
`UV_OFFLINE=1 uv tool run apache-airflow version`), just skip network resolution:

```bash
export UV_OFFLINE=1
bash run-airflow-standalone.sh
```

(Root cause seen locally: WSL2's IPv6 route was broken after a Docker Desktop restart —
plain `curl` to pypi.org hung, `curl -4` worked instantly. Confirmed via
`curl -sS -m 8 -o /dev/null -w "http_code=%{http_code}\n" https://pypi.org/simple/apache-airflow/`.
`UV_OFFLINE=1` sidesteps it entirely when the cache already has what's needed; otherwise fix
IPv6 with `sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1`.)

Login to the UI at http://localhost:8080 with `admin` / `admin` (written to
`~/airflow/simple_auth_manager_passwords.json.generated` by the script).

## 2. Confirm the DAG parses with no errors

```bash
uv tool run apache-airflow dags list-import-errors
```

Expected output: `No data found`.

## 3. Cheap CLI-level check (no Airflow, no API cost)

Exercises `pipeline/config.py` + `pipeline/run_dir.py` directly:

```bash
uv run python -m pipeline.cli prepare-run \
  --params-json '{"split":"test","subset":"verified","workers":1,"model":"nebius/moonshotai/Kimi-K2.6","task_slice":"0:1","run_id":"","cost_limit":3.0}' \
  --fallback-run-id smoketest
```

Check `runs/smoketest/config.json`: `subset` should resolve to the right
`dataset_name`, plus a real `git_sha` and `created_at`. Try a run_id with spaces/slashes
too, to confirm `_sanitize_run_id` behaves.

## 4. Trigger a real, minimal end-to-end DAG run

Keep `task_slice`/`workers` small to bound cost and time.

Via the UI (Airflow 3.x — no separate "Trigger w/ config" menu, the ▶ button opens the
config modal directly): open the `evaluate_agent` DAG → click ▶ (Trigger) → enter in
**Configuration JSON**:

```json
{ "task_slice": "0:1", "workers": 1 }
```

Or via CLI:

```bash
uv tool run apache-airflow dags trigger evaluate_agent --conf '{"task_slice":"0:1","workers":1}'
```

## 5. Watch the run

Tasks run in order: `prepare_run` → `run_agent` → `run_eval` → `summarize_and_log`. All
four should go green. On a red task, open its **Logs** tab — that's the raw subprocess
stdout/stderr from `_run_pipeline_step`, so the real Python traceback is right there.

## 6. Known gotcha: `run_eval` needs a working Docker daemon

`run_eval` shells out to the SWE-bench harness, which calls `docker.from_env()`.

- `docker.errors.DockerException: ... FileNotFoundError` → Docker Desktop's WSL
  integration isn't enabled for this distro (see Prerequisites above).
- `docker.errors.DockerException: ... PermissionError(13, 'Permission denied')` → the
  socket now exists and your user is in the `docker` group, but the *current shell
  session* predates that group membership. Open a brand-new terminal (new login session)
  and retry — don't reuse the old one.

After fixing, retry just the failed task: in the UI, click `run_eval` → **Clear** (this
also re-runs the downstream `summarize_and_log`). No need to redo `prepare_run`/`run_agent`
— their output (`preds.json`) is already on disk.

## 7. What "passing" looks like

All four tasks green, and `runs/<run_id>/config.json` + `run-agent/_result.json`
(`returncode: 0`) + `run-eval/_result.json` (`returncode: 0`) all present — confirms Phase
1's param wiring and subprocess step contract work end to end. (Checking the full
`manifest.json`/MLflow artifact shape is Phase 2's concern.)

---

# Testing Phase 3 (DockerOperator / docker-compose / S3 upload)

The container mechanics below (build, mounts, `prepare-run` -> `summarize` -> `upload`
against real `mlflow`/`minio` services, verified via their own APIs) have been exercised
directly with Docker Desktop and are confirmed working — see REPORT.md's "Scope and known
limitations" for the two bugs that exercise caught and fixed (mount shadowing `.venv`;
missing `git`). Not yet exercised: an actual Airflow-triggered `evaluate_agent` run (Airflow
doesn't run on native Windows — this checklist's steps 3-4 still need a real WSL2/Linux run).

**If you're testing steps 1-2 from native Windows git-bash** (not WSL2): git-bash's MSYS
layer silently rewrites anything that looks like an absolute POSIX path — both `-v`
sources *and* plain arguments like `--run-dir /mlops-assignment/runs/<id>` — into a
mangled Windows path before `docker` ever sees it, with no error. Prefix the command with
`MSYS_NO_PATHCONV=1` (or run from WSL2, which doesn't have this problem). This is purely a
local-shell quirk — Airflow's real `DockerOperator` talks to the daemon via `docker-py`'s
HTTP API, never through a shell, so it never hits this.

## 1. Build the pipeline image

```bash
docker build -t mlops-assignment-pipeline:latest .
```

Re-run this after any change to `Dockerfile` or `pipeline/` — `DockerOperator` only ever
uses whatever image tag is already sitting in the local Docker daemon; it does not rebuild.

## 2. Cheap CLI-level check of the new `upload` step (no Airflow, no API cost)

Needs a MinIO (or other S3-compatible) endpoint reachable — easiest via compose:
`docker compose up -d minio`. Then, reusing the Phase-1 `smoketest` run dir (with a
`run-agent/_result.json` / `run-eval/_result.json` / `manifest.json` faked in, or a real
completed run):

```bash
export S3_BUCKET=mlops-assignment-runs S3_ENDPOINT_URL=http://localhost:9000 \
       AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
uv run python -m pipeline.cli upload --run-dir runs/<run_id>
```

Check the MinIO console (http://localhost:9001, `minioadmin`/`minioadmin`) for
`runs/<run_id>/...` under the bucket, and that `runs/<run_id>/manifest.json`'s
`remote_artifact_uri` is no longer `null`.

## 3. Easy mode: `run-airflow-standalone.sh` with DockerOperator tasks

Same as Phase 1's section 4-6 above, but the script now also builds the pipeline image (if
missing) and sets `HOST_PROJECT_ROOT`/`PIPELINE_IMAGE` before starting Airflow. Watch for:

- **`docker.errors.DockerException` inside a task's logs** → same WSL2/Docker-Desktop-
  integration gotcha as Phase 1's `run_eval`, just now hitting every task (they're all
  `DockerOperator` now), not only `run_eval`.
- **Mounted paths inside the task container look empty / wrong files** → `HOST_PROJECT_ROOT`
  mismatch. Since tasks run as *sibling* containers via the host Docker daemon (not nested),
  the bind-mount source must be the repo's path as the **host** (not the Airflow process)
  sees it. `run-airflow-standalone.sh` sets this to `$(pwd)` on the assumption Airflow
  itself runs as a host/WSL2 process — true here, not true under `docker compose`.

## 4. Production-style mode: `docker compose up`

```bash
cp .env.example .env   # fill in NEBIUS_API_KEY, HOST_PROJECT_ROOT (repo path on the VM),
                        # DOCKER_GID (`getent group docker | cut -d: -f3`)
docker build -t mlops-assignment-pipeline:latest .
docker compose up -d
```

- Airflow UI: http://localhost:8080 (`admin`/`admin`). MLflow: http://localhost:5000.
  MinIO console: http://localhost:9001.
- **`airflow-scheduler` can't reach `/var/run/docker.sock` (permission denied)** → `DOCKER_GID`
  in `.env` doesn't match the host's `docker` group GID; re-check
  `getent group docker | cut -d: -f3` on the VM and restart the stack.
- **Task container can't resolve `mlflow`/`minio` by name** → `PIPELINE_DOCKER_NETWORK` (set
  automatically to `<project>_default` in `docker-compose.yaml`) doesn't match the actual
  compose network name if `COMPOSE_PROJECT_NAME`/the repo directory name was overridden;
  check with `docker network ls` and `docker compose config` and adjust `.env` if needed.
- **`_PIP_ADDITIONAL_REQUIREMENTS` slows every container start** — expected with this
  minimal compose setup (see REPORT.md); a custom Airflow image baking in
  `apache-airflow-providers-docker` would remove the reinstall-on-start cost, just not
  needed for a single-DAG assignment at this scale.

## 5. What "passing" looks like

All five tasks (`prepare_run` → `run_agent` → `run_eval` → `summarize_and_log` →
`upload_artifacts`) green under either mode, `runs/<run_id>/manifest.json`'s
`remote_artifact_uri` populated (not `null`), and that same URI visible as an MLflow param
on the run (`mlflow ui` or the `mlflow` service's UI).
