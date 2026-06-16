# Home assignment: Evaluation pipeline for coding-agent experiments

**What**: Home assignment.

**Where**: Nebius Academy course [AI Performance Engineering](https://academy.nebius.com/ai-engineering-il), MLOps module, lecture #6, "End-to-end ML pipeline".

**Author**: Simon Karasik.

**Learning objective**: Get hands-on experience turning an ad-hoc coding-agent evaluation script into an automated, observable, versioned, and durable Airflow pipeline with a structured data footprint: datasets, artifacts, metadata, metrics, logs, and trajectories.

**Inspired by**: https://github.com/GlebBerjoskin/mlops-assignment

---

## Legend

Imagine you are an MLOps engineer on a team that builds better coding agents. Think Claude Code, Codex, Cursor, OpenCode, mini-swe-agent, and similar systems.

Agent quality depends on two broad things:

1. **Harness**: the agent loop, prompts, tools, skills, retries, subagents, context management, and execution environment.
2. **Model**: the LLM that powers the harness, including decoding parameters and fine-tuned variants.

Your researchers want to experiment with both. Typical research loops look like this:

1. tweak a prompt or harness setting -> run the agent -> evaluate generated patches
2. fine-tune a model -> deploy it -> run the agent -> evaluate generated patches

Quality is measured on [SWE-bench](https://www.swebench.com/)-like tasks: the agent receives a real GitHub issue inside an isolated environment, tries to solve it, produces a patch, and the patch is judged by real unit tests.

Right now the researchers have several scripts on one VM. Someone SSHes in, runs them by hand, waits, copies logs, and pastes numbers into a doc. One experiment at a time. No queue. No durable run history. No reliable way to answer "which config produced this result?" or "why did this run fail?"

So, the team needs your help to turn these ad-hoc scripts into reliable, multi-user pipelines.

## Task

You are provided with ad-hoc scripts in `scripts/` to run [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) and evaluate the results using [SWE-bench](https://github.com/swe-bench/SWE-bench).

Sample outputs of `scripts/mini-swe-bench-batch.sh` and `scripts/swe-bench-eval.sh` are available in `sample/`.

Your goal is to turn these ad-hoc scripts from `scripts/` into a proper, configurable Airflow pipeline that implements the basic  `run-agent -> run-evaluation` workflow: run `mini-swe-agent` on a subset of SWE-bench instances and evaluate the results.

As a starting point with Airflow, you are provided with `run-airflow-standalone.sh` and a dag in `dags/` that re-implements `scripts/mini-swe-bench-single.sh`.

**Airflow pipeline requirements**:
- Configurable from Airflow parameters: `--split`, `--subset`, `--worker`. No hard-code.
- All run artifacts are properly structured. E.g.,
```
runs/
  <<run-id>>/
    run-agent/
      astropy__astropy-12907/
      preds.json
    run-eval/
```
- Run artifacts are saved to a remote long-term storage, such as Object Storage (S3).
- It's possible to re-construct the run based on the produced `<<run-id>>` folder: input SWE-bench tasks, configuration, output trajectories, etc. Basically, you can just send a directory to someone -- and they will be able to grab the whole picture.
- Airflow pipelines uses `DockerOperator` to run the scripts in isolated environments, instead of calling `uv run`. `Dockerfile` for the project is provided. In large-scale production, `DockerOperator` can be replaced with `KubernetesPodOperator`.
- Each run metrics and parameters are logged to `MLflow`, one can easily compare different runs.

**Deployment**
1. Airflow is deployed locally on a VM using `docker compose`: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#running-airflow-in-docker
2. MLflow is deployed locally as a part of the same `docker-compose.yaml`.

Ultimately, the pipeline may look like: `run-mini-swe-agent` -> `swe-bench-eval` -> `log-artifacts-to-s3` -> `log-metrics-to-mlflow`.

---

## Why This Matters

By the end of the assignment you should be able to:

- Model an ML experiment as a pipeline with explicit inputs, outputs, retries, and dependencies.
- Use Airflow for orchestration instead of manual shell ordering.
- Track experiment configs, datasets, model IDs, metrics, artifacts, and logs in MLflow.
- Run coding-agent evaluations in user-provided Docker images and collect reproducible outputs.
- Deploy and use the mini-swe-agent trajectory viewer to inspect what happened inside an agent run.
- Compare multiple experiments without losing track of which code, prompt, dataset, and model produced each result.

If done carefully, this assignment teaches the practical MLOps discipline that research code usually lacks: durability, repeatability, provenance, and operational visibility.

---

## Prerequisites

- A CPU VM with 8 CPU, 32 GB RAM, public IP. Can be created in Nebius.
- `NEBIUS_API_KEY` for Nebius Token Factory


You do not need a GPU VM for the orchestration parts. The inference part is handled by managed APIs.

---

## Phase 0: Setup

Create a VM with 8 CPU, 32 GB RAM, public IP. Add your public SSH key.

For simplicity, add this VM to your `~/.ssh/config`, for instance:

```
Host sbkarasik-academy-playground
  HostName 89.169.100.8
  User sbkarasik
  ForwardAgent yes
```

Connect to the VM. 

Install the basic tools:
```bash
# uv 
curl -LsSf https://astral.sh/uv/install.sh | sh

# Docker
# Add Docker's official GPG key:
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Let your user use `docker` without `sudo`
sudo usermod -aG docker "$USER"
sudo newgrp docker
```

Set up the starter repo:

```bash
git clone <repo-url>
cd <repo-folder>
uv sync
cp .env.example .env
```


Install the dependencies:
```bash
uv sync
```

Activate the venv: `source .venv/bin/activate`.

Add your `NEBIUS_API_KEY` to `.env`.

**Check your setup**:
- Run the script: `bash scripts/mini-swe-bench-single.sh`
- Via Airflow:
  - Run the Airflow: `bash run-airflow-standalone`
  - Forward port `8080` -- this is where Airflow is running.
    - VSCode/Cursor may do it automatically for you.
    - Pain SSH: `ssh -L 8080:localhost:8080 <user>@<vm-host>`.
  - Open it: http://localhost:8080
  - Try running the example DAG `mini-swe-bench-single`.


Congratulations! Your are all set.

## Final Deliverables

By the end of the mandatory assignment, your repo should contain:

| File or directory | What it is |
|---|---|
| `REPORT.md` | Final writeup |
| `infra/` or `docker-compose.yml` | Remote VM deployment for Airflow, MLflow, trajectory viewer, and code sync |
| `dags/evaluate_agent.py` | Configurable evaluation DAG |
| `src/` or `pipeline/` | Shared implementation code, operators, provider clients, config schemas |
| `configs/` | Reproducible configs for all evaluation runs, including execution image references |
| `.env.example` | Non-secret environment template for required services |
| `results/evaluate_agent_baseline.json` | Baseline evaluation result |
| `results/evaluate_agent_experiments.json` | Prompt and temperature experiment summary |
| `artifacts/` or external artifact URI references | Durable run evidence: task outputs, logs, trajectory references, or manifests |
| `screenshots/airflow_evaluate_agent.png` | Evaluation DAG run |
| `screenshots/mlflow_evaluate_agent.png` | MLflow comparison view for evaluation runs |
| `screenshots/mlflow_run_artifacts.png` | MLflow run with metrics, params, and artifacts |
| `screenshots/trajectory_viewer.png` | Trajectory viewer with an inspected mini-swe-agent run |

If artifacts are too large to commit, commit small indexes or manifests that point to their storage location.

Optional extension deliverables may include `dags/train_model.py`, `dags/train_model_and_evaluate_agent.py`, `results/fine_tuning_experiments.json`, and `screenshots/mlflow_fine_tuning.png`.

---

## Grading

We care more about engineering judgment and traceability than about one lucky metric. A weak result with excellent provenance and analysis is better than a pasted number nobody can reproduce.

| Area | Weight | What a strong submission shows |
|---|---:|---|
| **Remote Airflow deployment** | 15% | Airflow runs on a VM, DAG code updates automatically from Git/S3, and the setup can be reproduced without manual file edits on the VM. |
| **Docker execution model** | 15% | Pipeline actions run in user-provided Docker images, with image references controlled by config or another clear user-facing mechanism. |
| **Evaluation pipeline** | 25% | Configurable `evaluate-agent`, durable task-level evidence, SWE-bench-compatible judging, meaningful aggregate metrics, and no hidden giant script. |
| **MLflow tracking** | 15% | Runs log configs, metrics, artifacts or artifact references, code/data/model metadata, and comparison views for the experiments. |
| **Trajectory inspection** | 10% | mini-swe-agent trajectory viewer is deployed and used to inspect successful and failed runs. |
| **Experiment rigor** | 10% | Baseline, prompt, and temperature experiments are comparable, reproducible, and interpreted honestly. |
| **Report and runbook** | 10% | `REPORT.md` is concise, includes rerun instructions, explains failures, and states what would be improved next. |

