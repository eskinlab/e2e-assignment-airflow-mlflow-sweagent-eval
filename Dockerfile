FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    docker.io \
    git \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# The .git dir is bind-mounted at runtime (see dags/evaluate_agent.py), owned by whatever
# UID the host repo has - without this, git refuses it as "dubious ownership" and
# pipeline.config._git_sha() silently falls back to None.
RUN git config --system --add safe.directory /mlops-assignment

WORKDIR /mlops-assignment

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

ENV PATH="/mlops-assignment/.venv/bin:$PATH"

COPY scripts scripts/
COPY pipeline pipeline/

# Optional but useful if your script lacks executable bit or shebang issues:
RUN chmod +x scripts/*.sh

ENTRYPOINT ["python", "-m", "pipeline.cli"]
