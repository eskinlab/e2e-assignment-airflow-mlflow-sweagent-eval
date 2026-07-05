set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

# Phase 3: the DAG's tasks are DockerOperator, run against the host's Docker daemon
# (docker-outside-of-docker - this process itself is not containerized, so
# HOST_PROJECT_ROOT is simply this checkout's own path as the daemon sees it).
export HOST_PROJECT_ROOT=$(pwd)
export PIPELINE_IMAGE=${PIPELINE_IMAGE:-mlops-assignment-pipeline:latest}
export PIPELINE_DOCKER_NETWORK=${PIPELINE_DOCKER_NETWORK:-bridge}

if ! docker image inspect "$PIPELINE_IMAGE" >/dev/null 2>&1; then
  echo "Building pipeline image $PIPELINE_IMAGE ..."
  docker build -t "$PIPELINE_IMAGE" .
fi

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

uv tool run --with apache-airflow-providers-docker apache-airflow standalone
