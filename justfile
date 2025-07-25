_default:
    @just --choose -f {{ justfile() }} -d {{ invocation_directory() }}


install:
    uv lock --upgrade
    uv sync --all-extras --frozen
    @just hook


# - ruff format: Python 코드 포맷팅 
# - ruff check --fix: 린터 실행 및 수정 가능한 문제 자동 수정
# - mypy: 타입 어노테이션이 올바른지 확인하는 타입 체커
lint:
    uv run ruff format
    uv run ruff check --fix
    uv run mypy .

#   just test                    
#   just test tests/test_api.py  
#   just test -v                 vervose 
#   just test -k "test_login"    (패턴에 맞는 테스트 실행)
test *args:
    uv run --no-sync pytest {{ args }}

##### todo

# airflow dags report

# AIRFLOW
# init airflow db
airflow-init:
    @echo "Initializing Airflow Database..."
    uv run airflow db migrate
    @echo "Creating default user..."
    uv run airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

airflow-db-migrate:
    @echo "Migrating Airflow Database..."
    uv run airflow db migrate

airflow-webserver:
    uv run airflow webserver --port 8080

airflow-scheduler:
    uv run airflow scheduler

airflow-start:
    @echo "Airflow 웹서버 및 스케줄러 시작..."
    uv run airflow scheduler & uv run airflow webserver --port 8080

# Dags

# show all DAGs
airflow-dags:
    uv run airflow dags list

# DAG 실행 상태 확인
airflow-status:
    @echo "DAG 실행 상태 확인 중..."
    uv run airflow dags state

# DAG의 특정 작업 테스트
# 사용법: just airflow-test my_dag_id my_task_id 2024-01-01
airflow-test dag_id task_id execution_date:
    @echo "작업 테스트 중: {{task_id}} (DAG: {{dag_id}})..."
    uv run airflow tasks test {{dag_id}} {{task_id}} {{execution_date}}

run:
  uv run -m src.tui.main


# add git pre-commit hook
hook:
    uv run pre-commit install --install-hooks --overwrite

# unhook git pre-commit 
unhook:
    uv run pre-commit uninstall

# start mkdocs 개발 서버
docs:
    uv pip install -r requirements.txt
    uv run mkdocs serve

venv:
  @echo "Activating virtual environment..."
  @. .venv/bin/activate && exec $SHELL

# Deployment

status:
    @echo "Python version:"
    uv run python --version
    @echo "installed packages:"
    uv pip list
    @echo "Git status:"
    git status --short

# install, lint, test
dev-setup: install lint test
    @echo "Development environment setup complete."

