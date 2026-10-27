.DEFAULT_GOAL := help
PYTHON ?= python
COMPOSE ?= docker compose

.PHONY: help venv install-dev test lint format up down clean logs ps batch dbt-run dbt-test rabbitmq-mode

help: ## Lista los comandos disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

venv: ## Crea el entorno virtual local
	$(PYTHON) -m venv .venv

install-dev: ## Instala dependencias para tests y lint
	$(PYTHON) -m pip install -r requirements-dev.txt

test: ## Corre los tests unitarios (no requieren infraestructura)
	$(PYTHON) -m pytest

lint: ## Lint con ruff
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format: ## Formatea el código
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

up: ## Levanta todo el stack
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build

down: ## Baja el stack (conserva volúmenes)
	$(COMPOSE) down

clean: ## Baja el stack y borra volúmenes
	$(COMPOSE) down -v

logs: ## Sigue los logs de los servicios de la app
	$(COMPOSE) logs -f stream-processor dashboard event-producer airflow

ps: ## Estado de los servicios
	$(COMPOSE) ps

batch: ## Carga incremental manual de todas las entidades (dentro del contenedor de Airflow)
	for e in customers products order_lines; do \
		$(COMPOSE) exec airflow bash -c "cd /opt/pipeline && python -m batch.run --entity $$e"; \
	done

dbt-run: ## dbt run dentro del contenedor de Airflow
	$(COMPOSE) exec airflow dbt run --project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt --target-path /tmp/dbt-target --log-path /tmp/dbt-logs

dbt-test: ## dbt test dentro del contenedor de Airflow
	$(COMPOSE) exec airflow dbt test --project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt --target-path /tmp/dbt-target --log-path /tmp/dbt-logs

rabbitmq-mode: ## Reinicia producer y processor usando RabbitMQ en lugar de Kafka
	STREAM_BROKER=rabbitmq $(COMPOSE) up -d --force-recreate event-producer stream-processor
