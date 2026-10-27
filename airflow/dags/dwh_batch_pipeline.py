"""DAG de carga batch al Data Warehouse.

extract_load (por entidad, en paralelo) -> dbt source freshness -> dbt run -> dbt test

Cada tarea de extracción es idempotente: lee su watermark, extrae el delta,
hace MERGE a raw y avanza el watermark en la misma transacción. Si falla a
mitad de camino, el retry vuelve a arrancar desde el último watermark confirmado.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

PIPELINE_HOME = os.environ.get("PIPELINE_HOME", "/opt/pipeline")
DBT_DIR = f"{PIPELINE_HOME}/dbt"
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
ENTITIES = ("customers", "products", "order_lines")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="dwh_batch_pipeline",
    description="Carga incremental OLTP + API REST a SQL Server y modelado con dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,  # dos corridas simultáneas competirían por el mismo watermark
    tags=["dwh", "batch", "dbt"],
) as dag:
    with TaskGroup(group_id="extract_load") as extract_load:
        for entity in ENTITIES:
            BashOperator(
                task_id=f"load_{entity}",
                bash_command=f"cd {PIPELINE_HOME} && python -m batch.run --entity {entity}",
                env={"PYTHONPATH": PIPELINE_HOME},
                append_env=True,
            )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"dbt source freshness {DBT_FLAGS}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
        retries=0,  # un test fallido es un problema de datos, reintentar no lo arregla
    )

    extract_load >> dbt_freshness >> dbt_run >> dbt_test
