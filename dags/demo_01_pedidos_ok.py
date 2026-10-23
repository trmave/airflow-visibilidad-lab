"""DAG de laboratorio: ETL de pedidos, termina siempre en exito."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-datos",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_01_pedidos_ok",
    description="ETL de pedidos: extract -> transform -> load (siempre OK)",
    start_date=datetime(2026, 10, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "ok", "critico"],
) as dag:
    extract = BashOperator(task_id="extract", bash_command="echo 'extrayendo pedidos del dia'; sleep 4")
    transform = BashOperator(task_id="transform", bash_command="echo 'transformando 1200 filas'; sleep 7")
    load = BashOperator(task_id="load", bash_command="echo 'cargando al data warehouse'; sleep 3")
    extract >> transform >> load
