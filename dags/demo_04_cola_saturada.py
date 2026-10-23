"""DAG de laboratorio: max_active_runs=1 con tarea lenta: genera dag_runs en estado queued."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-datos",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_04_cola_saturada",
    description="Sincronizacion con max_active_runs=1: los runs se encolan",
    start_date=datetime(2026, 10, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "cola"],
) as dag:
    sync = BashOperator(task_id="sync_erp", bash_command="echo 'sincronizando ERP'; sleep 1500")
