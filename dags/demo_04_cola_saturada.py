"""DAG de laboratorio: tarea de 70 min con max_active_runs=1.

Cumple dos papeles en la demo: genera dag_runs 'queued' (por max_active_runs=1)
y es el único que supera la hora, para que el KPI "Corriendo > 1 hora" tenga
un caso real que enseñar.
"""
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
    # 70 minutos: supera el umbral de 1 hora que vigila el tablero.
    sync = BashOperator(task_id="sync_erp", bash_command="echo 'sincronizando ERP'; sleep 4200")
