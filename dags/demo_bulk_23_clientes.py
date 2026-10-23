"""DAG de relleno generado por scripts/generate_demo_dags.py"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "equipo-datos",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

FALLA = True


def paso_negocio(**_):
    """Falla de forma determinista según la fecha y la tasa del DAG."""
    from datetime import datetime as _dt
    if FALLA and _dt.utcnow().hour % 3 == 0:
        raise RuntimeError("dependencia externa no disponible (503)")
    print("paso de negocio completado")


with DAG(
    dag_id="demo_bulk_23_clientes",
    description="Actualización de maestros de cliente (#23)",
    start_date=datetime(2026, 9, 15),
    schedule="15 */4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "bulk", "clientes"],
) as dag:
    extraer = BashOperator(task_id="extraer", bash_command="echo extrayendo; sleep 2")
    transformar = PythonOperator(task_id="transformar", python_callable=paso_negocio)
    cargar = BashOperator(task_id="cargar", bash_command="echo cargando; sleep 2")
    extraer >> transformar >> cargar
