"""DAG de laboratorio: ETL que revienta a proposito para poblar el panel de fallos."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-datos",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

from airflow.operators.python import PythonOperator


def validar_esquema(**_):
    raise ValueError("Esquema origen cambiado: falta la columna 'importe_neto'")


with DAG(
    dag_id="demo_02_etl_fallido",
    description="ETL de facturacion que falla en la validacion de esquema",
    start_date=datetime(2026, 10, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "fallo"],
) as dag:
    extract = BashOperator(task_id="extract", bash_command="echo 'descargando facturas'; sleep 3")
    validar = PythonOperator(task_id="validar_esquema", python_callable=validar_esquema)
    load = BashOperator(task_id="load", bash_command="echo 'no deberia llegar aqui'")
    extract >> validar >> load
