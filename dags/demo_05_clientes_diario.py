"""DAG de laboratorio: Carga diaria de clientes con fallo controlado 1 de cada 4 dias."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-datos",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

from airflow.operators.python import PythonOperator


def cargar_clientes(**_):
    from datetime import datetime as _dt

    if _dt.utcnow().day % 4 == 0:
        raise RuntimeError("Timeout del origen CRM (504)")
    print("clientes cargados: 8421")


with DAG(
    dag_id="demo_05_clientes_diario",
    description="Carga diaria de clientes desde el CRM",
    start_date=datetime(2026, 10, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "critico"],
) as dag:
    extraer = BashOperator(task_id="extraer_crm", bash_command="echo 'extrayendo CRM'; sleep 5")
    cargar = PythonOperator(task_id="cargar", python_callable=cargar_clientes)
    extraer >> cargar
