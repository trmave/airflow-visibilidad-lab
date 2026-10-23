"""DAG de laboratorio: DAG que se queda corriendo mas de una hora (panel de running >1h)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-bi",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_03_reporte_lento",
    description="Reporte ejecutivo pesado: 2 horas de proceso",
    start_date=datetime(2026, 10, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "lento", "critico"],
) as dag:
    backup = BashOperator(task_id="backup_origen", bash_command="echo 'respaldando origen'; sleep 10")
    calculo = BashOperator(task_id="calculo_agregados", bash_command="echo 'calculando agregados de 24 meses...'; sleep 7200")
    publicar = BashOperator(task_id="publicar_reporte", bash_command="echo 'publicando PDF'")
    backup >> calculo >> publicar
