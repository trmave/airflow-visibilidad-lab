"""DAG de laboratorio: Generacion de features para el modelo de churn."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "equipo-ml",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_06_ml_features",
    description="Features de churn: agrega, entrena y publica",
    start_date=datetime(2026, 10, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "ml"],
) as dag:
    features = BashOperator(task_id="build_features", bash_command="echo 'construyendo features'; sleep 20")
    entrenar = BashOperator(task_id="train_model", bash_command="echo 'entrenando XGBoost'; sleep 30")
    publicar = BashOperator(task_id="publish_model", bash_command="echo 'publicando modelo'; sleep 5")
    features >> entrenar >> publicar
