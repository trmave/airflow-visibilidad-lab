#!/usr/bin/env python3
"""Genera 40 DAGs de relleno para que el tablero tenga volumen realista.

Una demo con 6 DAGs no convence a nadie: un Airflow de cliente tiene decenas o
cientos. Estos DAGs son reales (archivos .py que el scheduler parsea, con sus
tareas y sus horarios), no filas inventadas en la tabla `dag`; así el panel
"DAGs activos" cuenta exactamente lo que Airflow tiene cargado.

Cada DAG tiene una tasa de fallo distinta y determinista (derivada de su
nombre) para que el ranking de "Top 10 DAGs con más fallos" tenga sentido.

Uso:
    python3 scripts/generate_demo_dags.py [--cantidad 40]
"""
import argparse
import os
import random

PLANTILLA = '''"""DAG de relleno generado por scripts/generate_demo_dags.py"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {{
    "owner": "{owner}",
    "retries": {retries},
    "retry_delay": timedelta(minutes=2),
}}

FALLA = {falla}


def paso_negocio(**_):
    """Falla de forma determinista según la fecha y la tasa del DAG."""
    from datetime import datetime as _dt
    if FALLA and _dt.utcnow().hour % {modulo} == 0:
        raise RuntimeError("dependencia externa no disponible (503)")
    print("paso de negocio completado")


with DAG(
    dag_id="{dag_id}",
    description="{descripcion}",
    start_date=datetime(2026, 9, 15),
    schedule="{schedule}",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags={tags},
) as dag:
    extraer = BashOperator(task_id="extraer", bash_command="echo extrayendo; sleep 2")
    transformar = PythonOperator(task_id="transformar", python_callable=paso_negocio)
    cargar = BashOperator(task_id="cargar", bash_command="echo cargando; sleep 2")
    extraer >> transformar >> cargar
'''

DOMINIOS = [
    ("pedidos", "equipo-datos", "Sincronización de pedidos del día"),
    ("facturacion", "equipo-finanzas", "Cierre de facturación por sucursal"),
    ("stock", "equipo-datos", "Reconciliación de stock por almacén"),
    ("clientes", "equipo-datos", "Actualización de maestros de cliente"),
    ("cobranzas", "equipo-finanzas", "Recordatorios de cobranza"),
    ("marketing", "equipo-bi", "Atribución de campañas"),
    ("logistica", "equipo-datos", "Tracking de envíos"),
    ("churn", "equipo-ml", "Scoring de churn"),
    ("precios", "equipo-bi", "Recalculo de precios por competencia"),
    ("proveedores", "equipo-datos", "Conciliación de proveedores"),
]

SCHEDULES = ["*/15 * * * *", "0 * * * *", "30 2 * * *", "0 0 * * *", "*/30 * * * *", "15 */4 * * *"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cantidad", type=int, default=40)
    ap.add_argument("--destino", default="/srv/appdata/airflow-lab/dags")
    args = ap.parse_args()

    random.seed(7)  # determinista: mismos DAGs en cada ejecución
    escritos = []
    for i in range(1, args.cantidad + 1):
        dominio, owner, desc = DOMINIOS[i % len(DOMINIOS)]
        dag_id = f"demo_bulk_{i:02d}_{dominio}"
        falla = i % 5 != 0          # 1 de cada 5 DAGs nunca falla
        cuerpo = PLANTILLA.format(
            dag_id=dag_id,
            owner=owner,
            retries=1 if i % 3 else 3,
            falla=str(falla),
            modulo=random.choice([3, 5, 7, 11]),
            descripcion=f"{desc} (#{i:02d})",
            schedule=SCHEDULES[i % len(SCHEDULES)],
            tags='["demo", "bulk", "%s"]' % dominio,
        )
        path = os.path.join(args.destino, f"{dag_id}.py")
        with open(path, "w") as f:
            f.write(cuerpo)
        escritos.append(dag_id)

    print(f"escritos {len(escritos)} DAGs en {args.destino}")
    print("primero:", escritos[0], "· último:", escritos[-1])


if __name__ == "__main__":
    main()
