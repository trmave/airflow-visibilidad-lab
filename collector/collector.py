#!/usr/bin/env python3
"""Colector de KPIs de Airflow hacia InfluxDB.

Lee la metadata DB de Airflow con el rol de SOLO LECTURA y escribe los KPIs en
Influx cada INTERVAL segundos. Es la pieza de la fase 2: el almacén queda fuera
del clúster, así que el tablero sobrevive a la caída de Airflow.

En Kubernetes esto sería un CronJob (schedule "*/1 * * * *") con
concurrencyPolicy: Forbid. Aquí corre en bucle para poder demostrarlo.

Escribe:
  measurement airflow_kpi, tag instance=all
    dags_active, runs_success_24h, runs_failed_24h, runs_running,
    runs_running_over_1h, runs_queued_over_15m, collector_last_success
  measurement airflow_dag, tag dag_id
    running_minutes, failed_24h
"""
import os
import time

import psycopg2
import requests

DSN = os.environ["AIRFLOW_DB_DSN"]
INFLUX_URL = os.environ["INFLUX_URL"].rstrip("/")
TOKEN = os.environ["INFLUX_TOKEN"]
ORG = os.environ.get("INFLUX_ORG", "homelab")
BUCKET = os.environ.get("INFLUX_BUCKET", "airflow")
INTERVAL = int(os.environ.get("INTERVAL", "30"))

KPI_SQL = {
    "dags_active": "SELECT count(*) FROM dag WHERE is_active AND NOT is_subdag",
    "runs_success_24h": ("SELECT count(*) FROM dag_run WHERE state = 'success' "
                         "AND execution_date > now() - interval '24 hours'"),
    "runs_failed_24h": ("SELECT count(*) FROM dag_run WHERE state = 'failed' "
                        "AND execution_date > now() - interval '24 hours'"),
    "runs_running": "SELECT count(*) FROM dag_run WHERE state = 'running'",
    "runs_running_over_1h": ("SELECT count(*) FROM dag_run WHERE state = 'running' "
                             "AND start_date < now() - interval '1 hour'"),
    "runs_queued_over_15m": ("SELECT count(*) FROM dag_run WHERE state = 'queued' "
                             "AND queued_at < now() - interval '15 minutes'"),
}

# Por DAG: permite el panel de detalle sin volver a la metadata DB.
# Solo DAGs con runs vivos o fallos recientes (cardinalidad acotada).
DAG_SQL = """
SELECT dag_id,
       COALESCE(MAX(CASE WHEN state = 'running'
                         THEN round(extract(epoch FROM (now() - start_date)) / 60) END), 0)::int
         AS running_minutes,
       COUNT(*) FILTER (WHERE state = 'failed')::int AS failed_24h
FROM dag_run
WHERE execution_date > now() - interval '24 hours'
   OR state = 'running'
GROUP BY dag_id
HAVING COUNT(*) FILTER (WHERE state = 'failed') > 0 OR COUNT(*) FILTER (WHERE state = 'running') > 0
"""


def esc(tag: str) -> str:
    """Escapa etiquetas del line protocol de Influx."""
    return tag.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def collect() -> tuple[dict, list[tuple]]:
    with psycopg2.connect(DSN, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            kpis = {}
            for name, sql in KPI_SQL.items():
                cur.execute(sql)
                kpis[name] = int(cur.fetchone()[0])
            cur.execute(DAG_SQL)
            dags = cur.fetchall()
    kpis["collector_last_success"] = int(time.time())
    return kpis, dags


def write(kpis: dict, dags: list[tuple]) -> None:
    now_ns = time.time_ns()
    lines = []
    fields = ",".join(f"{k}={v}i" for k, v in kpis.items())
    lines.append(f"airflow_kpi,instance=all {fields} {now_ns}")
    for dag_id, running_minutes, failed_24h in dags:
        lines.append(
            f"airflow_dag,dag_id={esc(dag_id)} running_minutes={running_minutes}i,"
            f"failed_24h={failed_24h}i {now_ns}"
        )
    r = requests.post(
        f"{INFLUX_URL}/api/v2/write",
        params={"org": ORG, "bucket": BUCKET, "precision": "ns"},
        data="\n".join(lines).encode(),
        headers={"Authorization": f"Token {TOKEN}", "Content-Type": "text/plain; charset=utf-8"},
        timeout=10,
    )
    r.raise_for_status()
    return len(lines)


def main() -> None:
    print(f"colector arrancado · intervalo {INTERVAL}s · destino {INFLUX_URL} bucket={BUCKET}",
          flush=True)
    while True:
        t0 = time.time()
        try:
            kpis, dags = collect()
            n = write(kpis, dags)
            print(f"ok · {n} líneas · dags_activos={kpis['dags_active']} "
                  f"ok24h={kpis['runs_success_24h']} fail24h={kpis['runs_failed_24h']} "
                  f"running={kpis['runs_running']} >1h={kpis['runs_running_over_1h']} "
                  f"queued>15m={kpis['runs_queued_over_15m']} · {time.time()-t0:.2f}s", flush=True)
        except Exception as exc:  # noqa: BLE001 — el bucle no debe morir
            print(f"ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(max(5, INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    main()
