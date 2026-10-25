#!/usr/bin/env python3
"""Compara los mismos cinco KPIs en las tres vías, en el mismo instante.

Sirve para responder a la pregunta obvia: "¿los tres tableros dan los mismos
números?" La respuesta corta es **sí, con dos matices**: la vía B muestrea cada
30 s (llega tarde) y las tres se leen en milisegundos distintos, así que un run
que cambia de estado entre dos lecturas aparece distinto.

Uso:
    GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
        python3 scripts/compare_vias.py [--ventana "24 hours"]
"""
import argparse
import base64
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3050")
USER = os.environ.get("GRAFANA_USER", "admin")
PASS = os.environ.get("GRAFANA_PASS", "")

# (etiqueta, uid del dashboard, función que arma el payload de cada KPI)
KPIS = ["dags", "ok", "fallidos", "running", "lentos"]


def api(path, payload=None):
    req = urllib.request.Request(
        f"{GRAFANA}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        method="POST" if payload is not None else "GET")
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{USER}:{PASS}".encode()).decode())
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read().decode())


def valor(res) -> int | str:
    r = (res.get("results") or {}).get("A") or {}
    if "error" in r:
        return "err"
    frames = r.get("frames") or []
    if not frames:
        return "—"
    frame = frames[0]
    fields = frame["schema"]["fields"]
    campos = [f["name"] for f in fields]
    vals = frame["data"]["values"]
    if not vals:
        return "—"
    # Influx devuelve ['Time', '<campo>', ...]: hay que coger el primer campo que
    # NO sea de tipo time (si no, se lee la marca de tiempo en vez del valor).
    idx = next((i for i, f in enumerate(fields) if f.get("type") != "time"), 0)
    if idx >= len(vals) or not vals[idx]:
        return "—"
    v = vals[idx][0]
    return int(v) if isinstance(v, (int, float)) else v


def sql_a(panel_sql: str, ventana: str, horas: int) -> dict:
    sql = panel_sql.replace("$ventana", ventana)
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)",
                 lambda m: f"{m.group(1)} BETWEEN now() - interval '{horas} hours' AND now()", sql)
    return {"refId": "A", "datasource": {"type": "grafana-postgresql-datasource", "uid": "airflow-pg"},
            "rawSql": sql.strip(), "rawQuery": True, "editorMode": "code", "format": "table"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ventana", default="24 hours")
    args = ap.parse_args()
    horas = {"1 hour": 1, "6 hours": 6, "24 hours": 24, "7 days": 168}.get(args.ventana, 24)
    if not PASS:
        print("falta GRAFANA_PASS", file=sys.stderr)
        return 2

    # --- vía A: cinco paneles del dashboard principal ------------------------
    dash_a = api("/api/dashboards/uid/airflow-visibilidad")["dashboard"]["panels"]
    por_id = {p["id"]: p for p in dash_a}
    qa = {
        "dags": sql_a(por_id[1]["targets"][0]["rawSql"], args.ventana, horas),
        "ok": sql_a(por_id[2]["targets"][0]["rawSql"], args.ventana, horas),
        "fallidos": sql_a(por_id[3]["targets"][0]["rawSql"], args.ventana, horas),
        "running": sql_a("SELECT count(*) AS n FROM dag_run WHERE state='running'", args.ventana, horas),
        "lentos": sql_a(por_id[4]["targets"][0]["rawSql"], args.ventana, horas),
    }
    # --- vía B: el colector expone los mismos cinco -------------------------
    def flux(campo):
        return {"refId": "A", "datasource": {"type": "influxdb", "uid": "influx-airflow"},
                "query": f'from(bucket: "airflow") |> range(start: -15m) '
                         f'|> filter(fn: (r) => r._measurement == "airflow_kpi" and r._field == "{campo}") '
                         f'|> last()', "rawQuery": True, "resultFormat": "time_series"}
    qb = {"dags": flux("dags_active"), "ok": flux("runs_success_24h"), "fallidos": flux("runs_failed_24h"),
          "running": flux("runs_running"), "lentos": flux("runs_running_over_1h")}
    # --- vía C: una llamada a la API por KPI --------------------------------
    def inf(path, params):
        return {"refId": "A", "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "airflow-api"},
                "type": "json", "source": "url", "format": "table", "url": path, "parser": "backend",
                "url_options": {"method": "GET", "params": [{"key": k, "value": v} for k, v in params]},
                "columns": [{"selector": "total_entries", "text": "n", "type": "number"}]}
    desde = "${__from:date:iso}"
    qc = {
        "dags": inf("/dags", [("limit", "1")]),
        "ok": inf("/dags/~/dagRuns", [("state", "success"), ("execution_date_gte", desde), ("limit", "1")]),
        "fallidos": inf("/dags/~/dagRuns", [("state", "failed"), ("execution_date_gte", desde), ("limit", "1")]),
        "running": inf("/dags/~/dagRuns", [("state", "running"), ("limit", "1")]),
        "lentos": inf("/dags/~/dagRuns", [("state", "running"), ("start_date_lte", "${__to:date:iso}"),
                                          ("limit", "1")]),
    }
    # "lentos" en la vía C usa timeShift 1h → se le pide con to=now-1h
    to_c = {"lentos": "now-1h"}

    filas = {}
    for via, preguntas in (("A", qa), ("B", qb), ("C", qc)):
        for kpi, q in preguntas.items():
            payload = {"queries": [q], "from": "now-24h", "to": to_c.get(kpi, "now") if via == "C" else "now"}
            try:
                filas.setdefault(kpi, {})[via] = valor(api("/api/ds/query", payload))
            except Exception as exc:  # noqa: BLE001
                filas.setdefault(kpi, {})[via] = f"err({type(exc).__name__})"

    print(f"# Comparación de las tres vías · {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    print(f"# ventana: {args.ventana} (vía A usa la variable; B y C, 24 h fijas)\n")
    print(f"{'KPI':<12}{'A · DB':>10}{'B · InfluxDB':>16}{'C · API':>10}   {'A−C':>6}{'A−B':>6}")
    for kpi in KPIS:
        v = filas.get(kpi, {})
        a, b, c = v.get("A", "—"), v.get("B", "—"), v.get("C", "—")
        dac = (a - c) if isinstance(a, int) and isinstance(c, int) else "—"
        dab = (a - b) if isinstance(a, int) and isinstance(b, int) else "—"
        print(f"{kpi:<12}{str(a):>10}{str(b):>16}{str(c):>10}   {str(dac):>6}{str(dab):>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
