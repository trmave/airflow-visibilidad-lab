#!/usr/bin/env python3
"""Verifica el dashboard de la FASE 2 (histórico) ejecutando cada panel contra Grafana.

Igual que `verify_dashboard.py`, pero para el datasource de InfluxDB: envía el
Flux de cada panel a /api/ds/query y exige que devuelva filas.

Uso:
    GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
        python3 scripts/verify_historico.py
"""
import base64
import json
import os
import sys
import urllib.request

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3050")
USER = os.environ.get("GRAFANA_USER", "admin")
PASS = os.environ.get("GRAFANA_PASS", "")
UID_DASH = os.environ.get("DASHBOARD_UID", "airflow-historico")


def api(path, payload=None):
    req = urllib.request.Request(
        f"{GRAFANA}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        method="POST" if payload is not None else "GET",
    )
    req.add_header("Authorization",
                   "Basic " + base64.b64encode(f"{USER}:{PASS}".encode()).decode())
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read().decode())


def main() -> int:
    dash = api(f"/api/dashboards/uid/{UID_DASH}")["dashboard"]
    print(f"DASHBOARD  {dash['uid']} · {len(dash['panels'])} paneles · {dash['title']}")
    fallos = 0
    for p in dash["panels"]:
        if p["type"] == "text" or not p.get("targets"):
            print(f"[skip] {p['id']:>2} {p['title']} (sin consulta)")
            continue
        payload = {
            "queries": [{
                "refId": "A",
                "datasource": p["targets"][0]["datasource"],
                "query": p["targets"][0]["query"],
                "rawQuery": True,
                "resultFormat": "time_series",
            }],
            "from": "now-6h",
            "to": "now",
        }
        try:
            res = api("/api/ds/query", payload)["results"]["A"]
        except Exception as exc:  # noqa: BLE001
            fallos += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}: {exc}")
            continue
        if "error" in res:
            fallos += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}: {res['error'][:160]}")
            continue
        frames = res.get("frames", [])
        filas = sum(len(f["data"]["values"][0]) if f["data"]["values"] else 0 for f in frames)
        if filas == 0:
            fallos += 1
            print(f"[VACÍO] {p['id']:>2} {p['title']}: 0 filas")
            continue
        print(f"[ OK ] {p['id']:>2} {p['title']:<40} series={len(frames)} filas={filas}")

    print(f"RESULTADO: {len(dash['panels'])} paneles, {fallos} con problema")
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
