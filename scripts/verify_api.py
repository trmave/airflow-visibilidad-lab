#!/usr/bin/env python3
"""Verifica el dashboard de la OPCIÓN C (API REST) ejecutando cada panel en Grafana.

A diferencia de las vías A y B, aquí cada panel es una llamada HTTP a la API de
Airflow hecha por el plugin Infinity: si el panel devuelve filas aquí, funciona
también en el navegador.

Los paneles con `timeShift` (el de "corriendo > 1 h") se ejecutan con la ventana
desplazada, igual que hace Grafana.

Uso:
    GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
        python3 scripts/verify_api.py
"""
import base64
import json
import os
import sys
import urllib.request

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3050")
USER = os.environ.get("GRAFANA_USER", "admin")
PASS = os.environ.get("GRAFANA_PASS", "")
UID_DASH = os.environ.get("DASHBOARD_UID", "airflow-api")


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
    problemas = 0
    for p in dash["panels"]:
        if p["type"] == "text" or not p.get("targets"):
            print(f"[skip] {p['id']:>2} {p['title']} (sin consulta)")
            continue
        payload = {
            "queries": [p["targets"][0]],
            "from": "now-24h",
            "to": "now-1h" if p.get("timeShift") else "now",
        }
        try:
            res = api("/api/ds/query", payload)["results"]["A"]
        except Exception as exc:  # noqa: BLE001
            problemas += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}: {exc}")
            continue
        if "error" in res:
            problemas += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}: {res['error'][:150]}")
            continue
        frames = res.get("frames") or []
        if not frames:
            problemas += 1
            print(f"[VACÍO] {p['id']:>2} {p['title']}: sin frames")
            continue
        f = frames[0]
        cols = [x["name"] for x in f["schema"]["fields"]]
        n = len(f["data"]["values"][0]) if f["data"]["values"] else 0
        valores = [v[0] if v else None for v in f["data"]["values"]][:3]
        if n == 0 or not any(cols):
            problemas += 1
            print(f"[VACÍO] {p['id']:>2} {p['title']}: columnas={cols}")
            continue
        print(f"[ OK ] {p['id']:>2} {p['title']:<38} filas={n} cols={cols} {valores}")

    print(f"RESULTADO: {len(dash['panels'])} paneles, {problemas} con problema")
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main())
