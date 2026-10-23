#!/usr/bin/env python3
"""Verifica el dashboard de punta a punta, ejecutando cada panel CONTRA GRAFANA.

No lee la base de datos por su cuenta: manda el SQL de cada panel al endpoint
/api/ds/query de Grafana, con la misma credencial y el mismo datasource que usa
el dashboard. Si un panel devuelve filas aquí, el panel funciona en el navegador.

Uso:
    GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
        python3 scripts/verify_dashboard.py [--ventana "24 hours"]

Salida: tabla por panel con las filas devueltas + código de salida != 0 si
algún panel falla.
"""
import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3050")
USER = os.environ.get("GRAFANA_USER", "admin")
PASS = os.environ.get("GRAFANA_PASS", "")
UID_DASH = os.environ.get("DASHBOARD_UID", "airflow-visibilidad")


def api(path, payload=None, method=None):
    url = f"{GRAFANA}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"))
    token = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def resolve(sql: str, ventana: str, hours: int) -> str:
    """Sustituye las macros y la variable de dashboard por SQL válido.

    Los macros ($__timeFilter, $__interval) los resuelve Grafana dentro del
    panel; al llamar a la API hay que enviarlos ya resueltos (la API no tiene
    contexto de dashboard, solo de rango de tiempo).
    """
    sql = sql.replace("$ventana", ventana)
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)",
                 lambda m: f"{m.group(1)} BETWEEN now() - interval '{hours} hours' AND now()", sql)
    sql = re.sub(r"\$__timeGroupAlias\(([^,]+),\s*\$__interval\)",
                 lambda m: f"date_trunc('hour', {m.group(1)}) AS time", sql)
    sql = sql.replace("$__interval", "1 hour")
    return sql


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ventana", default="24 hours")
    args = ap.parse_args()
    hours = {"1 hour": 1, "6 hours": 6, "24 hours": 24, "7 days": 168, "30 days": 720}[args.ventana]

    print("=" * 78)
    print(f"GRAFANA  {GRAFANA}   versión: ", end="")
    print(api("/api/health")["version"])

    # --- datasource -------------------------------------------------------
    ds = api("/api/datasources/uid/airflow-pg")
    print(f"DATASOURCE  uid={ds['uid']}  type={ds['type']}  db={ds['database']}  user={ds['user']}")
    health = api("/api/datasources/uid/airflow-pg/health")
    print(f"            health: {health.get('status')}  {health.get('message','')[:90]}")

    # --- dashboard --------------------------------------------------------
    dash = api(f"/api/dashboards/uid/{UID_DASH}")["dashboard"]
    panels = dash["panels"]
    print(f"DASHBOARD   uid={dash['uid']}  paneles={len(panels)}  título={dash['title']}")

    errors = 0
    print("=" * 78)
    for p in panels:
        sql = resolve(p["targets"][0]["rawSql"], args.ventana, hours)
        payload = {
            "queries": [{
                "refId": "A",
                "datasource": {"type": "postgres", "uid": "airflow-pg"},
                "rawSql": sql,
                "format": p["targets"][0].get("format", "table"),
                "intervalMs": 3600000,
                "maxDataPoints": 500,
            }],
            "from": f"now-{hours}h",
            "to": "now",
        }
        try:
            res = api("/api/ds/query", payload)
        except urllib.error.HTTPError as e:
            errors += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}  HTTP {e.code}: {e.read()[:200]}")
            continue
        r = res["results"]["A"]
        if "error" in r:
            errors += 1
            print(f"[FAIL] {p['id']:>2} {p['title']}  -> {r['error']}")
            continue
        frames = r.get("frames", [])
        rows = []
        values, fields, n = [], [], 0
        if frames:
            fields = [f["name"] for f in frames[0]["schema"]["fields"]]
            values = frames[0]["data"]["values"]
            n = len(values[0]) if values else 0
            for i in range(min(n, 6)):
                rows.append(dict(zip(fields, [values[j][i] for j in range(len(values))])))
        extra = f" (+{n-6} filas más)" if n > 6 else ""
        print(f"[ OK ] {p['id']:>2} {p['title']}")
        print(f"       filas={n}{extra}  cols={fields}")
        for row in rows:
            print("       ", json.dumps(row, default=str, ensure_ascii=False))

    print("=" * 78)
    print(f"RESULTADO: {len(panels)} paneles verificados, {errors} con error")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
