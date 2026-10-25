#!/usr/bin/env python3
"""Benchmark de las tres vías del laboratorio.

Mide, para cada dashboard, lo mismo y en las mismas condiciones:

  · latencia por panel y por refresco completo (una pasada = todos los paneles)
  · bytes que devuelve Grafana por refresco
  · coste en CPU del componente que cada vía golpea (Postgres / InfluxDB / webserver)
  · comportamiento con volumen alto: se insertan 200.000 dag_runs y se repite

No inventa números: todo sale de /api/ds/query de Grafana y de los contadores
del sistema (cgroup de cada contenedor, pg_stat_database).

Uso:
    GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
        python3 scripts/benchmark.py --repeticiones 5 --json /tmp/bench.json
"""
import argparse
import base64
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3050")
USER = os.environ.get("GRAFANA_USER", "admin")
PASS = os.environ.get("GRAFANA_PASS", "")

CONTENEDORES = {
    "A": "airflow-lab-pg",
    "B": "airflow-lab-influx",
    "C": "airflow-lab-webserver",
}


def api(path, payload=None):
    req = urllib.request.Request(
        f"{GRAFANA}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        method="POST" if payload is not None else "GET",
    )
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{USER}:{PASS}".encode()).decode())
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=180) as r:
        cuerpo = r.read()
    return json.loads(cuerpo), len(cuerpo)


def resolve_sql(sql: str, ventana: str = "24 hours", horas: int = 24) -> str:
    """Resuelve macros y variables del dashboard de la vía A."""
    sql = sql.replace("$ventana", ventana)
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)",
                 lambda m: f"{m.group(1)} BETWEEN now() - interval '{horas} hours' AND now()", sql)
    sql = re.sub(r"\$__timeGroupAlias\(([^,]+),\s*\$__interval\)",
                 lambda m: f"date_trunc('hour', {m.group(1)}) AS time", sql)
    return sql.replace("$__interval", "1 hour")


def payload_de(panel: dict, via: str) -> dict:
    t = panel["targets"][0]
    if via == "A":
        q = {"refId": "A", "datasource": t["datasource"], "rawSql": resolve_sql(t["rawSql"]),
             "rawQuery": True, "editorMode": "code", "format": t.get("format", "table"),
             "intervalMs": 3600000, "maxDataPoints": 500}
    elif via == "B":
        q = {"refId": "A", "datasource": t["datasource"], "query": t["query"],
             "rawQuery": True, "resultFormat": "time_series"}
    else:
        q = dict(t)
    body = {"queries": [q], "from": "now-24h", "to": "now-1h" if panel.get("timeShift") else "now"}
    return body


def cpu_us(contenedor: str) -> int | None:
    """CPU acumulada del contenedor, en microsegundos (cgroup v2)."""
    try:
        cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", contenedor],
                             capture_output=True, text=True, timeout=10).stdout.strip()
        for ruta in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat",
                     f"/sys/fs/cgroup/docker/{cid}/cpu.stat",
                     f"/sys/fs/cgroup/cpu,cpuacct/docker/{cid}/cpuacct.usage"):
            if os.path.exists(ruta):
                txt = open(ruta).read()
                m = re.search(r"usage_usec\s+(\d+)", txt)
                if m:
                    return int(m.group(1))
                if txt.strip().isdigit():          # cgroup v1: nanosegundos
                    return int(txt.strip()) // 1000
    except Exception:
        return None
    return None


def cpu_en_reposo(contenedor: str, segundos: float = 10.0) -> float | None:
    """CPU que consume el contenedor SIN tráfico del benchmark, en ms/s.

    Sin esto, el CPU atribuido a un refresco incluye lo que el contenedor ya
    estaba haciendo (el scheduler de Airflow, el colector, Influx compactando…),
    y la comparación entre vías se vuelve injusta.
    """
    a = cpu_us(contenedor)
    if a is None:
        return None
    time.sleep(segundos)
    b = cpu_us(contenedor)
    if b is None or b <= a:
        return None
    return (b - a) / 1000.0 / segundos          # ms de CPU por segundo en reposo


def pg_stats() -> dict:
    salida = subprocess.run(
        ["docker", "exec", "airflow-lab-pg", "psql", "-U", "airflow", "-d", "airflow", "-tAc",
         "SELECT coalesce(sum(xact_commit),0), coalesce(sum(tup_returned),0), "
         "coalesce(sum(blk_read_time),0) FROM pg_stat_database"],
        capture_output=True, text=True, timeout=20).stdout.strip()
    a, b, c = (salida.split("|") + ["", "", ""])[:3]
    return {"commits": int(a or 0), "tuplas": int(b or 0), "blk_read_ms": float(c or 0)}


def medir(via: str, uid: str, repeticiones: int) -> dict:
    dash = api(f"/api/dashboards/uid/{uid}")[0]["dashboard"]
    paneles = [p for p in dash["panels"] if p.get("targets")]
    latencias, bytes_total, errores = [], 0, 0
    por_panel = {}

    cont = CONTENEDORES[via]
    reposo = cpu_en_reposo(cont, 10)
    cpu0 = cpu_us(cont)
    t_inicio = time.perf_counter()
    pg0 = pg_stats() if via == "A" else None

    for _ in range(repeticiones):
        for p in paneles:
            body = payload_de(p, via)
            t0 = time.perf_counter()
            try:
                _, nbytes = api("/api/ds/query", body)
                dt = time.perf_counter() - t0
            except (urllib.error.HTTPError, urllib.error.URLError):
                errores += 1
                dt = time.perf_counter() - t0
                nbytes = 0
            latencias.append(dt * 1000)
            bytes_total += nbytes
            por_panel.setdefault(p["title"], []).append(dt * 1000)

    t_fin = time.perf_counter()
    cpu1 = cpu_us(cont)
    pg1 = pg_stats() if via == "A" else None

    refresco = sum(statistics.mean(v) for v in por_panel.values())  # una pasada completa
    res = {
        "via": via,
        "dashboard": uid,
        "paneles": len(paneles),
        "llamadas": len(latencias),
        "errores": errores,
        "latencia_panel_ms": {
            "min": round(min(latencias), 1),
            "p50": round(statistics.median(latencias), 1),
            "media": round(statistics.mean(latencias), 1),
            "max": round(max(latencias), 1),
        },
        "refresco_completo_ms": round(refresco, 1),
        "bytes_por_refresco": round(bytes_total / repeticiones),
        "cpu_ms_por_refresco": (round((cpu1 - cpu0) / 1000 / repeticiones, 1)
                                if cpu0 and cpu1 and cpu1 > cpu0 else None),
        # CPU del contenedor menos lo que ya consumía en reposo: lo atribuible
        # de verdad a la carga del tablero.
        "cpu_reposo_ms_por_s": (round(reposo, 1) if reposo is not None else None),
        "cpu_neto_ms_por_refresco": (
            round((((cpu1 - cpu0) / 1000.0) - (reposo * (t_fin - t_inicio))) / repeticiones, 1)
            if (reposo is not None and cpu0 and cpu1 and cpu1 > cpu0) else None),
        "panel_mas_lento_ms": round(max((statistics.mean(v) for v in por_panel.values())), 1),
        "detalle_paneles": {k: round(statistics.mean(v), 1) for k, v in por_panel.items()},
    }
    if pg0 and pg1:
        res["postgres_por_refresco"] = {
            "commits": round((pg1["commits"] - pg0["commits"]) / repeticiones, 1),
            "tuplas_devueltas": round((pg1["tuplas"] - pg0["tuplas"]) / repeticiones),
            "blk_read_ms": round(pg1["blk_read_ms"] - pg0["blk_read_ms"], 1),
        }
    return res


def filas_dag_run() -> int:
    salida = subprocess.run(
        ["docker", "exec", "airflow-lab-pg", "psql", "-U", "airflow", "-d", "airflow", "-tAc",
         "SELECT count(*) FROM dag_run"], capture_output=True, text=True, timeout=20).stdout.strip()
    return int(salida or 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeticiones", type=int, default=5)
    ap.add_argument("--json", default="")
    ap.add_argument("--escenario", default="actual", help="etiqueta del escenario")
    ap.add_argument("--vias", default="A,B,C", help="vías a medir, p. ej. A o A,C")
    args = ap.parse_args()

    if not PASS:
        print("falta GRAFANA_PASS", file=sys.stderr)
        return 2

    print(f"# Benchmark · escenario: {args.escenario}")
    print(f"\nfilas en dag_run: {filas_dag_run()} · repeticiones por panel: {args.repeticiones}\n")

    resultados = {}
    todas = (("A", "airflow-visibilidad"), ("B", "airflow-historico"), ("C", "airflow-api"))
    pedidas = [v.strip().upper() for v in args.vias.split(",")]
    for via, uid in [t for t in todas if t[0] in pedidas]:
        r = medir(via, uid, args.repeticiones)
        resultados[via] = r
        print(f"--- Vía {via} ({uid}) · {r['paneles']} paneles")
        print(f"    panel p50: {r['latencia_panel_ms']['p50']} ms · máx: {r['latencia_panel_ms']['max']} ms")
        print(f"    refresco completo: {r['refresco_completo_ms']} ms · {r['bytes_por_refresco']} bytes")
        print(f"    CPU del origen: {r['cpu_ms_por_refresco']} ms por refresco "
              f"(en reposo: {r['cpu_reposo_ms_por_s']} ms/s · neto: {r['cpu_neto_ms_por_refresco']} ms)")
        if "postgres_por_refresco" in r:
            print(f"    Postgres: {r['postgres_por_refresco']}")
        if r["errores"]:
            print(f"    ERRORES: {r['errores']}")

    salida = {"escenario": args.escenario, "filas_dag_run": filas_dag_run(),
              "repeticiones": args.repeticiones, "resultados": resultados}
    if args.json:
        open(args.json, "w").write(json.dumps(salida, indent=2, ensure_ascii=False))
        print(f"\nescrito {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
