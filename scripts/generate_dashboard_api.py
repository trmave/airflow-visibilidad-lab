#!/usr/bin/env python3
"""Genera el dashboard de la OPCIÓN C: todo sale de la API REST de Airflow.

Grafana no habla REST: lo hace el plugin Infinity, que llama a /api/v1 y parsea
el JSON. Cada KPI es UNA llamada con limit=1; el conteo viene en `total_entries`.

Claves que costaron encontrar (ver docs/13):
  · parser: "backend"  → sin esto no parsea ni arrays ni escalares
  · sin root_selector para el escalar y una columna con selector "total_entries"
  · ${__from:date:iso} sí se interpola en los params (Grafana lo resuelve)
  · para "corriendo > 1 h" se usa `timeShift: 1h` en el panel, así __to = ahora-1h

Uso:
    python3 scripts/generate_dashboard_api.py > grafana/dashboards/airflow-api.json
"""
import json
import sys

DS = {"type": "yesoreyeram-infinity-datasource", "uid": "airflow-api"}


def q(path: str, params: list[tuple[str, str]], columns: list[dict],
      root: str | None = None) -> dict:
    """Consulta de tabla. `root` es la clave del array en el JSON de Airflow.

    Sin root_selector, Infinity devuelve las columnas vacías (las filas existen
    pero no se mapean). Con el array indicado, salen los valores.
    """
    t = {
        "refId": "A",
        "datasource": DS,
        "type": "json",
        "source": "url",
        "format": "table",
        "url": path,
        "parser": "backend",
        "url_options": {"method": "GET", "params": [{"key": k, "value": v} for k, v in params]},
        "columns": columns,
    }
    if root:
        t["root_selector"] = root
    return t


def contar(path: str, params: list[tuple[str, str]]) -> dict:
    """Un KPI = una llamada + total_entries como columna."""
    return q(path, params + [("limit", "1")],
             [{"selector": "total_entries", "text": "n", "type": "number"}])


def stat(pid, title, target, grid, thresholds=None, desc="", time_shift=None):
    p = {
        "id": pid, "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "orientation": "auto", "textMode": "auto", "colorMode": "value",
                    "graphMode": "none", "justifyMode": "auto"},
        "fieldConfig": {"defaults": {"mappings": [], "unit": "short",
                                     "thresholds": {"mode": "absolute",
                                                    "steps": thresholds or [{"color": "green", "value": None}]}},
                        "overrides": []},
    }
    if time_shift:
        p["timeShift"] = time_shift
    return p


def tabla(pid, title, target, grid, desc="", time_shift=None):
    p = {
        "id": pid, "type": "table", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target],
        "options": {"showHeader": True, "cellHeight": "sm",
                    "footer": {"show": False, "reducer": ["count"], "countRows": True, "fields": ""}},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"},
                                                "inspect": False}, "mappings": []},
                        "overrides": []},
    }
    if time_shift:
        p["timeShift"] = time_shift
    return p


panels = [
    stat(1, "DAGs en total",
         contar("/dags", []), {"h": 5, "w": 4, "x": 0, "y": 0},
         desc="Una llamada a /api/v1/dags con limit=1; el conteo viene en total_entries."),
    stat(2, "OK (ventana)",
         contar("/dags/~/dagRuns", [("state", "success"), ("execution_date_gte", "${__from:date:iso}")]),
         {"h": 5, "w": 5, "x": 4, "y": 0},
         desc="GET /api/v1/dags/~/dagRuns?state=success&execution_date_gte=<desde>"),
    stat(3, "Fallidos (ventana)",
         contar("/dags/~/dagRuns", [("state", "failed"), ("execution_date_gte", "${__from:date:iso}")]),
         {"h": 5, "w": 5, "x": 9, "y": 0},
         thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
         desc="Mismo endpoint con state=failed."),
    stat(4, "Corriendo ahora",
         contar("/dags/~/dagRuns", [("state", "running")]), {"h": 5, "w": 5, "x": 14, "y": 0},
         desc="Estado instantáneo, sin filtro de fecha."),
    stat(5, "Corriendo > 1 h",
         contar("/dags/~/dagRuns", [("state", "running"), ("start_date_lte", "${__to:date:iso}")]),
         {"h": 5, "w": 5, "x": 19, "y": 0},
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1},
                     {"color": "red", "value": 3}],
         time_shift="1h",
         desc="El panel aplica timeShift=1h, así __to = ahora-1h y el filtro start_date_lte "
              "devuelve los que arrancaron antes de ese momento."),

    tabla(6, "Corriendo ahora (detalle por API)",
          q("/dags/~/dagRuns", [("state", "running"), ("limit", "50")],
            [{"selector": "dag_id", "text": "dag_id", "type": "string"},
             {"selector": "start_date", "text": "inicio", "type": "string"}],
            root="dag_runs"),
          {"h": 9, "w": 12, "x": 0, "y": 5},
          desc="La API sí da el detalle: una llamada y las filas que devuelve."),
    tabla(7, "Fallidos recientes (detalle por API)",
          q("/dags/~/dagRuns", [("state", "failed"), ("execution_date_gte", "${__from:date:iso}"),
                                ("limit", "50"), ("order_by", "-execution_date")],
            [{"selector": "dag_id", "text": "dag_id", "type": "string"},
             {"selector": "execution_date", "text": "ejecución", "type": "string"}],
            root="dag_runs"),
          {"h": 9, "w": 12, "x": 12, "y": 5},
          desc="Máximo 100 filas por llamada: la API pagina."),

    {"id": 8, "type": "text", "title": "Límites de esta vía (medidos, no supuestos)",
     "datasource": DS, "gridPos": {"h": 7, "w": 24, "x": 0, "y": 14},
     "targets": [],
     "options": {"mode": "markdown", "code": {"language": "plaintext", "showLineNumbers": False,
                                              "showMiniMap": False}},
     "fieldConfig": {"defaults": {}, "overrides": []},
     "transforms": [],
     "description": "Lo que la API resuelve y lo que no."},
]

dashboard = {
    "uid": "airflow-api",
    "title": "Airflow 2.8.1 · Vía C (API REST)",
    "description": "Los KPIs leídos de la API REST de Airflow con el plugin Infinity y un usuario "
                   "con rol Viewer. Una llamada por panel, conteo en total_entries.",
    "tags": ["airflow", "demo", "opcion-c"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "editable": True,
    "graphTooltip": 1,
    "time": {"from": "now-24h", "to": "now"},
    "templating": {"list": []},
    "annotations": {"list": []},
    "links": [
        {"title": "Vía A · metadata DB", "url": "/d/airflow-visibilidad", "type": "link",
         "targetBlank": False, "icon": "external link"},
        {"title": "Vía B · InfluxDB", "url": "/d/airflow-historico", "type": "link",
         "targetBlank": False, "icon": "external link"},
    ],
    "panels": panels,
}
dashboard["panels"][-1]["options"]["content"] = """
### Qué resuelve esta vía

- Los **cinco números** salen de la API con **una llamada por panel** (`limit=1` y `total_entries`).
- No necesita **ninguna credencial de base de datos**: solo un usuario de Airflow con rol `Viewer`
  (comprobado: puede leer, y devuelve **403** al intentar disparar un DAG).
- **Coincide exactamente con la vía A** (misma fuente de verdad): diferencia medida 0 en los cinco KPIs.

### Lo que cuesta (medido, no estimado)

- **1,5 s por refresco completo**: 8 veces más lento que la vía A (172 ms) con el mismo volumen.
- **1,3 s de CPU por refresco en el webserver**, frente a 17 ms netos de la vía A: cada panel es una
  petición HTTP autenticada que a su vez consulta la base, así que se paga dos veces el mismo trabajo.
- **Carga**: cada refresco golpea el **webserver** (Gunicorn), la pieza más frágil del stack.
- **Requisito previo**: Airflow 2.8.1 solo acepta cookie de sesión por defecto; hubo que habilitar
  `basic_auth` y **reiniciar el webserver**.

### Lo que no resuelve

- **Serie temporal**: habría que hacer una llamada por cada punto del gráfico (24-48 por refresco).
- **Ranking por DAG**: la API no agrupa; serían 46 llamadas o paginar los fallidos.
- **No sobrevive a la caída del clúster** (como la vía A).

Otras vías: [A · metadata DB](/d/airflow-visibilidad) · [B · InfluxDB](/d/airflow-historico).
Método y números completos: `docs/14-benchmark.md`.
"""

json.dump(dashboard, sys.stdout, indent=2, ensure_ascii=False)
sys.stdout.write("\n")
