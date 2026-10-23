#!/usr/bin/env python3
"""Genera el dashboard de la FASE 2 (histórico) que lee de InfluxDB.

Es el gemelo del dashboard principal: mismos KPIs, otra fuente. Existe para
demostrar que externalizando las métricas el tablero sobrevive a la caída del
clúster de Airflow.

Uso:
    python3 scripts/generate_dashboard_historico.py > grafana/dashboards/airflow-historico.json
"""
import json
import sys

DS = {"type": "influxdb", "uid": "influx-airflow"}
BUCKET = "airflow"


def kpi_last(field: str, rango: str = "-15m") -> str:
    """Última muestra de un KPI global."""
    return (
        f'from(bucket: "{BUCKET}")\n'
        f'  |> range(start: {rango})\n'
        f'  |> filter(fn: (r) => r._measurement == "airflow_kpi" and r._field == "{field}")\n'
        f'  |> last()'
    )


def serie_campos(campos: list[str], ventana: str = "6h", paso: str = "1m") -> str:
    """Evolución temporal de varios campos del mismo measurement."""
    cond = " or ".join(f'r._field == "{c}"' for c in campos)
    return (
        f'from(bucket: "{BUCKET}")\n'
        f'  |> range(start: -{ventana})\n'
        f'  |> filter(fn: (r) => r._measurement == "airflow_kpi" and ({cond}))\n'
        f'  |> aggregateWindow(every: {paso}, fn: last, createEmpty: false)\n'
        f'  |> keep(columns: ["_time", "_value", "_field"])'
    )


SQL_TOP = f'''from(bucket: "{BUCKET}")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "airflow_dag" and r._field == "failed_24h")
  |> last()
  |> group()
  |> keep(columns: ["dag_id", "_value"])
  |> rename(columns: {{_value: "fallos"}})
  |> filter(fn: (r) => r.fallos > 0)
  |> sort(columns: ["fallos"], desc: true)
  |> limit(n: 10)'''

SQL_DETALLE = f'''from(bucket: "{BUCKET}")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "airflow_dag" and r._field == "running_minutes")
  |> last()
  |> group()
  |> keep(columns: ["dag_id", "_value"])
  |> rename(columns: {{_value: "minutos"}})
  |> filter(fn: (r) => r.minutos > 0)
  |> sort(columns: ["minutos"], desc: true)'''

SQL_LATIDO = f'''from(bucket: "{BUCKET}")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "airflow_kpi" and r._field == "collector_last_success")
  |> last()
  |> keep(columns: ["_time", "_value"])'''


def target(flux: str, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "query": flux, "rawQuery": True, "resultFormat": "time_series"}


def stat(pid, title, field, grid, thresholds=None, desc="", rango="-15m"):
    steps = thresholds or [{"color": "green", "value": None}]
    return {
        "id": pid, "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target(kpi_last(field, rango))],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "orientation": "auto", "textMode": "auto", "colorMode": "value",
                    "graphMode": "none", "justifyMode": "auto"},
        "fieldConfig": {"defaults": {"mappings": [], "unit": "short",
                                     "thresholds": {"mode": "absolute", "steps": steps}},
                        "overrides": []},
    }


def timeseries(pid, title, campos, grid, desc=""):
    return {
        "id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target(serie_campos(campos))],
        "options": {"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom", "calcs": []},
                    "tooltip": {"mode": "multi", "sort": "none"}},
        "fieldConfig": {"defaults": {"custom": {"drawStyle": "line", "lineInterpolation": "smooth",
                                                "lineWidth": 2, "fillOpacity": 10, "showPoints": "never",
                                                "spanNulls": True, "axisPlacement": "auto"},
                                     "color": {"mode": "palette-classic"}, "unit": "short"},
                        "overrides": []},
    }


def tabla(pid, title, flux, grid, desc="", campos=None):
    return {
        "id": pid, "type": "table", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target(flux)],
        "options": {"showHeader": True, "cellHeight": "sm",
                    "footer": {"show": False, "reducer": ["sum"], "countRows": False, "fields": ""}},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"},
                                                "inspect": False}, "mappings": []},
                        "overrides": campos or []},
    }


def barchart(pid, title, flux, grid, xfield, yfield, desc=""):
    return {
        "id": pid, "type": "barchart", "title": title, "description": desc, "datasource": DS,
        "gridPos": grid, "targets": [target(flux)],
        "options": {"xField": xfield, "yField": yfield, "orientation": "horizontal",
                    "showValue": "always", "stacking": "none", "groupWidth": 0.7, "barWidth": 0.9,
                    "barRadius": 0.1, "fullHighlight": False,
                    "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom", "calcs": []},
                    "tooltip": {"mode": "single", "sort": "none"}},
        "fieldConfig": {"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 80,
                                                "axisPlacement": "auto", "thresholdsStyle": {"mode": "off"}},
                                     "color": {"mode": "palette-classic"}, "unit": "short", "mappings": []},
                        "overrides": []},
    }


def texto(pid, title, contenido, grid):
    return {
        "id": pid, "type": "text", "title": title, "datasource": DS, "gridPos": grid,
        "options": {"mode": "markdown", "content": contenido, "code": {"language": "plaintext",
                                                                       "showLineNumbers": False,
                                                                       "showMiniMap": False}},
        "targets": [],
        "fieldConfig": {"defaults": {}, "overrides": []},
    }


panels = [
    texto(1, "Cómo leer este tablero", """
**Este tablero NO lee la metadata DB de Airflow: lee InfluxDB.**

- Un colector dentro del clúster lee los KPIs cada 30 s y los escribe aquí.
- Si el clúster cae, **este tablero sigue mostrando el histórico y la última foto**.
- Lo que no puede hacer: mostrar lo que pasa *mientras* el colector está caído.
  Para eso está la alerta de latido y el tablero en vivo (vía A).
""", {"h": 5, "w": 24, "x": 0, "y": 0}),

    stat(2, "DAGs activos", "dags_active", {"h": 5, "w": 6, "x": 0, "y": 5},
         desc="Última muestra del colector."),
    stat(3, "Runs OK (24 h)", "runs_success_24h", {"h": 5, "w": 6, "x": 6, "y": 5},
         desc="Contador recalculado por el colector en cada ciclo."),
    stat(4, "Runs fallidos (24 h)", "runs_failed_24h", {"h": 5, "w": 6, "x": 12, "y": 5},
         thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
    stat(5, "Corriendo > 1 hora", "runs_running_over_1h", {"h": 5, "w": 6, "x": 18, "y": 5},
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1},
                     {"color": "red", "value": 3}]),

    timeseries(6, "OK vs fallidos (24 h) en el tiempo",
               ["runs_success_24h", "runs_failed_24h"], {"h": 9, "w": 14, "x": 0, "y": 10},
               desc="Cada punto es lo que el colector midió en ese momento."),
    timeseries(7, "Corriendo y en cola en el tiempo",
               ["runs_running", "runs_queued_over_15m"], {"h": 9, "w": 10, "x": 14, "y": 10}),

    barchart(8, "Top DAGs con fallos (24 h)", SQL_TOP, {"h": 8, "w": 12, "x": 0, "y": 19},
             "dag_id", "fallos", desc="Etiquetado por dag_id: cardinalidad acotada."),
    tabla(9, "Corriendo ahora mismo, por DAG", SQL_DETALLE, {"h": 8, "w": 12, "x": 12, "y": 19},
          desc="El detalle sale del TSDB porque el colector manda un gauge por DAG.",
          campos=[{"matcher": {"id": "byName", "options": "minutos"},
                   "properties": [{"id": "unit", "value": "m"}]}]),

    tabla(10, "Última escritura del colector", SQL_LATIDO, {"h": 6, "w": 12, "x": 0, "y": 27},
          desc="Si esto se queda quieto, el tablero está mostrando historia y no presente. "
               "La alerta de latido es obligatoria en producción."),
    texto(11, "Qué demuestra", """
**Vía A (tablero en vivo):** lee la metadata DB. Siempre exacto, pero se cae con el clúster.

**Vía B (este tablero):** lee InfluxDB. Sobrevive a la caída del clúster y guarda historia.

La prueba: parar Postgres. El tablero A se pone en rojo; este sigue dibujando.
""", {"h": 6, "w": 12, "x": 12, "y": 27}),
]

dashboard = {
    "uid": "airflow-historico",
    "title": "Airflow 2.8.1 · Histórico (vía B · InfluxDB)",
    "description": "Mismos KPIs que el tablero en vivo, pero leídos de InfluxDB: sobrevive a la "
                   "caída del clúster y conserva el histórico fuera de la metadata DB.",
    "tags": ["airflow", "demo", "homelab", "fase2"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "editable": True,
    "graphTooltip": 1,
    "time": {"from": "now-6h", "to": "now"},
    "templating": {"list": []},
    "annotations": {"list": []},
    "links": [
        {"title": "Vía A · dashboard en vivo (metadata DB)",
         "url": "/d/airflow-visibilidad", "type": "link", "targetBlank": False,
         "icon": "external link"},
    ],
    "panels": panels,
}

json.dump(dashboard, sys.stdout, indent=2, ensure_ascii=False)
sys.stdout.write("\n")
