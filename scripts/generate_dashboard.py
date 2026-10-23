#!/usr/bin/env python3
"""Genera el dashboard de Grafana (JSON) para el laboratorio de Airflow 2.8.1.

Se genera por codigo y no a mano para que los paneles y el SQL queden
versionados y revisables en el repositorio, y para poder regenerar el JSON
si cambia una query.

Uso:
    python3 scripts/generate_dashboard.py > grafana/dashboards/airflow-visibilidad.json
"""
import json
import sys

DS = {"type": "grafana-postgresql-datasource", "uid": "airflow-pg"}

# ---------------------------------------------------------------------------
# SQL de cada panel. La variable $ventana es una variable de dashboard
# (custom) con valores tipo "1 hour", "24 hours", "7 days".
# ---------------------------------------------------------------------------
SQL = {
    "dags_activos": """
SELECT count(*) AS dags_activos,
       count(*) FILTER (WHERE is_paused) AS pausados
FROM dag
WHERE is_active AND NOT is_subdag
""",
    "ok": """
SELECT count(*) AS ok
FROM dag_run
WHERE state = 'success'
  AND execution_date > now() - interval '$ventana'
""",
    "fallidos": """
SELECT count(*) AS fallidos
FROM dag_run
WHERE state = 'failed'
  AND execution_date > now() - interval '$ventana'
""",
    "lentos": """
SELECT count(*) AS mas_de_1_hora
FROM dag_run
WHERE state = 'running'
  AND start_date < now() - interval '1 hour'
""",
    "cola": """
SELECT count(*) AS en_cola_mas_15min
FROM dag_run
WHERE state = 'queued'
  AND queued_at < now() - interval '15 minutes'
""",
    "serie": """
SELECT $__timeGroupAlias(execution_date, $__interval),
       state,
       count(*) AS runs
FROM dag_run
WHERE $__timeFilter(execution_date)
GROUP BY 1, 2
ORDER BY 1
""",
    "top_fallos": """
SELECT dag_id, count(*) AS fallos
FROM dag_run
WHERE state = 'failed'
  AND execution_date > now() - interval '$ventana'
GROUP BY dag_id
ORDER BY fallos DESC
LIMIT 10
""",
    "tabla_lentos": """
SELECT dag_id,
       run_id,
       start_date,
       round(extract(epoch FROM (now() - start_date)) / 60)::int AS minutos
FROM dag_run
WHERE state = 'running'
  AND start_date < now() - interval '1 hour'
ORDER BY start_date
LIMIT 50
""",
    "tabla_cola": """
SELECT dag_id,
       run_id,
       queued_at,
       round(extract(epoch FROM (now() - queued_at)) / 60)::int AS minutos_esperando
FROM dag_run
WHERE state = 'queued'
  AND queued_at < now() - interval '15 minutes'
ORDER BY queued_at
LIMIT 50
""",
    "ultimo_estado": """
WITH ultimo AS (
    SELECT dag_id,
           state,
           execution_date,
           row_number() OVER (PARTITION BY dag_id ORDER BY execution_date DESC) AS rn
    FROM dag_run
)
SELECT dag_id, state AS ultimo_estado, execution_date AS ultima_ejecucion
FROM ultimo
WHERE rn = 1
ORDER BY state, dag_id
""",
    "parseo": """
SELECT count(*) FILTER (WHERE has_import_errors) AS con_error_import,
       count(*) FILTER (WHERE last_parsed_time < now() - interval '2 hours') AS parseo_viejo
FROM dag
WHERE is_active
""",
}


def target(sql: str, fmt: str = "table", ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": DS,
        "rawQuery": True,
        "editorMode": "code",
        "format": fmt,
        "rawSql": sql.strip(),
    }


def stat(pid, title, sql, grid, unit="short", thresholds=None, desc=""):
    steps = thresholds or [{"color": "green", "value": None}]
    return {
        "id": pid,
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": grid,
        "targets": [target(sql)],
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
        },
        "fieldConfig": {
            "defaults": {
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": steps},
                "unit": unit,
            },
            "overrides": [],
        },
    }


def table(pid, title, sql, grid, desc=""):
    return {
        "id": pid,
        "type": "table",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": grid,
        "targets": [target(sql)],
        "options": {
            "showHeader": True,
            "cellHeight": "sm",
            "footer": {"show": False, "reducer": ["sum"], "countRows": False, "fields": ""},
        },
        "fieldConfig": {
            "defaults": {
                "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
                "mappings": [],
            },
            "overrides": [],
        },
    }


def timeseries(pid, title, sql, grid, desc=""):
    return {
        "id": pid,
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": grid,
        "targets": [target(sql, fmt="time_series")],
        "options": {
            "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom", "calcs": []},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "smooth",
                    "lineWidth": 2,
                    "fillOpacity": 12,
                    "gradientMode": "none",
                    "showPoints": "never",
                    "spanNulls": False,
                    "axisPlacement": "auto",
                    "scaleDistribution": {"type": "linear"},
                    "stacking": {"mode": "normal", "group": "A"},
                },
                "color": {"mode": "palette-classic"},
                "unit": "short",
            },
            "overrides": [],
        },
    }


def barchart(pid, title, sql, grid, desc=""):
    return {
        "id": pid,
        "type": "barchart",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": grid,
        "targets": [target(sql)],
        "options": {
            "xField": "dag_id",
            "yField": "fallos",
            "orientation": "horizontal",
            "showValue": "always",
            "stacking": "none",
            "groupWidth": 0.7,
            "barWidth": 0.9,
            "barRadius": 0.1,
            "fullHighlight": False,
            "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom", "calcs": []},
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "fieldConfig": {
            "defaults": {
                "custom": {"lineWidth": 1, "fillOpacity": 80, "gradientMode": "none",
                           "axisPlacement": "auto", "axisLabel": "", "scaleDistribution": {"type": "linear"},
                           "thresholdsStyle": {"mode": "off"}},
                "color": {"mode": "palette-classic"},
                "unit": "short",
                "mappings": [],
            },
            "overrides": [],
        },
    }


def bargauge(pid, title, sql, grid, desc=""):
    return {
        "id": pid,
        "type": "bargauge",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": grid,
        "targets": [target(sql)],
        "options": {
            "displayMode": "gradient",
            "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "showUnfilled": True,
            "valueMode": "color",
            "namePlacement": "auto",
            "sizing": "auto",
            "legend": {"show": False, "displayMode": "list", "placement": "bottom", "calcs": []},
        },
        "fieldConfig": {
            "defaults": {
                "mappings": [],
                "unit": "short",
                "color": {"mode": "palette-classic"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}, {"color": "red", "value": 5}],
                },
            },
            "overrides": [],
        },
    }


panels = [
    stat(1, "DAGs activos", SQL["dags_activos"],
         {"h": 5, "w": 6, "x": 0, "y": 0},
         desc="DAGs activos en el DagBag (is_active) sin subdags, y cuántos están pausados."),
    stat(2, "Runs OK · $ventana", SQL["ok"],
         {"h": 5, "w": 5, "x": 6, "y": 0},
         thresholds=[{"color": "green", "value": None}],
         desc="dag_run en estado success dentro de la ventana seleccionada."),
    stat(3, "Runs fallidos · $ventana", SQL["fallidos"],
         {"h": 5, "w": 4, "x": 11, "y": 0},
         thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
         desc="dag_run en estado failed dentro de la ventana seleccionada."),
    stat(4, "Corriendo > 1 hora", SQL["lentos"],
         {"h": 5, "w": 5, "x": 15, "y": 0},
         thresholds=[{"color": "green", "value": None},
                     {"color": "orange", "value": 1},
                     {"color": "red", "value": 3}],
         desc="dag_run en estado running con start_date de hace más de una hora."),
    stat(5, "En cola > 15 min", SQL["cola"],
         {"h": 5, "w": 4, "x": 20, "y": 0},
         thresholds=[{"color": "green", "value": None},
                     {"color": "orange", "value": 1},
                     {"color": "red", "value": 5}],
         desc="dag_run en estado queued: en Kubernetes suele ser un pod Pending."),
    timeseries(6, "Runs por estado en el tiempo", SQL["serie"],
               {"h": 9, "w": 14, "x": 0, "y": 5},
               desc="Conteo de dag_run agrupado por estado y por intervalo."),
    barchart(7, "Top 10 DAGs con más fallos · $ventana", SQL["top_fallos"],
             {"h": 9, "w": 10, "x": 14, "y": 5},
             desc="Los DAGs que más veces fallaron en la ventana."),
    table(8, "Corriendo más de 1 hora · detalle", SQL["tabla_lentos"],
          {"h": 8, "w": 12, "x": 0, "y": 14},
          desc="Quién se está pasando del umbral, con minutos transcurridos."),
    table(9, "En cola más de 15 minutos · detalle", SQL["tabla_cola"],
          {"h": 8, "w": 12, "x": 12, "y": 14},
          desc="Runs esperando turno: revisar recursos, pools y max_active_runs."),
    table(10, "Último estado de cada DAG", SQL["ultimo_estado"],
          {"h": 8, "w": 12, "x": 0, "y": 22},
          desc="El estado del run más reciente por DAG: la vista que se mira a diario."),
    stat(11, "Salud del parseo", SQL["parseo"],
         {"h": 8, "w": 6, "x": 12, "y": 22},
         thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
         desc="DAGs con error de import y DAGs cuyo último parseo tiene más de 2 horas."),
    stat(12, "Runs de las últimas 24 h", """
SELECT count(*) AS runs_24h
FROM dag_run
WHERE execution_date > now() - interval '24 hours'
""", {"h": 8, "w": 6, "x": 18, "y": 22},
         desc="Volumen total de ejecuciones del último día."),
]

dashboard = {
    "uid": "airflow-visibilidad",
    "title": "Airflow 2.8.1 · Visibilidad de DAGs",
    "description": "Total de DAGs, runs OK, fallidos, corriendo más de 1 hora y cola atascada. "
                   "Metadata DB de Airflow como fuente (solo lectura).",
    "tags": ["airflow", "demo", "homelab"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "editable": True,
    "graphTooltip": 1,
    "time": {"from": "now-24h", "to": "now"},
    "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m", "15m"]},
    "templating": {
        "list": [
            {
                "name": "ventana",
                "label": "Ventana",
                "type": "custom",
                "query": "1 hour : 1 hour,6 hours : 6 hours,24 hours : 24 hours,7 days : 7 days,30 days : 30 days",
                "current": {"text": "24 hours", "value": "24 hours"},
                "options": [],
                "includeAll": False,
                "multi": False,
                "hide": 0,
                "refresh": 0,
            }
        ]
    },
    "annotations": {"list": []},
    "links": [
        {"title": "Airflow UI", "url": "http://192.168.3.39:8085", "type": "link",
         "targetBlank": True, "icon": "external link"}
    ],
    "panels": panels,
}

json.dump(dashboard, sys.stdout, indent=2, ensure_ascii=False)
sys.stdout.write("\n")
