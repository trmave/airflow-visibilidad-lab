# 03 · Operación del laboratorio

## Mapa rápido

| Qué | Dónde |
|---|---|
| Grafana (dashboard) | <http://192.168.3.39:3050/d/airflow-visibilidad> |
| Airflow UI | <http://192.168.3.39:8085> (usuario `admin`) |
| Credenciales | `/srv/appdata/airflow-lab/.env` (640, `hermes:hermes`) |
| Compose | `/srv/appdata/airflow-lab/compose.yaml` |
| Metadata DB | contenedor `airflow-lab-pg`, base `airflow`, usuario `airflow` (admin) y `grafana_ro` (solo lectura) |
| Red | `airflow-lab-net` (aislada; solo se publican 3050 y 8085) |

> Este laboratorio **no** comparte nada con el resto del nodo: ni la base de datos de Komodo,
> ni el Postgres de `:5432`, ni Cockpit, ni la caja `192.168.3.60`. Se puede borrar entero sin
> efectos colaterales.

## Arrancar

```bash
cd /srv/appdata/airflow-lab
docker compose up -d
docker compose ps          # los 4 servicios deben quedar "healthy"
curl -s http://127.0.0.1:8085/health   # metadatabase + scheduler healthy
curl -s http://127.0.0.1:3050/api/health
```

Orden real de arranque: `airflow-pg` (healthcheck `pg_isready`) → `airflow-init`
(`db migrate` + usuario admin, termina con exit 0) → `airflow-webserver` y `airflow-scheduler`
→ `grafana` (+ `grafana-renderer`). El primer arranque tarda ~60-90 s porque migra el esquema.

## Parar / borrar

```bash
docker compose stop                  # conserva datos (volúmenes)
docker compose down                  # borra contenedores y red, conserva volúmenes
docker compose down -v               # borra también pgdata y grafanadata  ← irreversible
```

## Re-sembrar datos

```bash
# histórico de los 6 DAGs de historia + 40 de volumen (idempotente: borra y reinserta
# las filas con run_id synthetic__%)
docker exec -i airflow-lab-pg psql -U airflow -d airflow -f - < sql/20_seed_historico.sql
docker exec -i airflow-lab-pg psql -U airflow -d airflow -f - < sql/21_seed_bulk.sql
```

Los runs **reales** que genera el scheduler (los 46 DAGs están despausados) no se tocan.

## Verificar el dashboard

```bash
GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
  python3 scripts/verify_dashboard.py --ventana "24 hours"
# sale 0 si los 12 paneles responden; distinto de 0 si alguno falla
```

Este script es la prueba de que el dashboard funciona: manda el SQL **de cada panel** al
endpoint `/api/ds/query` de Grafana, con el mismo usuario y datasource que usa el navegador.

## Regenerar el dashboard o los DAGs

```bash
python3 scripts/generate_dashboard.py > grafana/dashboards/airflow-visibilidad.json
python3 scripts/generate_demo_dags.py --cantidad 40
# Grafana recarga los dashboards provisionados cada 30 s (updateIntervalSeconds)
```

## Capturas de pantalla

Grafana no trae Chromium: el navegador del host no arranca en este nodo (`libatk-1.0.so.0`
ausente). Por eso hay un contenedor `grafana-renderer` (Chromium propio) y el render se pide
por HTTP:

```bash
PW=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)
curl -s -u admin:$PW -o docs/img/dashboard.png \
  "http://127.0.0.1:3050/render/d/airflow-visibilidad?width=1800&height=1250&kiosk=1&from=now-24h&to=now&timezone=utc&var-ventana=24%20hours"
```

Un panel suelto: `.../render/d-solo/airflow-visibilidad/?panelId=8&width=900&height=400`.

## Diagnóstico (fallos que ya vimos)

| Síntoma | Causa | Solución |
|---|---|---|
| Grafana arranca pero no hay datasource ni dashboard | Provisioning con `permission denied` en `/etc/grafana/provisioning` (Grafana corre como uid 472 y las carpetas estaban en 750) | `chmod -R a+rX grafana/` |
| Todos los paneles en **"No data"** y **no llega ni un SELECT a Postgres** | Grafana ≥ 12 exige `jsonData.database`; con solo el campo `database` de nivel superior el front-end aborta con *"You do not currently have a default database configured"* | añadir `database: airflow` dentro de `jsonData` |
| `/render/...` devuelve 500 | Grafana 13 rechaza el `renderer_token` por defecto | `GF_RENDERING_RENDERER_TOKEN` en Grafana y `AUTH_TOKEN` en el renderer, con el mismo valor |
| El panel "en cola" siempre a 0 | los DAGs están pausados (`is_paused=true` por defecto en 2.8) o `max_active_runs` no limita nada | `airflow dags unpause <dag>` / poner `max_active_runs=1` |
| `airflow-init` sale con código ≠ 0 | la contraseña de Postgres del `.env` no coincide con la que se inicializó en el volumen | `docker compose down -v` y volver a levantar (pierde el histórico) |
| Los DAGs no aparecen tras crear el fichero | `DAG_DIR_LIST_INTERVAL` es 30 s y el DagBag tarda un ciclo más | esperar ~60 s y mirar `docker logs airflow-lab-scheduler` |

## Consultas de referencia

```bash
# ¿qué está corriendo ahora mismo?
docker exec airflow-lab-pg psql -U airflow -d airflow -c \
 "SELECT dag_id, state, start_date FROM dag_run WHERE state IN ('running','queued') ORDER BY start_date;"

# tamaño real de las tablas (para decidir si hace falta índice)
docker exec airflow-lab-pg psql -U airflow -d airflow -c \
 "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY 2 DESC LIMIT 5;"

# los paneles usan el rol de solo lectura: comprobar que NO puede escribir
docker exec airflow-lab-pg psql -U grafana_ro -d airflow -c "DELETE FROM dag_run WHERE id=1;"
# → ERROR: cannot execute DELETE in a read-only transaction
```

## Recursos que consume

| Contenedor | RAM típica | Notas |
|---|---|---|
| `airflow-lab-webserver` | ~350 MB | |
| `airflow-lab-scheduler` | ~450 MB | LocalExecutor: también ejecuta tareas |
| `airflow-lab-pg` | ~60 MB | 4.338 filas en `dag_run` |
| `airflow-lab-grafana` | ~150 MB | + plugin image-renderer descargado en el primer arranque |
| `airflow-lab-renderer` | picos de ~400 MB al renderizar | Chromium headless |

Total en reposo ≈ 1,2 GB. Los DAGs de demo hacen `sleep` cortos, así que el scheduler no se
calienta.
