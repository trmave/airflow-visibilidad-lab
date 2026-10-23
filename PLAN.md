# PLAN · laboratorio de visibilidad de Airflow 2.8.1

Estado: **desplegado y verificado** (2026-10-23). Documentación de presentación en
`README.md` y `docs/`.

## Escenario medido antes de desplegar

```text
disco       796 GB libres en / (LVM)
RAM         9,5 GiB disponibles de 15 GiB · 8 vCPU
puertos     5433 / 8085 / 3050 / 5555 libres
docker      29.9.0 · compose v5.6.0
imágenes    apache/airflow:2.8.1-python3.10 (1,94 GB), grafana/grafana-oss:13.0.2 (1,47 GB),
            postgres:16.11-alpine (395 MB), grafana-image-renderer:v5.12.8 (591 MB)
```

## Objetivo

Demostrar, con un Airflow 2.8.1 real, que un dashboard de Grafana puede responder: total de
DAGs, runs OK, runs fallidos, corriendo > 1 h y cola atascada, **sin tocar el Airflow**.

## Decisiones

| Decisión | Por qué |
|---|---|
| Metadata DB como fuente (no StatsD) | StatsD da counters acumulativos; no responde conteos por ventana ni ">1 h" |
| Grafana fuera del contenedor de Airflow | Aísla el fallo: el dashboard cae y las tareas siguen |
| `grafana_ro` (SELECT + `statement_timeout=30s`) | Grafana nunca usa el usuario de Airflow; un panel no puede colgar la metadata DB |
| LocalExecutor | Menos piezas que KubernetesExecutor y suficiente para 46 DAGs de demo |
| 40 DAGs generados + histórico sintético | Una demo con 6 DAGs y 300 filas no representa a un cliente |
| Dashboard generado por script | El SQL queda versionado y se revisa en un diff |
| Renderer en contenedor | El Chromium del host no arranca (falta `libatk-1.0.so.0`) |

Descartado: Prometheus + `airflow-exporter` (instala un plugin dentro del scheduler y hay que
reiniciarlo); API REST de Airflow (paginación, no es fuente de métricas).

## Carpetas

```text
/srv/appdata/airflow-lab/   compose.yaml · .env (640) · PLAN.md · dags/ · grafana/ · sql/ · scripts/ · docs/
```

Sin datos fríos: todo es NVMe (4.338 filas, 3,5 GB de imágenes). El laboratorio es desechable.

## Parámetros ajustados

| Parámetro | Valor | Razón |
|---|---|---|
| `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` | 30 s | ver cambios de DAG rápido en la demo |
| `max_active_runs` en `demo_04` | 1 | generar estados `queued` sin clúster |
| `statement_timeout` de `grafana_ro` | 30 s | proteger la metadata DB |
| `updateIntervalSeconds` del provisioning | 30 s | recargar dashboards sin reiniciar Grafana |
| Grafana `refresh` del dashboard | 30 s | demo viva |

## Fases ejecutadas y verificación

| # | Acción | Verificación (salida real) |
|---|---|---|
| 0 | carpetas, red, `.env` con claves `openssl rand` | `.env` 640, 6 claves por longitud |
| 1 | `compose config --quiet` | sin errores |
| 2 | `compose up -d` | 4 servicios `healthy` + `airflow-init` exit 0 (`INIT_OK`) |
| 3 | versión y flags reales | `airflow version` → 2.8.1 · Grafana 13.0.2 |
| 4 | rol `grafana_ro` | 6 tablas con SELECT + `statement_timeout=30s` |
| 5 | seed histórico | 930 + 3.360 filas · 46 DAGs activos |
| 6 | dashboard + datasource | 12 paneles, `health: OK` |
| 7 | verificación funcional | `12 paneles verificados, 0 con error` |
| 8 | alertas | 4 reglas provisionadas |
| 9 | captura | `render:200` PNG 1800x1250 |

## Riesgos / límites

- Datos sintéticos (marcados `synthetic__`), no producción.
- Sin backup: `down -v` borra todo. A propósito.
- Lectura anónima en Grafana y HTTP plano, solo para LAN de demo.
- `queued` se simula con `max_active_runs`, no con pods Pending.

## Pendiente

- Puntos de notificación de alertas (no hay a quién avisar desde el nodo).
- Alta en el vigilante por cron si el laboratorio deja de ser desechable.
- Plan de implantación en el cliente: `docs/04-plan-cliente.md`.
