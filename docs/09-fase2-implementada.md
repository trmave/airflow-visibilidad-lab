# 09 · Fase 2 implementada: el tablero que sobrevive a la caída

> Aquí está lo que se construyó **en el laboratorio** para poder demostrar la vía B. En el cliente
> sigue siendo una fase aparte (`docs/08`), pero ya no es teoría: está montado, verificado y con
> la prueba de caída hecha.

## Los dos tableros

| | Vía A · en vivo | Vía B · histórico |
|---|---|---|
| URL | `/d/airflow-visibilidad` | `/d/airflow-historico` |
| Fuente | metadata DB de Airflow (Postgres) | **InfluxDB** |
| Datos | siempre exactos, al segundo | últimas 24 h de muestras (cada 30 s) |
| Si cae el clúster | **paneles en error** | **sigue dibujando** |
| Retención | la que tenga la metadata DB | 30 días en el lab (2 años en cliente) |

Los dos están enlazados: cada uno lleva un botón al otro, para poder saltar de uno a otro durante
la presentación sin teclear URLs.

## Qué se añadió al laboratorio (5 piezas)

1. **`influxdb:2.9.1-alpine`** — setup automático por variables (org `homelab`, bucket `airflow`,
   retención `720h`). Puerto 8086 **sin publicar**: solo se usa dentro de la red del laboratorio.
2. **`collector/`** — imagen propia (`python:3.12-slim` + `psycopg2-binary` + `requests`). Bucle
   cada 30 s: 6 KPIs globales + 1 gauge por DAG + un **latido** (`collector_last_success`).
   Usa el **mismo rol de solo lectura** `grafana_ro` que el dashboard.
3. **Datasource `influx-airflow`** provisionado por YAML; el token entra por
   `$__env{INFLUX_TOKEN}`, nunca desde el repositorio.
4. **Dashboard `airflow-historico`** (11 paneles) generado por script con consultas **Flux**.
5. **`scripts/verify_historico.py`** — ejecuta cada panel de la vía B contra Grafana y exige filas.

```text
$ docker compose ps
airflow-lab-influx      Up (healthy)
airflow-lab-collector   Up
airflow-lab-grafana     Up (healthy)
airflow-lab-pg          Up (healthy)
airflow-lab-scheduler   Up (healthy)
airflow-lab-webserver   Up (healthy)
airflow-lab-renderer    Up (healthy)

$ docker logs airflow-lab-collector
colector arrancado · intervalo 30s · destino http://influxdb:8086 bucket=airflow
ok · 22 líneas · dags_activos=46 ok24h=973 fail24h=218 running=1 >1h=1 queued>15m=0 · 0.03s
```

## La prueba de la caída (hecha, no supuesta)

**Paso 1** — se para Postgres, que es lo mismo que decir "el clúster de Airflow se cayó":

```bash
docker stop airflow-lab-pg
```

**Paso 2** — el colector lo detecta y lo dice en su log:

```text
ERROR OperationalError: could not translate host name "airflow-pg" to address:
Temporary failure in name resolution
```

**Paso 3** — las dos capturas, tomadas **al mismo tiempo**:

| Vía A (metadata DB) | Vía B (InfluxDB) |
|---|---|
| ![Vía A caída](img/caida-viaA.png) | ![Vía B sigue](img/caida-viaB.png) |
| panel en **"No data"** con el triángulo de error | **46 · 973 · 218 · 1** y las tablas siguen |

**Paso 4** — todo se recupera solo:

```text
docker start airflow-lab-pg
# 20 s después:  airflow-lab-pg  Up (healthy)
# colector:      ok · 22 líneas · dags_activos=46 ok24h=973 …
# vía A:         RESULTADO: 12 paneles verificados, 0 con error
```

No hubo que tocar nada: ni reiniciar Grafana, ni el colector, ni el dashboard.

## Lo que esto demuestra (y lo que no)

**Demuestra:** el tablero puede vivir fuera del destino de Airflow. Los números y el histórico
siguen ahí cuando la fuente original desaparece.

**No demuestra:** que se pueda ver el presente sin fuente. Durante la caída el colector calla, así
que la vía B muestra **la última foto y el histórico**, no lo que está pasando. Eso se dice en la
demo y se cubre en producción con la alerta de latido:

```flux
// si el colector lleva > 5 min sin escribir, alertar
from(bucket:"airflow") |> range(start:-15m)
  |> filter(fn:(r)=> r._measurement=="airflow_kpi" and r._field=="collector_last_success")
  |> last()
  |> map(fn:(r)=> ({r with _value: float(v: now()) - float(v: r._collector_last_success)}))
```

## Diferencias con el cliente (para no confundir la maqueta con el diseño)

| Laboratorio | Cliente |
|---|---|
| Colector en bucle dentro de un contenedor | **CronJob** con `concurrencyPolicy: Forbid` |
| Token de Influx en `.env` | `Secret` de Kubernetes |
| Retención 30 días | 2 años + downsampling a 1 h |
| Un solo nodo | CronJob con `nodeSelector` o tolerations según el clúster |
| Sin alerta de latido activa | alerta obligatoria (> 5 min sin escritura) |

## Comandos útiles

```bash
# ¿qué está escribiendo el colector?
docker logs -f airflow-lab-collector

# consulta manual a Influx (dentro del contenedor)
docker exec -it airflow-lab-influx influx query '
  from(bucket:"airflow") |> range(start:-10m)
  |> filter(fn:(r)=> r._measurement=="airflow_kpi") |> last()' --org homelab

# verificar los 11 paneles de la vía B
GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
  python3 scripts/verify_historico.py

# regenerar el dashboard
python3 scripts/generate_dashboard_historico.py > grafana/dashboards/airflow-historico.json
```

## Coste real en el laboratorio

- 2 contenedores: InfluxDB (~120 MB de RAM) + colector (~40 MB).
- Imagen del colector: 206 MB.
- Disco: Influx crece ~1 MB/día con 7 campos cada 30 s (en el lab). Con retención de 30 días no se
  nota; con 2 años hay que activar downsampling.
