# 05 · Alertas

Las reglas viven en [`../grafana/provisioning/alerting/airflow.yaml`](../grafana/provisioning/alerting/airflow.yaml)
y se cargan solas al arrancar Grafana. Comprobado en el laboratorio:

```text
$ curl -s -u admin:*** http://127.0.0.1:3050/api/v1/provisioning/alert-rules
4 reglas
 - airflow-running-1h   | Airflow · DAG corriendo más de 1 hora                  | for=5m
 - airflow-queued-15m   | Airflow · Runs en cola más de 15 minutos                 | for=10m
 - airflow-fail-ratio   | Airflow · Tasa de fallos superior al 20 % en la última hora | for=15m
 - airflow-parse-stale  | Airflow · DAGs sin parsear hace más de 2 horas           | for=10m
```

## Las cuatro reglas y por qué cada una

| Regla | Condición | `for` | Severidad | Qué significa en la práctica |
|---|---|---|---|---|
| **running > 1 h** | `count(dag_run running con start_date < now()-1h) > 0` | 5 min | warning | Es *la* alerta del proyecto: hoy nadie se entera de que un DAG está colgado. En k8s suele ser un job que se quedó sin nodo, un sensor esperando para siempre o una tarea que consume más de lo que el pod permite |
| **queued > 15 min** | `count(dag_run queued con queued_at < now()-15min) > 0` | 10 min | warning | El run existe pero no arranca: pod `Pending` (CPU/memoria), PVC sin bound, `max_active_runs`, pool saturado |
| **ratio de fallos > 20 %** | `failed / total * 100 > 20` en 1 h | 15 min | info | Detecta una avería transversal (dependencia caída, cambio de esquema) que los paneles individuales no gritan |
| **parseo viejo > 2 h** | `count(dag activo con last_parsed_time < now()-2h) > 0` | 10 min | critical | La más silenciosa: si el scheduler deja de leer el repositorio, **todos los demás paneles mienten por omisión** |

## Decisiones de diseño de las alertas

- **`for` antes que umbral.** Un run que lleva 61 minutos no es un incidente; uno que lleva 65 sí
  (o no, y se sube el umbral). El margen evita que la primera alerta del proyecto sea un falso
  positivo el día de la demo.
- **`noDataState: OK`.** Si la consulta no devuelve filas (base vacía, reinicio), no se alerta:
  de eso ya se encarga la regla de parseo y el `execErrState: Error`.
- **Sin lista de DAGs excluidos en el YAML.** Si el cliente tiene DAGs lentos por diseño
  (backfills, esperas de terceros), la lista de exclusión se hace en el SQL
  (`AND dag_id NOT IN (…)`) y se documenta en el propio panel. Subir el umbral global es la
  peor opción: apaga la alerta para todos.
- **Una sola alerta "de negocio" al principio.** Recomendación al cliente: activar
  `running > 1 h` y `parse-stale` en la primera semana; las otras dos cuando el equipo ya mire
  el tablero a diario.

Estado real de las 4 reglas tras el arranque (motor de alertas evaluando cada minuto):

```text
Airflow · DAG corriendo más de 1 hora                  state=pending    health=ok
Airflow · Runs en cola más de 15 minutos               state=pending    health=ok
Airflow · Tasa de fallos superior al 20 % en la últi   state=inactive   health=ok
Airflow · DAGs sin parsear hace más de 2 horas         state=inactive   health=ok
```

- `pending` = la condición se cumple y está dentro del `for` (pasará a **Alerting**).
- `inactive` = la condición no se cumple ahora mismo (12,5 % de fallos < 20 %; nada viejo en el parseo).
- `health=ok` en las cuatro = la query y la expresión se evalúan sin error.

> Ojo con la regla del ratio: se escribió primero con **dos** columnas (`ratio`, `total`) y el
> motor de alertas la marcaba `health=error` aunque el SQL fuese correcto en `psql`. Con una sola
> columna + `HAVING count(*) >= 5` (menos de 5 runs = sin fila = `noDataState: OK`) quedó sana.

## Puntos de notificación (lo que falta para que esto avise)

El laboratorio **no** configura contacto a propósito: no hay a quién avisar y no se manda correo
desde una máquina de pruebas. En el cliente hace falta:

1. Un *contact point* (email, Slack, Teams, PagerDuty).
2. Una *notification policy* que enrute `severity=critical` al canal que despierta a alguien y
   `severity=info` al canal que se lee por la mañana.
3. Un *mute timing* para las ventanas de mantenimiento.

Ejemplo mínimo por API (adaptar al canal real):

```bash
curl -s -u admin:*** -X POST http://<grafana>/api/v1/provisioning/contact-points \
  -H 'Content-Type: application/json' -d '{
    "name": "airflow-oncall",
    "type": "slack",
    "settings": {"url": "<webhook del cliente>", "title": "Airflow"}
  }'
```

## Cómo probar una alerta sin esperar a que pase algo

En el laboratorio, la forma honesta de comprobar que la regla dispara es **provocar la condición**
y mirar el estado de la regla:

```bash
# 1. provocar: un run "running" con fecha de hace 2 horas
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c "
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, state, run_type,
                     external_trigger, clear_number)
VALUES ('demo_03_reporte_lento','test_alerta_running', now() - interval '2 hours',
        now() - interval '2 hours','running','manual',false,0);"

# 2. mirar el estado de la regla (Normal → Pending → Alerting tras el `for`)
PW=$(grep '^GRAFANA_ADMIN_PASSWORD=' /srv/appdata/airflow-lab/.env | cut -d= -f2)
curl -s -u admin:$PW http://127.0.0.1:3050/api/v1/provisioning/alert-rules/airflow-running-1h

# 3. limpiar
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c \
  "DELETE FROM dag_run WHERE run_id='test_alerta_running';"
```

Ojo: el estado de la regla tiene **un retraso de evaluación** (aquí 1 min de intervalo + el `for`
de 5 min). No es instantáneo y no debe probarse con un `sleep 10`.
