# 02 · KPIs, SQL y rendimiento

Todas las queries están pensadas para **PostgreSQL** (metadata DB de Airflow 2.8.x) y se
ejecutan con el rol `grafana_ro`. Al final están las variantes para MySQL y para Airflow 2.9+.

Variables que aparecen:

| Variable | Origen | Valores |
|---|---|---|
| `$ventana` | variable de dashboard (custom) | `1 hour`, `6 hours`, `24 hours`, `7 days`, `30 days` |
| `$__timeFilter(col)` | macro de Grafana | se expande a `col BETWEEN $__timeFrom() AND $__timeTo()` |
| `$__timeGroupAlias(col, $__interval)` | macro de Grafana | agrupa en buckets según el zoom |

---

## KPI 1 · Total de DAGs

```sql
SELECT count(*)                              AS dags_activos,
       count(*) FILTER (WHERE is_paused)     AS pausados
FROM dag
WHERE is_active AND NOT is_subdag;
```

**Cómo leerlo.** `is_active` significa "el scheduler lo tiene en el DagBag ahora mismo". Un DAG
borrado del repositorio desaparece de esta cuenta en la siguiente pasada de parseo, aunque
sigan sus `dag_run` históricos. Es la cifra correcta para "cuántos DAGs tiene el equipo",
no `count(*) FROM dag` (que incluye los que ya no existen).

**Trampa.** Si el DagBag falla al parsear un fichero, ese DAG **no** se cuenta: por eso el panel
11 (salud del parseo) va al lado. Un total que baja de golpe suele ser un error de import, no
un borrado.

---

## KPI 2 y 3 · Runs OK y runs fallidos

```sql
SELECT count(*) AS ok
FROM dag_run
WHERE state = 'success'
  AND execution_date > now() - interval '$ventana';

SELECT count(*) AS fallidos
FROM dag_run
WHERE state = 'failed'
  AND execution_date > now() - interval '$ventana';
```

**Cómo leerlo.** Cuenta **ejecuciones** (`dag_run`), no DAGs. Un DAG que corre cada 5 minutos
aporta 288 runs al día: es lo que interesa para medir salud operativa. Si el cliente pregunta
"¿cuántos DAGs están rotos?", la respuesta es el panel 10 (último estado por DAG), no este.

**Por qué `execution_date` y no `start_date`.** `execution_date` es la fecha lógica del run: no
cambia si el run se reintenta o se vuelve a encolar, y no se queda fuera de la ventana porque el
scheduler arrancase tarde. `start_date` puede ser `NULL` (runs en cola), lo que descuadraría los
totales.

**Ruido que conviene excluir en producción.** Añadir `AND run_type <> 'backfill'` si el cliente
hace reprocesos masivos: un backfill de dos años mete 50.000 runs `success` en una tarde y el
panel deja de significar nada.

**Estados posibles de `dag_run`** (Airflow 2.8): `queued`, `running`, `success`, `failed`,
`upstream_failed`, `skipped`. Si aparecen `upstream_failed` en volumen, el panel de fallos se
queda corto: conviene sumarlos o añadir un panel propio.

---

## KPI 4 · Corriendo más de 1 hora

```sql
SELECT count(*) AS mas_de_1_hora
FROM dag_run
WHERE state = 'running'
  AND start_date < now() - interval '1 hour';
```

Detalle (panel 8):

```sql
SELECT dag_id,
       run_id,
       start_date,
       round(extract(epoch FROM (now() - start_date)) / 60)::int AS minutos
FROM dag_run
WHERE state = 'running'
  AND start_date < now() - interval '1 hour'
ORDER BY start_date
LIMIT 50;
```

**Cómo leerlo.** Es el run que se pasa del umbral, con los minutos transcurridos para priorizar.
Un run de 3 horas puede ser legítimo (sensores, esperas externas, backfills). El umbral de 1 h
es una **decisión de negocio**: se puede cambiar sin tocar el SQL con otra variable
(`acumulativo > 1 hour`), y conviene excluir los DAGs conocidos como lentos por diseño para que
el panel no se vuelva ruido permanente.

**Por qué `start_date` y no `execution_date`.** Aquí medimos tiempo **transcurrido desde que
empezó**; la fecha lógica no dice nada sobre cuánto lleva corriendo.

**Coste.** Esta query usa el índice parcial `idx_dag_run_running_dags` que Airflow crea por
defecto (`state = 'running'`); es barata incluso con la tabla enorme.

---

## KPI 5 · Cola atascada (no lo pidió el cliente, lo necesita igual)

```sql
SELECT count(*) AS en_cola_mas_15min
FROM dag_run
WHERE state = 'queued'
  AND queued_at < now() - interval '15 minutes';
```

Detalle (panel 9):

```sql
SELECT dag_id,
       run_id,
       queued_at,
       round(extract(epoch FROM (now() - queued_at)) / 60)::int AS minutos_esperando
FROM dag_run
WHERE state = 'queued'
  AND queued_at < now() - interval '15 minutes'
ORDER BY queued_at
LIMIT 50;
```

**Cómo leerlo en Kubernetes.** Un run `queued` que no arranca es, casi siempre, un pod
`Pending`. Lo siguiente que se mira no es Airflow, es el clúster:

```bash
kubectl get pods -n airflow --field-selector=status.phase=Pending -o wide
kubectl describe pod -n airflow <pod>     # Events: Insufficient cpu / node(s) didn't match …
kubectl get pvc -n airflow                # PVC en Pending = sin StorageClass o sin volumen
```

También puede ser un pool saturado (`slot_pool`) o `max_active_runs` del DAG. El panel dice
*quién* espera; el `describe` dice *por qué*.

---

## Panel 6 · Serie temporal por estado

```sql
SELECT $__timeGroupAlias(execution_date, $__interval),
       state,
       count(*) AS runs
FROM dag_run
WHERE $__timeFilter(execution_date)
GROUP BY 1, 2
ORDER BY 1;
```

Grafana pivota `state` en una serie por estado. Ojo con el rango por defecto de los dashboards
de Airflow: si el cliente tiene retención de metadata (Airflow no borra `dag_run` por sí solo;
muchos equipos meten un `DELETE` por cron), la serie se corta sin avisar.

---

## Panel 7 · Top 10 DAGs con más fallos

```sql
SELECT dag_id, count(*) AS fallos
FROM dag_run
WHERE state = 'failed'
  AND execution_date > now() - interval '$ventana'
GROUP BY dag_id
ORDER BY fallos DESC
LIMIT 10;
```

Es el panel que convierte la cifra global ("35 fallos") en trabajo accionable: dice **qué** DAG.

---

## Panel 10 · Último estado de cada DAG

```sql
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
ORDER BY state, dag_id;
```

**Por qué existe.** Es la vista que el equipo mira a diario: "¿está todo bien ahora mismo?".
Un DAG cuyo último run quedó `failed` aparece en rojo aunque su tasa de fallo histórica sea 1 %.

**Coste.** El `row_number()` recorre todo `dag_run`. Con 4.338 filas es instantáneo; con 20 M
hay que filtrar primero:

```sql
WITH ultimo AS (
    SELECT dag_id, state, execution_date,
           row_number() OVER (PARTITION BY dag_id ORDER BY execution_date DESC) AS rn
    FROM dag_run
    WHERE execution_date > now() - interval '7 days'   -- ventana acotada
)
SELECT dag_id, state, execution_date FROM ultimo WHERE rn = 1;
```

---

## Panel 11 · Salud del parseo

```sql
SELECT count(*) FILTER (WHERE has_import_errors)                                  AS con_error_import,
       count(*) FILTER (WHERE last_parsed_time < now() - interval '2 hours')      AS parseo_viejo
FROM dag
WHERE is_active;
```

**Por qué está en el mismo dashboard.** Si el parseo se rompe, los paneles 1-5 mienten por
omisión: los DAGs desaparecen de la cuenta y los fallos dejan de registrarse. `parseo_viejo`
detecta el síntoma más silencioso de todos: el scheduler está vivo pero dejó de leer el
repositorio de DAGs.

---

## Rendimiento

### Lo que ya trae Airflow 2.8.1 (verificado en el código fuente)

```text
dag_run:
  Index("dag_id_state", dag_id, state)                      -- compuesto
  Index("idx_dag_run_dag_id", dag_id)
  Index("idx_dag_run_running_dags", state, dag_id) WHERE state='running'
  Index("idx_dag_run_queued_dags",  state, dag_id) WHERE state='queued'
  UniqueConstraint(dag_id, execution_date)
  UniqueConstraint(dag_id, run_id)
```

Consecuencia: **KPI 4 y KPI 5 son baratos siempre** (índices parciales). **KPI 2, 3, 6 y 7 no
tienen índice por fecha**: filtran por `state` + `execution_date` y, si la tabla es grande,
Postgres hace un *seq scan*. Con 4.000 filas no se nota; con 10 millones, sí.

### Tres salidas, en orden de preferencia

1. **Índice adicional** (una vez, `CONCURRENTLY` para no bloquear):

   ```sql
   CREATE INDEX CONCURRENTLY idx_dag_run_state_execution_date
       ON dag_run (state, execution_date);
   ```
   Airflow no se queja de índices extra y sobreviven a las migraciones. Ojo: ocupa espacio y
   añade coste a cada `INSERT` del scheduler (pequeño, pero real).

2. **Acotar la ventana**: si el cliente solo mira 24 h / 7 días, un panel con
   `execution_date > now() - interval '30 days'` fijo reduce el escaneo.

3. **Vista materializada refrescada por cron** (el plan B serio):

   ```sql
   CREATE MATERIALIZED VIEW mv_dag_run_diario AS
   SELECT dag_id,
          date_trunc('hour', execution_date) AS hora,
          state,
          count(*) AS runs
   FROM dag_run
   WHERE execution_date > now() - interval '90 days'
   GROUP BY 1, 2, 3;

   CREATE UNIQUE INDEX ON mv_dag_run_diario (dag_id, hora, state);
   -- refresco cada 5 min desde un CronJob del clúster:
   -- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dag_run_diario;
   ```
   El dashboard pasa a leer la vista; el "último estado por DAG" y "corriendo > 1 h" siguen
   yendo contra `dag_run` porque necesitan el segundo de ahora mismo.

### Antes de crear un índice: medir

```sql
-- tamaño real de las tablas que consulta el dashboard
SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid)) AS tamaño
FROM pg_stat_user_tables
WHERE relname IN ('dag','dag_run','task_instance')
ORDER BY pg_total_relation_size(relid) DESC;

-- ¿la query usa índice o escanea? (pegar la query del panel entre los $ )
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM dag_run WHERE state='failed' AND execution_date > now() - interval '24 hours';
```

Si sale `Seq Scan on dag_run` con más de ~2 M filas y el panel tarda segundos, es el momento del
índice. No antes: un índice que no se usa es coste puro en cada escritura del scheduler.

---

## Variantes

### MySQL (si la metadata DB es MySQL)

No existe `FILTER (WHERE …)` ni índices parciales. Los conteos se hacen con `SUM(CASE …)` y las
fechas con `UTC_TIMESTAMP()`:

```sql
SELECT SUM(CASE WHEN state='success' THEN 1 ELSE 0 END) AS ok,
       SUM(CASE WHEN state='failed'  THEN 1 ELSE 0 END) AS fallidos
FROM dag_run
WHERE execution_date > UTC_TIMESTAMP() - INTERVAL $horas HOUR;

SELECT COUNT(*) AS mas_de_1_hora
FROM dag_run
WHERE state='running' AND start_date < UTC_TIMESTAMP() - INTERVAL 1 HOUR;
```

Aquí `$horas` es una variable numérica (`24`, `168`), no un intervalo con texto. El índice
`idx_dag_run_running_dags` existe igual (sin la parte parcial; Airflow lo crea duplicado y
además fuerza el hint `USE INDEX (idx_dag_run_running_dags)` en su propio código).

### Airflow 2.9 / 3.x

`dag_run.execution_date` pasa a llamarse **`logical_date`**. Todo lo demás (estados, `start_date`,
`queued_at`, `run_type`) se mantiene. Basta reemplazar el nombre de la columna en los paneles
2, 3, 6, 7 y 10. En Airflow 3 además la tabla `dag` deja de tener `is_active` en la misma forma:
conviene revalidar el panel 1 contra el esquema real antes de migrar el dashboard.

### Tabla de equivalencias rápida

| Airflow 2.8.x | Airflow 2.9+ / 3.x |
|---|---|
| `dag_run.execution_date` | `dag_run.logical_date` |
| `dag.is_active` | `dag.is_stale` / `bundle_version` (revalidar) |
| `dag.schedule_interval` | `dag.timetable_summary` |

---

## Cómo validar los números contra la UI de Airflow

Cuando un panel dice 35 fallidos y el equipo dice "yo veo 30", casi siempre es una de estas tres:

1. **Zona horaria**: `execution_date` está en UTC. El panel filtra con `now()` de la base (UTC
   también), pero la UI de Airflow muestra la hora del navegador.
2. **Ventana**: la variable `$ventana` del dashboard no coincide con "las últimas 24 h" de la UI
   si alguien la dejó en `7 days`.
3. **`run_type`**: la UI incluye `backfill` y `manual`; el panel también, salvo que se excluya a
   propósito.

Comprobación directa, sin Grafana de por medio:

```bash
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c \
  "SELECT state, count(*) FROM dag_run
    WHERE execution_date > now() - interval '24 hours' GROUP BY 1 ORDER BY 2 DESC;"
```

Esa es la cifra de referencia. Si Grafana muestra algo distinto, el problema está en el panel,
no en los datos.
