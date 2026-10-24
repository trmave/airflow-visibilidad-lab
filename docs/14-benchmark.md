# 14 · Benchmark de las tres vías

> Todo lo de aquí está medido en el laboratorio con [`scripts/benchmark.py`](../scripts/benchmark.py),
> no estimado. Cada cifra se puede reproducir con los comandos del final.

## Método

- Para cada dashboard se ejecuta **el SQL/Flux/HTTP de cada panel** contra Grafana (`/api/ds/query`),
  4 repeticiones por panel, en el mismo minuto y con la misma ventana (24 h).
- Se mide: latencia por panel (p50), **latencia de un refresco completo** (todos los paneles una vez),
  bytes que devuelve Grafana y CPU acumulada del componente que cada vía golpea
  (`airflow-lab-pg` / `airflow-lab-influx` / `airflow-lab-webserver`, leída del cgroup).
- Tres escenarios de volumen: **5.567** filas en `dag_run`, **203.367** filas, y las 203.367 con el
  índice recomendado. Después se borran las filas de prueba y se vuelve al volumen original.

**Aviso honesto sobre la métrica de CPU:** es la del contenedor completo, así que incluye la
actividad propia del scheduler de Airflow durante la medición. Sirve para comparar la misma vía
entre escenarios, no como cifra absoluta. La métrica limpia es **tuplas devueltas por refresco**,
que sí es solo de las consultas.

## Resultado 1 · volumen actual (5.567 filas)

| | Vía A · metadata DB | Vía B · InfluxDB | Vía C · API REST |
|---|---|---|---|
| Paneles medidos | 12 | 9 | 7 |
| Latencia p50 por panel | **13,6 ms** | 19,6 ms | 200,5 ms |
| **Refresco completo** | **172 ms** | 187 ms | **1.454 ms** |
| Bytes por refresco | 8,9 KB | 31,1 KB | 44,0 KB |
| CPU del origen por refresco | 43,5 ms | 75,4 ms | 1.283,6 ms |
| Errores | 0 | 0 | 0 |

**Lectura:** con volumen pequeño, A y B empatan (170-190 ms por refresco). **C es 8 veces más
lenta** que A y consume 30 veces más CPU, porque cada panel es una petición HTTP autenticada al
webserver (Gunicorn) que a su vez consulta la base: se paga dos veces el mismo trabajo.

## Resultado 2 · volumen alto (203.367 filas, 88 MB)

| | A · metadata DB | B · InfluxDB | C · API REST |
|---|---|---|---|
| Latencia p50 | 24,0 ms (**+76 %**) | 22,9 ms (+17 %) | 206,9 ms (+3 %) |
| Refresco completo | **648 ms (+276 %)** | **211 ms (+13 %)** | 1.546 ms (+6 %) |
| Tuplas devueltas por refresco | **1.238.119** | n/a (lee sus muestras) | n/a (conteo en servidor) |
| CPU del origen | 911,5 ms | 93,2 ms | 1.343,1 ms |

**Lectura:** el volumen no afecta igual a las tres.

- **A se degrada con el volumen**: 1,24 millones de filas recorridas por refresco. Ese es el *seq
  scan* que avisábamos en `docs/02`: el panel "Último estado de cada DAG" pasa de 20 ms a **398 ms**
  él solo, porque hace `row_number()` sobre toda la tabla.
- **B no se entera** (+13 %): lee sus propias muestras cada 30 s, no la tabla de Airflow.
- **C tampoco** (+6 %): el conteo lo hace el servidor de Airflow, que sí consulta la tabla pero
  devuelve un número.

## Resultado 3 · el mismo volumen, con el índice recomendado

```sql
CREATE INDEX CONCURRENTLY idx_dag_run_state_execution_date ON dag_run (state, execution_date);
```

| | A sin índice | A con índice | Cambio |
|---|---|---|---|
| Latencia p50 por panel | 24,0 ms | **13,6 ms** | −43 % (vuelve al valor de volumen bajo) |
| Refresco completo | 648 ms | **506 ms** | −22 % |
| Tuplas devueltas por refresco | 1.238.119 | **252.345** | **÷5** |
| CPU del origen | 911,5 ms | **377,9 ms** | −59 % |

**Lectura:** el índice recupera casi todo el rendimiento de los paneles de conteo. **No arregla** el
panel "Último estado de cada DAG" (335 ms con índice), porque su problema es otro: la función de
ventana sobre todas las filas. Se arregla acotando la ventana:

```sql
-- en docs/02 está el detalle; basta filtrar antes de la ventana
WHERE execution_date > now() - interval '7 days'
```

## Cuadro comparativo final

| Criterio | A · metadata DB | B · InfluxDB | C · API REST |
|---|---|---|---|
| Refresco con volumen bajo | **172 ms** | 187 ms | 1.454 ms |
| Refresco con 203k filas | 648 ms (506 con índice) | **211 ms** | 1.546 ms |
| Sensibilidad al volumen | **alta** (×3,8) | ninguna | ninguna |
| Carga añadida al origen | media (base) | baja (colector) | **alta** (webserver) |
| Credencial necesaria | rol SQL `SELECT` | token de Influx | usuario Airflow (RBAC) |
| Cambios en Airflow | ninguno | ninguno | **config + reinicio** |
| Paneles que puede dar | 12 | 11 | 7 |
| Sobrevive a la caída del clúster | no | **sí** | no |
| Historia | la no purgada | **propia** | la no purgada |

## Qué recomienda el benchmark, según el tamaño del cliente

| Filas en `dag_run` | Recomendación |
|---|---|
| < 100.000 | **A directa**, sin índice. 170 ms por refresco es imperceptible |
| 100.000 – 2.000.000 | **A + el índice** (`CONCURRENTLY`, una vez). Medido: −59 % de CPU y tuplas ÷5 |
| > 2.000.000 | **B** (o A contra una réplica de lectura). A empieza a competir con el scheduler |
| Sin credenciales de base permitidas | **C**, asumiendo 1,5 s por refresco y solo los paneles de conteo |

Y una regla que sale de los números, no de la teoría: **el refresco de 1,5 s de C es aceptable para
un panel de operaciones, pero no para un dashboard de pared** (que refresca cada 30 s con 10
personas mirando). A y B sí aguantan ese uso.

## Lo que este benchmark NO mide (para no vender de más)

- **Concurrencia**: todo se midió con un solo cliente. Diez personas mirando a la vez multiplican la
  carga, y ahí la vía C (que ataca el webserver) es la más frágil.
- **Latencia de red real**: todo es loopback dentro del mismo nodo. En el cliente, A y C cruzan la
  red hacia el clúster; B cruza hacia la VM. Un RTT de 2 ms cambia poco, uno de 40 ms cambia todo.
- **InfluxDB con millones de series**: aquí hay 7 campos cada 30 s. Con etiquetas por DAG y años de
  historia, el `group by` de Flux también cuesta.
- **La vía C con paginación completa**: el ranking por DAG (46 llamadas) no se midió porque no está
  implementado; se estima a partir del coste por llamada (≈200 ms × 46 ≈ 9 s).
- **Compresión y caché del navegador**: Grafana no cachea SQL; sí cachea el bundle JS.

## Cómo reproducirlo

```bash
cd /srv/appdata/airflow-lab

# escenario actual
GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
  python3 scripts/benchmark.py --repeticiones 4 --escenario "actual" --json /tmp/bench.json

# una sola vía (más rápido)
GRAFANA_PASS=... python3 scripts/benchmark.py --vias A --repeticiones 4

# volumen alto: insertar 197.800 filas de prueba, medir, borrar
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c "
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state, run_type,
                     external_trigger, clear_number, data_interval_start, data_interval_end,
                     queued_at, updated_at)
SELECT d.dag_id, 'bench__' || d.dag_id || '__' || g,
       now() - (random() * interval '14 days'), now() - (random() * interval '14 days'), now(),
       CASE WHEN random() < 0.15 THEN 'failed' ELSE 'success' END, 'scheduled', false, 0,
       now() - interval '14 days', now(), now(), now()
FROM dag d CROSS JOIN generate_series(1, 4300) g WHERE d.dag_id LIKE 'demo_%';"
docker exec airflow-lab-pg psql -U airflow -d airflow -c "ANALYZE dag_run"
# … medir …
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c "DELETE FROM dag_run WHERE run_id LIKE 'bench__%'" -c "VACUUM ANALYZE dag_run"
```

## Datos crudos

Los JSON de las cuatro mediciones (actual, alto, alto+índice, vuelta) están en el laboratorio
(`/tmp/bench_*.json`) y los genera el propio script con `--json`. Las tablas de esta página salen
de ahí, sin retoques.
