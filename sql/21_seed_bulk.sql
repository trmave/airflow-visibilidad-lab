-- ===========================================================================
-- Histórico sintético para los 40 DAGs de relleno (demo_bulk_*).
-- Cada DAG recibe una tasa de fallo distinta y determinista, derivada de su
-- nombre, para que el ranking de fallos del dashboard sea creíble.
-- Se ejecuta DESPUÉS de que el scheduler haya parseado los DAGs.
-- ===========================================================================

BEGIN;

DELETE FROM dag_run WHERE run_id LIKE 'synthetic__bulk__%';

INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT d.dag_id,
       'synthetic__bulk__' || d.dag_id || '__' || to_char(ts, 'YYYYMMDDHH24'),
       ts,
       ts + interval '15 seconds',
       ts + interval '3 minutes',
       CASE
           WHEN (abs(hashtext(d.dag_id || to_char(ts, 'YYYYMMDDHH'))::bigint) % 100)
                < (abs(hashtext(d.dag_id)::bigint) % 25)
           THEN 'failed'
           ELSE 'success'
       END,
       'scheduled', false, 0,
       ts, ts + interval '4 hours',
       ts - interval '6 seconds',
       ts + interval '3 minutes'
FROM dag d
CROSS JOIN generate_series(now() - interval '14 days', now() - interval '4 hours', interval '4 hours') AS ts
WHERE d.dag_id LIKE 'demo_bulk_%';

COMMIT;

\echo '--- sembrado bulk ---'
SELECT count(*) AS runs_bulk FROM dag_run WHERE run_id LIKE 'synthetic__bulk__%';
SELECT state, count(*) FROM dag_run WHERE run_id LIKE 'synthetic__bulk__%' GROUP BY state;
