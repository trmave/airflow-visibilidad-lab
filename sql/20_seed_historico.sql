-- ===========================================================================
-- Datos sintéticos para el laboratorio.
--
-- POR QUÉ EXISTE ESTE FICHERO: una instalación nueva de Airflow tarda días en
-- tener volumen suficiente para que una serie temporal y un contador de 24 h
-- se vean como en producción. Estos runs se insertan directamente en la
-- metadata DB con run_id 'synthetic__*' para poder distinguirlos (y borrarlos)
-- de los runs reales que genera el scheduler.
--
-- No se ejecuta solo: lo lanza scripts/seed_historico.sh después de que
-- Airflow haya creado el esquema y haya parseado los DAGs.
-- ===========================================================================

BEGIN;

-- Idempotencia: si ya se sembró, no duplicar.
DELETE FROM dag_run WHERE run_id LIKE 'synthetic__%';

-- ---------------------------------------------------------------------------
-- demo_01_pedidos_ok · cada 30 min durante 14 días, 97 % éxito / 3 % fallo
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_01_pedidos_ok',
       'synthetic__01ok__' || to_char(ts, 'YYYYMMDDHH24MI'),
       ts,
       ts + interval '10 seconds',
       ts + interval '4 minutes',
       CASE WHEN (extract(epoch FROM ts)::bigint % 40) = 0 THEN 'failed' ELSE 'success' END,
       'scheduled', false, 0,
       ts, ts + interval '30 minutes',
       ts - interval '8 seconds',
       ts + interval '4 minutes'
FROM generate_series(now() - interval '14 days', now() - interval '40 minutes', interval '30 minutes') AS ts;

-- ---------------------------------------------------------------------------
-- demo_02_etl_fallido · cada 2 h, siempre falla (es su propósito)
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_02_etl_fallido',
       'synthetic__02fail__' || to_char(ts, 'YYYYMMDDHH24MI'),
       ts,
       ts + interval '15 seconds',
       ts + interval '90 seconds',
       'failed',
       'scheduled', false, 0,
       ts, ts + interval '2 hours',
       ts - interval '5 seconds',
       ts + interval '90 seconds'
FROM generate_series(now() - interval '14 days', now() - interval '2 hours', interval '2 hours') AS ts;

-- ---------------------------------------------------------------------------
-- demo_05_clientes_diario · 1 diario, con fallos puntuales
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_05_clientes_diario',
       'synthetic__05cli__' || to_char(ts, 'YYYYMMDD'),
       ts,
       ts + interval '20 seconds',
       ts + interval '6 minutes',
       CASE WHEN (extract(day FROM ts)::int % 4) = 0 THEN 'failed' ELSE 'success' END,
       'scheduled', false, 0,
       ts, ts + interval '1 day',
       ts - interval '10 seconds',
       ts + interval '6 minutes'
FROM generate_series(now() - interval '14 days', now() - interval '1 day', interval '1 day') AS ts;

-- ---------------------------------------------------------------------------
-- demo_06_ml_features · cada 6 h, 90 % éxito
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_06_ml_features',
       'synthetic__06ml__' || to_char(ts, 'YYYYMMDDHH24'),
       ts,
       ts + interval '30 seconds',
       ts + interval '55 minutes',
       CASE WHEN (extract(epoch FROM ts)::bigint % 36000) = 0 THEN 'failed' ELSE 'success' END,
       'scheduled', false, 0,
       ts, ts + interval '6 hours',
       ts - interval '12 seconds',
       ts + interval '55 minutes'
FROM generate_series(now() - interval '14 days', now() - interval '6 hours', interval '6 hours') AS ts;

-- ---------------------------------------------------------------------------
-- demo_03_reporte_lento · 1 diario OK + UNO corriendo desde hace 95 minutos.
-- Este es el que hace que el panel "Corriendo > 1 hora" valga 1 desde el
-- primer minuto, sin esperar dos horas a que el DAG real llegue al umbral.
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_03_reporte_lento',
       'synthetic__03slow__' || to_char(ts, 'YYYYMMDD'),
       ts,
       ts,
       ts + interval '25 minutes',
       'success',
       'scheduled', false, 0,
       ts, ts + interval '1 day', ts - interval '6 seconds', ts + interval '25 minutes'
FROM generate_series(now() - interval '14 days', now() - interval '1 day', interval '1 day') AS ts;

INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
VALUES ('demo_03_reporte_lento', 'synthetic__03slow__running',
        now() - interval '95 minutes', now() - interval '95 minutes', NULL, 'running',
        'scheduled', false, 0,
        now() - interval '95 minutes', now(),
        now() - interval '96 minutes', now() - interval '95 minutes');

-- ---------------------------------------------------------------------------
-- demo_04_cola_saturada · 2 corriendo + 3 en cola (esperando turno por
-- max_active_runs=1). En Kubernetes estos 'queued' son pods Pending.
-- ---------------------------------------------------------------------------
INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
SELECT 'demo_04_cola_saturada',
       'synthetic__04queue__' || to_char(ts, 'YYYYMMDDHH24MI'),
       ts,
       NULL,
       NULL,
       'queued',
       'scheduled', false, 0,
       ts, ts + interval '10 minutes',
       ts - interval '3 minutes',
       ts
FROM generate_series(now() - interval '37 minutes', now() - interval '17 minutes', interval '5 minutes') AS ts;

INSERT INTO dag_run (dag_id, run_id, execution_date, start_date, end_date, state,
                     run_type, external_trigger, clear_number,
                     data_interval_start, data_interval_end, queued_at, updated_at)
VALUES ('demo_04_cola_saturada', 'synthetic__04queue__running',
        now() - interval '55 minutes', now() - interval '55 minutes', NULL, 'running',
        'scheduled', false, 0,
        now() - interval '55 minutes', now(),
        now() - interval '56 minutes', now() - interval '55 minutes');

COMMIT;

\echo '--- resumen de lo sembrado ---'
SELECT state, count(*) FROM dag_run WHERE run_id LIKE 'synthetic__%' GROUP BY state ORDER BY 2 DESC;
\echo '--- total de filas en dag_run ---'
SELECT count(*) FROM dag_run;
