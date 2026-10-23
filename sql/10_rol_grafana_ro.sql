-- ===========================================================================
-- Rol de solo lectura para Grafana sobre la metadata DB de Airflow.
-- Se ejecuta una vez, después de que `airflow db migrate` haya creado el
-- esquema. La contraseña se pasa como variable de psql (nunca en el fichero):
--
--   docker exec -i airflow-lab-pg psql -U airflow -d airflow \
--     -v ro_pass="$(grep '^GRAFANA_RO_PASSWORD=' /srv/appdata/airflow-lab/.env | cut -d= -f2)" \
--     -f - < sql/10_rol_grafana_ro.sql
--
-- Por qué un rol aparte: Grafana NUNCA debe conectar con el usuario de
-- Airflow. Con SELECT sobre las vistas de estado basta, y el statement_timeout
-- impide que un panel mal escrito bloquee la metadata DB de producción.
-- ===========================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN;
    END IF;
END
$$;

ALTER ROLE grafana_ro PASSWORD :'ro_pass';

-- Un panel colgado no debe quedarse 10 minutos escaneando dag_run.
ALTER ROLE grafana_ro SET statement_timeout = '30s';
ALTER ROLE grafana_ro SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;

GRANT CONNECT ON DATABASE airflow TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;

-- Solo las tablas que necesitan los paneles. Ni `log`, ni `ab_user`,
-- ni `serialized_dag` (contiene el código de los DAGs).
GRANT SELECT ON dag, dag_run, task_instance, dag_tag, import_error, slot_pool TO grafana_ro;

-- Verificación: debe devolver 6 tablas y el timeout de 30s.
\echo '--- privilegios ---'
SELECT table_name FROM information_schema.role_table_grants
WHERE grantee = 'grafana_ro' ORDER BY 1;
\echo '--- timeout del rol ---'
SELECT rolname, rolconfig FROM pg_roles WHERE rolname = 'grafana_ro';
