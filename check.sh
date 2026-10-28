#!/usr/bin/env bash
# Comprobación rápida de que el laboratorio está sano. No cambia nada.
#   bash check.sh
set -uo pipefail
cd "$(dirname "$0")"

PUERTO_AIRFLOW=${LAB_WEB_PORT:-8085}
PUERTO_GRAFANA=${LAB_GRAFANA_PORT:-3050}
PUERTO_WEBHOOK=${LAB_WEBHOOK_PORT:-8099}
FALLOS=0
ok()   { printf '  \033[0;32m✓\033[0m %s\n' "$*"; }
mal()  { printf '  \033[0;31m✗\033[0m %s\n' "$*"; FALLOS=$((FALLOS+1)); }
info() { printf '  · %s\n' "$*"; }

echo "▸ Contenedores"
for c in pg webserver scheduler grafana influx collector webhook; do
  estado=$(docker inspect -f '{{.State.Status}}' "airflow-lab-$c" 2>/dev/null || echo "ausente")
  [ "$estado" = "running" ] && ok "airflow-lab-$c · running" || mal "airflow-lab-$c · $estado"
done

echo "▸ Servicios"
[ "$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PUERTO_AIRFLOW/health")" = "200" ] \
  && ok "Airflow responde en :$PUERTO_AIRFLOW" || mal "Airflow no responde en :$PUERTO_AIRFLOW"
[ "$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PUERTO_GRAFANA/api/health")" = "200" ] \
  && ok "Grafana responde en :$PUERTO_GRAFANA" || mal "Grafana no responde en :$PUERTO_GRAFANA"
[ "$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PUERTO_WEBHOOK/")" = "200" ] \
  && ok "Receptor de alertas en :$PUERTO_WEBHOOK" || info "receptor de alertas no responde (no es crítico)"

echo "▸ Datos"
DAGS=$(docker exec airflow-lab-pg psql -U airflow -d airflow -tAc "select count(*) from dag where is_active" 2>/dev/null | tr -d ' ')
RUNS=$(docker exec airflow-lab-pg psql -U airflow -d airflow -tAc "select count(*) from dag_run" 2>/dev/null | tr -d ' ')
[ "${DAGS:-0}" -ge 40 ] 2>/dev/null && ok "$DAGS DAGs activos" || mal "DAGs activos: ${DAGS:-0} (esperado 46)"
[ "${RUNS:-0}" -ge 1000 ] 2>/dev/null && ok "$RUNS ejecuciones en la metadata DB" || mal "ejecuciones: ${RUNS:-0}"

echo "▸ Tableros"
if [ -f .env ]; then
  PW=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2-)
  for uid in airflow-visibilidad airflow-historico airflow-api; do
    code=$(curl -s -u "admin:$PW" -m 8 -o /dev/null -w '%{http_code}' \
           "http://127.0.0.1:$PUERTO_GRAFANA/api/dashboards/uid/$uid")
    [ "$code" = "200" ] && ok "/d/$uid cargado" || mal "/d/$uid → HTTP $code"
  done
else
  mal "no hay .env: ejecuta antes  bash bootstrap.sh"
fi

echo
if [ "$FALLOS" -eq 0 ]; then
  printf '\033[0;32mTodo correcto.\033[0m Abre http://localhost:%s\n' "$PUERTO_GRAFANA"
else
  printf '\033[0;31m%d problema(s).\033[0m Prueba: bash bootstrap.sh\n' "$FALLOS"
fi
exit $([ "$FALLOS" -eq 0 ] && echo 0 || echo 1)
