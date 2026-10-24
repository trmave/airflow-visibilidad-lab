#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh · levanta el laboratorio entero desde cero, en un comando.
#
#   git clone <repo> airflow-lab && cd airflow-lab
#   bash bootstrap.sh
#
# Qué hace, en orden:
#   1. Comprueba requisitos (docker, compose, puertos libres).
#   2. Genera .env con claves aleatorias (si no existe).
#   3. Construye y levanta los 8 servicios.
#   4. Espera a que la metadata DB esté sana y a que `airflow db migrate` termine.
#   5. Crea el rol de solo lectura `grafana_ro` (idempotente).
#   6. Espera a que el scheduler parsee los 46 DAGs y los despausa.
#   7. Siembra el histórico sintético (930 + 3.360 filas).
#   8. Verifica los dos dashboards contra Grafana (12 + 11 paneles).
#
# Es idempotente: si ya está levantado, no rompe nada y vuelve a verificar.
# Para empezar de cero de verdad:  docker compose down -v && bash bootstrap.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"

PUERTO_AIRFLOW=8085
PUERTO_GRAFANA=3050
PUERTO_WEBHOOK=8099
FALLOS=0

paso()  { printf '\n\033[1;36m▸ %s\033[0m\n' "$*"; }
ok()    { printf '  \033[0;32m✓\033[0m %s\n' "$*"; }
mal()   { printf '  \033[0;31m✗\033[0m %s\n' "$*"; FALLOS=$((FALLOS+1)); }
info()  { printf '  · %s\n' "$*"; }

# --- 1 · requisitos -----------------------------------------------------------
paso "1/8 Requisitos"
command -v docker >/dev/null 2>&1 && ok "docker $(docker --version | cut -d' ' -f3 | tr -d ,)" || { mal "docker no está instalado"; exit 1; }
docker compose version >/dev/null 2>&1 && ok "compose $(docker compose version --short)" || { mal "docker compose no disponible"; exit 1; }
LIBRES=0
for p in "$PUERTO_AIRFLOW" "$PUERTO_GRAFANA" "$PUERTO_WEBHOOK"; do
  if nc -z -w1 127.0.0.1 "$p" >/dev/null 2>&1; then
    # Si el que ocupa el puerto es nuestro propio contenedor, es un reinicio: vale.
    if docker ps --format '{{.Ports}}' | grep -q ":$p->"; then info "puerto $p en uso por el propio laboratorio (ok)"
    else mal "puerto $p ocupado por otro proceso"; fi
  else ok "puerto $p libre"; fi
  LIBRES=$((LIBRES+1))
done

# --- 2 · secretos ------------------------------------------------------------
paso "2/8 Secretos (.env)"
if [ -f .env ]; then
  ok ".env ya existe (no se toca: regenerarlo desincroniza la base)"
else
  umask 027
  FERNET=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
  cat > .env <<EOF
PG_AIRFLOW_PASSWORD=$(openssl rand -hex 24)
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 12)
AIRFLOW_FERNET_KEY=$FERNET
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)
GRAFANA_RO_PASSWORD=$(openssl rand -hex 24)
RENDERER_TOKEN=$(openssl rand -hex 24)
INFLUX_ADMIN_PASSWORD=$(openssl rand -hex 12)
INFLUX_TOKEN=$(openssl rand -hex 32)
EOF
  chmod 640 .env
  ok ".env creado con 8 claves aleatorias (640)"
fi
grep -q '^INFLUX_TOKEN=' .env || { echo "INFLUX_TOKEN=$(openssl rand -hex 32)" >> .env; ok "añadido INFLUX_TOKEN (faltaba)"; }

# --- 3 · levantar ------------------------------------------------------------
paso "3/8 Levantando servicios"
sudo chmod -R a+rX grafana 2>/dev/null || chmod -R a+rX grafana
docker compose build collector >/dev/null 2>&1 && ok "imagen del colector construida"
docker compose up -d >/tmp/bootstrap-up.log 2>&1 && ok "docker compose up -d" || { mal "falló compose up (ver /tmp/bootstrap-up.log)"; tail -5 /tmp/bootstrap-up.log; }

# --- 4 · esperar la base y la migración -------------------------------------
paso "4/8 Esperando la metadata DB y la migración de Airflow"
for i in $(seq 1 60); do
  docker compose ps airflow-pg --format '{{.Health}}' 2>/dev/null | grep -q healthy && { ok "Postgres sana (${i}0s)"; break; }
  sleep 10
done
for i in $(seq 1 40); do
  estado=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' airflow-lab-init 2>/dev/null || echo "none -1")
  case "$estado" in
    "exited 0") ok "airflow db migrate + usuario admin: OK"; break;;
    "running 0") info "migrando…";;
    *) if [ "$i" = "40" ]; then mal "airflow-init no terminó bien ($estado)"; fi;;
  esac
  sleep 10
done

# --- 5 · rol de solo lectura -------------------------------------------------
paso "5/8 Rol grafana_ro (solo lectura)"
ROPASS=$(grep '^GRAFANA_RO_PASSWORD=' .env | cut -d= -f2)
if docker exec -i airflow-lab-pg psql -U airflow -d airflow -v ro_pass="$ROPASS" -f - < sql/10_rol_grafana_ro.sql >/tmp/bootstrap-rol.log 2>&1; then
  tablas=$(docker exec -i airflow-lab-pg psql -U airflow -d airflow -tAc \
    "SELECT count(*) FROM information_schema.role_table_grants WHERE grantee='grafana_ro'")
  [ "${tablas:-0}" -ge 6 ] && ok "rol grafana_ro con SELECT sobre $tablas tablas" || mal "el rol no tiene los permisos esperados"
else
  mal "no se pudo crear el rol (ver /tmp/bootstrap-rol.log)"
fi

# --- 6 · DAGs parseados y despausados ---------------------------------------
paso "6/8 Esperando a que el scheduler parsee los DAGs"
for i in $(seq 1 30); do
  dags=$(docker exec -i airflow-lab-pg psql -U airflow -d airflow -tAc "SELECT count(*) FROM dag" 2>/dev/null | tr -d ' ')
  if [ "${dags:-0}" -ge 46 ]; then ok "$dags DAGs en la metadata DB"; break; fi
  sleep 10
done
docker exec -i airflow-lab-pg psql -U airflow -d airflow -c \
  "UPDATE dag SET is_paused=false WHERE is_paused" >/dev/null 2>&1 && ok "DAGs despausados (sin esto no hay ejecuciones)"

# --- 7 · histórico sintético -------------------------------------------------
paso "7/8 Sembrando histórico"
docker exec -i airflow-lab-pg psql -U airflow -d airflow -f - < sql/20_seed_historico.sql >/tmp/bootstrap-seed1.log 2>&1 \
  && ok "histórico de los 6 DAGs base" || mal "falló el primer seed"
docker exec -i airflow-lab-pg psql -U airflow -d airflow -f - < sql/21_seed_bulk.sql >/tmp/bootstrap-seed2.log 2>&1 \
  && ok "histórico de los 40 DAGs de volumen" || mal "falló el seed bulk"
filas=$(docker exec -i airflow-lab-pg psql -U airflow -d airflow -tAc "SELECT count(*) FROM dag_run" | tr -d ' ')
info "dag_run: $filas filas"

# --- 8 · verificar los dos dashboards ---------------------------------------
paso "8/8 Verificando contra Grafana"
export GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)
for i in $(seq 1 30); do
  curl -s -m 3 http://127.0.0.1:$PUERTO_GRAFANA/api/health 2>/dev/null | grep -q '"database": "ok"' && break
  sleep 5
done
if python3 scripts/verify_dashboard.py --ventana "24 hours" >/tmp/bootstrap-vA.log 2>&1; then
  ok "vía A · $(grep -c '^\[ OK \]' /tmp/bootstrap-vA.log) paneles OK (metadata DB)"
else
  mal "vía A con errores (ver /tmp/bootstrap-vA.log)"; grep '\[FAIL\]' /tmp/bootstrap-vA.log | head -3
fi
# El colector necesita un ciclo para tener datos en Influx.
sleep 45
if python3 scripts/verify_historico.py >/tmp/bootstrap-vB.log 2>&1; then
  ok "vía B · $(grep -c '^\[ OK \]' /tmp/bootstrap-vB.log) paneles OK (InfluxDB)"
else
  mal "vía B con errores (ver /tmp/bootstrap-vB.log)"; grep -E '\[FAIL\]|\[VACÍO\]' /tmp/bootstrap-vB.log | head -3
fi

# --- resumen -----------------------------------------------------------------
IP=$(hostname -I | awk '{print $1}')
printf '\n\033[1m%s\033[0m\n' "──────────────────────────────────────────────────────────"
printf '  Vía A · en vivo    http://%s:%s/d/airflow-visibilidad\n' "$IP" "$PUERTO_GRAFANA"
printf '  Vía B · histórico  http://%s:%s/d/airflow-historico\n' "$IP" "$PUERTO_GRAFANA"
printf '  Airflow            http://%s:%s\n' "$IP" "$PUERTO_AIRFLOW"
printf '  Alertas recibidas  http://%s:%s\n' "$IP" "$PUERTO_WEBHOOK"
printf '  Credenciales       .env (640) — usuario admin de Grafana y de Airflow\n'
printf '\033[1m%s\033[0m\n' "──────────────────────────────────────────────────────────"
if [ "$FALLOS" -eq 0 ]; then
  printf '\033[0;32mTodo listo. 0 fallos.\033[0m\n'; exit 0
else
  printf '\033[0;31mTerminó con %s problema(s).\033[0m\n' "$FALLOS"; exit 1
fi
