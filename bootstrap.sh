#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh · levanta el laboratorio entero desde cero, en un comando.
#
#   git clone <repo> airflow-lab && cd airflow-lab
#   bash bootstrap.sh              # levanta (o repara) el laboratorio
#   bash bootstrap.sh --limpio     # borra volúmenes y lo recrea TODO desde cero
#
# Qué hace, en orden:
#   1. Comprueba requisitos (docker, compose, puertos libres).
#   2. Genera .env con claves aleatorias (si no existe).
#   3. Construye y levanta los 8 servicios (con --limpio, antes borra los volúmenes).
#   4. Espera a que la metadata DB esté sana y a que `airflow db migrate` termine.
#   5. Crea el rol de solo lectura `grafana_ro` (idempotente).
#   5b. Crea el usuario Viewer de la API y genera el datasource de la vía C.
#   6. Espera a que el scheduler parsee los 46 DAGs y los despausa.
#   7. Siembra el histórico sintético (930 + 3.360 filas).
#   8. Verifica las tres vías contra Grafana (13 + 12 + 8 paneles).
#
# Es idempotente: si ya está levantado, no rompe nada y vuelve a verificar.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"

# -----------------------------------------------------------------------------
# Uso:
#   bash bootstrap.sh            → levanta (o repara) el laboratorio
#   bash bootstrap.sh --limpio   → borra volúmenes y lo recrea TODO desde cero
# -----------------------------------------------------------------------------
LIMPIO=0
for arg in "$@"; do
  case "$arg" in
    --limpio|--from-scratch) LIMPIO=1 ;;
    -h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "opción desconocida: $arg (usa --limpio o --help)"; exit 2 ;;
  esac
done

# Puertos y host configurables: si algo está ocupado, basta exportar otra cosa
#   LAB_WEB_PORT=9085 LAB_GRAFANA_PORT=4050 LAB_WEBHOOK_PORT=9099 bash bootstrap.sh
PUERTO_AIRFLOW=${LAB_WEB_PORT:-8085}
PUERTO_GRAFANA=${LAB_GRAFANA_PORT:-3050}
PUERTO_WEBHOOK=${LAB_WEBHOOK_PORT:-8099}
IP=$(hostname -I 2>/dev/null | awk '{print $1}')
export LAB_WEB_PORT=$PUERTO_AIRFLOW LAB_GRAFANA_PORT=$PUERTO_GRAFANA LAB_WEBHOOK_PORT=$PUERTO_WEBHOOK
export LAB_HOST=${LAB_HOST:-${IP:-localhost}}
export LAB_PROJECT=${LAB_PROJECT:-airflow-lab}
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
grep -q '^AIRFLOW_API_PASSWORD=' .env || { echo "AIRFLOW_API_PASSWORD=$(openssl rand -hex 12)" >> .env; ok "añadido AIRFLOW_API_PASSWORD (faltaba)"; }

# --- 3 · levantar ------------------------------------------------------------
paso "3/8 Levantando servicios"
if [ "$LIMPIO" = "1" ]; then
  info "modo --limpio: se borran contenedores y volúmenes (pgdata, grafanadata, influxdata)"
  if docker compose down -v >/tmp/bootstrap-down.log 2>&1; then
    ok "stack anterior eliminado (0 contenedores, 0 volúmenes)"
  else
    mal "no se pudo eliminar el stack anterior (ver /tmp/bootstrap-down.log)"
  fi
fi
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

# --- 5b · opción C: usuario Viewer + datasource de la API -------------------
paso "5b/8 Opción C · usuario de API y datasource"
APIPASS=$(grep '^AIRFLOW_API_PASSWORD=' .env | cut -d= -f2)
# El usuario se crea con el CLI DENTRO del webserver: hay que esperar a que esté
# arriba, o el `users create` falla y la vía C se queda sin credencial (bug real
# detectado en la primera reconstrucción con --limpio).
for i in $(seq 1 40); do
  docker exec airflow-lab-webserver curl -sf -o /dev/null http://localhost:8080/health >/dev/null 2>&1 && break
  sleep 5
done
salida_api=$(docker exec airflow-lab-webserver bash -lc \
  "airflow users create --username grafana_api --password '$APIPASS' \
   --firstname Grafana --lastname API --role Viewer --email grafana-api@lab.local" 2>&1)
case "$salida_api" in
  *"created with role"*)  ok "usuario grafana_api (rol Viewer) creado";;
  *"already exists"*)     info "usuario grafana_api ya existía";;
  *)                      mal "no se pudo crear grafana_api: $(printf '%s' "$salida_api" | tail -1)";;
esac
ROL_API=$(docker exec airflow-lab-pg psql -U airflow -d airflow -tAc \
  "SELECT r.name FROM ab_user u JOIN ab_user_role ur ON ur.user_id=u.id
   JOIN ab_role r ON r.id=ur.role_id WHERE u.username='grafana_api'" | tr -d ' ')
if [ "$ROL_API" = "Viewer" ]; then
  ok "verificado en la base: grafana_api → Viewer (lee, no puede disparar DAGs)"
else
  mal "grafana_api no tiene el rol Viewer (encontrado: '${ROL_API:-ninguno}')"
fi
if [ -f grafana/provisioning/datasources/airflow-api.yaml.example ]; then
  python3 - "$PWD" <<'PY'
import base64, pathlib, sys
root = pathlib.Path(sys.argv[1])
env = dict(l.split("=", 1) for l in (root / ".env").read_text().splitlines() if "=" in l and not l.startswith("#"))
b64 = base64.b64encode(f"grafana_api:{env['AIRFLOW_API_PASSWORD']}".encode()).decode()
tpl = (root / "grafana/provisioning/datasources/airflow-api.yaml.example").read_text()
(root / "grafana/provisioning/datasources/airflow-api.yaml").write_text(tpl.replace("__BASIC_B64__", b64))
PY
  ok "datasource de la API generado desde .env (no está en el repo)"
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
if curl -s -m 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:$PUERTO_GRAFANA/api/dashboards/uid/airflow-api | grep -q 200; then
  ok "vía C · dashboard de la API REST provisionado"
else
  info "vía C no disponible (plugin Infinity o datasource sin generar)"
fi

# --- resumen -----------------------------------------------------------------
printf '\n\033[1m%s\033[0m\n' "──────────────────────────────────────────────────────────"
printf '  Vía A · en vivo    http://%s:%s/d/airflow-visibilidad\n' "$IP" "$PUERTO_GRAFANA"
printf '  Vía B · histórico  http://%s:%s/d/airflow-historico\n' "$IP" "$PUERTO_GRAFANA"
printf '  Vía C · API REST   http://%s:%s/d/airflow-api\n' "$IP" "$PUERTO_GRAFANA"
printf '  Airflow            http://%s:%s\n' "$IP" "$PUERTO_AIRFLOW"
printf '  Alertas recibidas  http://%s:%s\n' "$IP" "$PUERTO_WEBHOOK"
printf '  Credenciales       .env (640) — usuario admin de Grafana y de Airflow\n'
printf '\033[1m%s\033[0m\n' "──────────────────────────────────────────────────────────"
if [ "$FALLOS" -eq 0 ]; then
  printf '\033[0;32mTodo listo. 0 fallos.\033[0m\n'; exit 0
else
  printf '\033[0;31mTerminó con %s problema(s).\033[0m\n' "$FALLOS"; exit 1
fi
