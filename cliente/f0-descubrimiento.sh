#!/usr/bin/env bash
# =============================================================================
# f0-descubrimiento.sh · Reconocimiento de SOLO LECTURA para la fase F0
#
# QUÉ HACE
#   Recorre el clúster y el Airflow del cliente y genera un informe en Markdown
#   con todo lo que hace falta para dimensionar el dashboard: versión real,
#   motor y tamaño de la metadata DB, índices, réplicas, Prometheus, Ingress…
#
# QUÉ NO HACE
#   · No escribe NADA (ni kubectl apply, ni SQL de escritura, ni toca pods).
#   · No imprime credenciales: las cadenas de conexión se enmascaran.
#   · No necesita acceso a la base desde fuera: consulta desde dentro del pod
#     del scheduler, con la sesión que Airflow ya tiene abierta.
#
# CÓMO SE USA
#   export AIRFLOW_NS=airflow                # (opcional; si no, lo detecta)
#   export GRAFANA_URL=http://10.0.0.5:3000  # (opcional; para probar la ruta)
#   bash f0-descubrimiento.sh | tee f0-informe.md
#
#   Requisitos: kubectl con contexto del clúster, permisos de lectura (get/list)
#   y `exec` sobre los pods de Airflow.
# =============================================================================
set -uo pipefail

NS="${AIRFLOW_NS:-}"
GRAFANA_URL="${GRAFANA_URL:-}"

hr() { printf '\n---\n\n'; }
h2() { printf '\n## %s\n\n' "$*"; }
ok() { printf -- '- ✓ %s\n' "$*"; }
ko() { printf -- '- ✗ %s\n' "$*"; }
dato() { printf -- '- %s\n' "$*"; }

# Enmascara usuario:contraseña de cualquier cadena de conexión.
mask() {
  sed -E 's#(://)[^:/@]+:[^@]+@#\1***:***@#g' \
    | sed -E 's/(password|pwd|token|secret|apikey)([\"'"'"']?\s*[=:]\s*)[^\s,"]+/\1\2***/Ig'
}

titulo() {
  cat <<EOF
# F0 · Descubrimiento de Airflow y observabilidad

- Fecha: $(date -u '+%Y-%m-%d %H:%M UTC')
- Ejecutado por: $(whoami)@$(hostname)
- Modo: **solo lectura** (sin escrituras en el clúster ni en la base de datos)
EOF
}

preflight() {
  h2 "1 · Acceso al clúster"
  if ! command -v kubectl >/dev/null 2>&1; then
    ko "kubectl no está instalado. Abortando."
    exit 1
  fi
  datum=$(kubectl version -o json 2>/dev/null | head -c 4000)
  srv=$(printf '%s' "$datum" | grep -o '"gitVersion": *"[^"]*"' | tail -1 | cut -d'"' -f4)
  ctx=$(kubectl config current-context 2>/dev/null)
  dato "contexto: \`$ctx\`"
  dato "versión del API server: ${srv:-desconocida}"
  if ! kubectl auth can-i list pods >/dev/null 2>&1; then
    ko "sin permisos para listar pods. Abortando."
    exit 1
  fi
  ok "permisos de lectura sobre pods"

  h2 "2 · Nodos"
  printf '\n| Nodo | Rol | Versión | Estado |\n|---|---|---|---|\n'
  kubectl get nodes -o json 2>/dev/null | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(1)
for n in d.get("items",[]):
    rol=",".join(k.split("/")[-1] for k in n["metadata"].get("labels",{}) if k.startswith("node-role.kubernetes.io/"))
    st=[c["type"]+"="+c["status"] for c in n.get("status",{}).get("conditions",[]) if c["type"]=="Ready"]
    print(f"| {n[\"metadata\"][\"name\"]} | {rol or \"worker\"} | {n[\"status\"][\"nodeInfo\"][\"kubeletVersion\"]} | {st[0] if st else \"?\"} |")
' || ko "no se pudo leer la lista de nodos"
}

detectar_ns() {
  [ -n "$NS" ] && return
  NS=$(kubectl get deploy -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null \
        | grep -i 'airflow' | head -1 | cut -d/ -f1)
  [ -z "$NS" ] && NS=$(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null \
        | grep -i 'airflow' | head -1 | cut -d/ -f1)
}

deploy_de() {
  # $1 = patrón (scheduler|webserver|api-server)
  kubectl get deploy -n "$NS" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | grep -iE "$1" | head -1
}

airflow_info() {
  h2 "3 · Airflow"
  detectar_ns
  if [ -z "$NS" ]; then
    ko "no encontré el namespace de Airflow. Repite con AIRFLOW_NS=<namespace>."
    return
  fi
  dato "namespace: \`$NS\`"

  local sched web
  sched=$(deploy_de 'scheduler')
  web=$(deploy_de 'webserver|api-server')
  [ -n "$sched" ] && dato "deployment del scheduler: \`$sched\`" || ko "no encontré el scheduler"
  [ -n "$web" ] && dato "deployment del webserver: \`$web\`"

  if [ -n "$sched" ]; then
    local reps img ver exec
    reps=$(kubectl get deploy -n "$NS" "$sched" -o jsonpath='{.spec.replicas}' 2>/dev/null)
    img=$(kubectl get deploy -n "$NS" "$sched" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)
    dato "réplicas del scheduler: **${reps:-?}** (si son ≥2, hay HA y los gauges por instancia se repiten)"
    dato "imagen: \`${img:-?}\`"
    ver=$(kubectl exec -n "$NS" "deploy/$sched" -- airflow version 2>/dev/null | tail -1)
    dato "**versión real de Airflow**: \`${ver:-no se pudo leer}\`"
    exec_=$(kubectl exec -n "$NS" "deploy/$sched" -- airflow config get-value core executor 2>/dev/null | tail -1)
    dato "executor: \`${exec_:-?}\`"
    local statsd
    statsd=$(kubectl exec -n "$NS" "deploy/$sched" -- airflow config get-value metrics statsd_on 2>/dev/null | tail -1)
    dato "statsd_on: \`${statsd:-?}\`"
  fi
}

db_info() {
  h2 "4 · Metadata DB"
  local sched
  sched=$(deploy_de 'scheduler')
  if [ -z "$sched" ]; then
    ko "sin pod de scheduler: no puedo inspeccionar la base desde dentro."
    return
  fi

  local dsn
  dsn=$(kubectl exec -n "$NS" "deploy/$sched" -- \
        airflow config get-value database sql_alchemy_conn 2>/dev/null | tail -1 | mask)
  dato "cadena de conexión (credenciales enmascaradas): \`${dsn:-?}\`"
  case "$dsn" in
    *postgresql*) dato "motor: **PostgreSQL** (el SQL del dashboard ya está probado en este motor)";;
    *mysql*)      dato "motor: **MySQL** (hay variante del SQL: sin \`FILTER (WHERE …)\` ni índices parciales)";;
    *sqlite*)     ko "motor: SQLite — no apto para producción con Grafana; hay que migrar antes";;
    *)            dato "motor: no reconocido, revisar la cadena";;
  esac

  h2 "5 · Tamaño e índices (consultados desde el propio scheduler)"
  printf '\n| Dato | Valor |\n|---|---|\n'
  kubectl exec -i -n "$NS" "deploy/$sched" -- python3 - <<'PY' 2>/dev/null
import sys
try:
    from airflow.utils.session import create_session
    from sqlalchemy import text
except Exception:
    print("| sesión | error: no pude importar Airflow dentro del pod |"); sys.exit(0)

consultas = [
    ("DAGs activos", "SELECT count(*) FROM dag WHERE is_active AND NOT is_subdag"),
    ("Filas en dag_run", "SELECT count(*) FROM dag_run"),
    ("Filas en task_instance", "SELECT count(*) FROM task_instance"),
    ("Run más antiguo", "SELECT min(execution_date) FROM dag_run"),
    ("Run más reciente", "SELECT max(execution_date) FROM dag_run"),
    ("Versión del motor", "SELECT version()"),
    ("Tamaño de dag_run", "SELECT pg_size_pretty(pg_total_relation_size('dag_run'))"),
    ("Tamaño de task_instance", "SELECT pg_size_pretty(pg_total_relation_size('task_instance'))"),
]
indices = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'dag_run' ORDER BY 1"
por_estado = "SELECT state, count(*) FROM dag_run GROUP BY state ORDER BY 2 DESC"

with create_session() as s:
    for nombre, q in consultas:
        try:
            v = s.execute(text(q)).scalar()
            print(f"| {nombre} | `{v}` |")
        except Exception as e:
            print(f"| {nombre} | no disponible ({str(e).splitlines()[0][:60]}) |")
    print("\n### Índices de dag_run\n")
    try:
        rows = s.execute(text(indices)).fetchall()
        if not rows:
            print("_Sin índices propios (o motor distinto de Postgres)._")
        for name, ddl in rows:
            print(f"- `{name}`")
    except Exception as e:
        print(f"_No disponible: {str(e).splitlines()[0][:80]}_")
    print("\n### dag_run por estado\n")
    try:
        for state, n in s.execute(text(por_estado)).fetchall():
            print(f"- {state}: **{n}**")
    except Exception as e:
        print(f"_No disponible: {str(e).splitlines()[0][:80]}_")
PY
}

entorno_obs() {
  h2 "6 · Observabilidad existente"
  local prom sm
  prom=$(kubectl get svc -A 2>/dev/null | grep -iE 'prometheus|kube-prometheus' | head -3)
  if [ -n "$prom" ]; then
    ok "Prometheus en el clúster:"
    printf '%s\n' "$prom" | sed 's/^/  - /'
  else
    ko "no se detecta Prometheus en el clúster (relevante: si existe, la fase 2 usa Pushgateway)"
  fi
  sm=$(kubectl api-resources --api-group=monitoring.coreos.com 2>/dev/null | grep -c servicemonitors || true)
  dato "CRD de ServiceMonitor: $([ "${sm:-0}" -gt 0 ] && echo 'sí' || echo 'no')"

  local ing
  ing=$(kubectl get ingress -A 2>/dev/null | grep -i airflow | head -3)
  if [ -n "$ing" ]; then
    ok "Airflow publicado por Ingress (sirve para el chequeo externo de salud):"
    printf '%s\n' "$ing" | sed 's/^/  - /'
  else
    ko "Airflow no tiene Ingress: el chequeo externo habrá que hacerlo con port-forward o VPN"
  fi

  local cron
  cron=$(kubectl get cronjob -n "$NS" 2>/dev/null | grep -iE 'clean|purge|prune' | head -3)
  if [ -n "$cron" ]; then
    ok "hay CronJob de limpieza (define la retención real del histórico):"
    printf '%s\n' "$cron" | sed 's/^/  - /'
  else
    dato "sin CronJob de purga visible: el histórico disponible es toda la vida de la metadata DB"
  fi
}

grafana_check() {
  h2 "7 · Ruta desde el clúster hasta Grafana"
  if [ -z "$GRAFANA_URL" ]; then
    dato "no se pasó GRAFANA_URL: este punto queda pendiente (lo comprueba la VM de Grafana)"
    return
  fi
  local sched code
  sched=$(deploy_de 'scheduler')
  [ -z "$sched" ] && { ko "sin scheduler para probar"; return; }
  code=$(kubectl exec -n "$NS" "deploy/$sched" -- \
         curl -s -o /dev/null -m 5 -w '%{http_code}' "${GRAFANA_URL%/}/api/health" 2>/dev/null || echo "000")
  if [ "$code" = "200" ]; then
    ok "el pod del scheduler alcanza Grafana ($GRAFANA_URL → 200)"
  else
    ko "el pod del scheduler NO alcanza Grafana ($GRAFANA_URL → $code)"
  fi
}

cierre() {
  h2 "8 · Lo que este informe permite decidir ya"
  cat <<'EOF'
- [ ] Motor de la metadata DB → define el SQL (Postgres vs MySQL).
- [ ] Filas en `dag_run` → decide si hace falta `CREATE INDEX CONCURRENTLY` o vista materializada.
- [ ] Réplicas del scheduler → si son ≥2, el conteo de DAGs por instancia hay que deduplicar.
- [ ] ¿Hay Prometheus? → decide si la fase 2 va con Pushgateway o con InfluxDB.
- [ ] ¿Hay Ingress? → decide cómo se hace el chequeo externo de salud.
- [ ] ¿Hay CronJob de purga? → define cuánta historia tendrá el tablero.
- [ ] Motor SQLite → hay que migrar antes de cualquier dashboard.

**Falta comprobar desde la VM de Grafana** (este script no puede):
- [ ] `nc -zv <host-db> 5432` — la VM necesita ruta hasta la base.
- [ ] `curl -s localhost:3000/api/health` y versión de Grafana.
- [ ] ¿El datasource de Postgres ya existe? ¿Con qué usuario?
EOF
}

titulo
preflight
airflow_info
db_info
entorno_obs
grafana_check
cierre

hr
printf '_Informe generado en modo solo lectura por `f0-descubrimiento.sh`._\n'
