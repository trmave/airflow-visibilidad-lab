# 11 · Rehacer el laboratorio desde cero

Esta es la prueba de que el repositorio basta: **un host limpio + este repo = el laboratorio
completo**, sin pasos que estén solo en la cabeza de nadie. Se probó borrando todo
(`docker compose down -v`) y volviendo a levantarlo con `bootstrap.sh`.

## Requisitos

| | Mínimo | Probado en |
|---|---|---|
| Sistema | Linux con Docker ≥ 24 | Ubuntu, Docker 29.9.0 + Compose v5.6.0 |
| RAM | 3 GB libres (pico ~1,7 GB) | 15 GiB, 9,5 GiB disponibles |
| Disco | 6 GB (imágenes ~5 GB, datos < 1 GB) | 796 GB libres |
| CPU | 2 vCPU | 8 |
| Puertos libres | 3050, 8085, 8099 | los mismos |
| Red | salida a Docker Hub (bajar 4 imágenes) | — |

## Instalación en un comando

```bash
git clone https://github.com/trmave/airflow-visibilidad-lab.git airflow-lab
cd airflow-lab
bash bootstrap.sh
```

Si el host no tiene el repo (o no hay red hacia GitHub), copiar el directorio y ejecutar igual:
el bootstrap no usa git.

## Qué hace `bootstrap.sh`, paso a paso

| Paso | Acción | Verificación que imprime |
|---|---|---|
| 1 | Comprueba docker, compose y que los 3 puertos estén libres | `✓ docker 29.9.0`, `✓ puerto 3050 libre` |
| 2 | Genera `.env` con **8 claves aleatorias** (no existía antes) | `✓ .env creado con 8 claves aleatorias (640)` |
| 3 | Construye la imagen del colector y levanta los 8 servicios | `✓ docker compose up -d` |
| 4 | Espera a Postgres y a que `airflow db migrate` termine | `✓ airflow db migrate + usuario admin: OK` |
| 5 | Crea el rol **de solo lectura** `grafana_ro` | `✓ rol grafana_ro con SELECT sobre 6 tablas` |
| 6 | Espera al parseo de los 46 DAGs y los despausa | `✓ 46 DAGs en la metadata DB` |
| 7 | Siembra el histórico sintético (930 + 3.360 filas) | `✓ histórico de los 40 DAGs de volumen` |
| 8 | Verifica **los dos dashboards** contra Grafana | `✓ vía A · 12 paneles OK` / `✓ vía B · 9 paneles OK` |

Es **idempotente**: si ya está levantado, no borra nada y vuelve a verificar. Para empezar de cero
de verdad hay que borrar los volúmenes:

```bash
docker compose down -v      # borra pgdata, grafanadata e influxdata
bash bootstrap.sh
```

## Salida real de una reconstrucción desde cero

Ejecutado en el laboratorio el 2026-10-24, tras `docker compose down -v` (0 contenedores, 0
volúmenes) y sin tocar ninguna configuración a mano:

```text
▸ 1/8 Requisitos
  ✓ docker 29.9.0
  ✓ compose 5.6.0
  ✓ puerto 8085 libre
  ✓ puerto 3050 libre
  ✓ puerto 8099 libre

▸ 2/8 Secretos (.env)
  ✓ .env ya existe (no se toca: regenerarlo desincroniza la base)

▸ 3/8 Levantando servicios
  ✓ imagen del colector construida
  ✓ docker compose up -d

▸ 4/8 Esperando la metadata DB y la migración de Airflow
  ✓ Postgres sana (10s)
  ✓ airflow db migrate + usuario admin: OK

▸ 5/8 Rol grafana_ro (solo lectura)
  ✓ rol grafana_ro con SELECT sobre 6 tablas

▸ 6/8 Esperando a que el scheduler parsee los DAGs
  ✓ 46 DAGs en la metadata DB
  ✓ DAGs despausados (sin esto no hay ejecuciones)

▸ 7/8 Sembrando histórico
  ✓ histórico de los 6 DAGs base
  ✓ histórico de los 40 DAGs de volumen
  · dag_run: 4300 filas

▸ 8/8 Verificando contra Grafana
  ✓ vía A · 12 paneles OK (metadata DB)
  ✓ vía B · 9 paneles OK (InfluxDB)

──────────────────────────────────────────────────────────
  Vía A · en vivo    http://192.168.3.39:3050/d/airflow-visibilidad
  Vía B · histórico  http://192.168.3.39:3050/d/airflow-historico
  Airflow            http://192.168.3.39:8085
  Alertas recibidas  http://192.168.3.39:8099
  Credenciales       .env (640) — usuario admin de Grafana y de Airflow
──────────────────────────────────────────────────────────
Todo listo. 0 fallos.
```

## Mapa de ficheros: qué tocar para cambiar qué

| Quiero… | Fichero |
|---|---|
| Cambiar puertos | `compose.yaml` (`3050`, `8085`, `8099`) y `bootstrap.sh` (`PUERTO_*`) |
| Cambiar el umbral de "lento" | `scripts/generate_dashboard.py` (SQL `interval '1 hour'`) + la regla en `grafana/provisioning/alerting/airflow.yaml` → regenerar el JSON |
| Añadir/quitar paneles | `scripts/generate_dashboard.py` (vía A) o `scripts/generate_dashboard_historico.py` (vía B) |
| Cambiar la ventana por defecto | `scripts/generate_dashboard.py` (`time`, `templating`) |
| Cambiar retención del histórico | `compose.yaml` → `DOCKER_INFLUXDB_INIT_RETENTION` |
| Cambiar el destinatario de alertas | `grafana/provisioning/alerting/contact-points.yaml` + `policies.yaml` |
| Añadir DAGs de demo | `scripts/generate_demo_dags.py` (`--cantidad N`) → regenerar y `chmod 644` |
| Cambiar el rol de solo lectura | `sql/10_rol_grafana_ro.sql` |

Después de tocar un generador:

```bash
python3 scripts/generate_dashboard.py > grafana/dashboards/airflow-visibilidad.json
# Grafana recarga los dashboards provisionados en ≤ 30 s (o: docker compose restart grafana)
```

## Verificación manual (si quieres comprobarlo tú, sin el bootstrap)

```bash
# 1. los servicios están sanos
docker compose ps

# 2. Airflow responde y la base está sana
curl -s http://127.0.0.1:8085/health

# 3. los 12 paneles de la vía A devuelven datos
GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
  python3 scripts/verify_dashboard.py --ventana "24 hours"

# 4. los paneles de la vía B devuelven datos (espera ~1 min tras arrancar)
GRAFANA_PASS=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2) \
  python3 scripts/verify_historico.py
```

Los dos scripts salen con código **0** si todo está bien y distinto de 0 si algo falla: se pueden
meter en un cron o en un pipeline.

## Accesos

| Qué | URL |
|---|---|
| Vía A · dashboard en vivo | `http://<IP>:3050/d/airflow-visibilidad` |
| Vía B · dashboard histórico | `http://<IP>:3050/d/airflow-historico` |
| Airflow UI | `http://<IP>:8085` (usuario `admin`) |
| Alertas recibidas | `http://<IP>:8099` |
| Grafana admin | `http://<IP>:3050` (usuario `admin`) |

Las contraseñas están en `.env` (permisos 640). **No** están en el repositorio.

## Cómo se retira

```bash
docker compose down -v            # contenedores + volúmenes (borra datos)
docker image rm airflow-lab-collector   # la imagen propia, si quieres
sudo rm -rf /ruta/airflow-lab     # el directorio
```

## Si algo falla

| Síntoma | Dónde mirar |
|---|---|
| Provisioning "permission denied" en Grafana | `docs/06` §2 (`chmod -R a+rX grafana`) |
| Paneles en "No data" sin error visible | `docs/06` §1 (`jsonData.database`) |
| Vía B vacía los primeros segundos | es normal: el colector escribe cada 30 s |
| `airflow-init` sale con código ≠ 0 | `.env` regenerado con la DB ya inicializada → `down -v` y volver |
| Todo verde menos la vía B | `docker logs airflow-lab-collector` |

Más detalle operativo: [`03-operacion.md`](03-operacion.md) y los 10 errores reales en
[`06-lecciones.md`](06-lecciones.md).

## Checklist de "está bien montado"

- [ ] `docker compose ps` → 8 servicios, todos `Up` (y `healthy` donde aplique)
- [ ] `/health` de Airflow → `metadatabase: healthy` y `scheduler: healthy`
- [ ] `verify_dashboard.py` → 12 paneles, 0 errores
- [ ] `verify_historico.py` → paneles de la vía B, 0 problemas
- [ ] 4 reglas de alerta con `health=ok`
- [ ] `curl http://127.0.0.1:8099/` responde 200 (destinatario listo)
- [ ] `dag_run` tiene miles de filas (no está vacío)
- [ ] el rol `grafana_ro` **no** puede escribir (probarlo: `UPDATE` debe fallar)
