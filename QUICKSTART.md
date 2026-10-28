# Empezar en 3 comandos

Esto levanta un **Airflow 2.8.1 completo** (con 46 DAGs de ejemplo y datos históricos) más
**Grafana con tres tableros** ya provisionados, para ver cuántos DAGs hay, cuántos acaban bien,
cuántos fallan y cuáles llevan más de una hora corriendo.

## Requisitos

- Docker **24+** con el plugin `docker compose` (Docker Desktop en Windows/macOS también vale).
- **4 GB de RAM** libres y **10 GB de disco** (las imágenes de Airflow pesan ~2,5 GB).
- Puertos libres: **8085** (Airflow), **3050** (Grafana), **8099** (receptor de alertas).

```bash
docker --version && docker compose version   # comprobar los dos
```

## Los 3 comandos

```bash
git clone https://github.com/trmave/airflow-visibilidad-lab.git
cd airflow-visibilidad-lab
bash bootstrap.sh
```

La primera vez tarda **4-6 minutos** (descarga de imágenes). Al final verás:

```text
Todo listo. 0 fallos.

  Vía A · en vivo    http://<tu-ip>:3050/d/airflow-visibilidad
  Vía B · histórico  http://<tu-ip>:3050/d/airflow-historico
  Vía C · API REST   http://<tu-ip>:3050/d/airflow-api
  Airflow            http://<tu-ip>:8085
```

Si en algún momento quieres borrar todo y empezar limpio:

```bash
bash bootstrap.sh --limpio
```

## Entrar

| Qué | URL | Usuario | Contraseña |
|---|---|---|---|
| Grafana | `http://localhost:3050` | `admin` | en `.env` → `GRAFANA_ADMIN_PASSWORD` |
| Airflow | `http://localhost:8085` | `admin` | en `.env` → `AIRFLOW_ADMIN_PASSWORD` |

```bash
# la contraseña de Grafana es la única que necesitas para mirar los tableros
grep '^GRAFANA_ADMIN_PASSWORD=' .env
```

Las claves las genera el propio `bootstrap.sh` la primera vez: **no vienen en el repositorio**.

## Qué deberías ver

- **46 DAGs activos**, ~1.500 ejecuciones en las últimas 24 h, unos 300 fallos (a propósito: hay DAGs
  de demo que fallan siempre).
- **Tres tableros**, los tres con los mismos cinco números (46 · OK · fallidos · corriendo · lentos):

| Tablero | Cómo obtiene el dato | Cuándo usarlo |
|---|---|---|
| `/d/airflow-visibilidad` | Lee la base de datos de Airflow | El día a día: es el más rápido y exacto |
| `/d/airflow-historico` | Lee InfluxDB, que se rellena cada 30 s | Si se cae el clúster y quieres seguir viendo historia |
| `/d/airflow-api` | Consulta la API REST de Airflow | Si no te dan credenciales de base de datos |

## ¿Quedó bien? Comprobación en 10 segundos

```bash
bash check.sh
```

Verifica contenedores, salud, los tres tableros y los cinco números. Si algo falla lo dice con el
motivo.

## Si algo sale mal

| Síntoma | Causa y solución |
|---|---|
| `puerto 3050 en uso` | Otro servicio lo ocupa. Levántalo en otros puertos: `LAB_GRAFANA_PORT=4050 bash bootstrap.sh` |
| Grafana abre pero los paneles dicen *No data* | El histórico aún no se sembró: espera 1 minuto y pulsa *Refresh*. Si sigue, `bash bootstrap.sh` (es idempotente y repara). |
| `airflow-lab-* no aparece` | `docker compose ps` para ver qué murió; `docker compose logs <servicio>` |
| El tablero de la API da error 400 | Falta el usuario de la API: `bash bootstrap.sh` lo vuelve a crear y verifica |
| Todo lento y el disco al 100% | Airflow necesita su tiempo la primera vez; mira `docker stats` |

## Parar, arrancar, borrar

```bash
docker compose stop      # parar sin perder datos
docker compose start     # arrancar de nuevo
bash bootstrap.sh --limpio   # borrar TODO (contenedores + volúmenes) y recrear
```

## Cambiar puertos (si están ocupados)

```bash
LAB_WEB_PORT=9085 LAB_GRAFANA_PORT=3050 LAB_WEBHOOK_PORT=9099 bash bootstrap.sh
```

## Por dónde seguir

- `docs/11-rehacer-desde-cero.md` — la reconstrucción explicada paso a paso.
- `docs/12-propuesta.md` — la comparativa de las tres vías para presentar a un cliente.
- `docs/14-benchmark.md` — cuánto tarda y cuánta CPU cuesta cada vía, con método.
- `docs/02-kpis-y-sql.md` — las consultas SQL, para copiar en el entorno real.
- `docs/15-entrega-a-un-tercero.md` — qué hay en el repositorio y qué no, si te lo pasan a ti.
