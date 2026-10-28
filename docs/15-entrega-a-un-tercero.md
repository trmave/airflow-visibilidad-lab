# Entrega a un tercero

Guía para quien recibe este repositorio y tiene que levantarlo sin ayuda. Si eres esa persona:
empieza por **`QUICKSTART.md`** (3 comandos) y vuelve aquí solo si algo no cuadra.

## Qué te están dando

Un laboratorio **reproducible** que demuestra cómo dar visibilidad a un Airflow 2.8.1 sin tocar
Airflow: un dashboard de Grafana con cinco indicadores (DAGs totales, ejecuciones correctas,
fallidas, corriendo y cuántas llevan más de una hora), implementado **de tres formas distintas**
para poder compararlas.

Corre entero en Docker sobre una sola máquina. **No necesita** un clúster de Kubernetes, ni Azure,
ni acceso a ningún cliente.

## Qué NO incluye (a propósito)

- **Credenciales**: no hay `.env` en el repositorio. `bootstrap.sh` genera claves aleatorias la
  primera vez. Si alguien te pasa un `.env`, no lo commitees.
- **Datos reales**: los 46 DAGs son de demostración y el histórico es sintético (fechas y estados
  generados con SQL). Nada viene de una empresa.
- **El Airflow del cliente**: este repo replica el patrón, no se conecta a nada externo.

## Cómo te lo pueden pasar

**Opción 1 — repositorio Git (recomendado)**

```bash
git clone https://github.com/trmave/airflow-visibilidad-lab.git
cd airflow-visibilidad-lab
bash bootstrap.sh
```

**Opción 2 — archivo comprimido** (si te lo dan en un `.tgz` o un `git bundle`)

```bash
tar xzf airflow-visibilidad-lab.tgz && cd airflow-visibilidad-lab
bash bootstrap.sh

# o, con un bundle:
git clone airflow-visibilidad.bundle airflow-visibilidad-lab && cd airflow-visibilidad-lab
bash bootstrap.sh
```

## Los primeros diez minutos

1. `bash bootstrap.sh` (4-6 min la primera vez, descarga imágenes).
2. Abre `http://localhost:3050` con el usuario `admin` y la contraseña de `.env`.
3. Abre los tres tableros: `/d/airflow-visibilidad`, `/d/airflow-historico`, `/d/airflow-api`.
   Los tres deben mostrar **los mismos cinco números**: 46 · ~1.2k · ~305 · 1 · 0/1.
4. Abre Airflow en `http://localhost:8085` y mira los 46 DAGs.
5. `bash check.sh` para confirmar que todo está sano.

## Cómo está montado (para orientarte rápido)

```text
compose.yaml            8 servicios: Airflow (webserver+scheduler+init), Postgres,
                        Grafana, InfluxDB, colector, renderer de imágenes, webhook
dags/                   46 DAGs en Python (6 con historia + 40 de volumen)
sql/                    rol de solo lectura + semillas del histórico
grafana/                datasources, los 3 tableros y las 4 reglas de alerta
collector/              Airflow -> InfluxDB cada 30 s (la "vía B")
webhook/                receptor de alertas, para probar la cadena completa
scripts/                generadores, verificadores y benchmark
cliente/                script de descubrimiento (solo lectura) para un entorno real
docs/                   15 documentos: arquitectura, SQL, propuesta, benchmark, reconstrucción
bootstrap.sh            levanta y verifica todo, en un comando
check.sh                comprobación rápida de salud
QUICKSTART.md           los 3 comandos
```

## Si los puertos están ocupados

```bash
LAB_WEB_PORT=9085 LAB_GRAFANA_PORT=4050 LAB_WEBHOOK_PORT=9099 bash bootstrap.sh
```

Y si quieres dos copias a la vez en la misma máquina:

```bash
LAB_PROJECT=lab2 LAB_WEB_PORT=9085 LAB_GRAFANA_PORT=4050 LAB_WEBHOOK_PORT=9099 bash bootstrap.sh
```

`LAB_PROJECT` separa los volúmenes (cada copia tiene su propia base). Ojo: los nombres de contenedor
son fijos, así que **dos copias no pueden correr a la vez**; para eso hay que parar la primera.

## Qué hay dentro de cada tablero

- **A · `airflow-visibilidad`** — lee la metadata DB de Airflow. Es la vía buena: rápida y exacta.
- **B · `airflow-historico`** — lee InfluxDB, que rellena un colector cada 30 s. Sobrevive a la caída
  del clúster y conserva historia propia. Va 30-60 s por detrás.
- **C · `airflow-api`** — consulta la API REST de Airflow. Solo si no hay credenciales de base.

Cada tablero tiene arriba una nota que lo explica, y enlaces a los otros dos.

## Para llevarlo a un cliente real

1. `cliente/f0-descubrimiento.sh` — informe de descubrimiento, **solo lectura**, para ejecutar en el
   entorno del cliente (desde el pod del scheduler si la base es privada).
2. `docs/02-kpis-y-sql.md` — las consultas, para copiar tal cual.
3. `docs/07-bd-en-k8s.md` — particularidades cuando la base vive en Kubernetes.
4. `docs/12-propuesta.md` y `docs/14-benchmark.md` — la comparativa de vías con números medidos.
5. **`docs/13-opcion-api-rest.md`** — la opción sin credenciales de base.

Diferencias con el entorno real: aquí todo está en Docker en una máquina; en un cliente típico
Airflow vive en Kubernetes (o AKS con endpoint privado), la base puede estar fuera del clúster y
Grafana en otro sitio. Para ese caso, `docs/12-propuesta.md` describe las tres formas de conectar
(leer la base, empujar métricas o consultar la API).

## Reset y limpieza

```bash
bash bootstrap.sh --limpio    # borra contenedores y volúmenes, recrea todo y verifica
docker compose down -v        # lo mismo sin verificar
```

Los volúmenes son del proyecto: no tocan nada más de tu máquina.

## Checklist de entrega (para quien lo envía)

- [ ] `git status` limpio y último commit subido.
- [ ] `.env` **no** está en el repo (`git ls-files | grep -c '^\.env$'` → 0).
- [ ] `bash bootstrap.sh --limpio` termina con **0 fallos** en una máquina limpia.
- [ ] `bash check.sh` en verde.
- [ ] Los tres tableros cargan con datos (captura de cada uno).
- [ ] Aviso al receptor: la primera ejecución tarda ~5 min y descarga ~2,5 GB.
