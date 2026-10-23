# 06 · Lecciones: los errores reales de este montaje

Todo lo de aquí pasó en este nodo, en este laboratorio. Se documenta porque son los mismos
tropiezos que aparecerán en el entorno del cliente, y cada uno costó tiempo descubrirlo.

## 1 · Los paneles decían "No data" y **no llegaba ni una query** a Postgres

El síntoma más confuso del montaje. `curl` contra `/api/ds/query` devolvía datos correctos, el
"Save & test" del datasource daba OK, y sin embargo los 12 paneles mostraban *No data* con un
triángulo de error y **cero SQL registrado en el log de Postgres**.

Cómo se encontró (el método, que sirve para cualquier panel vacío):

```bash
# 1. ¿el backend responde? (si responde, el problema es el front-end)
curl -s -X POST http://127.0.0.1:3050/api/ds/query -d '{"queries":[{...}]}'

# 2. ¿la query llega a la base? (si NO llega, el navegador la abortó antes)
docker exec airflow-lab-pg psql -U airflow -c "ALTER SYSTEM SET log_statement='all'" -c "SELECT pg_reload_conf()"

# 3. capturar el error del navegador que usa el renderer
#    (LOG_LEVEL=debug en grafana-image-renderer → Runtime.consoleAPICalled)
```

El log del navegador headless dijo, literalmente:

> `Error: You do not currently have a default database configured for this data source. Postgres requires a default database with which to connect.`

**Causa:** Grafana ≥ 12 (plugin `grafana-postgresql-datasource`) exige la base por defecto
dentro de `jsonData.database`. Tener solo el campo `database` de nivel superior funciona para el
backend (`/api/ds/query`, health check) y **no** para el front-end.

**Solución** en el provisioning:

```yaml
datasources:
  - name: Airflow metadata
    uid: airflow-pg
    type: postgres
    database: airflow        # ← el backend lo usa
    jsonData:
      database: airflow      # ← el front-end exige ESTE
```

Lección: cuando un panel está vacío pero el datasource "funciona", el error casi nunca está en
el SQL sino en la resolución del datasource por el front-end.

## 2 · Provisioning "permission denied" y Grafana sin datasource

Grafana corre como uid `472`. Las carpetas de provisioning las había creado con `umask 027`
(750) y dueño `hermes`. Resultado:

```text
level=error msg="can't read datasource provisioning files from directory"
  path=/etc/grafana/provisioning/datasources error="open …: permission denied"
```

Grafana arranca igual, sano, y sin nada. Hay que mirar los logs, no el contenedor.

**Solución:** `chmod -R a+rX grafana/` (lectura para todos, ejecución solo en directorios).

## 3 · Renombrado del plugin de Postgres

En Grafana ≥ 11 el id del datasource dejó de ser `postgres`. `GET /api/datasources` devuelve
`"type":"grafana-postgresql-datasource"`. Un dashboard con `"type":"postgres"` puede seguir
funcionando (Grafana resuelve por `uid`), pero es una fuente de confusión al revisar el JSON:

```bash
# la fuente de verdad, siempre
curl -s -u admin:*** http://localhost:3000/api/datasources | jq -r '.[] | .type'
```

## 4 · El renderer de imágenes no arranca con el token por defecto

```text
Error: ✗ failed to start rendering service: Using the default
[rendering]renderer_token is not allowed for production settings
```

Hay que poner el mismo token en los dos lados:

- Grafana: `GF_RENDERING_RENDERER_TOKEN`
- `grafana-image-renderer`: `AUTH_TOKEN` (cabecera `X-Auth-Token`)

Y `GF_INSTALL_PLUGINS` está deprecado en Grafana 13 (avisa y sigue funcionando); el sustituto es
`GF_PLUGINS_PREINSTALL`.

## 5 · Airflow 2.8 crea los DAGs **pausados**

Con `[core] dags_are_paused_at_creation = True` (por defecto), el scheduler parsea los 46 DAGs,
los cuenta en `dag` y **no genera ni una ejecución**. El dashboard queda vacío y parece roto.

```bash
airflow dags unpause demo_01_pedidos_ok      # o: UPDATE dag SET is_paused=false WHERE …
```

En el cliente esto es relevante: un DAG pausado no genera runs, así que no aparece en "fallidos"
ni en "corriendo". Por eso el panel 1 muestra **activos + pausados** por separado: si hay 40
pausados, los otros paneles no están mintiendo, están contando otra cosa.

## 6 · Insertar `dag_run` por SQL directo: las columnas NOT NULL

Para sembrar histórico no basta con `dag_id`, `state` y `execution_date`. En Airflow 2.8.1:

```text
dag_id        NOT NULL
run_id        NOT NULL
run_type      NOT NULL      ← 'scheduled' | 'manual' | 'backfill' | 'dataset_triggered'
execution_date NOT NULL
clear_number  NOT NULL      ← default 0, pero del lado de Python: el INSERT crudo debe ponerlo
```

`external_trigger` y `clear_number` tienen `default=` en el modelo (cliente), no `server_default`
en la base: si se omiten en un `INSERT` SQL, el motor los rechaza.

## 7 · Verificar paneles contra la API exige resolver las macros

`/api/ds/query` no tiene contexto de dashboard: `$__timeFilter`, `$__timeGroupAlias`,
`$__interval` y las variables (`$ventana`) **no** se resuelven solas. El script
`verify_dashboard.py` las sustituye antes de enviar. Si un panel "falla" solo en la verificación,
casi siempre es esto y no el panel.

## 8 · Visualizaciones: el tipo importa

El ranking de fallos empezó como `bargauge` y se veía como un único número gigante. Con datos en
formato tabla, el tipo correcto es `barchart` con `xField`/`yField`:

```json
{"type": "barchart", "options": {"xField": "dag_id", "yField": "fallos", "orientation": "horizontal"}}
```

## 9 · El navegador del host no sirve para capturas

`chrome-linux64/chrome: error while loading shared libraries: libatk-1.0.so.0`. El Chromium que
instala Hermes no arranca en este nodo (faltan librerías del sistema). Alternativa que sí
funciona y quedó montada: **`grafana-image-renderer`** en su propio contenedor, con Chromium
incluido, y el render por HTTP (`/render/d/<uid>`). Cero dependencias del host.

## 10 · Publicar puertos en Docker no respeta `ufw`

`ufw` de este nodo solo permite 22 y 9090, pero los contenedores publican `3050` y `8085`
directamente por la cadena `DOCKER-USER` de `iptables`. Es decir: **el dashboard está accesible
desde la LAN aunque `ufw` diga lo contrario**, que es exactamente lo que se buscaba para la demo
y exactamente lo que no debe pasar en producción. En el cliente, la regla es al revés: publicar
en `127.0.0.1` y entrar por túnel, o poner la NetworkPolicy/regla de firewall delante.

## Resumen para el cliente

| # | Tropiezo | Cómo se evita |
|---|---|---|
| 1 | Paneles vacíos sin error visible | `jsonData.database` en el datasource (Grafana ≥ 12) |
| 2 | Provisioning ilegible | permisos `a+rX` en las carpetas de provisioning |
| 3 | Tipo de datasource renombrado | leer el `type` real de `/api/datasources` |
| 5 | DAGs pausados = tablero vacío | despausar o mostrar el conteo de pausados |
| 6 | `INSERT` en `dag_run` rechazado | `run_type`, `clear_number` y `run_id` explícitos |
| 10 | Dashboard expuesto a la LAN | publicar en localhost + túnel/NetworkPolicy |
