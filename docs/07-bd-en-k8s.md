# 07 · La metadata DB está dentro del clúster: qué se puede hacer

> Escenario del cliente: Airflow **y su metadata DB** en el mismo clúster de Kubernetes.
> Grafana en una VM, fuera. El dashboard lee la DB: si el clúster cae, el tablero se queda
> ciego (Grafana sigue vivo, pero no tiene a quién preguntar).

## Qué cambia (y qué no)

**Cambia:** el tablero comparte destino con el clúster. Una caída total, un `kubectl drain` de
mantenimiento o un nodo único que se reinicia dejan a Grafana sin datos.

**No cambia:** el MVP sigue siendo válido. No hay que tocar el Airflow, el rol de solo lectura
funciona igual, y el 95 % de las caídas reales no son "el clúster entero", son un DAG colgado o
un pod Pending — casos que el dashboard detecta perfectamente.

Lo que sí hay que añadir es **una forma de distinguir dos situaciones que se ven iguales**:

- "Airflow está roto" → el tablero lo cuenta con datos.
- "No puedo ver Airflow" → el tablero solo puede decir que hay un error.

## Escalones, del más barato al más caro

### Escalón 0 · Aceptar el riesgo y detectar la ceguera (coste ≈ 0)

Dos piezas, ninguna infraestructura nueva:

1. **Alerta de error de consulta.** Las reglas ya llevan `execErrState: Error`: si la DB no
   responde, la regla pasa a **Error** y notifica. No se queda muda en "sin datos".
2. **Chequeo externo del webserver.** Un `curl` a `/health` de Airflow desde la VM (o desde
   Uptime Kuma, Grafana Synthetic Monitoring, un `blackbox_exporter`), cada minuto. Es la única
   señal que sigue funcionando cuando todo lo de dentro está caído.

```bash
# desde la VM de Grafana, vía Ingress/LB del Airflow
curl -s -o /dev/null -w '%{http_code}\n' http://airflow.<cliente>/health
# 200 = vivo · 5xx/000 = caído (y entonces el tablero también estará en error)
```

Con esto el equipo sabe **que** no ve, en lugar de creer que todo está bien porque el dashboard
está en blanco. Es lo mínimo honesto y cuesta una tarde.

### Escalón 1 · Réplica de lectura de la DB (coste bajo-medio)

Si la DB la gestiona un operador (CloudNativePG, Patroni, Zalando, Bitnami con réplicas), suele
haber ya una réplica. Grafana apunta a la **réplica**, no al primario:

- el dashboard deja de cargar la DB que el scheduler necesita para avanzar (beneficio inmediato
  incluso sin caídas);
- si el primario cae, la réplica sobrevive el tiempo suficiente para seguir viendo el histórico;
- si la réplica se promociona o se rehace, el dashboard se recupera solo al reconectar.

Lo que hay que comprobar antes de prometerlo:

```bash
kubectl get cluster -A                      # CNPG
kubectl get pods -n <ns> -l cnpg.io/instanceRole=replica
kubectl get svc -n <ns>                     # ¿hay un Service -ro / -replica?
```

Sin réplica, montarla **sí** es un cambio de arquitectura de la base de datos: no entra en este
proyecto sin hablarlo con quien la administra.

### Escalón 2 · Exportar los agregados fuera del clúster (coste medio, recomendado si piden historia)

> Diseño completo, modelo de métricas, esqueleto del colector y criterios de aceptación:
> [`08-fase2-externalizacion.md`](08-fase2-externalizacion.md). **Decidido: no entra en el MVP.**

La idea: no mover la DB, mover **el resultado de las 5 preguntas**. Un `CronJob` dentro del
clúster (que sí alcanza la DB) calcula los KPIs cada minuto y los empuja a la VM.

```text
clúster                                    VM
┌──────────────────────────┐              ┌──────────────────────────┐
│ CronJob (cada 1 min)     │  push HTTP   │ Prometheus + Pushgateway │
│   SELECT … 5 KPIs        │ ───────────► │   (o InfluxDB)           │
│   (o la query completa)  │              │        ▲                 │
└──────────────────────────┘              │        │                 │
                                          │    Grafana               │
                                          └──────────────────────────┘
```

- **Gana:** el dashboard sobrevive a la caída del clúster y además empieza a tener historia
  larga, independiente de la purga de la metadata DB.
- **Cuesta:** un colector en la VM (Prometheus o InfluxDB), un `CronJob` de ~40 líneas y definir
  retención.
- **Ojo:** es la puerta de entrada a la fase F4 (Prometheus). Si el cliente ya tiene Prometheus en
  la VM, este escalón sale casi gratis.

### Escalón 3 · TSDB completo con métricas propias de Airflow (coste alto)

`airflow-exporter` o StatsD → Prometheus/InfluxDB. Da además `scheduler_heartbeat`,
`executor.open_slots`, `pool.running_slots`, `dagbag_size`. Requiere instalar un plugin **dentro**
del scheduler y reiniciarlo: ya es un cambio al Airflow, con su ventana y su riesgo.

### Escalón 4 · Sacar la DB del clúster (decisión de arquitectura del cliente)

RDS/Cloud SQL o una VM dedicada con Patroni. Es lo que de verdad desacopla el destino del
dashboard del destino de Airflow. Normalmente lo decide el equipo de plataforma, no este proyecto.

## Matriz de decisión

| Escenario del cliente | Qué recomiendo |
|---|---|
| Clúster de 1 nodo, sin réplicas, sin Prometheus | Escalón 0. Y decirlo claro: el tablero no sobrevive a una caída total |
| Clúster con DB en operador y réplica disponible | Escalón 0 + 1 |
| Piden historia de meses/años o "ver algo aunque el clúster esté caído" | Escalón 0 + 2 |
| Ya tienen Prometheus en la VM y quieren todo en un sitio | Escalón 2/3 |
| La DB es un punto crítico para el negocio | Escalón 4, decisión del cliente |

## Qué preguntar en F0 (esto decide todo lo anterior)

1. ¿Qué gestiona la DB: operador (cuál), StatefulSet a mano, o servicio externo?
2. ¿Cuántos nodos tiene el clúster y cuántos workers de Airflow?
3. ¿Hay réplicas? ¿Hay backups y se han restaurado alguna vez?
4. ¿El clúster se drena para mantenimiento? ¿Con qué frecuencia?
5. ¿El webserver de Airflow está expuesto por Ingress/LB (para el chequeo externo)?
6. ¿Hay Prometheus/Grafana ya en la VM, y con qué retención?
7. ¿Cuántas filas tiene `dag_run` y cada cuánto purgan?

## Cómo decirlo en la reunión

> "Con la base dentro del clúster, el tablero y el Airflow comparten destino: si se cae todo el
> clúster, el tablero queda ciego. Lo primero que hacemos es que ese silencio **avise** en vez de
> pasar desapercibido. Si quieren que el tablero siga mostrando historia cuando el clúster esté
> caído, hay un paso intermedio barato: un proceso dentro del clúster que cada minuto saque los
> números a su VM. Eso ya es otra fase y tiene su propio alcance."

Lo que **no** hay que prometer: que un dashboard que lee de la DB sobreviva a la caída de esa DB.
Ningún panel puede mostrar datos que no existen; lo único honesto es avisar de que no los tiene.
