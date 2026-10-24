#!/usr/bin/env python3
"""Receptor de notificaciones de Grafana para el laboratorio.

Guarda en memoria las últimas alertas recibidas y las muestra en HTML, para
poder demostrar la cadena completa: regla → aviso → destinatario.

  POST /alerta   ← lo llama Grafana (contact point tipo webhook)
  GET  /         ← lista de alertas recibidas, en HTML
  GET  /json     ← lo mismo en JSON

No persiste nada: es un laboratorio. En el cliente esto es Slack, Teams,
PagerDuty o el correo del equipo de guardia.
"""
import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

ALERTAS: list[dict] = []
LOCK = threading.Lock()
MAX = 50

PAGINA = """<!doctype html><meta charset="utf-8">
<title>Alertas recibidas · laboratorio Airflow</title>
<style>
 body{{font-family:system-ui,sans-serif;background:#111821;color:#e6edf3;margin:0;padding:24px}}
 h1{{font-size:18px;font-weight:600;margin:0 0 4px}}
 p.sub{{color:#8b949e;font-size:13px;margin:0 0 20px}}
 table{{border-collapse:collapse;width:100%;font-size:13px}}
 th{{text-align:left;color:#8b949e;font-weight:500;border-bottom:1px solid #30363d;padding:8px 6px}}
 td{{border-bottom:1px solid #21262d;padding:8px 6px;vertical-align:top}}
 .firing{{color:#f85149;font-weight:600}}
 .resolved{{color:#3fb950;font-weight:600}}
 code{{color:#d2a8ff;font-size:12px}}
 .vacío{{color:#8b949e;padding:32px 0;text-align:center}}
</style>
<h1>Alertas recibidas en el laboratorio</h1>
<p class="sub">{n} notificación(es) · esta página es el "destinatario" de la cadena de alertas</p>
{tabla}
"""


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            cuerpo = json.loads(raw.decode() or "{}")
        except Exception:
            cuerpo = {"raw": raw.decode(errors="replace")[:2000]}

        resumen = []
        for a in cuerpo.get("alerts", []) or [cuerpo]:
            etiquetas = a.get("labels", {})
            resumen.append({
                "estado": a.get("status", "?"),
                "nombre": etiquetas.get("alertname", "?"),
                "severidad": etiquetas.get("severity", "-"),
                "resumen": (a.get("annotations", {}) or {}).get("summary", ""),
                "inicio": a.get("startsAt", ""),
            })
        registro = {
            "recibido": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "titulo": cuerpo.get("title", ""),
            "estado": cuerpo.get("status", ""),
            "alertas": resumen,
        }
        with LOCK:
            ALERTAS.insert(0, registro)
            del ALERTAS[MAX:]
        print(f"[{registro['recibido']}] {registro['estado']} · {len(resumen)} alerta(s) · "
              f"{', '.join(r['nombre'] for r in resumen)}", flush=True)

        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/json"):
            cuerpo = json.dumps(ALERTAS, ensure_ascii=False, indent=2).encode()
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(cuerpo)
            return

        with LOCK:
            filas = []
            for r in ALERTAS:
                for a in r["alertas"]:
                    clase = "firing" if a["estado"] == "firing" else "resolved"
                    filas.append(
                        f"<tr><td>{r['recibido']}</td>"
                        f"<td class='{clase}'>{a['estado']}</td>"
                        f"<td>{a['nombre']}</td><td>{a['severidad']}</td>"
                        f"<td>{a['resumen']}</td>"
                        f"<td><code>{a['inicio'][:19]}</code></td></tr>"
                    )
            tabla = (
                "<table><tr><th>Recibido (UTC)</th><th>Estado</th><th>Alerta</th>"
                "<th>Severidad</th><th>Resumen</th><th>Inicio</th></tr>"
                + "".join(filas) + "</table>"
                if filas else
                "<p class='vacío'>Todavía no ha llegado ninguna alerta. "
                "Cuando una regla pase a <b>firing</b>, aparecerá aquí.</p>"
            )
            html = PAGINA.format(n=len(ALERTAS), tabla=tabla)

        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, *args):  # silencio: ya imprimimos lo importante
        return


if __name__ == "__main__":
    print("receptor de alertas escuchando en :8099", flush=True)
    HTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
