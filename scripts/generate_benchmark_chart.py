#!/usr/bin/env python3
"""Genera un gráfico SVG del benchmark (sin dependencias: SVG a mano).

Datos: docs/14-benchmark.md · mediciones reales con scripts/benchmark.py
Uso:
    python3 scripts/generate_benchmark_chart.py > docs/img/benchmark-refresco.svg
"""
import sys

# milisegundos por refresco completo (todos los paneles una vez)
DATOS = [
    ("Volumen actual\n5.567 filas", [("A · metadata DB", 172, "#2563EB"),
                                     ("B · InfluxDB", 187, "#16A34A"),
                                     ("C · API REST", 1454, "#DC2626")]),
    ("Volumen alto\n203.367 filas", [("A · metadata DB", 648, "#2563EB"),
                                     ("B · InfluxDB", 211, "#16A34A"),
                                     ("C · API REST", 1546, "#DC2626")]),
    ("Alto + índice\n203.367 filas", [("A · metadata DB", 506, "#60A5FA")]),
]

ANCHO, ALTO = 980, 520
MARGEN_IZQ, MARGEN_SUP = 250, 70
ANCHO_BARRA, ALTO_BARRA, SEPARACION = 620, 34, 10
ESCALA = 620 / 1600.0          # 1600 ms = ancho máximo de barra

partes = [
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{ANCHO}" height="{ALTO}" '
    f'viewBox="0 0 {ANCHO} {ALTO}" font-family="system-ui,-apple-system,Segoe UI,sans-serif">',
    f'<rect width="{ANCHO}" height="{ALTO}" fill="#0F172A"/>',
    '<text x="40" y="38" fill="#F8FAFC" font-size="19" font-weight="600">'
    'Latencia de un refresco completo del dashboard (menos es mejor)</text>',
    '<text x="40" y="58" fill="#94A3B8" font-size="12.5">'
    'Medido con scripts/benchmark.py · 4 repeticiones por panel · ventana de 24 h</text>',
]

y = MARGEN_SUP + 18
for titulo, barras in DATOS:
    alto_grupo = len(barras) * (ALTO_BARRA + SEPARACION)
    partes.append(f'<text x="40" y="{y + 14}" fill="#E2E8F0" font-size="13" font-weight="600">'
                  f'{titulo.splitlines()[0]}</text>')
    partes.append(f'<text x="40" y="{y + 30}" fill="#64748B" font-size="11">'
                  f'{titulo.splitlines()[1]}</text>')
    for i, (nombre, valor, color) in enumerate(barras):
        by = y + i * (ALTO_BARRA + SEPARACION)
        ancho = max(3, valor * ESCALA)
        partes.append(f'<rect x="{MARGEN_IZQ}" y="{by}" width="{ancho}" height="{ALTO_BARRA}" '
                      f'rx="3" fill="{color}"/>')
        etiqueta_color = "#0F172A" if ancho > 140 else "#F8FAFC"
        tx = MARGEN_IZQ + 10 if ancho > 140 else MARGEN_IZQ + ancho + 8
        partes.append(f'<text x="{tx}" y="{by + 22}" fill="{etiqueta_color}" font-size="12.5" '
                      f'font-weight="600">{valor} ms</text>')
        partes.append(f'<text x="{MARGEN_IZQ - 14}" y="{by + 22}" fill="#94A3B8" font-size="12" '
                      f'text-anchor="end">{nombre}</text>')
    y += alto_grupo + 34

partes.append('<text x="40" y="500" fill="#64748B" font-size="11.5">'
              'Vía A = SQL a la metadata DB · Vía B = InfluxDB · Vía C = API REST de Airflow '
              '(plugin Infinity)</text>')
partes.append("</svg>")
sys.stdout.write("\n".join(partes) + "\n")
