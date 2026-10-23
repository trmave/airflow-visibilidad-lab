FROM python:3.12-slim

WORKDIR /app
# Versiones fijas: el colector es la pieza que no debe cambiar sola.
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 requests==2.32.3

COPY collector.py /app/collector.py
CMD ["python", "-u", "/app/collector.py"]
