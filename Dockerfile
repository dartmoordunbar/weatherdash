FROM python:3.12-slim

LABEL org.opencontainers.image.title="WeatherDash" \
      org.opencontainers.image.description="Live dashboard and recorder for a Davis WeatherLink Live" \
      org.opencontainers.image.source="https://github.com/dartmoordunbar/weatherdash"

ENV PYTHONUNBUFFERED=1 \
    DB_PATH=/data/weather.db \
    PORT=8080

RUN useradd --system --uid 1000 --home /app weatherdash \
 && mkdir -p /data && chown weatherdash /data

WORKDIR /app
COPY server.py ./
COPY static ./static

USER weatherdash
VOLUME ["/data"]
EXPOSE 8080/tcp 22222/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8080\")}/api/now', timeout=4)"

CMD ["python", "server.py"]
