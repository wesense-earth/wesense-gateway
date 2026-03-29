FROM python:3.11-slim

WORKDIR /app

# Install wesense-ingester-core (sibling directory, same COPY pattern as archiver)
COPY wesense-ingester-core/ /app/wesense-ingester-core/
RUN pip install --no-cache-dir /app/wesense-ingester-core

# Bust cache for application code on every CI build
ARG CACHE_BUST=1

# Install gateway
COPY wesense-gateway/ /app/wesense-gateway/
RUN pip install --no-cache-dir /app/wesense-gateway

EXPOSE 8080

COPY wesense-gateway/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

ENTRYPOINT ["/app/entrypoint.sh"]
