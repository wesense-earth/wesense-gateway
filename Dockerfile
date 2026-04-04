FROM python:3.11-slim

WORKDIR /app

# Ensure latest pip for security fixes and dependency resolution
RUN pip install --no-cache-dir --upgrade pip

# Bust cache when ingester-core or app code changes
ARG CACHE_BUST=1

# Install wesense-ingester-core (sibling directory, same COPY pattern as archiver)
COPY wesense-ingester-core/ /app/wesense-ingester-core/
RUN pip install --no-cache-dir /app/wesense-ingester-core

# Install storage broker
COPY wesense-storage-broker/ /app/wesense-storage-broker/
RUN pip install --no-cache-dir /app/wesense-storage-broker

EXPOSE 8080

COPY wesense-storage-broker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, ssl, os; ctx=ssl.create_default_context() if os.getenv('TLS_ENABLED','')!='true' else ssl._create_unverified_context(); urllib.request.urlopen(('https' if os.getenv('TLS_ENABLED','')=='true' else 'http')+'://localhost:8080/health', context=ctx)"

ENTRYPOINT ["/app/entrypoint.sh"]
