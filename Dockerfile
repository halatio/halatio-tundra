FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-install DuckDB extensions to a shared directory so all users can load
# them without network access at runtime.
RUN mkdir -p /opt/duckdb_extensions && \
    python -c "\
import duckdb, os; \
conn = duckdb.connect(config={'extension_directory': '/opt/duckdb_extensions', 'home_directory': '/root'}); \
[conn.execute(f'INSTALL {e}') for e in ['httpfs', 'excel', 'postgres', 'mysql']]; \
conn.close(); \
print('DuckDB extensions pre-installed')" && \
    chmod -R 755 /opt/duckdb_extensions

COPY app/ ./app/

# DuckDB temp/spill directory (must exist and be writable at runtime)
RUN mkdir -p /tmp/duckdb_swap && chmod 777 /tmp/duckdb_swap

# Non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
