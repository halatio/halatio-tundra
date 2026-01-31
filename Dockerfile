FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install build dependencies for ConnectorX and system utilities
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create temp directory for Polars streaming with proper permissions
RUN mkdir -p /tmp/polars_streaming && \
    chmod 755 /tmp/polars_streaming

# Create non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

ENV PYTHONUNBUFFERED=1
ENV POLARS_MAX_THREADS=4
ENV TMPDIR=/tmp/polars_streaming

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
