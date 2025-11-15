# ============================================================
# DOCKERFILE - MONGO PERSISTER SERVICE
# ============================================================
FROM python:3.11-slim

LABEL maintainer="Proyecto_IX_Data-Engineer_G5"
LABEL description="MongoDB Persister - Guarda mensajes crudos de Kafka en MongoDB"
LABEL version="1.0"

# Variables de entorno para Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    librdkafka-dev \
    gcc \
    g++ \
    make && \
    rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src/ ./src/

# Crear usuario no-root
RUN useradd -m -u 1000 mongouser && \
    chown -R mongouser:mongouser /app

# Cambiar a usuario no-root
USER mongouser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD ps aux | grep -v grep | grep persister.py || exit 1

# Comando por defecto
CMD ["python", "src/persister.py"]
