FROM python:3.11-slim

WORKDIR /app

# Evita bytecode y buffering
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependencias del sistema (psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src/ ./src/

# Comando por defecto: servicio en modo bucle
CMD ["python", "-m", "src.sql-persister"]


