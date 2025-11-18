# Imagen ligera de Python 3.11
FROM python:3.11-slim

# Evitar buffering para que los logs salgan en tiempo real
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Instala dependencias
# (requirements.txt debe estar junto al Dockerfile)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
# Nota: asumimos que en el contexto de build existe una carpeta "src/"
COPY src/ ./src/

# Comando por defecto (puede ser sobreescrito desde docker-compose)
# Ejecutamos como módulo para que __main__ configure el logging
CMD ["python", "-u", "-m", "src.processor"]

