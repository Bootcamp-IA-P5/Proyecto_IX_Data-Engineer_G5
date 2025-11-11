#!/bin/bash

# ============================================================
# SCRIPT DE EJECUCIÓN - KAFKA CONSUMER
# ============================================================
# Script para ejecutar el Kafka Consumer en modo local
# Para ejecución en Docker, usa docker-compose
# ============================================================

# Navegar al directorio del consumer
cd "$(dirname "$0")"

# Verificar que Python3 esté instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 no está instalado"
    echo "Instala Python 3: brew install python3"
    exit 1
fi

# Verificar que exista el archivo .env
if [ ! -f .env ]; then
    echo "⚠️  Advertencia: No existe archivo .env"
    echo "Creando .env desde .env.example..."
    cp .env.example .env
    echo "✅ Archivo .env creado. Revisa la configuración antes de continuar."
    echo ""
fi

# Verificar que las dependencias estén instaladas
if ! python3 -c "import confluent_kafka" 2>/dev/null; then
    echo "⚠️  Advertencia: confluent-kafka no está instalado"
    echo "Instalando dependencias..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Error instalando dependencias"
        exit 1
    fi
fi

echo "🚀 Iniciando Kafka Consumer..."
echo "================================"
echo "📋 Configuración:"
echo "   - Broker: $(grep KAFKA_BOOTSTRAP_SERVERS .env | cut -d '=' -f2)"
echo "   - Topic: $(grep KAFKA_TOPIC .env | cut -d '=' -f2)"
echo "   - Group: $(grep KAFKA_GROUP_ID .env | cut -d '=' -f2)"
echo "================================"
echo ""
echo "Presiona Ctrl+C para detener el consumer"
echo ""

# Ejecutar el consumer con python3
python3 src/consumer.py
