#!/bin/bash

# Script para ejecutar el Kafka Consumer

# Navegar al directorio del consumerscd "$(dirname "$0")"

echo "🚀 Iniciando Kafka Consumer..."
echo "================================"

# Ejecutar el consumer
python src/consumer.py
