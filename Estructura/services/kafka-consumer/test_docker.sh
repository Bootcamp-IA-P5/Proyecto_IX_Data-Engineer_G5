#!/bin/bash

# ============================================================
# SCRIPT DE PRUEBA - KAFKA CONSUMER DOCKER
# ============================================================
# Script para probar la construcción y ejecución del contenedor
# ============================================================

set -e  # Salir si hay algún error

echo "🧪 PRUEBA DE KAFKA CONSUMER - DOCKER"
echo "===================================="
echo ""

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función para imprimir con color
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Navegar al directorio del servicio
cd "$(dirname "$0")"

# 1. Verificar que existe .env
echo "1️⃣  Verificando archivo .env..."
if [ -f .env ]; then
    print_success "Archivo .env encontrado"
    echo "   Configuración actual:"
    echo "   - Broker: $(grep KAFKA_BOOTSTRAP_SERVERS .env | cut -d '=' -f2)"
    echo "   - Topic: $(grep KAFKA_TOPIC .env | cut -d '=' -f2)"
    echo "   - Group: $(grep KAFKA_GROUP_ID .env | cut -d '=' -f2)"
else
    print_error "Archivo .env no encontrado"
    echo "   Creando desde .env.example..."
    cp .env.example .env
    print_info "Por favor, ajusta .env antes de continuar"
    exit 1
fi
echo ""

# 2. Verificar que existen los archivos necesarios
echo "2️⃣  Verificando archivos requeridos..."
required_files=("dockerfile" "requirements.txt" "src/consumer.py" "src/models.py")
all_exist=true

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        print_success "$file encontrado"
    else
        print_error "$file NO encontrado"
        all_exist=false
    fi
done

if [ "$all_exist" = false ]; then
    print_error "Faltan archivos necesarios"
    exit 1
fi
echo ""

# 3. Limpiar archivos innecesarios
echo "3️⃣  Limpiando archivos innecesarios..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
print_success "Archivos .pyc y __pycache__ eliminados"
echo ""

# 4. Verificar que Docker está corriendo
echo "4️⃣  Verificando Docker..."
if ! docker info > /dev/null 2>&1; then
    print_error "Docker no está corriendo"
    echo "   Inicia Docker Desktop y vuelve a intentar"
    exit 1
fi
print_success "Docker está corriendo"
echo ""

# 5. Verificar que la red kafka-net existe
echo "5️⃣  Verificando red kafka-net..."
if docker network inspect kafka-net > /dev/null 2>&1; then
    print_success "Red kafka-net encontrada"
else
    print_error "Red kafka-net no encontrada"
    echo "   Ejecuta primero: docker-compose -f docker-compose-kafka.yml up -d"
    exit 1
fi
echo ""

# 6. Construir la imagen
echo "6️⃣  Construyendo imagen Docker..."
if docker build -t kafka-consumer:test . ; then
    print_success "Imagen construida exitosamente"
    
    # Mostrar tamaño de la imagen
    image_size=$(docker images kafka-consumer:test --format "{{.Size}}")
    echo "   Tamaño de la imagen: $image_size"
else
    print_error "Error construyendo la imagen"
    exit 1
fi
echo ""

# 7. Verificar la imagen
echo "7️⃣  Verificando imagen..."
if docker run --rm kafka-consumer:test python --version; then
    print_success "Imagen funcional"
else
    print_error "Error ejecutando la imagen"
    exit 1
fi
echo ""

# 8. Verificar estructura interna
echo "8️⃣  Verificando estructura interna..."
echo "   Contenido de /app:"
docker run --rm kafka-consumer:test ls -la /app
echo ""

# 9. Verificar dependencias instaladas
echo "9️⃣  Verificando dependencias Python..."
if docker run --rm kafka-consumer:test python -c "import confluent_kafka; print('confluent-kafka:', confluent_kafka.version())"; then
    print_success "Dependencias instaladas correctamente"
else
    print_error "Error con dependencias"
    exit 1
fi
echo ""

# 10. Verificar que Kafka está corriendo
echo "🔟 Verificando Kafka..."
if docker ps | grep -q kafka; then
    print_success "Kafka está corriendo"
    kafka_status="READY"
else
    print_error "Kafka NO está corriendo"
    kafka_status="NOT READY"
    echo "   Ejecuta: docker-compose -f docker-compose-kafka.yml up -d"
fi
echo ""

# Resumen
echo "===================================="
echo "📊 RESUMEN DE PRUEBAS"
echo "===================================="
print_success "Todas las pruebas básicas pasaron"
echo ""
echo "Estado de servicios:"
echo "   - Imagen Docker: ✅ Construida"
echo "   - Dependencias: ✅ Instaladas"
echo "   - Red kafka-net: ✅ Disponible"
echo "   - Kafka: $kafka_status"
echo ""

if [ "$kafka_status" = "READY" ]; then
    echo "🎯 LISTO PARA EJECUTAR"
    echo ""
    echo "Comandos sugeridos:"
    echo "   # Ejecutar con docker-compose:"
    echo "   docker-compose -f ../../docker-compose-services.yml up -d kafka-consumer"
    echo ""
    echo "   # Ver logs:"
    echo "   docker-compose -f ../../docker-compose-services.yml logs -f kafka-consumer"
    echo ""
    echo "   # O ejecutar manualmente:"
    echo "   docker run -d --name kafka-consumer --network kafka-net --env-file .env kafka-consumer:test"
else
    echo "⚠️  KAFKA NO ESTÁ LISTO"
    echo ""
    echo "Primero ejecuta:"
    echo "   cd ../.."
    echo "   docker-compose -f docker-compose-kafka.yml up -d"
    echo "   # Espera 30 segundos para que Kafka inicie"
    echo "   docker-compose -f docker-compose-services.yml up -d kafka-consumer"
fi

echo ""
echo "✅ Pruebas completadas"
