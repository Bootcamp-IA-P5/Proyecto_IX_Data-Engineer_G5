#!/bin/bash

# Script de ayuda para configurar el generador de datos

echo "🚀 Setup del Generador de Datos y Kafka"
echo "========================================"
echo ""

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para imprimir mensajes
print_step() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

echo "Este script te ayudará a configurar el generador de datos."
echo ""

# Verificar si Docker está instalado
if ! command -v docker &> /dev/null; then
    print_error "Docker no está instalado. Por favor instala Docker primero."
    exit 1
fi

print_step "Docker está instalado"

# Verificar si Docker Compose está instalado
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose no está instalado."
    exit 1
fi

print_step "Docker Compose está instalado"

# Preguntar dónde clonar el repo
echo ""
echo "¿Dónde quieres clonar el generador de datos?"
echo "Recomendado: ~/Desktop/Kafka/"
read -p "Ruta (Enter para usar la recomendada): " CLONE_PATH

if [ -z "$CLONE_PATH" ]; then
    CLONE_PATH="$HOME/Desktop/Kafka"
fi

# Crear directorio si no existe
mkdir -p "$CLONE_PATH"

echo ""
print_step "Usando ruta: $CLONE_PATH"

# Verificar si ya existe
if [ -d "$CLONE_PATH/data-engineering-educational-project" ]; then
    print_warning "El directorio ya existe. ¿Quieres actualizarlo?"
    read -p "(s/N): " UPDATE
    if [ "$UPDATE" = "s" ] || [ "$UPDATE" = "S" ]; then
        cd "$CLONE_PATH/data-engineering-educational-project"
        git pull
        print_step "Repositorio actualizado"
    fi
else
    # Clonar el repositorio
    print_step "Clonando repositorio educativo..."
    cd "$CLONE_PATH"
    git clone https://github.com/Factoria-F5-dev/data-engineering-educational-project.git
    
    if [ $? -eq 0 ]; then
        print_step "Repositorio clonado exitosamente"
    else
        print_error "Error al clonar el repositorio"
        exit 1
    fi
fi

# Ir al directorio
cd "$CLONE_PATH/data-engineering-educational-project"

# Levantar servicios
echo ""
print_step "Levantando Kafka y generador de datos..."
docker compose up -d

if [ $? -eq 0 ]; then
    print_step "Servicios levantados exitosamente"
else
    print_error "Error al levantar los servicios"
    exit 1
fi

# Esperar a que Kafka esté listo
echo ""
print_step "Esperando a que Kafka esté listo (10 segundos)..."
sleep 10

# Verificar estado
echo ""
print_step "Estado de los servicios:"
docker compose ps

echo ""
echo "========================================"
print_step "¡Setup completado!"
echo ""
echo "Información importante:"
echo "  📍 Kafka Broker: localhost:29092"
echo "  📍 Topic: probando"
echo "  📍 Ubicación: $CLONE_PATH/data-engineering-educational-project"
echo ""
echo "Comandos útiles:"
echo "  • Ver logs: cd $CLONE_PATH/data-engineering-educational-project && docker compose logs -f"
echo "  • Detener: cd $CLONE_PATH/data-engineering-educational-project && docker compose down"
echo "  • Reiniciar: cd $CLONE_PATH/data-engineering-educational-project && docker compose restart"
echo ""
print_step "Ahora puedes ejecutar el consumer de Kafka desde nuestro proyecto"
