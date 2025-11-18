"""
Script para resetear todo el sistema con datos de prueba reducidos.
Utiliza variables de entorno para credenciales y parámetros de conexión.
No muestra contraseñas en claro y es útil para desarrollo y pruebas rápidas.

Este script realiza:
  1. Detiene todos los servicios definidos en docker-compose-services.yml y docker-compose-kafka.yml
  2. Elimina el volumen de MongoDB (BORRA TODOS LOS DATOS)
  3. Reconstruye la infraestructura (Kafka, Zookeeper, Kafdrop, MongoDB, etc.)
  4. Levanta random-generator por 2 minutos para generar datos de prueba
  5. Detiene random-generator
  6. Levanta data-processor
  7. Muestra estadísticas de los datos generados
"""
import os
import sys
import time
import subprocess
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Cargar .env
load_dotenv(ROOT_DIR / '.env')

MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'hrpro_db')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))

def run_command(cmd, description, cwd=None):
    """Ejecuta un comando y muestra el resultado"""
    print(f"\n{'='*80}")
    print(f"🔧 {description}")
    print(f"{'='*80}")
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        cwd=cwd or ROOT_DIR
    )
    if result.returncode == 0:
        print(f"✅ {description} completado")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"❌ Error en {description}")
        if result.stderr:
            print(result.stderr)
        return False
    return True

def main():
    print("=" * 80)
    print(" " * 20 + "🔄 RESETEO COMPLETO DEL SISTEMA")
    print("=" * 80)
    print(f"📁 Directorio raíz: {ROOT_DIR}")

    print("\n⚠️  ADVERTENCIA:")
    print("Este script va a:")
    print("  1. Detener todos los servicios (Kafka, Zookeeper, Kafdrop, MongoDB, etc.)")
    print("  2. Eliminar el volumen de MongoDB (BORRARÁ TODOS LOS DATOS)")
    print("  3. Reconstruir la infraestructura")
    print("  4. Levantar random-generator por 2 minutos")
    print("  5. Detener random-generator")
    print("  6. Levantar data-processor")
    print("  7. Mostrar estadísticas de los datos generados")
    response = input("\n¿Continuar? (escribe 'SI' para confirmar): ")
    if response != 'SI':
        print("\n❌ Operación cancelada")
        return

    # 1. Detener todos los servicios
    run_command(
        "docker-compose -f docker-compose-services.yml down",
        "Deteniendo servicios de aplicación"
    )
    run_command(
        "docker-compose -f docker-compose-kafka.yml down",
        "Deteniendo servicios de Kafka"
    )

    # 2. Eliminar volumen de MongoDB
    run_command(
        "docker volume rm pix_g5_dataengineer_mongodb_data",
        "Eliminando volumen de MongoDB"
    )

    # 3. Levantar infraestructura (Kafka, Zookeeper, Kafdrop, MongoDB, etc.)
    run_command(
        "docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop",
        "Levantando Kafka, Zookeeper y Kafdrop"
    )
    run_command(
        "docker-compose -f docker-compose-services.yml up -d mongo mongo-viewer",
        "Levantando MongoDB y Mongo Viewer"
    )

    print("\n⏳ Esperando 15 segundos para que MongoDB y Kafka estén listos...")
    time.sleep(15)

    # 4. Verificar que MongoDB esté listo
    print("\n🔍 Verificando conexión a MongoDB...")
    try:
        client = MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource='admin',
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print("✅ MongoDB está listo")
        client.close()
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {e}")
        return

    # 5. Levantar random-generator
    run_command(
        "docker-compose -f docker-compose-kafka.yml up -d random_generator",
        "Levantando random-generator"
    )

    # 6. Esperar 2 minutos generando datos
    print("\n⏳ Generando datos de prueba durante 2 minutos...")
    print("Puedes ver los logs con: docker logs -f random_generator")
    for remaining in range(120, 0, -10):
        print(f"   Tiempo restante: {remaining} segundos...")
        time.sleep(10)

    # 7. Detener random-generator
    run_command(
        "docker-compose -f docker-compose-kafka.yml stop random_generator",
        "Deteniendo random-generator"
    )

    # 8. Ver estadísticas de datos generados
    print("\n📊 Estadísticas de datos generados:")
    try:
        client = MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource='admin'
        )
        db = client[MONGO_DATABASE]
        count = db.raw_messages.count_documents({})
        print(f"   Total mensajes en raw_messages: {count:,}")
        client.close()
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")

    # 9. Levantar data-processor
    run_command(
        "docker-compose -f docker-compose-services.yml up -d --build data-processor",
        "Levantando data-processor"
    )

    print("\n" + "=" * 80)
    print("✅ SISTEMA RESETEADO Y LISTO")
    print("=" * 80)
    print("\n📋 Comandos útiles:")
    print(f"  - Ver logs del procesador: docker logs -f data-processor")
    print(f"  - Dashboard de monitoreo: python {__file__.replace(str(ROOT_DIR), '.')}")
    print(f"  - Reiniciar generación: docker-compose -f docker-compose-kafka.yml restart random_generator")
    print("=" * 80)

if __name__ == "__main__":
    main()