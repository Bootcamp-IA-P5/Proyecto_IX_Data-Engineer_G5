"""
Script para resetear todo el sistema con datos de prueba reducidos
Útil para desarrollo y pruebas rápidas
"""
import os
import sys
import time
import subprocess
from pathlib import Path
from pymongo import MongoClient

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

# Cargar .env
load_dotenv(ROOT_DIR / '.env')

MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'hrpro_db')


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
    print("  1. Detener todos los servicios")
    print("  2. Eliminar el volumen de MongoDB (BORRARÁ TODOS LOS DATOS)")
    print("  3. Reconstruir MongoDB desde cero")
    print("  4. Levantar random-generator por 2 minutos")
    print("  5. Detener random-generator")
    print("  6. Levantar data-processor")
    
    response = input("\n¿Continuar? (escribe 'SI' para confirmar): ")
    
    if response != 'SI':
        print("\n❌ Operación cancelada")
        return
    
    # 1. Detener todos los servicios
    if not run_command(
        "docker-compose -f docker-compose-services.yml down",
        "Deteniendo servicios"
    ):
        return
    
    # 2. Eliminar volumen de MongoDB
    if not run_command(
        "docker volume rm pix_g5_dataengineer_mongodb_data",
        "Eliminando volumen de MongoDB"
    ):
        print("⚠️  El volumen puede no existir o estar en uso")
    
    # 3. Levantar infraestructura (MongoDB + Kafka)
    if not run_command(
        "docker-compose -f docker-compose-infra.yml up -d",
        "Levantando infraestructura (MongoDB + Kafka)"
    ):
        return
    
    print("\n⏳ Esperando 15 segundos para que MongoDB esté listo...")
    time.sleep(15)
    
    # 4. Verificar que MongoDB esté listo
    print("\n🔍 Verificando conexión a MongoDB...")
    try:
        client = MongoClient(
            f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/?authSource=admin",
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print("✅ MongoDB está listo")
        client.close()
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {e}")
        return
    
    # 5. Levantar random-generator
    if not run_command(
        "docker-compose -f docker-compose-services.yml up -d random-generator",
        "Levantando random-generator"
    ):
        return
    
    # 6. Esperar 2 minutos generando datos
    print("\n⏳ Generando datos de prueba durante 2 minutos...")
    print("Puedes ver los logs con: docker logs -f random-generator")
    
    for remaining in range(120, 0, -10):
        print(f"   Tiempo restante: {remaining} segundos...")
        time.sleep(10)
    
    # 7. Detener random-generator
    if not run_command(
        "docker-compose -f docker-compose-services.yml stop random-generator",
        "Deteniendo random-generator"
    ):
        return
    
    # 8. Ver estadísticas de datos generados
    print("\n📊 Estadísticas de datos generados:")
    try:
        client = MongoClient(
            f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/?authSource=admin"
        )
        db = client[MONGO_DATABASE]
        count = db.raw_messages.count_documents({})
        print(f"   Total mensajes en raw_messages: {count:,}")
        client.close()
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
    
    # 9. Levantar data-processor
    if not run_command(
        "docker-compose -f docker-compose-services.yml up -d --build data-processor",
        "Levantando data-processor"
    ):
        return
    
    print("\n" + "=" * 80)
    print("✅ SISTEMA RESETEADO Y LISTO")
    print("=" * 80)
    print("\n📋 Comandos útiles:")
    print(f"  - Ver logs del procesador: docker logs -f data-processor")
    print(f"  - Dashboard de monitoreo: python {__file__.replace(str(ROOT_DIR), '.')}")
    print(f"  - Reiniciar generación: docker-compose -f docker-compose-services.yml start random-generator")
    print("=" * 80)


if __name__ == "__main__":
    main()