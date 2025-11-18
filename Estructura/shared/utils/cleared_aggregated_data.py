"""
Vacía/borra los datos agregados/procesados de la colección aggregated_data en MongoDB usando docker exec y autenticación por variables de entorno.
"""

import subprocess
import os
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')
AGGREGATED_COLLECTION = os.getenv('AGGREGATED_COLLECTION')

print("=" * 80)
print(" " * 25 + "🧹 LIMPIEZA DE AGGREGATED_DATA")
print("=" * 80)
print(f"\n🔌 Borrando datos agregados/procesados de MongoDB (via docker exec con autenticación)...")

try:
    # Ejecutar el borrado dentro del contenedor CON autenticación
    result = subprocess.run([
        'docker', 'exec', MONGO_CONTAINER, 'mongosh', MONGO_DATABASE,
        '--username', MONGO_USER,
        '--password', MONGO_PASS,
        '--authenticationDatabase', 'admin',
        '--quiet', '--eval',
        f'''
        var n = db.{AGGREGATED_COLLECTION}.countDocuments({{}});
        print("Documentos a borrar: " + n);
        db.{AGGREGATED_COLLECTION}.deleteMany({{}});
        print("OK");
        '''
    ], capture_output=True, text=True, check=True, timeout=120)

    print(result.stdout.strip())
    print("\n" + "=" * 80)

except subprocess.TimeoutExpired:
    print("❌ Timeout: La consulta tardó más de 30 segundos")
except subprocess.CalledProcessError as e:
    print(f"❌ Error ejecutando mongosh:")
    print(f"   Stderr: {e.stderr}")
    print(f"   Stdout: {e.stdout}")
except Exception as e:
    print(f"❌ Error inesperado: {type(e).__name__}: {e}")