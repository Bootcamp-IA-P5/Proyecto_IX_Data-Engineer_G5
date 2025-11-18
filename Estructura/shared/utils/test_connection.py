"""
Test de conexión a MongoDB usando docker exec y mongosh.
Muestra cuántos documentos hay en raw_messages y aggregated_data (si existen).
Salida mejorada y más clara.
"""

import os
import subprocess
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

print("=" * 80)
print("🧪 TEST DE CONEXIÓN A MONGODB (docker exec)")
print("=" * 80)

try:
    result = subprocess.run([
        'docker', 'exec', MONGO_CONTAINER, 'mongosh', MONGO_DATABASE,
        '--username', MONGO_USER,
        '--password', MONGO_PASS,
        '--authenticationDatabase', 'admin',
        '--quiet', '--eval',
        '''
        var collections = db.getCollectionNames();
        if (collections.includes("raw_messages")) {
            var raw_count = db.raw_messages.countDocuments({});
            print("[OK] RAW_MESSAGES: " + raw_count + " documentos encontrados.");
        } else {
            print("[WARN!] RAW_MESSAGES: La colección no existe.");
        }
        if (collections.includes("aggregated_data")) {
            var agg_count = db.aggregated_data.countDocuments({});
            print("[OK] AGGREGATED_DATA: " + agg_count + " documentos encontrados.");
        } else {
            print("[WARN!] AGGREGATED_DATA: La colección no existe.");
        }
        '''
    ], capture_output=True, text=True, check=True, timeout=30)

    print("\n--- RESULTADO DE LA CONSULTA ---\n")
    print(result.stdout.strip())
    print("\n--------------------------------")
    print("🎉 ¡Conexión y consulta a MongoDB realizadas correctamente!\n")

except subprocess.TimeoutExpired:
    print("❌ Timeout: La consulta tardó más de 30 segundos")
except subprocess.CalledProcessError as e:
    print(f"❌ Error ejecutando mongosh:")
    print(f"   Stderr: {e.stderr}")
    print(f"   Stdout: {e.stdout}")
except Exception as e:
    print(f"❌ Error inesperado: {type(e).__name__}: {e}")