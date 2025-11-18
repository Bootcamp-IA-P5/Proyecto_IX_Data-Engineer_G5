"""
Limpia/borrar todos los documentos de la colección raw_messages en MongoDB usando docker exec y mongosh.
Usa variables de entorno para la configuración.
"""

import os
import subprocess
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

print("=" * 80)
print("🧹 LIMPIANDO COLECCIÓN raw_messages")
print("=" * 80)

try:
    result = subprocess.run([
        'docker', 'exec', MONGO_CONTAINER, 'mongosh', MONGO_DATABASE,
        '--username', MONGO_USER,
        '--password', MONGO_PASS,
        '--authenticationDatabase', 'admin',
        '--quiet', '--eval',
        '''
        var deleted = db.raw_messages.deleteMany({});
        print("✅ Se han borrado " + deleted.deletedCount + " documentos de raw_messages.");
        '''
    ], capture_output=True, text=True, check=True, timeout=120)

    print(result.stdout.strip())
except subprocess.TimeoutExpired:
    print("❌ Timeout: La consulta tardó más de 30 segundos")
except subprocess.CalledProcessError as e:
    print(f"❌ Error ejecutando mongosh:")
    print(f"   Stderr: {e.stderr}")
    print(f"   Stdout: {e.stdout}")
except Exception as e:
    print(f"❌ Error inesperado: {type(e).__name__}: {e}")