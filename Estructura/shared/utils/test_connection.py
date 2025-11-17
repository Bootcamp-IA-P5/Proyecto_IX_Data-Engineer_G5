"""
Test simple de conexión a MongoDB usando distintas configuraciones.
Utiliza variables de entorno para credenciales y parámetros de conexión.
Permite verificar si la base de datos está accesible desde el host y diagnosticar problemas de autenticación o red.
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import sys

# Cargar variables de entorno
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT'))
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

print("=" * 80)
print("🧪 TEST DE CONEXIÓN A MONGODB")
print("=" * 80)

# Configuraciones a probar (sin mostrar la contraseña en claro)
configs = [
    ("Sin auth", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"),
    ("Admin con authSource", f"mongodb://{MONGO_USER}:***@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"),
    ("Admin sin authSource", f"mongodb://{MONGO_USER}:***@{MONGO_HOST}:{MONGO_PORT}/"),
    ("Admin con directConnection", None),  # Especial
]

for name, uri in configs:
    print(f"\n{'─' * 80}")
    print(f"🔄 Probando: {name}")
    
    if uri:
        print(f"   URI: {uri}")
    else:
        print(f"   Params: host={MONGO_HOST}, port={MONGO_PORT}, username={MONGO_USER}, authSource=admin, directConnection=True")
    
    try:
        if uri:
            # Usar la contraseña real solo en la conexión, nunca mostrarla
            real_uri = uri.replace('***', MONGO_PASS)
            client = MongoClient(real_uri, serverSelectionTimeoutMS=3000)
        else:
            client = MongoClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
                username=MONGO_USER,
                password=MONGO_PASS,
                authSource='admin',
                directConnection=True,
                serverSelectionTimeoutMS=3000
            )
        
        # Ping a admin database
        result = client.admin.command('ping')
        print(f"   ✅ Ping exitoso: {result}")
        
        # Intentar acceder a la base de datos
        db = client[MONGO_DATABASE]
        count = db.raw_messages.count_documents({})
        print(f"   ✅ Acceso a {MONGO_DATABASE}: {count:,} documentos en raw_messages")
        
        client.close()
        print(f"\n🎉 ¡ÉXITO! Esta configuración funciona")
        sys.exit(0)
        
    except ConnectionFailure as e:
        print(f"   ❌ Error de conexión: {e}")
    except OperationFailure as e:
        print(f"   ❌ Error de autenticación: {e}")
    except Exception as e:
        print(f"   ❌ Error inesperado: {type(e).__name__}: {e}")

print(f"\n{'═' * 80}")
print("💥 Ninguna configuración funcionó")
print("═" * 80)

print("\n🔍 DIAGNÓSTICO:")
print("1. Verifica que MongoDB esté exponiendo el puerto:")
print(f"   docker port {MONGO_CONTAINER}")
print("\n2. Intenta conectar con mongosh desde tu host:")
print(f"   docker exec {MONGO_CONTAINER} mongosh --username {MONGO_USER} --password *** --authenticationDatabase admin")
print("\n3. Verifica la configuración de red:")
print(f"   docker inspect {MONGO_CONTAINER} | grep -i port")