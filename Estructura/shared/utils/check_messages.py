"""
Analizador de patrones de mensajes en raw_messages
Útil para entender la estructura de los datos
"""
import sys
from pathlib import Path
from pymongo import MongoClient
import json
import os

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

# Cargar .env
load_dotenv(ROOT_DIR / '.env')

# Leer credenciales desde .env
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'hrpro_db')

# Probar diferentes URIs
URIS_TO_TRY = [
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/{MONGO_DATABASE}?authSource=admin",
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/admin",
    f"mongodb://localhost:27017/",  # Sin auth
]

print(f"🔌 Conectando a: {MONGO_DATABASE}")

# Conectar a MongoDB
client = None
for uri in URIS_TO_TRY:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("✅ Conexión exitosa a MongoDB")
        break
    except:
        continue

if not client:
    print("❌ No se pudo conectar a MongoDB")
    print("\n💡 Verifica que MongoDB esté ejecutándose:")
    print("   docker ps | grep mongo")
    exit(1)

db = client[MONGO_DATABASE]
collection = db['raw_messages']

# Obtener 50 mensajes aleatorios
print("\n📥 Obteniendo mensajes de muestra...")
messages = list(collection.aggregate([{'$sample': {'size': 50}}]))

if not messages:
    print("⚠️  No hay mensajes en raw_messages")
    print("\n💡 Genera datos con:")
    print("   docker-compose -f docker-compose-services.yml up -d random-generator")
    client.close()
    exit(0)

# Agrupar por campos
patterns = {}
for msg in messages:
    fields = tuple(sorted([k for k in msg.keys() if not k.startswith('_')]))
    
    if fields not in patterns:
        patterns[fields] = {'count': 0, 'examples': []}
    
    patterns[fields]['count'] += 1
    if len(patterns[fields]['examples']) < 2:
        patterns[fields]['examples'].append(msg)

# Mostrar resultados
print("=" * 80)
print("PATRONES DE MENSAJES ENCONTRADOS")
print("=" * 80)

for i, (fields, data) in enumerate(patterns.items(), 1):
    print(f"\n{'='*80}")
    print(f"TIPO {i}: {', '.join(fields)}")
    print(f"Cantidad en muestra: {data['count']}")
    print(f"\nEjemplo:")
    clean_example = {k: v for k, v in data['examples'][0].items() if not k.startswith('_')}
    print(json.dumps(clean_example, indent=2, default=str, ensure_ascii=False))
    print("=" * 80)

# Estadísticas generales
total_raw = collection.count_documents({})
processed = collection.count_documents({'processed': True})
pending = collection.count_documents({'processed': {'$ne': True}})

print(f"\n📊 ESTADÍSTICAS:")
print(f"   Total mensajes: {total_raw:,}")
print(f"   Procesados: {processed:,}")
print(f"   Pendientes: {pending:,}")

client.close()
print("\n✅ Análisis completado")