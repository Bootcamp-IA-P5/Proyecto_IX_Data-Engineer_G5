"""
Estadísticas rápidas de MongoDB
Útil para verificar el estado del sistema sin dashboard
"""
import sys
from pathlib import Path
from pymongo import MongoClient
import os

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

# Cargar .env
load_dotenv(ROOT_DIR / '.env')

MONGO_USERNAME = os.getenv('MONGO_USERTEST')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORDTEST')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')

# Probar diferentes configuraciones de URI
URIS_TO_TRY = [
    # Conectar directamente a la base de datos sin authSource
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/{MONGO_DATABASE}",
    # Con authSource igual a la base de datos
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/{MONGO_DATABASE}?authSource={MONGO_DATABASE}",
    # Sin especificar base de datos en la URI
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/?authSource={MONGO_DATABASE}",
]


def try_connect():
    """Intenta conectar con diferentes URIs"""
    for i, uri in enumerate(URIS_TO_TRY, 1):
        try:
            print(f"🔄 Intento {i}/3...", end=" ")
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            # Probar acceso a la base de datos específica
            db = client[MONGO_DATABASE]
            db.command('ping')
            print("✅")
            return client
        except Exception as e:
            print(f"❌ {type(e).__name__}")
            if i == len(URIS_TO_TRY):
                raise e
            continue
    return None


def main():
    print("=" * 80)
    print(" " * 25 + "📊 ESTADÍSTICAS RÁPIDAS")
    print("=" * 80)
    print(f"\n🔌 Conectando con usuario '{MONGO_USERNAME}' a base de datos '{MONGO_DATABASE}'...")
    
    try:
        client = try_connect()
        print("\n✅ Conexión exitosa")
        
        db = client[MONGO_DATABASE]
        
        # Raw Messages
        raw_total = db.raw_messages.count_documents({})
        raw_processed = db.raw_messages.count_documents({'processed': True})
        raw_pending = raw_total - raw_processed
        
        print("\n📥 RAW MESSAGES:")
        print(f"   Total:      {raw_total:,}")
        print(f"   Procesados: {raw_processed:,} ({raw_processed/raw_total*100:.1f}%)" if raw_total > 0 else "   Procesados: 0 (0.0%)")
        print(f"   Pendientes: {raw_pending:,}")
        
        # Aggregated Data
        agg_total = db.aggregated_data.count_documents({})
        agg_complete = db.aggregated_data.count_documents({'is_complete': True})
        
        print("\n👥 DATOS AGREGADOS:")
        print(f"   Total personas: {agg_total:,}")
        print(f"   Completos (5 tipos): {agg_complete:,}")
        
        if agg_total > 0:
            print(f"   Completitud: {agg_complete/agg_total*100:.2f}%")
        
        # Distribución de tipos
        print("\n📊 DISTRIBUCIÓN DE TIPOS:")
        pipeline = [
            {'$unwind': '$types_received'},
            {'$group': {'_id': '$types_received', 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}}
        ]
        
        for doc in db.aggregated_data.aggregate(pipeline):
            print(f"   {doc['_id']}: {doc['count']:,}")
        
        print("\n" + "=" * 80)
        
        client.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"\n💡 Detalles:")
        print(f"   Username: {MONGO_USERNAME}")
        print(f"   Database: {MONGO_DATABASE}")
        print(f"\n🔧 Intentos realizados:")
        print(f"   1. URI: mongodb://user:***@localhost:27017/{MONGO_DATABASE}")
        print(f"   2. URI: mongodb://user:***@localhost:27017/{MONGO_DATABASE}?authSource={MONGO_DATABASE}")
        print(f"   3. URI: mongodb://user:***@localhost:27017/?authSource={MONGO_DATABASE}")
        exit(1)


if __name__ == "__main__":
    main()