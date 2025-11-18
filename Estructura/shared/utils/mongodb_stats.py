"""
Estadísticas rápidas de MongoDB
Útil para verificar el estado del sistema sin dashboard

Obtiene estadísticas rápidas y globales de la base de datos MongoDB.
Muestra el total de mensajes, procesados, pendientes, personas agregadas,
completitud y distribución por tipo. Útil para auditoría y diagnóstico general.

"""
import subprocess
import json
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

print("=" * 80)
print(" " * 25 + "📊 ESTADÍSTICAS RÁPIDAS")
print("=" * 80)
print(f"\n🔌 Obteniendo datos de MongoDB (via docker exec con autenticación)...")

try:
    # Ejecutar consultas dentro del contenedor CON autenticación
    result = subprocess.run([
        'docker', 'exec', MONGO_CONTAINER, 'mongosh', MONGO_DATABASE,
        '--username', MONGO_USER,
        '--password', MONGO_PASS,
        '--authenticationDatabase', 'admin',
        '--quiet', '--eval',
        '''
        const data = {
            raw_total: db.raw_messages.countDocuments({}),
            raw_processed: db.raw_messages.countDocuments({processed: true}),
            agg_total: db.aggregated_data.countDocuments({}),
            agg_complete: db.aggregated_data.countDocuments({is_complete: true}),
            types: db.aggregated_data.aggregate([
                {$unwind: "$types_received"},
                {$group: {_id: "$types_received", count: {$sum: 1}}},
                {$sort: {_id: 1}}
            ]).toArray()
        };
        print(JSON.stringify(data));
        '''
    ], capture_output=True, text=True, check=True, timeout=30)
    
    # Parsear resultado
    output = result.stdout.strip()
    data = json.loads(output)
    
    raw_total = data['raw_total']
    raw_processed = data['raw_processed']
    raw_pending = raw_total - raw_processed
    agg_total = data['agg_total']
    agg_complete = data['agg_complete']
    
    print("\n📥 RAW MESSAGES:")
    print(f"   Total:      {raw_total:,}")
    if raw_total > 0:
        print(f"   Procesados: {raw_processed:,} ({raw_processed/raw_total*100:.1f}%)")
        print(f"   Pendientes: {raw_pending:,}")
    
    print("\n👥 DATOS AGREGADOS:")
    print(f"   Total personas: {agg_total:,}")
    print(f"   Completos (5 tipos): {agg_complete:,}")
    
    if agg_total > 0:
        print(f"   Completitud: {agg_complete/agg_total*100:.2f}%")
    
    # Distribución de tipos
    if data['types']:
        print("\n📊 DISTRIBUCIÓN DE TIPOS:")
        type_icons = {
            'bank': '💰',
            'location': '📍',
            'net': '🌐',
            'personal': '👤',
            'professional': '💼'
        }
        
        for item in data['types']:
            icon = type_icons.get(item['_id'], '📄')
            print(f"   {icon} {item['_id']}: {item['count']:,}")
    
    print("\n" + "=" * 80)
    
except subprocess.TimeoutExpired:
    print("❌ Timeout: La consulta tardó más de 30 segundos")
    sys.exit(1)
except subprocess.CalledProcessError as e:
    print(f"❌ Error ejecutando mongosh:")
    print(f"   Stderr: {e.stderr}")
    print(f"   Stdout: {e.stdout}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"❌ Error parseando respuesta JSON: {e}")
    print(f"   Output recibido:")
    print(result.stdout)
    sys.exit(1)
except Exception as e:
    print(f"❌ Error inesperado: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)