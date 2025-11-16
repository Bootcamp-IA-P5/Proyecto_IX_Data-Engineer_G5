"""
Dashboard de monitoreo del Data Processor
Muestra estadísticas en tiempo real
"""
import os
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from pathlib import Path

# Agregar el directorio raíz al path para imports
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
load_dotenv(ROOT_DIR / '.env')

# Configuración
MONGO_USERNAME = os.getenv('MONGO_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'hrpro_db')

# Probar diferentes URIs
URIS_TO_TRY = [
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/{MONGO_DATABASE}?authSource=admin",
    f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/admin",
    f"mongodb://localhost:27017/",  # Sin auth
]


def try_connect():
    """Intenta conectar con diferentes URIs"""
    for uri in URIS_TO_TRY:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            return client
        except:
            continue
    return None


# Conectar
client = try_connect()
if not client:
    print("❌ No se pudo conectar a MongoDB")
    print("Verifica que MongoDB esté ejecutándose: docker ps | grep mongo")
    sys.exit(1)

db = client[MONGO_DATABASE]


def clear_screen():
    """Limpia la pantalla"""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_stats():
    """Obtiene estadísticas de las colecciones"""
    raw_total = db.raw_messages.count_documents({})
    raw_processed = db.raw_messages.count_documents({'processed': True})
    raw_pending = raw_total - raw_processed
    
    agg_total = db.aggregated_data.count_documents({})
    agg_complete = db.aggregated_data.count_documents({'is_complete': True})
    
    # Distribución de tipos recibidos
    pipeline = [
        {'$unwind': '$types_received'},
        {'$group': {'_id': '$types_received', 'count': {'$sum': 1}}},
        {'$sort': {'_id': 1}}
    ]
    type_dist = {doc['_id']: doc['count'] for doc in db.aggregated_data.aggregate(pipeline)}
    
    # Distribución de completitud
    completeness_dist = {}
    for i in range(1, 6):
        count = db.aggregated_data.count_documents({'types_received': {'$size': i}})
        if count > 0:
            completeness_dist[i] = count
    
    return {
        'raw_total': raw_total,
        'raw_processed': raw_processed,
        'raw_pending': raw_pending,
        'agg_total': agg_total,
        'agg_complete': agg_complete,
        'type_dist': type_dist,
        'completeness_dist': completeness_dist
    }


def format_number(n):
    """Formatea números con separadores"""
    return f"{n:,}"


def calculate_eta(processed, total, elapsed_seconds):
    """Calcula tiempo estimado restante"""
    if processed == 0 or total == 0:
        return "Calculando..."
    
    rate = processed / elapsed_seconds
    remaining = total - processed
    eta_seconds = remaining / rate if rate > 0 else 0
    
    hours = int(eta_seconds // 3600)
    minutes = int((eta_seconds % 3600) // 60)
    seconds = int(eta_seconds % 60)
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def print_dashboard(stats, start_time):
    """Imprime el dashboard"""
    clear_screen()
    
    elapsed = time.time() - start_time
    
    # Header
    print("=" * 80)
    print(" " * 20 + "📊 DATA PROCESSOR - DASHBOARD")
    print("=" * 80)
    print(f"Hora: {datetime.now().strftime('%H:%M:%S')} | Tiempo transcurrido: {int(elapsed)}s")
    print("=" * 80)
    
    # Raw Messages
    print("\n📥 RAW MESSAGES:")
    print("-" * 80)
    total = stats['raw_total']
    processed = stats['raw_processed']
    pending = stats['raw_pending']
    progress_pct = (processed / total * 100) if total > 0 else 0
    
    print(f"  Total:      {format_number(total)}")
    print(f"  Procesados: {format_number(processed)} ({progress_pct:.2f}%)")
    print(f"  Pendientes: {format_number(pending)}")
    
    if total > 0:
        # Barra de progreso
        bar_width = 50
        filled = int(bar_width * processed / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"\n  [{bar}] {progress_pct:.1f}%")
        
        # ETA
        eta = calculate_eta(processed, total, elapsed)
        rate = processed / elapsed if elapsed > 0 else 0
        print(f"\n  Velocidad: {format_number(int(rate))} msg/s")
        print(f"  ETA: {eta}")
    else:
        print("\n  ⚠️  No hay mensajes en la base de datos")
    
    # Aggregated Data
    print("\n\n👥 DATOS AGREGADOS:")
    print("-" * 80)
    print(f"  Total personas: {format_number(stats['agg_total'])}")
    print(f"  Completos (5 tipos): {format_number(stats['agg_complete'])}")
    
    if stats['agg_total'] > 0:
        complete_pct = (stats['agg_complete'] / stats['agg_total']) * 100
        print(f"  Completitud: {complete_pct:.2f}%")
    
    # Distribución de tipos
    if stats['type_dist']:
        print("\n  Distribución de tipos recibidos:")
        type_names = {
            'bank': '💰 Bank',
            'location': '📍 Location',
            'net': '🌐 Net',
            'personal': '👤 Personal',
            'professional': '💼 Professional'
        }
        
        for type_key, count in sorted(stats['type_dist'].items()):
            type_label = type_names.get(type_key, type_key)
            print(f"    {type_label}: {format_number(count)}")
    
    # Distribución de completitud
    if stats['completeness_dist']:
        print("\n  Distribución de completitud:")
        for num_types, count in sorted(stats['completeness_dist'].items()):
            print(f"    {num_types} tipo(s): {format_number(count)} personas")
    
    print("\n" + "=" * 80)
    print("Presiona Ctrl+C para salir")
    print("=" * 80)


def main():
    """Loop principal del dashboard"""
    print("🚀 Iniciando dashboard de monitoreo...")
    print(f"📁 Directorio raíz: {ROOT_DIR}")
    print("✅ Conectado a MongoDB")
    print("Esperando 3 segundos...")
    time.sleep(3)
    
    start_time = time.time()
    
    try:
        while True:
            stats = get_stats()
            print_dashboard(stats, start_time)
            time.sleep(5)  # Actualizar cada 5 segundos
            
    except KeyboardInterrupt:
        print("\n\n🛑 Dashboard detenido")
        client.close()


if __name__ == "__main__":
    main()