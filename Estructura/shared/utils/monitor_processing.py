"""
Dashboard de monitoreo del Data Processor
Muestra estadísticas en tiempo real

Monitoriza en tiempo real el estado del procesamiento de mensajes en MongoDB.
Muestra estadísticas actualizadas, barra de progreso, velocidad de procesamiento,
ETA estimado y distribución de tipos de datos agregados.
Ideal para supervisar el avance del sistema y detectar cuellos de botella.
"""

import os
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_stats():
    try:
        result = os.popen(
            f'docker exec {MONGO_CONTAINER} mongosh {MONGO_DATABASE} '
            f'--username {MONGO_USER} --password {MONGO_PASS} --authenticationDatabase admin --quiet --eval "'
            "const data = {"
            "raw_total: db.raw_messages.countDocuments({}),"
            "raw_processed: db.raw_messages.countDocuments({processed: true}),"
            "agg_total: db.aggregated_data.countDocuments({}),"
            "agg_complete: db.aggregated_data.countDocuments({is_complete: true}),"
            "types: db.aggregated_data.aggregate(["
            "{$unwind: '$types_received'},"
            "{$group: {_id: '$types_received', count: {$sum: 1}}},"
            "{$sort: {_id: 1}}"
            "]).toArray()"
            "};"
            "print(JSON.stringify(data));"
            '"'
        ).read().strip()
        return json.loads(result)
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return None

def draw_progress_bar(percentage, width=50):
    filled = int(width * percentage / 100)
    bar = '█' * filled + '░' * (width - filled)
    return f"[{bar}] {percentage:.1f}%"

def format_time(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m {int(seconds%60)}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"

print("🚀 Iniciando monitor de procesamiento...")
print("   Presiona Ctrl+C para detener\n")

previous_stats = None
start_time = time.time()

try:
    while True:
        stats = get_stats()
        if stats is None:
            time.sleep(5)
            continue

        clear_screen()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("=" * 80)
        print(f"{'📊 MONITOR DE PROCESAMIENTO':^80}")
        print(f"{'Actualizado: ' + current_time:^80}")
        print("=" * 80)

        raw_total = stats['raw_total']
        raw_processed = stats['raw_processed']
        raw_pending = raw_total - raw_processed
        raw_percentage = (raw_processed / raw_total * 100) if raw_total > 0 else 0

        print(f"\n📥 RAW MESSAGES:")
        print(f"   Total:      {raw_total:>12,}")
        print(f"   Procesados: {raw_processed:>12,}")
        print(f"   Pendientes: {raw_pending:>12,}")
        print(f"\n   {draw_progress_bar(raw_percentage)}")

        if previous_stats:
            processed_delta = raw_processed - previous_stats['raw_processed']
            speed = processed_delta / 5
            if speed > 0:
                eta_seconds = raw_pending / speed
                print(f"\n   ⚡ Velocidad: {speed:.1f} msg/s")
                print(f"   ⏱️  ETA: {format_time(eta_seconds)}")

        agg_total = stats['agg_total']
        agg_complete = stats['agg_complete']
        agg_percentage = (agg_complete / agg_total * 100) if agg_total > 0 else 0

        print(f"\n👥 DATOS AGREGADOS:")
        print(f"   Total personas:        {agg_total:>12,}")
        print(f"   Registros completos:   {agg_complete:>12,}")
        print(f"   Completitud:           {agg_percentage:>11.2f}%")

        if stats['types']:
            print(f"\n📊 DISTRIBUCIÓN DE TIPOS:")
            type_icons = {
                'bank': '💰',
                'location': '📍',
                'net': '🌐',
                'personal': '👤',
                'professional': '💼'
            }
            for item in stats['types']:
                icon = type_icons.get(item['_id'], '📄')
                count = item['count']
                percentage = (count / agg_total * 100) if agg_total > 0 else 0
                print(f"   {icon} {item['_id']:<12} {count:>12,}  ({percentage:>5.1f}%)")

        elapsed = time.time() - start_time
        print(f"\n{'─' * 80}")
        print(f"   ⏱️  Tiempo de monitoreo: {format_time(elapsed)}")
        print(f"   🔄 Próxima actualización en 5s...")
        print("=" * 80)

        previous_stats = stats
        time.sleep(5)

except KeyboardInterrupt:
    print("\n\n✅ Monitor detenido por el usuario")
    sys.exit(0)
except Exception as e:
    print(f"\n\n❌ Error inesperado: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)