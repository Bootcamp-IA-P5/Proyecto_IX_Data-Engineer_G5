"""
Configuración del servicio data-processor
Lee variables de entorno pasadas por Docker Compose
"""
import os


# ============================================================
# CONFIGURACIÓN DE MONGODB
# ============================================================
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')
AGGREGATED_COLLECTION = os.getenv('AGGREGATED_COLLECTION')

# ============================================================
# CONFIGURACIÓN DE PROCESAMIENTO
# ============================================================
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
PROCESSING_INTERVAL = int(os.getenv('PROCESSING_INTERVAL', '10'))
GROUPING_WINDOW = int(os.getenv('GROUPING_WINDOW', '60'))

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ============================================================
# CONFIGURACIÓN DE PERFORMANCE
# ============================================================
STATS_INTERVAL = int(os.getenv('STATS_INTERVAL', '10'))


def print_config():
    """Imprime la configuración actual (ocultando credenciales)"""
    print("=" * 60)
    print("CONFIGURACIÓN DEL DATA PROCESSOR")
    print("=" * 60)
    
    # Ocultar credenciales en la URI de MongoDB
    if MONGO_URI and '@' in MONGO_URI:
        # Extraer solo host:port/
        mongo_display = MONGO_URI.split('@')[1]
        print(f"MongoDB URI: {mongo_display}")
    else:
        print(f"MongoDB URI: {MONGO_URI}")
    
    print(f"MongoDB Database: {MONGO_DATABASE}")
    print(f"Colección origen: {MONGO_COLLECTION}")
    print(f"Colección destino: {AGGREGATED_COLLECTION}")
    print(f"Batch Size: {BATCH_SIZE}")
    print(f"Processing Interval: {PROCESSING_INTERVAL}s")
    print(f"Grouping Window: {GROUPING_WINDOW}s")
    print(f"Stats Interval: {STATS_INTERVAL}s")
    print(f"Log Level: {LOG_LEVEL}")
    print("=" * 60)