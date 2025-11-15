"""
Configuración del servicio mongo-persister
Lee variables de entorno pasadas por Docker Compose
"""
import os

# ============================================================
# CONFIGURACIÓN DE KAFKA
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET')

# ============================================================
# CONFIGURACIÓN DE MONGODB
# ============================================================
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DATABASE')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

# Validación de variables de entorno críticas
def validate_env_vars():
    missing = []
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append('KAFKA_BOOTSTRAP_SERVERS')
    if not KAFKA_TOPIC:
        missing.append('KAFKA_TOPIC')
    if not MONGO_URI:
        missing.append('MONGO_URI')
    if not MONGO_DB:
        missing.append('MONGO_DATABASE')
    if not MONGO_COLLECTION:
        missing.append('MONGO_COLLECTION')
    if missing:
        raise RuntimeError(
            f"Faltan las siguientes variables de entorno requeridas: {', '.join(missing)}"
        )

validate_env_vars()
# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ============================================================
# CONFIGURACIÓN DE PERFORMANCE
# ============================================================
MONGO_BATCH_SIZE = int(os.getenv('MONGO_BATCH_SIZE', '100'))
STATS_INTERVAL = int(os.getenv('STATS_INTERVAL', '10'))


def print_config():
    """Imprime la configuración actual (ocultando credenciales)"""
    print("=" * 60)
    print("CONFIGURACIÓN DEL MONGO PERSISTER")
    print("=" * 60)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Kafka Group ID: {KAFKA_GROUP_ID}")
    print(f"Kafka Auto Offset Reset: {KAFKA_AUTO_OFFSET_RESET}")
    
    # Ocultar credenciales en la URI de MongoDB
    if MONGO_URI and '@' in MONGO_URI:
        # Extraer solo host:port/
        mongo_display = MONGO_URI.split('@')[1]
        print(f"MongoDB URI: {mongo_display}")
    else:
        print(f"MongoDB URI: {MONGO_URI}")
    
    print(f"MongoDB Database: {MONGO_DB}")
    print(f"MongoDB Collection: {MONGO_COLLECTION}")
    print(f"Batch Size: {MONGO_BATCH_SIZE}")
    print(f"Log Level: {LOG_LEVEL}")
    print("=" * 60)