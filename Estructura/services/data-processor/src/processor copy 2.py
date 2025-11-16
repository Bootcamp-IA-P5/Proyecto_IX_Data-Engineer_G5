"""
Procesador de datos - Lee mensajes raw y los clasifica
"""
import time
import logging
from datetime import datetime
from . import config
from .mongo_client import get_mongo_client

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


def detect_message_type(message):
    """Detectar tipo de mensaje basándose en campos presentes"""
    fields = set(message.keys())
    
    # Personal: address, IPv4
    if 'address' in fields or 'IPv4' in fields:
        return 'personal'
    
    # Financial: passport, IBAN, salary
    if 'passport' in fields or 'IBAN' in fields or 'salary' in fields:
        return 'financial'
    
    # Employment: fullname, company, job
    if 'fullname' in fields or 'company' in fields or 'job' in fields:
        return 'employment'
    
    return 'unknown'


def process_personal(message):
    """Procesar datos personales"""
    return {
        '_id': message['_id'],
        'address': message.get('address'),
        'ipv4': message.get('IPv4'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }


def process_financial(message):
    """Procesar datos financieros"""
    return {
        '_id': message['_id'],
        'passport': message.get('passport'),
        'iban': message.get('IBAN'),
        'salary': message.get('salary'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }


def process_employment(message):
    """Procesar datos laborales"""
    return {
        '_id': message['_id'],
        'fullname': message.get('fullname'),
        'company': message.get('company'),
        'company_address': message.get('company address'),
        'company_phone': message.get('company_telfnumber'),
        'company_email': message.get('company_email'),
        'job': message.get('job'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }


def process_batch(mongo_client):
    """
    Procesar un lote de mensajes
    
    Args:
        mongo_client: Instancia de MongoDBClient
        
    Returns:
        Cantidad de mensajes procesados
    """
    # Obtener mensajes sin procesar
    messages = mongo_client.get_unprocessed_documents(config.BATCH_SIZE)
    
    if not messages:
        return 0
    
    stats = {'personal': 0, 'financial': 0, 'employment': 0, 'unknown': 0}
    processed_ids = []
    
    for msg in messages:
        msg_type = detect_message_type(msg)
        
        try:
            if msg_type == 'personal':
                processed = process_personal(msg)
                if mongo_client.insert_personal(processed):
                    stats['personal'] += 1
                    processed_ids.append(msg['_id'])
                
            elif msg_type == 'financial':
                processed = process_financial(msg)
                if mongo_client.insert_financial(processed):
                    stats['financial'] += 1
                    processed_ids.append(msg['_id'])
                
            elif msg_type == 'employment':
                processed = process_employment(msg)
                if mongo_client.insert_employment(processed):
                    stats['employment'] += 1
                    processed_ids.append(msg['_id'])
                
            else:
                stats['unknown'] += 1
                logger.warning(f"⚠️  Mensaje desconocido: {msg['_id']} - Campos: {list(msg.keys())}")
                # También marcar los unknown como procesados
                processed_ids.append(msg['_id'])
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje {msg['_id']}: {e}")
    
    # Marcar mensajes como procesados
    if processed_ids:
        mongo_client.mark_as_processed(processed_ids)
    
    logger.info(f"✅ Procesados {len(messages)} mensajes: "
                f"Personal={stats['personal']}, "
                f"Financial={stats['financial']}, "
                f"Employment={stats['employment']}, "
                f"Unknown={stats['unknown']}")
    
    return len(messages)


def print_stats(mongo_client):
    """Imprime estadísticas de las colecciones"""
    stats = mongo_client.get_stats()
    
    logger.info("=" * 60)
    logger.info("📊 ESTADÍSTICAS DE PROCESAMIENTO")
    logger.info("=" * 60)
    logger.info(f"Raw Messages:")
    logger.info(f"  - Total: {stats.get('raw_total', 0)}")
    logger.info(f"  - Procesados: {stats.get('raw_processed', 0)}")
    logger.info(f"  - Pendientes: {stats.get('raw_pending', 0)}")
    logger.info(f"Datos Clasificados:")
    logger.info(f"  - Personal: {stats.get('personal_total', 0)}")
    logger.info(f"  - Financial: {stats.get('financial_total', 0)}")
    logger.info(f"  - Employment: {stats.get('employment_total', 0)}")
    logger.info("=" * 60)


def main():
    """Loop principal del procesador"""
    logger.info("🚀 Iniciando Data Processor...")
    config.print_config()
    
    # Obtener cliente MongoDB
    mongo_client = get_mongo_client()
    
    # Contador para mostrar estadísticas periódicamente
    batch_count = 0
    
    while True:
        try:
            processed = process_batch(mongo_client)
            
            if processed == 0:
                logger.info(f"⏳ No hay mensajes. Esperando {config.POLL_INTERVAL}s...")
                time.sleep(config.POLL_INTERVAL)
            else:
                batch_count += 1
                time.sleep(1)
                
                # Mostrar estadísticas cada STATS_INTERVAL batches
                if batch_count % config.STATS_INTERVAL == 0:
                    print_stats(mongo_client)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Deteniendo procesador...")
            print_stats(mongo_client)
            mongo_client.close()
            break
            
        except Exception as e:
            logger.error(f"❌ Error en loop principal: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()