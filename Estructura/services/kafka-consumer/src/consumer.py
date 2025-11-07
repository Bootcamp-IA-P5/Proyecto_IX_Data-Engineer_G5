"""
Kafka Consumer para el proyecto HR Pro
Consume mensajes en tiempo real del topic de Kafka y los procesa.
"""

from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import sys
import logging
import os
from dotenv import load_dotenv
from models import (
    PersonalData, Location, ProfessionalData, 
    BankData, NetData, identify_message_type
)

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración del consumer
consumer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),  # Dirección del broker Kafka
    'group.id': os.getenv('KAFKA_GROUP_ID', 'hr-pro-consumer-group'),    # ID del grupo de consumidores
    'auto.offset.reset': 'earliest',         # Leer desde el principio si no hay offset
    'enable.auto.commit': False,             # Control manual de commits
    'session.timeout.ms': 6000,              # Timeout de sesión
    'max.poll.interval.ms': 300000           # Intervalo máximo entre polls
}

# Nombre del topic a consumir
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'user-tracker')

# Estadísticas
stats = {
    'total_messages': 0,
    'personal': 0,
    'location': 0,
    'professional': 0,
    'bank': 0,
    'net': 0,
    'unknown': 0,
    'errors': 0
}


def parse_message(message_value: str) -> dict:
    """
    Parsea el mensaje JSON de Kafka.
    
    Args:
        message_value: Valor del mensaje en formato string
        
    Returns:
        dict: Mensaje parseado
        
    Raises:
        json.JSONDecodeError: Si el JSON es inválido
    """
    try:
        return json.loads(message_value)
    except json.JSONDecodeError as e:
        logger.error(f"Error parseando JSON: {e}")
        raise


def process_message(data: dict) -> None:
    """
    Procesa un mensaje identificando su tipo y mostrándolo.
    
    Args:
        data: Diccionario con los datos del mensaje
    """
    message_type = identify_message_type(data)
    
    if message_type == 'personal':
        personal_data = PersonalData.from_dict(data)
        stats['personal'] += 1
        logger.info(f"📋 PERSONAL DATA: {personal_data.name} {personal_data.last_name} | Passport: {personal_data.passport}")
        
    elif message_type == 'location':
        location = Location.from_dict(data)
        stats['location'] += 1
        logger.info(f"📍 LOCATION: {location.fullname} | City: {location.city}")
        
    elif message_type == 'professional':
        prof_data = ProfessionalData.from_dict(data)
        stats['professional'] += 1
        logger.info(f"💼 PROFESSIONAL: {prof_data.fullname} | Company: {prof_data.company}")
        
    elif message_type == 'bank':
        bank_data = BankData.from_dict(data)
        stats['bank'] += 1
        logger.info(f"💰 BANK DATA: Passport: {bank_data.passport} | IBAN: {bank_data.IBAN[:10]}...")
        
    elif message_type == 'net':
        net_data = NetData.from_dict(data)
        stats['net'] += 1
        logger.info(f"🌐 NET DATA: {net_data.IPv4} | Address: {net_data.address[:30]}...")
        
    else:
        stats['unknown'] += 1
        logger.warning(f"❓ UNKNOWN MESSAGE TYPE: {list(data.keys())}")


def print_stats():
    """Imprime las estadísticas de mensajes procesados"""
    logger.info("=" * 60)
    logger.info("📊 ESTADÍSTICAS DE CONSUMO")
    logger.info(f"Total mensajes: {stats['total_messages']}")
    logger.info(f"  📋 Personal Data: {stats['personal']}")
    logger.info(f"  📍 Location: {stats['location']}")
    logger.info(f"  💼 Professional: {stats['professional']}")
    logger.info(f"  💰 Bank Data: {stats['bank']}")
    logger.info(f"  🌐 Net Data: {stats['net']}")
    logger.info(f"  ❓ Unknown: {stats['unknown']}")
    logger.info(f"  ❌ Errors: {stats['errors']}")
    logger.info("=" * 60)


def consume_messages():
    """
    Función principal que consume mensajes de Kafka en tiempo real.
    """
    # Crear instancia del consumer
    consumer = Consumer(consumer_config)
    
    try:
        # Suscribirse al topic
        consumer.subscribe([TOPIC_NAME])
        logger.info(f"🚀 Consumer iniciado. Escuchando topic: {TOPIC_NAME}")
        logger.info(f"📡 Broker: {consumer_config['bootstrap.servers']}")
        logger.info(f"👥 Group ID: {consumer_config['group.id']}")
        logger.info("-" * 60)
        
        message_count = 0
        
        # Loop infinito para consumir mensajes
        while True:
            # Poll para obtener mensajes (timeout de 1 segundo)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No hay mensajes, continuar
                continue
                
            if msg.error():
                # Verificar si es un error o solo fin de partición
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Fin de partición alcanzado: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    stats['errors'] += 1
                    logger.error(f"Error en consumer: {msg.error()}")
                continue
            
            try:
                # Decodificar el mensaje
                message_value = msg.value().decode('utf-8')
                
                # Parsear JSON
                data = parse_message(message_value)
                
                # Procesar mensaje
                process_message(data)
                
                # Incrementar contadores
                stats['total_messages'] += 1
                message_count += 1
                
                # Commit manual del offset
                consumer.commit(asynchronous=False)
                
                # Mostrar estadísticas cada 100 mensajes
                if message_count % 100 == 0:
                    print_stats()
                    
            except json.JSONDecodeError:
                stats['errors'] += 1
                logger.error(f"Error parseando mensaje en offset {msg.offset()}")
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error procesando mensaje: {e}")
                
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupción recibida. Cerrando consumer...")
        print_stats()
        
    except Exception as e:
        logger.error(f"Error fatal en consumer: {e}")
        raise
        
    finally:
        # Cerrar el consumer
        consumer.close()
        logger.info("✅ Consumer cerrado correctamente")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🎯 KAFKA CONSUMER - HR PRO PROJECT")
    logger.info("=" * 60)
    consume_messages()

