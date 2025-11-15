"""
Mongo Persister - Servicio principal
Lee mensajes de Kafka y los guarda en MongoDB SIN procesarlos
"""
import logging
import json
import signal
import sys
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from typing import Dict, List
import config
from mongo_client import get_mongo_client

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

# Control de cierre graceful
shutdown_requested = False


def signal_handler(sig, frame):
    """Maneja señales de cierre (Ctrl+C, SIGTERM)"""
    global shutdown_requested
    logger.info("🛑 Señal de cierre recibida. Finalizando...")
    shutdown_requested = True


class MongoPersister:
    """Servicio que persiste mensajes de Kafka en MongoDB"""
    
    def __init__(self):
        """Inicializa el persister"""
        self.consumer = None
        self.mongo_client = None
        self.stats = {
            'total_consumed': 0,
            'total_inserted': 0,
            'errors': 0,
            'start_time': time.time(),
            'topic_warning_shown': False  # ✅ Flag para warning inicial
        }
        self.message_buffer: List[Dict] = []
        
    def setup(self):
        """Configura conexiones a Kafka y MongoDB"""
        logger.info("🚀 Iniciando Mongo Persister")
        config.print_config()
        
        # Conectar a MongoDB
        logger.info("📦 Conectando a MongoDB...")
        self.mongo_client = get_mongo_client()
        
        # Configurar consumer de Kafka
        logger.info("📨 Configurando consumer de Kafka...")
        consumer_config = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': config.KAFKA_GROUP_ID,
            'auto.offset.reset': config.KAFKA_AUTO_OFFSET_RESET,
            'enable.auto.commit': False,  # Control manual de commits
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 300000
        }
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([config.KAFKA_TOPIC])
        
        logger.info(f"✅ Suscrito al topic: {config.KAFKA_TOPIC}")
        logger.info("✅ Mongo Persister listo. Esperando mensajes...")
    
    def process_message(self, kafka_message) -> Dict:
        """
        Procesa un mensaje de Kafka
        
        Args:
            kafka_message: Mensaje de Kafka
            
        Returns:
            Dict con el mensaje parseado
        """
        try:
            # Parsear JSON
            message_value = kafka_message.value().decode('utf-8')
            data = json.loads(message_value)
            
            # Agregar metadata de Kafka
            data['_kafka_metadata'] = {
                'topic': kafka_message.topic(),
                'partition': kafka_message.partition(),
                'offset': kafka_message.offset(),
                'timestamp': kafka_message.timestamp()[1] if kafka_message.timestamp()[0] > 0 else None
            }
            
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando JSON: {e}")
            self.stats['errors'] += 1
            return None
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje: {e}")
            self.stats['errors'] += 1
            return None
    
    def flush_buffer(self):
        """Inserta los mensajes del buffer en MongoDB"""
        if not self.message_buffer:
            return
        
        try:
            expected = len(self.message_buffer)
            inserted = self.mongo_client.insert_batch(self.message_buffer)
            self.stats['total_inserted'] += inserted
            
            # Solo mostrar warning si realmente hay un problema
            # (no si son duplicados esperados por idempotencia)
            if inserted < expected and inserted > 0:
                logger.debug(f"⏭️  {expected - inserted} mensajes ya existían (duplicados)")
            elif inserted == 0 and expected > 0:
                logger.debug(f"⏭️  Todos los {expected} mensajes ya existían (duplicados)")
            
            self.message_buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ Error en flush_buffer: {e}")
            self.stats['errors'] += 1
            self.message_buffer.clear()
    
    def print_stats(self):
        """Muestra estadísticas del persister"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_consumed'] / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 60)
        logger.info("📊 ESTADÍSTICAS DEL MONGO PERSISTER")
        logger.info("=" * 60)
        logger.info(f"Total consumidos: {self.stats['total_consumed']:,}")
        logger.info(f"Total insertados: {self.stats['total_inserted']:,}")
        logger.info(f"Errores: {self.stats['errors']}")
        logger.info(f"Rate: {rate:.2f} mensajes/segundo")
        logger.info(f"Tiempo: {elapsed:.2f} segundos")
        
        # Estadísticas de MongoDB
        mongo_stats = self.mongo_client.get_stats()
        logger.info(f"Documentos en MongoDB: {mongo_stats['total_documents']:,}")
        logger.info("=" * 60)
    
    def run(self):
        """Loop principal del persister"""
        try:
            self.setup()
            
            last_stats_time = time.time()
            
            while not shutdown_requested:
                # Poll por mensajes
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No hay mensajes, flush buffer si tiene datos
                    if self.message_buffer:
                        self.flush_buffer()
                        self.consumer.commit(asynchronous=False)
                    continue
                
                # ✅ MANEJO MEJORADO DE ERRORES
                if msg.error():
                    # Manejo especial para topic no disponible
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Solo mostrar warning una vez al inicio
                        if not self.stats['topic_warning_shown']:
                            logger.warning(
                                f"⏳ Topic '{config.KAFKA_TOPIC}' aún no disponible. "
                                f"Esperando a que el generador de datos lo cree..."
                            )
                            self.stats['topic_warning_shown'] = True
                        continue  # ✅ No contar como error
                    
                    # Fin de partición (normal)
                    elif msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Fin de partición alcanzado: {msg.partition()}")
                        continue
                    
                    # Otros errores sí se reportan
                    else:
                        logger.error(f"❌ Error en Kafka: {msg.error()}")
                        self.stats['errors'] += 1
                        continue
                
                # ✅ Notificar cuando el topic está disponible
                if self.stats['total_consumed'] == 0 and self.stats['topic_warning_shown']:
                    logger.info(f"✅ Topic '{config.KAFKA_TOPIC}' ahora disponible. Procesando mensajes...")
                
                # Procesar mensaje
                self.stats['total_consumed'] += 1
                processed_message = self.process_message(msg)
                
                if processed_message:
                    self.message_buffer.append(processed_message)
                
                # Flush buffer si alcanzó el tamaño configurado
                if len(self.message_buffer) >= config.MONGO_BATCH_SIZE:
                    self.flush_buffer()
                    # Commit después de insertar
                    self.consumer.commit(asynchronous=False)
                
                # Mostrar estadísticas periódicamente
                if time.time() - last_stats_time >= config.STATS_INTERVAL:
                    self.print_stats()
                    last_stats_time = time.time()
            
            # Flush y commit final antes de cerrar
            logger.info("🔄 Flush final de mensajes...")
            self.flush_buffer()
            self.consumer.commit(asynchronous=False)
            self.print_stats()
            
        except KeyboardInterrupt:
            logger.info("⌨️  Interrupción por teclado")
        except Exception as e:
            logger.error(f"❌ Error en run(): {e}", exc_info=True)
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos antes de cerrar"""
        logger.info("🧹 Limpiando recursos...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Consumer de Kafka cerrado")
        
        if self.mongo_client:
            self.mongo_client.close()
        
        logger.info("👋 Mongo Persister finalizado")


def main():
    """Punto de entrada principal"""
    # Registrar handlers de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Crear y ejecutar persister
    persister = MongoPersister()
    persister.run()


if __name__ == "__main__":
    main()