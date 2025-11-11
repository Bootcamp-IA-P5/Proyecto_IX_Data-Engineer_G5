"""
Servicio de Kafka - Productor y Consumidor
Maneja todas las interacciones con Kafka
"""
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import json
import logging
from typing import Dict, List, Optional, Callable
import asyncio
from datetime import datetime

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class KafkaService:
    """Servicio para gestionar Kafka Producer y Consumer"""
    
    def __init__(self):
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None
        self.admin_client: Optional[AdminClient] = None
        self.is_initialized = False
        
        # Configuración del producer
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'hr-pro-api-producer',
            'acks': 'all',  # Esperar confirmación de todos los brokers
            'retries': 3,
            'compression.type': 'snappy',
        }
        
        # Configuración del consumer
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': settings.kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 300000
        }
    
    async def initialize(self):
        """Inicializar el servicio de Kafka"""
        try:
            logger.info("📡 Inicializando Kafka Service...")
            
            # Crear producer
            self.producer = Producer(self.producer_config)
            logger.info("✅ Kafka Producer creado")
            
            # Crear admin client
            self.admin_client = AdminClient({
                'bootstrap.servers': settings.kafka_bootstrap_servers
            })
            logger.info("✅ Kafka Admin Client creado")
            
            # Verificar conexión
            metadata = self.admin_client.list_topics(timeout=5)
            logger.info(f"✅ Conectado a Kafka. Topics disponibles: {len(metadata.topics)}")
            
            # Crear topics si no existen
            await self._create_topics_if_not_exist()
            
            self.is_initialized = True
            logger.info("✅ Kafka Service inicializado correctamente")
            
        except Exception as e:
            logger.error(f"❌ Error inicializando Kafka Service: {e}")
            raise
    
    async def _create_topics_if_not_exist(self):
        """Crear topics necesarios si no existen"""
        topics_to_create = [
            settings.kafka_topic_user_tracker,
            settings.kafka_topic_personal,
            settings.kafka_topic_location,
            settings.kafka_topic_professional,
            settings.kafka_topic_bank,
            settings.kafka_topic_net,
        ]
        
        existing_topics = self.admin_client.list_topics().topics.keys()
        
        new_topics = [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in topics_to_create
            if topic not in existing_topics
        ]
        
        if new_topics:
            fs = self.admin_client.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"✅ Topic creado: {topic}")
                except Exception as e:
                    logger.warning(f"⚠️  Error creando topic {topic}: {e}")
    
    def delivery_callback(self, err, msg):
        """Callback para confirmar entrega de mensajes"""
        if err:
            logger.error(f"❌ Error enviando mensaje: {err}")
        else:
            logger.debug(
                f"✅ Mensaje enviado a {msg.topic()} "
                f"[partition: {msg.partition()}, offset: {msg.offset()}]"
            )
    
    async def produce_message(
        self,
        topic: str,
        value: Dict,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Enviar un mensaje a Kafka
        
        Args:
            topic: Nombre del topic
            value: Datos a enviar (serán convertidos a JSON)
            key: Clave del mensaje (opcional)
            headers: Headers del mensaje (opcional)
        
        Returns:
            Dict con información del envío
        """
        try:
            # Agregar metadata
            value['_metadata'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'hr-pro-api',
                'version': settings.app_version
            }
            
            # Serializar a JSON
            message_value = json.dumps(value).encode('utf-8')
            message_key = key.encode('utf-8') if key else None
            
            # Convertir headers a formato Kafka
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            # Variable para capturar resultado
            result = {'success': False, 'partition': None, 'offset': None}
            
            def custom_callback(err, msg):
                if err:
                    logger.error(f"❌ Error enviando mensaje: {err}")
                    result['error'] = str(err)
                else:
                    result['success'] = True
                    result['partition'] = msg.partition()
                    result['offset'] = msg.offset()
                    logger.debug(
                        f"✅ Mensaje enviado a {msg.topic()} "
                        f"[partition: {msg.partition()}, offset: {msg.offset()}]"
                    )
            
            # Producir mensaje
            self.producer.produce(
                topic=topic,
                value=message_value,
                key=message_key,
                headers=kafka_headers,
                callback=custom_callback
            )
            
            # Flush para asegurar envío
            self.producer.flush(timeout=5)
            
            logger.info(f"📤 Mensaje enviado a topic: {topic}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error produciendo mensaje: {e}")
            return {'success': False, 'error': str(e)}
    
    async def produce_batch(
        self,
        messages: List[Dict]
    ) -> List[Dict]:
        """
        Enviar múltiples mensajes a Kafka
        
        Args:
            messages: Lista de mensajes con formato {topic, key, value, headers}
        
        Returns:
            Lista de resultados para cada mensaje
        """
        results = []
        
        for msg_data in messages:
            result = await self.produce_message(
                topic=msg_data.get('topic'),
                value=msg_data.get('value'),
                key=msg_data.get('key'),
                headers=msg_data.get('headers')
            )
            results.append(result)
        
        # Contar estadísticas
        success_count = sum(1 for r in results if r.get('success'))
        failed_count = len(results) - success_count
        
        logger.info(
            f"📊 Batch enviado: {success_count} exitosos, "
            f"{failed_count} fallidos"
        )
        
        return results
    
    def create_consumer(self, topics: List[str]) -> Consumer:
        """
        Crear un consumer para los topics especificados
        
        Args:
            topics: Lista de topics a consumir
        
        Returns:
            Consumer configurado
        """
        consumer = Consumer(self.consumer_config)
        consumer.subscribe(topics)
        logger.info(f"✅ Consumer creado para topics: {topics}")
        return consumer
    
    async def consume_messages(
        self,
        topics: List[str],
        callback: Callable,
        max_messages: int = 100,
        timeout: float = 1.0
    ) -> Dict[str, int]:
        """
        Consumir mensajes de Kafka
        
        Args:
            topics: Lista de topics a consumir
            callback: Función a llamar por cada mensaje
            max_messages: Número máximo de mensajes a consumir
            timeout: Timeout para poll
        
        Returns:
            Estadísticas de consumo
        """
        consumer = self.create_consumer(topics)
        stats = {'consumed': 0, 'errors': 0}
        
        try:
            for _ in range(max_messages):
                msg = consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"❌ Error en consumer: {msg.error()}")
                        stats['errors'] += 1
                        continue
                
                try:
                    # Decodificar mensaje
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    # Llamar al callback
                    await callback(value, msg.topic())
                    
                    # Commit manual
                    consumer.commit(asynchronous=False)
                    stats['consumed'] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando mensaje: {e}")
                    stats['errors'] += 1
            
        finally:
            consumer.close()
            logger.info(
                f"📊 Consumo finalizado: {stats['consumed']} mensajes, "
                f"{stats['errors']} errores"
            )
        
        return stats
    
    async def consume_messages(
        self,
        topic: str,
        max_messages: int = 10,
        timeout: float = 5.0
    ) -> List[Dict]:
        """
        Consumir mensajes de un topic específico
        
        Args:
            topic: Nombre del topic
            max_messages: Número máximo de mensajes a consumir
            timeout: Timeout en segundos
        
        Returns:
            Lista de mensajes consumidos
        """
        consumer_config = self.consumer_config.copy()
        consumer_config['group.id'] = f"{consumer_config['group.id']}-temp-{datetime.utcnow().timestamp()}"
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        messages = []
        start_time = datetime.utcnow()
        
        try:
            while len(messages) < max_messages:
                # Verificar timeout
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed >= timeout:
                    break
                
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"❌ Error en consumer: {msg.error()}")
                        continue
                
                try:
                    # Decodificar mensaje
                    value = json.loads(msg.value().decode('utf-8'))
                    key = msg.key().decode('utf-8') if msg.key() else None
                    
                    # Extraer headers
                    headers = {}
                    if msg.headers():
                        headers = {k: v.decode('utf-8') for k, v in msg.headers()}
                    
                    messages.append({
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': key,
                        'value': value,
                        'timestamp': datetime.fromtimestamp(msg.timestamp()[1] / 1000),
                        'headers': headers
                    })
                    
                except Exception as e:
                    logger.error(f"❌ Error decodificando mensaje: {e}")
            
        finally:
            consumer.close()
            logger.info(f"📥 Consumidos {len(messages)} mensajes del topic {topic}")
        
        return messages
    
    async def list_topics(self) -> List[str]:
        """
        Listar todos los topics disponibles
        
        Returns:
            Lista de nombres de topics
        """
        return await self.list_all_topics()
    
    async def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Crear un nuevo topic
        
        Args:
            topic_name: Nombre del topic
            num_partitions: Número de particiones
            replication_factor: Factor de replicación
            config: Configuración adicional
        
        Returns:
            True si se creó exitosamente
        """
        try:
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=config or {}
            )
            
            fs = self.admin_client.create_topics([new_topic])
            
            # Esperar a que se complete
            for topic, f in fs.items():
                f.result()
                logger.info(f"✅ Topic creado: {topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando topic {topic_name}: {e}")
            return False
    
    async def delete_topic(self, topic_name: str) -> bool:
        """
        Eliminar un topic
        
        Args:
            topic_name: Nombre del topic a eliminar
        
        Returns:
            True si se eliminó exitosamente
        """
        try:
            fs = self.admin_client.delete_topics([topic_name])
            
            # Esperar a que se complete
            for topic, f in fs.items():
                f.result()
                logger.info(f"✅ Topic eliminado: {topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error eliminando topic {topic_name}: {e}")
            return False
    
    async def get_topic_info(self, topic: str) -> Optional[Dict]:
        """
        Obtener información de un topic
        
        Args:
            topic: Nombre del topic
        
        Returns:
            Dict con información del topic o None si no existe
        """
        try:
            metadata = self.admin_client.list_topics(topic=topic, timeout=5)
            
            if topic not in metadata.topics:
                return None
            
            topic_metadata = metadata.topics[topic]
            
            return {
                'name': topic,
                'partitions': len(topic_metadata.partitions),
                'partition_info': [
                    {
                        'id': p_id,
                        'leader': p_meta.leader,
                        'replicas': p_meta.replicas,
                        'isrs': p_meta.isrs
                    }
                    for p_id, p_meta in topic_metadata.partitions.items()
                ]
            }
        
        except Exception as e:
            logger.error(f"❌ Error obteniendo info del topic {topic}: {e}")
            return None
    
    async def list_all_topics(self) -> List[str]:
        """
        Listar todos los topics disponibles
        
        Returns:
            Lista de nombres de topics
        """
        try:
            metadata = self.admin_client.list_topics(timeout=5)
            topics = list(metadata.topics.keys())
            # Filtrar topics internos de Kafka
            topics = [t for t in topics if not t.startswith('__')]
            return topics
        except Exception as e:
            logger.error(f"❌ Error listando topics: {e}")
            return []
    
    async def close(self):
        """Cerrar conexiones de Kafka"""
        logger.info("🛑 Cerrando Kafka Service...")
        
        if self.producer:
            self.producer.flush(timeout=10)
            logger.info("✅ Producer cerrado")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Consumer cerrado")
        
        self.is_initialized = False
        logger.info("✅ Kafka Service cerrado correctamente")


# Variable global para el servicio de Kafka
_kafka_service: Optional[KafkaService] = None


def set_kafka_service(service: KafkaService):
    """Establecer la instancia global del servicio de Kafka"""
    global _kafka_service
    _kafka_service = service


def get_kafka_service() -> KafkaService:
    """
    Dependency para obtener el servicio de Kafka en FastAPI
    
    Returns:
        Instancia del servicio de Kafka
        
    Raises:
        RuntimeError: Si el servicio no está inicializado
    """
    if _kafka_service is None:
        raise RuntimeError("Kafka service no inicializado")
    return _kafka_service
