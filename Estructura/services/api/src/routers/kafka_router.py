"""
Kafka Router
Endpoints para interactuar con Apache Kafka
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any
import logging
from datetime import datetime

from ..services.kafka_service import KafkaService, get_kafka_service
from ..schemas.kafka_schemas import (
    KafkaMessage,
    KafkaMessageResponse,
    KafkaBatchMessages,
    KafkaTopicCreate,
    KafkaTopicInfo,
    TopicsList,
    ConsumeMessagesRequest,
    ConsumedMessage
)

# Configurar logger
logger = logging.getLogger(__name__)

# Crear router
router = APIRouter(
    prefix="/kafka",
    tags=["Kafka"],
    responses={404: {"description": "Not found"}},
)


@router.post(
    "/produce",
    response_model=KafkaMessageResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Enviar mensaje a Kafka",
    description="Envía un mensaje individual a un topic de Kafka"
)
async def produce_message(
    message: KafkaMessage,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> KafkaMessageResponse:
    """
    Envía un mensaje a Kafka
    
    Args:
        message: Mensaje a enviar (topic, key, value, headers)
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Respuesta con información del mensaje enviado
        
    Raises:
        HTTPException: Si hay un error al enviar el mensaje
    """
    try:
        logger.info(f"Enviando mensaje al topic: {message.topic}")
        
        result = await kafka_service.produce_message(
            topic=message.topic,
            key=message.key,
            value=message.value,
            headers=message.headers
        )
        
        return KafkaMessageResponse(
            success=True,
            topic=message.topic,
            partition=result.get('partition'),
            offset=result.get('offset'),
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error al enviar mensaje: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al enviar mensaje: {str(e)}"
        )


@router.post(
    "/produce/batch",
    response_model=List[KafkaMessageResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Enviar múltiples mensajes a Kafka",
    description="Envía múltiples mensajes en batch a Kafka"
)
async def produce_batch_messages(
    batch: KafkaBatchMessages,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> List[KafkaMessageResponse]:
    """
    Envía múltiples mensajes a Kafka en batch
    
    Args:
        batch: Objeto con lista de mensajes
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Lista de respuestas para cada mensaje
        
    Raises:
        HTTPException: Si hay un error al enviar los mensajes
    """
    try:
        logger.info(f"Enviando batch de {len(batch.messages)} mensajes")
        
        # Preparar mensajes para batch
        messages_data = [
            {
                'topic': msg.topic,
                'key': msg.key,
                'value': msg.value,
                'headers': msg.headers
            }
            for msg in batch.messages
        ]
        
        results = await kafka_service.produce_batch(messages_data)
        
        responses = []
        for msg, result in zip(batch.messages, results):
            responses.append(KafkaMessageResponse(
                success=result.get('success', False),
                topic=msg.topic,
                partition=result.get('partition'),
                offset=result.get('offset'),
                timestamp=datetime.utcnow(),
                error=result.get('error')
            ))
        
        return responses
        
    except Exception as e:
        logger.error(f"Error al enviar batch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al enviar batch: {str(e)}"
        )


@router.post(
    "/consume",
    response_model=List[ConsumedMessage],
    status_code=status.HTTP_200_OK,
    summary="Consumir mensajes de Kafka",
    description="Consume mensajes de un topic de Kafka"
)
async def consume_messages(
    request: ConsumeMessagesRequest,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> List[ConsumedMessage]:
    """
    Consume mensajes de un topic
    
    Args:
        request: Configuración de consumo (topic, max_messages, timeout)
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Lista de mensajes consumidos
        
    Raises:
        HTTPException: Si hay un error al consumir mensajes
    """
    try:
        logger.info(f"Consumiendo mensajes del topic: {request.topic}")
        
        messages = await kafka_service.consume_messages(
            topic=request.topic,
            max_messages=request.max_messages,
            timeout=request.timeout
        )
        
        consumed = []
        for msg in messages:
            consumed.append(ConsumedMessage(
                topic=msg['topic'],
                partition=msg['partition'],
                offset=msg['offset'],
                key=msg.get('key'),
                value=msg['value'],
                timestamp=msg['timestamp'],
                headers=msg.get('headers')
            ))
        
        logger.info(f"Se consumieron {len(consumed)} mensajes")
        return consumed
        
    except Exception as e:
        logger.error(f"Error al consumir mensajes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al consumir mensajes: {str(e)}"
        )


@router.get(
    "/topics",
    response_model=TopicsList,
    status_code=status.HTTP_200_OK,
    summary="Listar topics",
    description="Obtiene la lista de todos los topics disponibles en Kafka"
)
async def list_topics(
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> TopicsList:
    """
    Lista todos los topics de Kafka
    
    Args:
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Lista de topics con su cantidad
        
    Raises:
        HTTPException: Si hay un error al obtener los topics
    """
    try:
        logger.info("Obteniendo lista de topics")
        
        topics = await kafka_service.list_topics()
        
        return TopicsList(
            topics=topics,
            count=len(topics)
        )
        
    except Exception as e:
        logger.error(f"Error al listar topics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al listar topics: {str(e)}"
        )


@router.post(
    "/topics",
    response_model=Dict[str, Any],
    status_code=status.HTTP_201_CREATED,
    summary="Crear topic",
    description="Crea un nuevo topic en Kafka con la configuración especificada"
)
async def create_topic(
    topic: KafkaTopicCreate,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> Dict[str, Any]:
    """
    Crea un nuevo topic en Kafka
    
    Args:
        topic: Configuración del topic a crear
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Confirmación de creación
        
    Raises:
        HTTPException: Si hay un error al crear el topic
    """
    try:
        logger.info(f"Creando topic: {topic.name}")
        
        success = await kafka_service.create_topic(
            topic_name=topic.name,
            num_partitions=topic.num_partitions,
            replication_factor=topic.replication_factor,
            config=topic.config
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No se pudo crear el topic {topic.name}"
            )
        
        return {
            "success": True,
            "topic": topic.name,
            "partitions": topic.num_partitions,
            "replication_factor": topic.replication_factor,
            "message": f"Topic {topic.name} creado exitosamente"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al crear topic: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al crear topic: {str(e)}"
        )


@router.get(
    "/topics/{topic_name}",
    response_model=KafkaTopicInfo,
    status_code=status.HTTP_200_OK,
    summary="Obtener información de un topic",
    description="Obtiene información detallada de un topic específico"
)
async def get_topic_info(
    topic_name: str,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> KafkaTopicInfo:
    """
    Obtiene información de un topic específico
    
    Args:
        topic_name: Nombre del topic
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Información del topic
        
    Raises:
        HTTPException: Si el topic no existe o hay un error
    """
    try:
        logger.info(f"Obteniendo información del topic: {topic_name}")
        
        # Verificar que el topic existe
        topics = await kafka_service.list_topics()
        if topic_name not in topics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Topic {topic_name} no encontrado"
            )
        
        # Obtener metadata (esto es simplificado, Kafka no expone fácilmente toda la metadata)
        # En una implementación completa, usarías AdminClient para obtener más detalles
        
        return KafkaTopicInfo(
            name=topic_name,
            partitions=1,  # Valor por defecto, mejorar con AdminClient
            replication_factor=1,
            config={}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener información del topic: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener información: {str(e)}"
        )


@router.delete(
    "/topics/{topic_name}",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Eliminar topic",
    description="Elimina un topic de Kafka (usar con precaución)"
)
async def delete_topic(
    topic_name: str,
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> Dict[str, Any]:
    """
    Elimina un topic de Kafka
    
    Args:
        topic_name: Nombre del topic a eliminar
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Confirmación de eliminación
        
    Raises:
        HTTPException: Si hay un error al eliminar el topic
    """
    try:
        logger.warning(f"Eliminando topic: {topic_name}")
        
        success = await kafka_service.delete_topic(topic_name)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No se pudo eliminar el topic {topic_name}"
            )
        
        return {
            "success": True,
            "topic": topic_name,
            "message": f"Topic {topic_name} eliminado exitosamente"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar topic: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al eliminar topic: {str(e)}"
        )
