"""
Health Check Router
Endpoints para verificar el estado de la API y sus dependencias
"""

from fastapi import APIRouter, Depends, status
from typing import Dict, Any
import logging
from datetime import datetime
from ..services.kafka_service import KafkaService, get_kafka_service

# Configurar logger
logger = logging.getLogger(__name__)

# Crear router
router = APIRouter(
    prefix="/health",
    tags=["Health Check"],
    responses={404: {"description": "Not found"}},
)


@router.get(
    "/",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Health Check General",
    description="Verifica que la API está funcionando correctamente"
)
async def health_check() -> Dict[str, Any]:
    """
    Endpoint básico de health check
    
    Returns:
        Dict con el estado de la API
    """
    return {
        "status": "healthy",
        "service": "HR-Pro-API",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }


@router.get(
    "/ready",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Readiness Check",
    description="Verifica que la API y todas sus dependencias están listas"
)
async def readiness_check(
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> Dict[str, Any]:
    """
    Verifica que todos los servicios dependientes están disponibles
    
    Args:
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Dict con el estado de todos los servicios
    """
    checks = {
        "api": "healthy",
        "kafka": "unknown",
    }
    
    # Verificar Kafka
    try:
        if kafka_service and kafka_service.producer:
            checks["kafka"] = "healthy"
        else:
            checks["kafka"] = "unhealthy"
    except Exception as e:
        logger.error(f"Error checking Kafka: {e}")
        checks["kafka"] = "unhealthy"
    
    # Determinar estado general
    overall_status = "healthy" if all(
        status == "healthy" for status in checks.values()
    ) else "unhealthy"
    
    return {
        "status": overall_status,
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get(
    "/liveness",
    response_model=Dict[str, str],
    status_code=status.HTTP_200_OK,
    summary="Liveness Check",
    description="Verifica que la API está viva (para Kubernetes)"
)
async def liveness_check() -> Dict[str, str]:
    """
    Endpoint simple para verificar que el proceso está vivo
    
    Returns:
        Dict con el estado
    """
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get(
    "/kafka",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Kafka Health Check",
    description="Verifica específicamente el estado de Kafka"
)
async def kafka_health_check(
    kafka_service: KafkaService = Depends(get_kafka_service)
) -> Dict[str, Any]:
    """
    Verifica el estado de la conexión a Kafka
    
    Args:
        kafka_service: Servicio de Kafka inyectado
        
    Returns:
        Dict con información detallada de Kafka
    """
    try:
        # Obtener metadata de Kafka
        topics = await kafka_service.list_topics()
        
        return {
            "status": "healthy",
            "connected": True,
            "bootstrap_servers": kafka_service.bootstrap_servers,
            "topics_available": len(topics),
            "topics": topics,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error checking Kafka health: {e}")
        return {
            "status": "unhealthy",
            "connected": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
