"""
Kafka Schemas
Modelos Pydantic para validación de datos relacionados con Kafka
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, List
from datetime import datetime


class KafkaMessage(BaseModel):
    """Schema para enviar un mensaje a Kafka"""
    topic: str = Field(..., description="Nombre del topic de Kafka")
    key: Optional[str] = Field(None, description="Key del mensaje (opcional)")
    value: Dict[str, Any] = Field(..., description="Contenido del mensaje en formato JSON")
    headers: Optional[Dict[str, str]] = Field(None, description="Headers del mensaje (opcional)")
    
    @field_validator('topic')
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("El topic no puede estar vacío")
        return v.strip()
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic": "personal-data",
                "key": "user-123",
                "value": {
                    "id": "123",
                    "nombre": "Juan Pérez",
                    "edad": 30
                },
                "headers": {
                    "source": "api",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }


class KafkaMessageResponse(BaseModel):
    """Schema para la respuesta después de enviar un mensaje"""
    success: bool = Field(..., description="Si el mensaje se envió correctamente")
    topic: str = Field(..., description="Topic al que se envió")
    partition: Optional[int] = Field(None, description="Partición donde se almacenó")
    offset: Optional[int] = Field(None, description="Offset del mensaje")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Timestamp de la operación")
    error: Optional[str] = Field(None, description="Mensaje de error si falló")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "topic": "personal-data",
                "partition": 0,
                "offset": 12345,
                "timestamp": "2024-01-15T10:30:00Z",
                "error": None
            }
        }


class KafkaBatchMessages(BaseModel):
    """Schema para enviar múltiples mensajes en batch"""
    messages: List[KafkaMessage] = Field(..., description="Lista de mensajes a enviar")
    
    @field_validator('messages')
    @classmethod
    def validate_messages(cls, v: List[KafkaMessage]) -> List[KafkaMessage]:
        if not v or len(v) == 0:
            raise ValueError("Debe incluir al menos un mensaje")
        if len(v) > 1000:
            raise ValueError("No se pueden enviar más de 1000 mensajes en un batch")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "messages": [
                    {
                        "topic": "personal-data",
                        "key": "user-123",
                        "value": {"id": "123", "nombre": "Juan"}
                    },
                    {
                        "topic": "professional-data",
                        "key": "user-123",
                        "value": {"id": "123", "cargo": "Developer"}
                    }
                ]
            }
        }


class KafkaTopicCreate(BaseModel):
    """Schema para crear un nuevo topic"""
    name: str = Field(..., description="Nombre del topic")
    num_partitions: int = Field(1, ge=1, le=100, description="Número de particiones")
    replication_factor: int = Field(1, ge=1, le=3, description="Factor de replicación")
    config: Optional[Dict[str, str]] = Field(None, description="Configuración adicional del topic")
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("El nombre del topic no puede estar vacío")
        if not v.replace('-', '').replace('_', '').isalnum():
            raise ValueError("El nombre del topic solo puede contener letras, números, guiones y guiones bajos")
        return v.strip()
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "new-topic",
                "num_partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": "604800000",
                    "compression.type": "gzip"
                }
            }
        }


class KafkaTopicInfo(BaseModel):
    """Schema con información de un topic"""
    name: str = Field(..., description="Nombre del topic")
    partitions: int = Field(..., description="Número de particiones")
    replication_factor: Optional[int] = Field(None, description="Factor de replicación")
    config: Optional[Dict[str, Any]] = Field(None, description="Configuración del topic")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "personal-data",
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": "604800000",
                    "compression.type": "gzip"
                }
            }
        }


class TopicsList(BaseModel):
    """Schema para listar topics"""
    topics: List[str] = Field(..., description="Lista de nombres de topics")
    count: int = Field(..., description="Cantidad de topics")
    
    class Config:
        json_schema_extra = {
            "example": {
                "topics": [
                    "personal-data",
                    "professional-data",
                    "bank-data"
                ],
                "count": 3
            }
        }


class ConsumeMessagesRequest(BaseModel):
    """Schema para solicitar consumo de mensajes"""
    topic: str = Field(..., description="Topic del cual consumir")
    max_messages: int = Field(10, ge=1, le=100, description="Máximo número de mensajes a consumir")
    timeout: float = Field(5.0, ge=0.1, le=30.0, description="Timeout en segundos")
    
    @field_validator('topic')
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("El topic no puede estar vacío")
        return v.strip()
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic": "personal-data",
                "max_messages": 10,
                "timeout": 5.0
            }
        }


class ConsumedMessage(BaseModel):
    """Schema para un mensaje consumido"""
    topic: str = Field(..., description="Topic del mensaje")
    partition: int = Field(..., description="Partición del mensaje")
    offset: int = Field(..., description="Offset del mensaje")
    key: Optional[str] = Field(None, description="Key del mensaje")
    value: Dict[str, Any] = Field(..., description="Valor del mensaje")
    timestamp: datetime = Field(..., description="Timestamp del mensaje")
    headers: Optional[Dict[str, str]] = Field(None, description="Headers del mensaje")
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic": "personal-data",
                "partition": 0,
                "offset": 12345,
                "key": "user-123",
                "value": {"id": "123", "nombre": "Juan"},
                "timestamp": "2024-01-15T10:30:00Z",
                "headers": {"source": "api"}
            }
        }
