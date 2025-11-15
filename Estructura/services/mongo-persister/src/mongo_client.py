"""
Cliente de MongoDB para el mongo-persister
Maneja la conexión y operaciones con MongoDB
"""
import logging
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from datetime import datetime
from typing import Dict, List, Optional
import config

logger = logging.getLogger(__name__)


class MongoDBClient:
    """Cliente para manejar conexiones y operaciones con MongoDB"""
    
    def __init__(self):
        """Inicializa la conexión a MongoDB"""
        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection: Optional[Collection] = None
        self._connect()
    
    def _connect(self):
        """Establece conexión con MongoDB"""
        try:
            logger.info(f"Conectando a MongoDB: {config.MONGO_DB}")
            self.client = MongoClient(
                config.MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            
            self.client.admin.command('ping')
            
            self.db = self.client[config.MONGO_DB]
            self.collection = self.db[config.MONGO_COLLECTION]
            
            logger.info(f"✅ Conectado a MongoDB: {config.MONGO_DB}.{config.MONGO_COLLECTION}")
            
            self._create_indexes()
            
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"❌ Error: No se pudo conectar a MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado al conectar a MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Crea índices en MongoDB para mejorar consultas"""
        try:
            # Índice por timestamp de inserción
            self.collection.create_index([("_inserted_at", -1)])
            # Índice por tipo de mensaje
            self.collection.create_index([("message_type", 1)])
            # Índice por passport (para búsquedas por persona)
            self.collection.create_index([("passport", 1)])
            # Índice único usando _kafka_metadata (evita duplicados)
            self.collection.create_index(
                [("_kafka_metadata.partition", 1), ("_kafka_metadata.offset", 1)],
                unique=True,
                name="kafka_unique_offset"
            )
            logger.info("✅ Índices creados correctamente")
        except Exception as e:
            logger.warning(f"⚠️  Error creando índices: {e}")
    
    def insert_message(self, message: Dict) -> bool:
        """
        Inserta un mensaje individual en MongoDB (con idempotencia)
        
        Args:
            message: Diccionario con el mensaje de Kafka
            
        Returns:
            bool: True si se insertó correctamente
        """
        try:
            message['_inserted_at'] = datetime.utcnow()
            
            # Extraer partition y offset desde _kafka_metadata
            if '_kafka_metadata' in message:
                partition = message['_kafka_metadata']['partition']
                offset = message['_kafka_metadata']['offset']
                message['_id'] = f"{partition}_{offset}"
            
            self.collection.insert_one(message)
            return True
            
        except errors.DuplicateKeyError:
            logger.debug(f"⏭️  Mensaje duplicado omitido")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error insertando mensaje: {e}")
            return False
    
    def insert_batch(self, messages: List[Dict]) -> int:
        """
        Inserta múltiples mensajes en batch con idempotencia
        (Evita duplicados usando partition + offset como clave única)
        
        Args:
            messages: Lista de mensajes a insertar
            
        Returns:
            int: Número de mensajes procesados (insertados + duplicados omitidos)
        """
        if not messages:
            return 0
        
        try:
            now = datetime.utcnow()
            for msg in messages:
                msg['_inserted_at'] = now
                
                # Extraer partition y offset desde _kafka_metadata
                if '_kafka_metadata' in msg:
                    partition = msg['_kafka_metadata']['partition']
                    offset = msg['_kafka_metadata']['offset']
                    msg['_id'] = f"{partition}_{offset}"
            
            # Insertar en batch (ordered=False para continuar aunque haya duplicados)
            result = self.collection.insert_many(messages, ordered=False)
            return len(result.inserted_ids)
            
        except errors.BulkWriteError as e:
            # Algunos mensajes se insertaron, otros eran duplicados
            inserted = e.details.get('nInserted', 0)
            duplicates = len([err for err in e.details.get('writeErrors', []) 
                             if err.get('code') == 11000])  # 11000 = DuplicateKeyError
            
            # Solo loguear si hay duplicados (nivel DEBUG)
            if duplicates > 0:
                logger.debug(f"⏭️  {duplicates} duplicados omitidos, {inserted} nuevos insertados")
            
            # Si TODOS son duplicados, retornar la cantidad total para evitar warnings
            if inserted == 0 and duplicates == len(messages):
                logger.debug(f"✅ Todos los {duplicates} mensajes ya existían (idempotencia)")
                return duplicates  # Retornar la cantidad total procesada
            
            return inserted
            
        except Exception as e:
            logger.error(f"❌ Error en batch insert: {e}")
            return 0
    
    def get_stats(self) -> Dict:
        """Obtiene estadísticas de la colección"""
        try:
            total_docs = self.collection.count_documents({})
            
            pipeline = [
                {"$group": {
                    "_id": "$message_type",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]
            
            type_stats = list(self.collection.aggregate(pipeline))
            
            return {
                "total_documents": total_docs,
                "by_type": type_stats
            }
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas: {e}")
            return {"total_documents": 0, "by_type": []}
    
    def close(self):
        """Cierra la conexión a MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Conexión a MongoDB cerrada")


# Singleton para reutilizar conexión
_mongo_client: Optional[MongoDBClient] = None


def get_mongo_client() -> MongoDBClient:
    """Obtiene instancia singleton del cliente MongoDB"""
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoDBClient()
    return _mongo_client