"""
Cliente MongoDB para Data Processor
Lee de raw_messages y escribe en aggregated_data
"""
import logging
from typing import Dict, List, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from . import config

logger = logging.getLogger(__name__)


class MongoDBClient:
    """
    Cliente MongoDB para el Data Processor
    
    Responsabilidades:
    1. Leer documentos crudos de raw_messages (sin procesar)
    2. Guardar documentos agregados en aggregated_data
    3. Marcar documentos crudos como procesados
    4. Obtener estadísticas de colecciones
    """
    
    def __init__(self):
        """Inicializa cliente MongoDB"""
        self.uri = config.MONGO_URI
        self.database_name = config.MONGO_DATABASE
        self.raw_collection_name = config.MONGO_COLLECTION
        self.aggregated_collection_name = config.AGGREGATED_COLLECTION
        
        self.client: Optional[MongoClient] = None
        self.db = None
        self.raw_collection = None
        self.aggregated_collection = None
    
    def connect(self):
        """Conecta a MongoDB"""
        try:
            logger.info(f"📡 Conectando a MongoDB: {self.database_name}")
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión
            self.client.admin.command('ping')
            
            # Obtener referencias a BD y colecciones
            self.db = self.client[self.database_name]
            self.raw_collection = self.db[self.raw_collection_name]
            self.aggregated_collection = self.db[self.aggregated_collection_name]
            
            logger.info(f"✅ Conectado a MongoDB: {self.database_name}")
            logger.info(f"   - Colección origen: {self.raw_collection_name}")
            logger.info(f"   - Colección destino: {self.aggregated_collection_name}")
            
        except PyMongoError as e:
            logger.error(f"❌ Error conectando a MongoDB: {e}")
            raise
    
    def get_unprocessed_documents(self, limit: int) -> List[Dict]:
        """
        Obtiene documentos crudos que aún no han sido procesados
        
        Args:
            limit: Cantidad máxima de documentos
            
        Returns:
            Lista de documentos sin procesar
        """
        try:
            # Buscar documentos con processed=False o sin el campo processed
            query = {
                '$or': [
                    {'processed': False},
                    {'processed': {'$exists': False}}
                ]
            }
            
            # Leer documentos
            cursor = self.raw_collection.find(query).limit(limit)
            documents = list(cursor)
            
            logger.info(f"📥 Obtenidos {len(documents)} documentos sin procesar")
            
            return documents
            
        except PyMongoError as e:
            logger.error(f"❌ Error obteniendo documentos: {e}")
            return []
    
    def upsert_aggregated(self, doc: Dict) -> bool:
        """
        Guarda o actualiza un documento agregado
        Usa upsert para evitar duplicados (busca por passport)
        
        Args:
            doc: Documento agregado con datos de una persona
            
        Returns:
            True si se guardó correctamente
        """
        try:
            passport = doc.get('passport')
            
            if not passport:
                logger.error("❌ Documento sin passport, no se puede guardar")
                return False
            
            # Upsert por passport (crear si no existe, actualizar si existe)
            result = self.aggregated_collection.update_one(
                {'passport': passport},
                {'$set': doc},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"✅ Insertado: passport={passport}")
            elif result.modified_count > 0:
                logger.info(f"🔄 Actualizado: passport={passport}")
            
            return True
            
        except PyMongoError as e:
            logger.error(f"❌ Error guardando documento agregado: {e}")
            return False
    
    def mark_as_processed(self, doc_ids: List) -> int:
        """
        Marca documentos crudos como procesados
        
        Args:
            doc_ids: Lista de _id de documentos a marcar
            
        Returns:
            Cantidad de documentos marcados
        """
        if not doc_ids:
            return 0
        
        try:
            result = self.raw_collection.update_many(
                {'_id': {'$in': doc_ids}},
                {'$set': {'processed': True}}
            )
            
            count = result.modified_count
            logger.info(f"✅ Marcados {count} documentos como procesados")
            
            return count
            
        except PyMongoError as e:
            logger.error(f"❌ Error marcando documentos: {e}")
            return 0
    
    def get_stats(self) -> Dict:
        """
        Obtiene estadísticas de las colecciones
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            raw_total = self.raw_collection.count_documents({})
            raw_processed = self.raw_collection.count_documents({'processed': True})
            raw_pending = self.raw_collection.count_documents({
                '$or': [
                    {'processed': False},
                    {'processed': {'$exists': False}}
                ]
            })
            aggregated_total = self.aggregated_collection.count_documents({})
            aggregated_complete = self.aggregated_collection.count_documents({'is_complete': True})
            
            return {
                'raw_total': raw_total,
                'raw_processed': raw_processed,
                'raw_pending': raw_pending,
                'aggregated_total': aggregated_total,
                'aggregated_complete': aggregated_complete
            }
            
        except PyMongoError as e:
            logger.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def close(self):
        """Cierra conexión a MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Conexión a MongoDB cerrada")


# Singleton para reutilizar la conexión
_mongo_client_instance = None


def get_mongo_client() -> MongoDBClient:
    """
    Obtiene instancia única de MongoDBClient (patrón Singleton)
    
    Returns:
        Instancia de MongoDBClient
    """
    global _mongo_client_instance
    
    if _mongo_client_instance is None:
        _mongo_client_instance = MongoDBClient()
        _mongo_client_instance.connect()
    
    return _mongo_client_instance