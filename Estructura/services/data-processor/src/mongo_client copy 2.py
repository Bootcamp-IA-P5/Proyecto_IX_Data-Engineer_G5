"""
Cliente MongoDB para Data Processor
Lee de raw_messages y escribe en personal_data, financial_data, employment_data
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
    2. Guardar documentos clasificados en personal_data, financial_data, employment_data
    3. Marcar documentos crudos como procesados
    4. Obtener estadísticas de colecciones
    """
    
    def __init__(self):
        """Inicializa cliente MongoDB"""
        self.uri = config.MONGO_URI
        self.database_name = config.MONGO_DATABASE
        self.raw_collection_name = config.MONGO_COLLECTION
        
        # Colecciones de salida
        self.personal_collection_name = config.PERSONAL_COLLECTION
        self.financial_collection_name = config.FINANCIAL_COLLECTION
        self.employment_collection_name = config.EMPLOYMENT_COLLECTION
        
        self.client: Optional[MongoClient] = None
        self.db = None
        self.raw_collection = None
        self.personal_collection = None
        self.financial_collection = None
        self.employment_collection = None
    
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
            self.personal_collection = self.db[self.personal_collection_name]
            self.financial_collection = self.db[self.financial_collection_name]
            self.employment_collection = self.db[self.employment_collection_name]
            
            logger.info(f"✅ Conectado a MongoDB: {self.database_name}")
            logger.info(f"   - Colección origen: {self.raw_collection_name}")
            logger.info(f"   - Colecciones destino:")
            logger.info(f"     • Personal: {self.personal_collection_name}")
            logger.info(f"     • Financial: {self.financial_collection_name}")
            logger.info(f"     • Employment: {self.employment_collection_name}")
            
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
            
            if documents:
                logger.info(f"📥 Obtenidos {len(documents)} documentos sin procesar")
            
            return documents
            
        except PyMongoError as e:
            logger.error(f"❌ Error obteniendo documentos: {e}")
            return []
    
    def insert_personal(self, doc: Dict) -> bool:
        """
        Guarda documento en personal_data
        
        Args:
            doc: Documento con datos personales
            
        Returns:
            True si se guardó correctamente
        """
        try:
            self.personal_collection.insert_one(doc)
            logger.debug(f"✅ Personal guardado: {doc.get('_id')}")
            return True
            
        except PyMongoError as e:
            logger.error(f"❌ Error guardando personal: {e}")
            return False
    
    def insert_financial(self, doc: Dict) -> bool:
        """
        Guarda documento en financial_data
        
        Args:
            doc: Documento con datos financieros
            
        Returns:
            True si se guardó correctamente
        """
        try:
            self.financial_collection.insert_one(doc)
            logger.debug(f"✅ Financial guardado: {doc.get('_id')}")
            return True
            
        except PyMongoError as e:
            logger.error(f"❌ Error guardando financial: {e}")
            return False
    
    def insert_employment(self, doc: Dict) -> bool:
        """
        Guarda documento en employment_data
        
        Args:
            doc: Documento con datos laborales
            
        Returns:
            True si se guardó correctamente
        """
        try:
            self.employment_collection.insert_one(doc)
            logger.debug(f"✅ Employment guardado: {doc.get('_id')}")
            return True
            
        except PyMongoError as e:
            logger.error(f"❌ Error guardando employment: {e}")
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
            
            personal_total = self.personal_collection.count_documents({})
            financial_total = self.financial_collection.count_documents({})
            employment_total = self.employment_collection.count_documents({})
            
            return {
                'raw_total': raw_total,
                'raw_processed': raw_processed,
                'raw_pending': raw_pending,
                'personal_total': personal_total,
                'financial_total': financial_total,
                'employment_total': employment_total
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