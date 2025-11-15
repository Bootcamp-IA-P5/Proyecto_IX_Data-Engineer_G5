"""
Tests unitarios para DataPersister
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


class TestDataPersister(unittest.TestCase):
    """Suite de tests para DataPersister"""
    
    @patch('src.persister.MongoClient')
    @patch('src.persister.KafkaConsumer')
    def test_init_successful(self, mock_consumer, mock_mongo):
        """Test: Inicialización exitosa del persister"""
        from src.persister import DataPersister
        
        # Configurar mocks
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
        mock_mongo_instance = MagicMock()
        mock_mongo.return_value = mock_mongo_instance
        
        # Ejecutar
        persister = DataPersister()
        
        # Verificar
        self.assertIsNotNone(persister)
        mock_consumer.assert_called_once()
        mock_mongo.assert_called_once()
    
    @patch('src.persister.MongoClient')
    @patch('src.persister.KafkaConsumer')
    def test_process_message_valid(self, mock_consumer, mock_mongo):
        """Test: Procesamiento de mensaje válido"""
        from src.persister import DataPersister
        
        # Configurar mocks
        mock_consumer.return_value = MagicMock()
        mock_mongo_instance = MagicMock()
        mock_mongo.return_value = mock_mongo_instance
        
        persister = DataPersister()
        
        # Mensaje de prueba
        mock_message = MagicMock()
        mock_message.value = b'{"test": "data"}'
        mock_message.partition = 0
        mock_message.offset = 123
        mock_message.timestamp = 1234567890000
        
        # Procesar
        result = persister.process_message(mock_message)
        
        # Verificar
        self.assertTrue(result)
    
    @patch('src.persister.MongoClient')
    @patch('src.persister.KafkaConsumer')
    def test_process_message_invalid_json(self, mock_consumer, mock_mongo):
        """Test: Manejo de JSON inválido"""
        from src.persister import DataPersister
        
        mock_consumer.return_value = MagicMock()
        mock_mongo.return_value = MagicMock()
        
        persister = DataPersister()
        
        # Mensaje con JSON inválido
        mock_message = MagicMock()
        mock_message.value = b'{invalid json}'
        mock_message.partition = 0
        mock_message.offset = 123
        
        # Procesar (debería manejar error gracefully)
        result = persister.process_message(mock_message)
        
        # Verificar que no crashea
        self.assertIsNotNone(result)
    
    @patch('src.persister.MongoClient')
    @patch('src.persister.KafkaConsumer')
    def test_batch_insert(self, mock_consumer, mock_mongo):
        """Test: Inserción por lotes"""
        from src.persister import DataPersister
        
        mock_consumer.return_value = MagicMock()
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = lambda self, key: mock_collection
        mock_mongo_instance = MagicMock()
        mock_mongo_instance.__getitem__ = lambda self, key: mock_db
        mock_mongo.return_value = mock_mongo_instance
        
        persister = DataPersister()
        
        # Simular batch de 3 documentos
        persister.batch = [
            {'data': 'doc1'},
            {'data': 'doc2'},
            {'data': 'doc3'}
        ]
        
        # Insertar batch
        persister.insert_batch()
        
        # Verificar que se llamó insert_many
        mock_collection.insert_many.assert_called_once()
        # Verificar que el batch se limpió
        self.assertEqual(len(persister.batch), 0)


class TestMongoClient(unittest.TestCase):
    """Tests para MongoClient helper"""
    
    @patch('src.mongo_client.MongoClient')
    def test_connection(self, mock_mongo):
        """Test: Conexión a MongoDB"""
        from src.mongo_client import MongoDBClient
        
        mock_mongo_instance = MagicMock()
        mock_mongo.return_value = mock_mongo_instance
        
        # Crear cliente
        client = MongoDBClient()
        
        # Verificar
        self.assertIsNotNone(client)
        mock_mongo.assert_called_once()


if __name__ == '__main__':
    unittest.main()