"""
Tests para configuración
"""
import unittest
import os
from unittest.mock import patch


class TestConfig(unittest.TestCase):
    """Tests para config.py"""
    
    @patch.dict(os.environ, {
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'KAFKA_TOPIC': 'test_topic',
        'MONGO_URI': 'mongodb://localhost:27017',
        'MONGO_DATABASE': 'test_db',
        'COLLECTION_NAME': 'test_collection'
    })
    def test_config_from_env(self):
        """Test: Configuración desde variables de entorno"""
        import src.config as config
        
        # Recargar módulo para tomar nuevas variables
        import importlib
        importlib.reload(config)
        
        self.assertEqual(config.KAFKA_BOOTSTRAP_SERVERS, 'localhost:9092')
        self.assertEqual(config.KAFKA_TOPIC, 'test_topic')
        self.assertEqual(config.MONGO_URI, 'mongodb://localhost:27017')
        self.assertEqual(config.MONGO_DATABASE, 'test_db')
        self.assertEqual(config.COLLECTION_NAME, 'test_collection')
    
    def test_config_defaults(self):
        """Test: Valores por defecto de configuración"""
        import src.config as config
        
        # Verificar que existen las variables
        self.assertIsNotNone(config.KAFKA_BOOTSTRAP_SERVERS)
        self.assertIsNotNone(config.KAFKA_TOPIC)
        self.assertIsNotNone(config.MONGO_URI)


if __name__ == '__main__':
    unittest.main()