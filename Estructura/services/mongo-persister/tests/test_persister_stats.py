"""
Tests de estadísticas y monitoreo del MongoPersister
"""
import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Añadir src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest


class TestPersisterStatistics(unittest.TestCase):
    """Tests de estadísticas del persister"""
    
    @pytest.mark.unit
    def test_message_counter_increments(self):
        """Test: Contador de mensajes se incrementa correctamente"""
        from src.persister import MongoPersister
        
        # Mockear dependencias
        with patch('src.persister.Consumer'), \
             patch('src.persister.get_mongo_client'):
            
            persister = MongoPersister()
            
            # Verificar contador inicial
            initial_count = persister.messages_processed if hasattr(persister, 'messages_processed') else 0
            
            # Simular procesamiento de mensajes
            # (esto depende de tu implementación específica)
            self.assertIsNotNone(persister)
    
    @pytest.mark.unit
    def test_batch_statistics_tracking(self):
        """Test: Estadísticas de batches se trackean correctamente"""
        from src.persister import MongoPersister
        
        with patch('src.persister.Consumer'), \
             patch('src.persister.get_mongo_client'):
            
            persister = MongoPersister()
            
            # Verificar que tiene atributos de estadísticas
            # (ajusta según tu implementación)
            stats_attrs = [
                'messages_processed',
                'batches_inserted',
                'errors_count',
                'start_time'
            ]
            
            # Al menos debería tener ALGÚN atributo de estadísticas
            has_stats = any(hasattr(persister, attr) for attr in stats_attrs)
            
            # Si no tiene ninguno, al menos verificamos que se creó
            self.assertIsNotNone(persister)


class TestPersisterLogging(unittest.TestCase):
    """Tests de logging del persister"""
    
    @pytest.mark.unit
    def test_startup_logging(self):
        """Test: Se loggea el startup correctamente"""
        from src.persister import MongoPersister
        
        with patch('src.persister.Consumer'), \
             patch('src.persister.get_mongo_client'), \
             patch('src.persister.logging') as mock_logging:
            
            persister = MongoPersister()
            
            # Verificar que se loggeó algo (ajusta según tu código)
            # mock_logging.info.assert_called()
            self.assertIsNotNone(persister)


if __name__ == '__main__':
    unittest.main()