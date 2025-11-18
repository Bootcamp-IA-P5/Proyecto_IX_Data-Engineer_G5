"""
Tests unitarios para DataProcessor
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


class TestDataProcessor(unittest.TestCase):
    """Suite de tests para DataProcessor"""
    
    @patch('src.processor.get_mongo_client')
    def test_init(self, mock_get_client):
        """Test: Inicialización del procesador"""
        from src.processor import DataProcessor
        
        processor = DataProcessor()
        self.assertIsNotNone(processor)
        self.assertEqual(processor.stats['total_processed'], 0)
        self.assertEqual(len(processor.person_buffer), 0)
    
    def test_validate_document_valid(self, mock_get_client):
        """Test: Validación de documento válido"""
        from src.processor import DataProcessor
        
        with patch('src.processor.get_mongo_client'):
            processor = DataProcessor()
            
            valid_doc = {
                '_kafka_metadata': {'partition': 0, 'offset': 123},
                '_inserted_at': datetime.utcnow(),
                'passport': 'ABC123',
                'message_type': 'personal_info'
            }
            
            self.assertTrue(processor.validate_document(valid_doc))
    
    def test_extract_person_key(self):
        """Test: Extracción de passport"""
        from src.processor import DataProcessor
        
        with patch('src.processor.get_mongo_client'):
            processor = DataProcessor()
            
            doc = {'passport': 'XYZ789'}
            self.assertEqual(processor.extract_person_key(doc), 'XYZ789')
    
    def test_is_person_complete(self):
        """Test: Verificación de persona completa"""
        from src.processor import DataProcessor
        
        with patch('src.processor.get_mongo_client'):
            processor = DataProcessor()
            
            # Persona completa
            complete_person = {
                'passport': 'ABC123',
                'personal': {'name': 'John'},
                'contact': {'email': 'john@example.com'},
                'address': {'city': 'Madrid'},
                'employment': {'company': 'Acme'},
                'benefits': {'insurance': 'Premium'}
            }
            self.assertTrue(processor.is_person_complete(complete_person))
            
            # Persona incompleta
            incomplete_person = {
                'passport': 'XYZ789',
                'personal': {'name': 'Jane'},
                'contact': None,
                'address': None,
                'employment': None,
                'benefits': None
            }
            self.assertFalse(processor.is_person_complete(incomplete_person))
    
    # TODO: Añadir más tests
    # - test_accumulate_person_data
    # - test_create_aggregated_document
    # - test_flush_person_buffer


if __name__ == '__main__':
    unittest.main()