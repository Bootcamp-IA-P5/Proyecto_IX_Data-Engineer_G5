"""
Tests para configuración
"""
import unittest
import sys
import os
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest


@pytest.mark.unit
class TestConfigPrintFunction(unittest.TestCase):
    """Tests de la función print_config()"""
    
    def test_print_config_does_not_crash(self):
        """Test: print_config() no crashea"""
        from src import config
        
        # Capturar stdout
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        
        try:
            config.print_config()
            output = sys.stdout.getvalue()
            
            # Verificar que imprime algo
            self.assertIn("CONFIGURACIÓN", output)
            self.assertIn("Kafka", output)
            self.assertIn("MongoDB", output)
        finally:
            sys.stdout = old_stdout
    
    def test_print_config_output_format(self):
        """Test: print_config() tiene formato correcto"""
        from src import config
        
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        
        try:
            config.print_config()
            output = sys.stdout.getvalue()
            
            # Verificar formato
            self.assertIn("=" * 60, output)
            self.assertIn("Bootstrap Servers:", output)
            self.assertIn("Database:", output)
        finally:
            sys.stdout = old_stdout


if __name__ == '__main__':
    unittest.main()