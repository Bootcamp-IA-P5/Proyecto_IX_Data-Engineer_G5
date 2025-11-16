"""
Configuración global de pytest
Carga variables de entorno ANTES de cualquier test
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# FORZAR CARGA DE .env.tests
# ============================================================
# Obtener directorio raíz del proyecto (mongo-persister/)
project_root = Path(__file__).parent.parent
env_tests_path = project_root / '.env.tests'

# Cargar .env.tests con override=True para sobrescribir .env
if env_tests_path.exists():
    load_dotenv(env_tests_path, override=True)
    print(f"\n✅ Loaded .env.tests from: {env_tests_path}")
    
    # Debug: Mostrar variables cargadas
    print(f"   MONGO_DB: {os.getenv('MONGO_DB')}")
    print(f"   MONGO_URI: {os.getenv('MONGO_URI')}")
    print(f"   KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}\n")
else:
    print(f"\n⚠️  .env.tests NOT FOUND at: {env_tests_path}\n")
    
# ============================================================
# ASEGURAR QUE 'src' ESTÁ EN PATH
# ============================================================
src_path = str(project_root / 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)