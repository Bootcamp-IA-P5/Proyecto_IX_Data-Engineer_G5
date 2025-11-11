"""
Configuración de la aplicación usando Pydantic Settings
"""
from pydantic_settings import BaseSettings
from typing import List
from functools import lru_cache


class Settings(BaseSettings):
    """Configuración de la aplicación"""
    
    # Aplicación
    app_name: str = "HR-Pro-API"
    app_version: str = "1.0.0"
    environment: str = "development"
    debug: bool = True
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_user_tracker: str = "probando"
    kafka_group_id: str = "api-consumer-group"
    
    # Kafka Topics por tipo
    kafka_topic_personal: str = "personal-data"
    kafka_topic_location: str = "location-data"
    kafka_topic_professional: str = "professional-data"
    kafka_topic_bank: str = "bank-data"
    kafka_topic_net: str = "net-data"
    
    # MongoDB
    mongodb_url: str = "mongodb://mongo:27017"
    mongodb_database: str = "hr_pro_db"
    mongodb_collection_users: str = "users"
    
    # PostgreSQL
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "hrpro"
    postgres_password: str = "hrpro123"
    postgres_db: str = "hr_pro_db"
    
    # Redis
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    
    # URLs de microservicios
    data_processor_url: str = "http://data-processor:8001"
    mongo_persister_url: str = "http://mongo-persister:8002"
    sql_persister_url: str = "http://sql-persister:8003"
    
    # Seguridad
    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "http://localhost:8000"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Obtiene la configuración de forma singleton"""
    return Settings()
