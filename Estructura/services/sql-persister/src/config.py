# Estructura/services/sql-persister/src/sql_config.py
import os

# Mongo (lectura)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "hrpro_db")
AGGREGATED_COLLECTION = os.getenv("AGGREGATED_COLLECTION", "aggregated_data")

# Postgres (Supabase)
PG_HOST = os.getenv("PG_HOST", "")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

# Tuning
SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", "1000"))
SQL_UPSERT_STRATEGY = os.getenv("SQL_UPSERT_STRATEGY", "merge")
PG_POOL_MIN = int(os.getenv("PG_POOL_MIN", "1"))
PG_POOL_MAX = int(os.getenv("PG_POOL_MAX", "5"))
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "10"))
