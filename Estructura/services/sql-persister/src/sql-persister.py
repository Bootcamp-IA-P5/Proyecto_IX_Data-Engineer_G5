# src/sql-persister.py
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

# ============================
# Config & Logging
# ============================
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - sql-persister - %(levelname)s - %(message)s",
)
logger = logging.getLogger("sql-persister")

# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DATABASE", "hrpro_db")
AGG_COLL = os.getenv("AGGREGATED_COLLECTION", "aggregated_data")
AGG_TS_FIELD = os.getenv("AGGREGATED_TS_FIELD", "updated_at")  # campo ts para incremental

# Postgres / Supabase
PG_HOST = os.getenv("PG_HOST", "aws-1-eu-west-1.pooler.supabase.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")  # must be require for Supabase pooler
PG_POOL_MODE = os.getenv("PG_POOL_MODE", "session").lower()  # session -> NullPool
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "10"))

# ETL tuning
SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", "500"))
SQL_UPSERT_STRATEGY = os.getenv("SQL_UPSERT_STRATEGY", "upsert")  # 'upsert' o 'insert'
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))

SYNC_TABLE = "_sql_sync_state"

# ============================
# DDL Bootstrap (create tables if not exist)
# ============================
DDL_BOOTSTRAP = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.bank (
  id BIGSERIAL PRIMARY KEY,
  grouping_key TEXT NOT NULL UNIQUE,
  swift TEXT,
  iban TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.payroll (
  id BIGSERIAL PRIMARY KEY,
  grouping_key TEXT NOT NULL UNIQUE,
  gross NUMERIC,
  net NUMERIC,
  currency TEXT,
  period_month INT,
  period_year INT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.address (
  id BIGSERIAL PRIMARY KEY,
  grouping_key TEXT NOT NULL UNIQUE,
  street TEXT,
  city TEXT,
  postal_code TEXT,
  country TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.personal (
  id BIGSERIAL PRIMARY KEY,
  grouping_key TEXT NOT NULL UNIQUE,
  first_name TEXT,
  last_name TEXT,
  birthdate DATE,
  email TEXT,
  phone TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.idcard (
  id BIGSERIAL PRIMARY KEY,
  grouping_key TEXT NOT NULL UNIQUE,
  doc_number TEXT,
  doc_type TEXT,
  expires_on DATE,
  updated_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{SYNC_TABLE} (
  id INT PRIMARY KEY DEFAULT 1,
  last_ts TIMESTAMPTZ
);
"""

def ensure_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL_BOOTSTRAP))

# ============================
# Connections
# ============================
def make_mongo() -> MongoClient:
    logger.info("🔗 Conectando a MongoDB…")
    client = MongoClient(MONGO_URI, connectTimeoutMS=10000, socketTimeoutMS=20000, serverSelectionTimeoutMS=10000)

    # Índices “suaves”: si existen con otro nombre, ignoramos el error 85
    try:
        client[MONGO_DB][AGG_COLL].create_index([(AGG_TS_FIELD, ASCENDING)], name=f"idx_{AGG_TS_FIELD}", background=True)
    except OperationFailure as e:
        # code 85 = IndexOptionsConflict (otro nombre mismo key)
        pass
    try:
        client[MONGO_DB][AGG_COLL].create_index([("types_received", ASCENDING)], name="idx_types_received", background=True)
    except OperationFailure:
        pass
    try:
        client[MONGO_DB][AGG_COLL].create_index([("is_complete", ASCENDING)], name="idx_is_complete", background=True)
    except OperationFailure:
        pass
    try:
        client[MONGO_DB][AGG_COLL].create_index([("_grouping_key", ASCENDING)], name="idx_grouping_key", background=True)
    except OperationFailure:
        pass

    logger.info("✅ Mongo OK | db=%s coll=%s chk=%s", MONGO_DB, AGG_COLL, SYNC_TABLE)
    return client

def make_pg_engine() -> Engine:
    logger.info("🔗 Conectando a Postgres (Supabase pooler=%s)…", PG_POOL_MODE)
    # Cadena SQLAlchemy recomendada por Supabase pooler IPv4
    db_url = (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        f"?sslmode={PG_SSLMODE}"
    )
    engine_kwargs = {
        "connect_args": {"connect_timeout": PG_CONNECT_TIMEOUT},
        "execution_options": {"schema_translate_map": {None: PG_SCHEMA}},
    }
    # Si PG_POOL_MODE=session => desactivar client-side pooling
    if PG_POOL_MODE == "session":
        engine = create_engine(db_url, poolclass=NullPool, **engine_kwargs)
        logger.info("✅ Postgres OK | host=%s db=%s schema=%s", PG_HOST, PG_DB, PG_SCHEMA)
        return engine
    else:
        # pool por defecto
        engine = create_engine(db_url, **engine_kwargs)
        logger.info("✅ Postgres OK | host=%s db=%s schema=%s", PG_HOST, PG_DB, PG_SCHEMA)
        return engine

# ============================
# Sync state helpers
# ============================
def get_last_ts(engine: Engine) -> Optional[datetime]:
    with engine.begin() as conn:
        row = conn.execute(
            text(f"SELECT last_ts FROM {PG_SCHEMA}.{SYNC_TABLE} WHERE id=1")
        ).fetchone()
        if row and row[0]:
            return row[0]
        # bootstrap row
        conn.execute(text(f"INSERT INTO {PG_SCHEMA}.{SYNC_TABLE}(id, last_ts) VALUES (1, NULL) ON CONFLICT (id) DO NOTHING"))
        return None

def set_last_ts(engine: Engine, ts: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE {PG_SCHEMA}.{SYNC_TABLE} SET last_ts = :ts WHERE id=1"),
            {"ts": ts},
        )

# ============================
# Fetch docs ready to persist
# ============================
def fetch_ready_docs(mongo: MongoClient, since: Optional[datetime]) -> List[Dict[str, Any]]:
    coll = mongo[MONGO_DB][AGG_COLL]

    base_filter = {
        "$or": [
            {"is_complete": True},
            {"$expr": {"$gte": [{"$size": "$types_received"}, 5]}}
        ]
    }
    if since:
        base_filter[AGG_TS_FIELD] = {"$gt": since}

    cursor = coll.find(base_filter).sort(AGG_TS_FIELD, 1).limit(5000)
    return list(cursor)

# ============================
# Row builders per type
# ============================
def _safe_get(d: Dict[str, Any], key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default

def to_bank_row(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = _safe_get(doc, "data", {})
    bank = _safe_get(data, "bank", {})
    if not bank:
        return None
    return {
        "grouping_key": doc.get("_grouping_key"),
        "swift": _safe_get(bank, "swift"),
        "iban": _safe_get(bank, "iban"),
        "updated_at": _python_ts(doc.get(AGG_TS_FIELD)),
        "created_at": _python_ts(doc.get("created_at")),
    }

def to_payroll_row(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = _safe_get(doc, "data", {})
    payroll = _safe_get(data, "payroll", {})
    if not payroll:
        return None
    return {
        "grouping_key": doc.get("_grouping_key"),
        "gross": _safe_get(payroll, "gross"),
        "net": _safe_get(payroll, "net"),
        "currency": _safe_get(payroll, "currency"),
        "period_month": _safe_get(payroll, "period_month"),
        "period_year": _safe_get(payroll, "period_year"),
        "updated_at": _python_ts(doc.get(AGG_TS_FIELD)),
        "created_at": _python_ts(doc.get("created_at")),
    }

def to_address_row(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = _safe_get(doc, "data", {})
    address = _safe_get(data, "address", {})
    if not address:
        return None
    return {
        "grouping_key": doc.get("_grouping_key"),
        "street": _safe_get(address, "street"),
        "city": _safe_get(address, "city"),
        "postal_code": _safe_get(address, "postal_code"),
        "country": _safe_get(address, "country"),
        "updated_at": _python_ts(doc.get(AGG_TS_FIELD)),
        "created_at": _python_ts(doc.get("created_at")),
    }

def to_personal_row(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = _safe_get(doc, "data", {})
    personal = _safe_get(data, "personal", {})
    if not personal:
        return None
    return {
        "grouping_key": doc.get("_grouping_key"),
        "first_name": _safe_get(personal, "first_name"),
        "last_name": _safe_get(personal, "last_name"),
        "birthdate": _safe_get(personal, "birthdate"),
        "email": _safe_get(personal, "email"),
        "phone": _safe_get(personal, "phone"),
        "updated_at": _python_ts(doc.get(AGG_TS_FIELD)),
        "created_at": _python_ts(doc.get("created_at")),
    }

def to_idcard_row(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = _safe_get(doc, "data", {})
    idcard = _safe_get(data, "idcard", {})
    if not idcard:
        return None
    return {
        "grouping_key": doc.get("_grouping_key"),
        "doc_number": _safe_get(idcard, "doc_number"),
        "doc_type": _safe_get(idcard, "doc_type"),
        "expires_on": _safe_get(idcard, "expires_on"),
        "updated_at": _python_ts(doc.get(AGG_TS_FIELD)),
        "created_at": _python_ts(doc.get("created_at")),
    }

def _python_ts(v) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    # intentamos parseo simple ISO-8601
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

# ============================
# UPSERTS
# ============================
UPSERTS = {
    "bank": text(f"""
        INSERT INTO {PG_SCHEMA}.bank (grouping_key, swift, iban, updated_at, created_at)
        VALUES (:grouping_key, :swift, :iban, COALESCE(:updated_at, now()), COALESCE(:created_at, now()))
        ON CONFLICT (grouping_key) DO UPDATE SET
          swift = EXCLUDED.swift,
          iban = EXCLUDED.iban,
          updated_at = GREATEST({PG_SCHEMA}.bank.updated_at, EXCLUDED.updated_at)
    """),
    "payroll": text(f"""
        INSERT INTO {PG_SCHEMA}.payroll (grouping_key, gross, net, currency, period_month, period_year, updated_at, created_at)
        VALUES (:grouping_key, :gross, :net, :currency, :period_month, :period_year, COALESCE(:updated_at, now()), COALESCE(:created_at, now()))
        ON CONFLICT (grouping_key) DO UPDATE SET
          gross = EXCLUDED.gross,
          net = EXCLUDED.net,
          currency = EXCLUDED.currency,
          period_month = EXCLUDED.period_month,
          period_year = EXCLUDED.period_year,
          updated_at = GREATEST({PG_SCHEMA}.payroll.updated_at, EXCLUDED.updated_at)
    """),
    "address": text(f"""
        INSERT INTO {PG_SCHEMA}.address (grouping_key, street, city, postal_code, country, updated_at, created_at)
        VALUES (:grouping_key, :street, :city, :postal_code, :country, COALESCE(:updated_at, now()), COALESCE(:created_at, now()))
        ON CONFLICT (grouping_key) DO UPDATE SET
          street = EXCLUDED.street,
          city = EXCLUDED.city,
          postal_code = EXCLUDED.postal_code,
          country = EXCLUDED.country,
          updated_at = GREATEST({PG_SCHEMA}.address.updated_at, EXCLUDED.updated_at)
    """),
    "personal": text(f"""
        INSERT INTO {PG_SCHEMA}.personal (grouping_key, first_name, last_name, birthdate, email, phone, updated_at, created_at)
        VALUES (:grouping_key, :first_name, :last_name, :birthdate, :email, :phone, COALESCE(:updated_at, now()), COALESCE(:created_at, now()))
        ON CONFLICT (grouping_key) DO UPDATE SET
          first_name = EXCLUDED.first_name,
          last_name  = EXCLUDED.last_name,
          birthdate  = EXCLUDED.birthdate,
          email      = EXCLUDED.email,
          phone      = EXCLUDED.phone,
          updated_at = GREATEST({PG_SCHEMA}.personal.updated_at, EXCLUDED.updated_at)
    """),
    "idcard": text(f"""
        INSERT INTO {PG_SCHEMA}.idcard (grouping_key, doc_number, doc_type, expires_on, updated_at, created_at)
        VALUES (:grouping_key, :doc_number, :doc_type, :expires_on, COALESCE(:updated_at, now()), COALESCE(:created_at, now()))
        ON CONFLICT (grouping_key) DO UPDATE SET
          doc_number = EXCLUDED.doc_number,
          doc_type   = EXCLUDED.doc_type,
          expires_on = EXCLUDED.expires_on,
          updated_at = GREATEST({PG_SCHEMA}.idcard.updated_at, EXCLUDED.updated_at)
    """),
}

def upsert_rows(engine: Engine, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    stmt = UPSERTS[table]
    # chunking por batch
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), SQL_BATCH_SIZE):
            chunk = rows[i:i+SQL_BATCH_SIZE]
            conn.execute(stmt, chunk)
            total += len(chunk)
    return total

# ============================
# Main loop
# ============================
def main_loop():
    logger.info("🚀 Iniciando SQL Persister (Mongo ➜ Postgres) | modo incremental")

    # Conexiones
    mongo = make_mongo()
    engine = make_pg_engine()

    # Bootstrap de tablas
    ensure_tables(engine)

    while True:
        try:
            since = get_last_ts(engine)
            docs = fetch_ready_docs(mongo, since)
            if not docs:
                logger.info("⏳ Sin documentos completos nuevos. Reviso otra vez en %ss…", POLL_SECONDS)
                time.sleep(POLL_SECONDS)
                continue

            # Transformar docs -> filas por tipo
            bank_rows: List[Dict[str, Any]] = []
            payroll_rows: List[Dict[str, Any]] = []
            address_rows: List[Dict[str, Any]] = []
            personal_rows: List[Dict[str, Any]] = []
            idcard_rows: List[Dict[str, Any]] = []

            max_ts: Optional[datetime] = since

            for d in docs:
                # mantener max_ts para commit incremental
                doc_ts = d.get(AGG_TS_FIELD) or d.get("updated_at") or d.get("created_at")
                py_ts = _python_ts(doc_ts)
                if py_ts and (max_ts is None or py_ts > max_ts):
                    max_ts = py_ts

                # Solo si la clave de agrupación existe
                gk = d.get("_grouping_key")
                if not gk:
                    continue

                # Bank
                r = to_bank_row(d)
                if r:
                    bank_rows.append(r)

                # Payroll
                r = to_payroll_row(d)
                if r:
                    payroll_rows.append(r)

                # Address
                r = to_address_row(d)
                if r:
                    address_rows.append(r)

                # Personal
                r = to_personal_row(d)
                if r:
                    personal_rows.append(r)

                # ID Card
                r = to_idcard_row(d)
                if r:
                    idcard_rows.append(r)

            # Upserts por tabla (cada una independiente)
            inserted_total = 0
            inserted_total += upsert_rows(engine, "bank", bank_rows)
            inserted_total += upsert_rows(engine, "payroll", payroll_rows)
            inserted_total += upsert_rows(engine, "address", address_rows)
            inserted_total += upsert_rows(engine, "personal", personal_rows)
            inserted_total += upsert_rows(engine, "idcard", idcard_rows)

            logger.info("✅ Upsert completado | filas insertadas/actualizadas: %s (bank=%s, payroll=%s, address=%s, personal=%s, idcard=%s)",
                        inserted_total, len(bank_rows), len(payroll_rows), len(address_rows), len(personal_rows), len(idcard_rows))

            # Avanzar el puntero incremental
            if max_ts:
                set_last_ts(engine, max_ts)

        except Exception as e:
            logger.exception("❌ Error en ciclo principal: %s", e)

        # Espera siguiente polling
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main_loop()
