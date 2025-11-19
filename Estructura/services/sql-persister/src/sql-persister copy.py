#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sql-persister.py
----------------
Persiste datos de MongoDB (colección "aggregated_data") en Postgres (Supabase).

Puntos clave:
- Construye la URL de SQLAlchemy con sslmode=require.
- Respeta PG_POOL_MODE (transaction/session => NullPool).
- Lee variables en minúsculas (user/password/host/port/dbname) y también PG_*.
- Hace sanity-check de conexiones y bucle principal con reintentos.

Requisitos (requirements.txt):
  - pymongo
  - python-dotenv
  - SQLAlchemy>=2
  - psycopg2-binary
  - tenacity (opcional; aquí no se usa para mantenerlo simple)
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
from typing import Any, Dict, Optional

from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import NullPool


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("sql-persister")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


log = setup_logger()


# ------------------------------------------------------------------------------
# Utilidades de entorno
# ------------------------------------------------------------------------------

def _env_first(*keys: str, default: Optional[str] = None) -> Optional[str]:
    """
    Devuelve la primera variable de entorno NO vacía entre las indicadas.
    Busca también variantes en mayúsculas/minúsculas.
    """
    for k in keys:
        # exacta
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
        # fallback upper y lower
        v = os.getenv(k.upper())
        if v is not None and str(v).strip() != "":
            return v
        v = os.getenv(k.lower())
        if v is not None and str(v).strip() != "":
            return v
    return default


def load_settings() -> Dict[str, Any]:
    load_dotenv()  # permite .env

    st: Dict[str, Any] = {}

    # Mongo
    st["MONGO_URI"] = _env_first("MONGO_URI", default="mongodb://mongo:27017")
    st["MONGO_DATABASE"] = _env_first("MONGO_DATABASE", default="hrpro_db")
    st["AGGREGATED_COLLECTION"] = _env_first("AGGREGATED_COLLECTION", default="aggregated_data")
    st["STATE_COLLECTION"] = _env_first("STATE_COLLECTION", default="_sql_sync_state")

    # Postgres (permitimos claves cortas y PG_*)
    st["PG_USER"] = _env_first("user", "PG_USER", default=None)
    st["PG_PASSWORD"] = _env_first("password", "PG_PASSWORD", default=None)
    st["PG_HOST"] = _env_first("host", "PG_HOST", default=None)
    st["PG_PORT"] = int(_env_first("port", "PG_PORT", default="5432") or 5432)
    st["PG_DATABASE"] = _env_first("dbname", "PG_DATABASE", default="postgres")
    st["PG_SCHEMA"] = _env_first("PG_SCHEMA", default="public")
    st["PG_SSLMODE"] = _env_first("PG_SSLMODE", default="require")  # fijo a 'require'
    st["PG_CONNECT_TIMEOUT"] = int(_env_first("connect_timeout", "PG_CONNECT_TIMEOUT", default="10") or 10)

    # Pool mode (define si desactivamos pool del cliente)
    # - transaction | session => usando pooler de Supabase => NullPool recomendado
    # - direct => conexión directa al clúster => puedes usar pool normal de SQLAlchemy
    st["PG_POOL_MODE"] = (_env_first("PG_POOL_MODE", default="transaction") or "transaction").lower()

    use_nullpool_env = _env_first("USE_SQLA_NULLPOOL", default=None)
    if use_nullpool_env is not None:
        st["USE_SQLA_NULLPOOL"] = use_nullpool_env.lower() not in ("0", "false", "no")
    else:
        st["USE_SQLA_NULLPOOL"] = st["PG_POOL_MODE"] in ("transaction", "session")

    # Tuning ETL
    st["SQL_BATCH_SIZE"] = int(_env_first("SQL_BATCH_SIZE", default="500") or 500)
    st["SQL_UPSERT_STRATEGY"] = _env_first("SQL_UPSERT_STRATEGY", default="upsert")

    # Validaciones mínimas
    missing = [k for k in ("PG_USER", "PG_PASSWORD", "PG_HOST") if not st.get(k)]
    if missing:
        raise RuntimeError(f"Faltan variables obligatorias de Postgres: {missing}. "
                           f"Define user/password/host (o PG_USER/PG_PASSWORD/PG_HOST) en el entorno.")

    return st


# ------------------------------------------------------------------------------
# Conexiones
# ------------------------------------------------------------------------------

def build_pg_url(st: Dict[str, Any]) -> str:
    """
    Construye la DATABASE_URL con sslmode=require (por petición explícita).
    """
    return (
        f"postgresql+psycopg2://{st['PG_USER']}:{st['PG_PASSWORD']}"
        f"@{st['PG_HOST']}:{st['PG_PORT']}/{st['PG_DATABASE']}"
        f"?sslmode={st['PG_SSLMODE']}&connect_timeout={st['PG_CONNECT_TIMEOUT']}"
    )


def make_pg_engine(st: Dict[str, Any]):
    url = build_pg_url(st)
    log.info("🔗 Conectando a Postgres (Supabase) vía pooler=%s | NullPool=%s",
             st["PG_POOL_MODE"], st["USE_SQLA_NULLPOOL"])
    engine = (
        create_engine(url, poolclass=NullPool, future=True)
        if st["USE_SQLA_NULLPOOL"]
        else create_engine(url, future=True)
    )
    # Sanity check + search_path
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("select 1")
            conn.exec_driver_sql(f"set search_path = {st['PG_SCHEMA']}, public")
        log.info("✅ Postgres OK | host=%s db=%s schema=%s",
                 st["PG_HOST"], st["PG_DATABASE"], st["PG_SCHEMA"])
    except OperationalError as e:
        log.error("❌ No se pudo conectar a Postgres: %s", str(e).strip())
        raise
    return engine


def make_mongo(st: Dict[str, Any]) -> MongoClient:
    log.info("🔗 Conectando a MongoDB…")
    try:
        client = MongoClient(st["MONGO_URI"], serverSelectionTimeoutMS=5000)
        # sanity check
        client.admin.command("ping")
        db = client[st["MONGO_DATABASE"]]
        _ = db[st["AGGREGATED_COLLECTION"]].name
        log.info("✅ Mongo OK | db=%s coll=%s chk=%s",
                 st["MONGO_DATABASE"], st["AGGREGATED_COLLECTION"], st["STATE_COLLECTION"])
        return client
    except PyMongoError as e:
        log.error("❌ No se pudo conectar a MongoDB: %s", str(e).strip())
        raise


# ------------------------------------------------------------------------------
# ETL (ejemplo mínimo de incremental)
# ------------------------------------------------------------------------------

def get_last_ts(db) -> Optional[datetime]:
    """
    Estado en Mongo (colección de estado). Si no existe, None.
    """
    doc = db[settings["STATE_COLLECTION"]].find_one({"_id": "last_ts"})
    if not doc:
        return None
    val = doc.get("value")
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(val)
    except Exception:
        return None


def set_last_ts(db, ts: datetime) -> None:
    db[settings["STATE_COLLECTION"]].update_one(
        {"_id": "last_ts"},
        {"$set": {"value": ts.isoformat()}},
        upsert=True,
    )


def fetch_batch(db, since: Optional[datetime], limit: int):
    """
    Lee de aggregated_data. Se asume que hay un campo temporal: updated_at o created_at.
    Ajusta al que tengas realmente.
    """
    coll = db[settings["AGGREGATED_COLLECTION"]]
    ts_field = os.getenv("AGGREGATED_TS_FIELD", "updated_at")

    filt = {}
    if since is not None:
        filt[ts_field] = {"$gt": since}

    cursor = (
        coll.find(filt)
        .sort(ts_field, 1)  # ascendente
        .limit(limit)
    )
    docs = list(cursor)
    return docs, ts_field


def upsert_batch_pg(engine, docs, ts_field: str):
    """
    Ejemplo de upsert genérico a una tabla 'aggregated_data' en el schema configurado.
    Adapta los campos/PK a tu modelo real en Postgres.
    """
    if not docs:
        return 0, None

    # Detecta último timestamp para checkpoint
    max_ts = None
    for d in docs:
        ts = d.get(ts_field)
        if isinstance(ts, datetime):
            if max_ts is None or ts > max_ts:
                max_ts = ts

    # Transforma doc -> columnas. Ejemplo plano en JSONB.
    # Supón una tabla:
    #   CREATE TABLE IF NOT EXISTS {schema}.aggregated_data (
    #       id TEXT PRIMARY KEY,
    #       payload JSONB NOT NULL,
    #       updated_at TIMESTAMPTZ
    #   );
    schema = settings["PG_SCHEMA"]
    table = "aggregated_data"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ
    );
    """
    rows = []
    for d in docs:
        _id = str(d.get("_id"))
        upd = d.get(ts_field)
        # normaliza a aware UTC si viene naive
        if isinstance(upd, datetime) and upd.tzinfo is None:
            upd = upd.replace(tzinfo=timezone.utc)
        rows.append((_id, json.dumps(d, default=str), upd))

    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
        # upsert
        ins = text(f"""
            INSERT INTO {schema}.{table} (id, payload, updated_at)
            VALUES (:id, CAST(:payload AS JSONB), :updated_at)
            ON CONFLICT (id) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
        """)
        conn.execute(ins, [{"id": r[0], "payload": r[1], "updated_at": r[2]} for r in rows])

    return len(rows), max_ts


# ------------------------------------------------------------------------------
# Bucle principal
# ------------------------------------------------------------------------------

def main_loop():
    log.info("🚀 Iniciando SQL Persister (Mongo ➜ Postgres) | modo incremental")
    client = make_mongo(settings)
    db = client[settings["MONGO_DATABASE"]]

    engine = make_pg_engine(settings)

    idle_secs = int(os.getenv("SQL_POLL_INTERVAL", "5") or 5)
    batch_size = settings["SQL_BATCH_SIZE"]

    while True:
        try:
            last_ts = get_last_ts(db)
            docs, ts_field = fetch_batch(db, last_ts, batch_size)

            if not docs:
                time.sleep(idle_secs)
                continue

            n, max_ts = upsert_batch_pg(engine, docs, ts_field)
            if n > 0 and max_ts is not None:
                set_last_ts(db, max_ts)
                log.info("✅ Persistidos %d docs | checkpoint=%s", n, max_ts.isoformat())
        except (SQLAlchemyError, PyMongoError) as e:
            log.error("❌ Error en ciclo ETL: %s", str(e).strip())
            # Si es conectividad Postgres, re-construye engine
            try:
                engine.dispose(close=True)
            except Exception:
                pass
            time.sleep(3)
            try:
                engine = make_pg_engine(settings)
            except Exception:
                # espera y reintenta en siguiente iteración
                time.sleep(5)
        except KeyboardInterrupt:
            log.info("🛑 Interrumpido por el usuario.")
            break
        except Exception as e:
            log.exception("❌ Error inesperado en main_loop: %s", e)
            time.sleep(5)


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        settings = load_settings()
    except Exception as e:
        log.error("❌ Configuración inválida: %s", e)
        sys.exit(1)

    try:
        main_loop()
    except OperationalError as e:
        log.error("❌ Error fatal de conexión a Postgres: %s", str(e).strip())
        sys.exit(2)
    except Exception as e:
        log.exception("❌ Falló la ejecución principal: %s", e)
        sys.exit(3)
