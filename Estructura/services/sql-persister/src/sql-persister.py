#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sql-persister.py
----------------
Persiste datos de MongoDB (colección "aggregated_data") en Postgres (Supabase) en modelo RELACIONAL
(sin JSONB), con tablas: persons, personal, location, professional, bank, net.

Características:
- Incremental por timestamp (updated_at por defecto) + filtro de personas COMPLETAS (5/5 tipos).
- Upsert por persona (PK = person_id = _grouping_key de Mongo).
- Integridad referencial (FKs desde tablas de detalle a persons).
- RLS habilitado al arranque: SELECT para 'authenticated' y escritura para el rol del pooler (por defecto 'postgres').
- Índice de Mongo asegurado: {is_complete:1, updated_at:1} para acelerar el incremental.
- Control de frecuencia por SQL_POLL_SECONDS (fallback SLEEP_SECONDS_EMPTY).
- Sin crear tablas extra (solo persons, personal, location, professional, bank, net).

ENV esperadas (en .env):
  # Mongo
  MONGO_URI=mongodb://mongo:27017
  MONGO_DATABASE=hrpro_db
  AGGREGATED_COLLECTION=aggregated_data
  AGGREGATED_TS_FIELD=updated_at

  # Filtro de tipos requeridos (por defecto los 5)
  REQUIRED_TYPES=personal,location,professional,bank,net

  # Postgres (Supabase Pooler)
  PG_USER=postgres
  PG_PASSWORD=...
  PG_HOST=aws-1-eu-west-1.pooler.supabase.com
  PG_PORT=6543
  PG_DATABASE=postgres
  PG_SCHEMA=public
  PG_SSLMODE=require
  PG_POOL_MODE=session   # session|transaction|direct  (session/transaction => NullPool)

  # Ciclo
  SQL_POLL_SECONDS=2.0
  SQL_BATCH_SIZE=1000
  LOG_LEVEL=INFO          # DEBUG para más detalle

  # RLS
  ENABLE_RLS=1
  RLS_READ_ROLE=authenticated
  RLS_WRITER_ROLE=postgres
"""

from __future__ import annotations

import os
import sys
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import NullPool


# =============================================================================
# Logging
# =============================================================================

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("sql-persister")
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level, logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

log = setup_logger()


# =============================================================================
# Utils entorno
# =============================================================================

def _env_first(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
        v = os.getenv(k.upper())
        if v is not None and str(v).strip() != "":
            return v
        v = os.getenv(k.lower())
        if v is not None and str(v).strip() != "":
            return v
    return default

def load_settings() -> Dict[str, Any]:
    load_dotenv()  # .env

    st: Dict[str, Any] = {}

    # Mongo
    st["MONGO_URI"] = _env_first("MONGO_URI", default="mongodb://mongo:27017")
    st["MONGO_DATABASE"] = _env_first("MONGO_DATABASE", default="hrpro_db")
    st["AGGREGATED_COLLECTION"] = _env_first("AGGREGATED_COLLECTION", default="aggregated_data")
    st["AGGREGATED_TS_FIELD"] = _env_first("AGGREGATED_TS_FIELD", default="updated_at")

    # Requeridos
    req = _env_first("REQUIRED_TYPES", default="personal,location,professional,bank,net")
    st["REQUIRED_TYPES"] = [t.strip() for t in (req or "").split(",") if t.strip()]

    # Postgres
    st["PG_USER"] = _env_first("PG_USER", "user")
    st["PG_PASSWORD"] = _env_first("PG_PASSWORD", "password")
    st["PG_HOST"] = _env_first("PG_HOST", "host")
    st["PG_PORT"] = int(_env_first("PG_PORT", "port", default="5432") or 5432)
    st["PG_DATABASE"] = _env_first("PG_DATABASE", "dbname", default="postgres")
    st["PG_SCHEMA"] = _env_first("PG_SCHEMA", default="public")
    st["PG_SSLMODE"] = _env_first("PG_SSLMODE", default="require")
    st["PG_POOL_MODE"] = (_env_first("PG_POOL_MODE", default="session") or "session").lower()
    st["USE_SQLA_NULLPOOL"] = st["PG_POOL_MODE"] in ("transaction", "session")

    # Ciclo
    poll = _env_first("SQL_POLL_SECONDS", default=None)
    if poll is None:
        # compat con var vieja
        poll = _env_first("SLEEP_SECONDS_EMPTY", default="2.0")
    st["SQL_POLL_SECONDS"] = float(poll or 2.0)
    st["SQL_BATCH_SIZE"] = int(_env_first("SQL_BATCH_SIZE", default="1000") or 1000)

    # RLS
    st["ENABLE_RLS"] = (_env_first("ENABLE_RLS", default="1") or "1").lower() not in ("0", "false", "no")
    st["RLS_READ_ROLE"] = _env_first("RLS_READ_ROLE", default="authenticated")
    st["RLS_WRITER_ROLE"] = _env_first("RLS_WRITER_ROLE", default="postgres")

    # Validaciones Postgres
    missing = [k for k in ("PG_USER", "PG_PASSWORD", "PG_HOST") if not st.get(k)]
    if missing:
        raise RuntimeError(f"Faltan variables obligatorias de Postgres: {missing}")

    return st


# =============================================================================
# Conexiones
# =============================================================================

def build_pg_url(st: Dict[str, Any]) -> str:
    return (
        f"postgresql+psycopg2://{st['PG_USER']}:{st['PG_PASSWORD']}"
        f"@{st['PG_HOST']}:{st['PG_PORT']}/{st['PG_DATABASE']}"
        f"?sslmode={st['PG_SSLMODE']}"
    )

def make_pg_engine(st: Dict[str, Any]):
    url = build_pg_url(st)
    log.info("🔗 Postgres conectar | host=%s db=%s schema=%s pooler=%s|direct nullpool=%s",
             st["PG_HOST"], st["PG_DATABASE"], st["PG_SCHEMA"], st["PG_POOL_MODE"], st["USE_SQLA_NULLPOOL"])
    engine = create_engine(url, poolclass=NullPool, future=True) if st["USE_SQLA_NULLPOOL"] else create_engine(url, future=True)
    with engine.connect() as conn:
        conn.exec_driver_sql("select 1")
        conn.exec_driver_sql(f"set search_path = {st['PG_SCHEMA']}, public")
    log.info("✅ Postgres OK")
    return engine

def make_mongo(st: Dict[str, Any]) -> MongoClient:
    log.info("🔗 MongoDB conectar…")
    client = MongoClient(st["MONGO_URI"], serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    log.info("✅ Mongo OK | db=%s coll=%s", st["MONGO_DATABASE"], st["AGGREGATED_COLLECTION"])
    return client


# =============================================================================
# Índices Mongo para acelerar el incremental
# =============================================================================

def ensure_mongo_indexes_for_persister(db, coll_name: str, ts_field: str):
    try:
        db[coll_name].create_index(
            [("is_complete", 1), (ts_field, 1)],
            name="idx_complete_updated"
        )
        # opcional: si alguna vez consultas sólo por ts
        # db[coll_name].create_index([(ts_field, 1)], name="idx_updated_only")
    except Exception as e:
        log.warning("⚠️ No se pudo asegurar índice Mongo idx_complete_updated: %s", e)


# =============================================================================
# DDL Postgres + RLS
# =============================================================================

DDL_SQL = """
-- PERSONS (maestra por persona)
CREATE TABLE IF NOT EXISTS {schema}.persons (
  person_id      TEXT PRIMARY KEY,
  grouping_key   TEXT UNIQUE,
  passport       TEXT,
  email          TEXT,
  phone          TEXT,
  tax_id         TEXT,
  ssn            TEXT,
  created_at     TIMESTAMPTZ,
  updated_at     TIMESTAMPTZ,
  is_complete    BOOLEAN DEFAULT FALSE,
  types_received TEXT[]
);

-- Tablas por dominio (1:1 con persons)
CREATE TABLE IF NOT EXISTS {schema}.personal (
  person_id   TEXT PRIMARY KEY REFERENCES {schema}.persons(person_id) ON DELETE CASCADE,
  fullname    TEXT,
  first_name  TEXT,
  last_name   TEXT,
  name        TEXT,
  surname     TEXT,
  email       TEXT,
  phone       TEXT,
  dob         TEXT,
  birthdate   TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.location (
  person_id    TEXT PRIMARY KEY REFERENCES {schema}.persons(person_id) ON DELETE CASCADE,
  address      TEXT,
  street       TEXT,
  city         TEXT,
  state        TEXT,
  province     TEXT,
  postal_code  TEXT,
  zip          TEXT,
  country      TEXT,
  lat          TEXT,
  lon          TEXT,
  latitude     TEXT,
  longitude    TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.professional (
  person_id        TEXT PRIMARY KEY REFERENCES {schema}.persons(person_id) ON DELETE CASCADE,
  company          TEXT,
  position         TEXT,
  job_title        TEXT,
  department       TEXT,
  salary           TEXT,
  contract_type    TEXT,
  experience_years TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.bank (
  person_id     TEXT PRIMARY KEY REFERENCES {schema}.persons(person_id) ON DELETE CASCADE,
  iban          TEXT,
  swift         TEXT,
  bic           TEXT,
  account_number TEXT,
  bank_name     TEXT,
  card_number   TEXT,
  card_brand    TEXT,
  card_exp      TEXT,
  card_holder   TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.net (
  person_id   TEXT PRIMARY KEY REFERENCES {schema}.persons(person_id) ON DELETE CASCADE,
  ip          TEXT,
  ipv4        TEXT,
  ipv6        TEXT,
  mac         TEXT,
  hostname    TEXT,
  domain      TEXT,
  url         TEXT,
  ssid        TEXT,
  user_agent  TEXT,
  browser     TEXT,
  os          TEXT
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_persons_email    ON {schema}.persons(email);
CREATE INDEX IF NOT EXISTS idx_persons_passport ON {schema}.persons(passport);
CREATE INDEX IF NOT EXISTS idx_persons_updated  ON {schema}.persons(updated_at);
"""

RLS_ENABLE_SQL = """
-- Habilitar RLS en tablas
ALTER TABLE {schema}.persons      ENABLE ROW LEVEL SECURITY;
ALTER TABLE {schema}.personal     ENABLE ROW LEVEL SECURITY;
ALTER TABLE {schema}.location     ENABLE ROW LEVEL SECURITY;
ALTER TABLE {schema}.professional ENABLE ROW LEVEL SECURITY;
ALTER TABLE {schema}.bank         ENABLE ROW LEVEL SECURITY;
ALTER TABLE {schema}.net          ENABLE ROW LEVEL SECURITY;

-- Política de lectura para 'authenticated'
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname='{schema}' AND tablename='persons' AND policyname='read_authenticated'
  ) THEN
    CREATE POLICY read_authenticated ON {schema}.persons FOR SELECT TO {read_role} USING (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='personal' AND policyname='read_authenticated') THEN
    CREATE POLICY read_authenticated ON {schema}.personal FOR SELECT TO {read_role} USING (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='location' AND policyname='read_authenticated') THEN
    CREATE POLICY read_authenticated ON {schema}.location FOR SELECT TO {read_role} USING (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='professional' AND policyname='read_authenticated') THEN
    CREATE POLICY read_authenticated ON {schema}.professional FOR SELECT TO {read_role} USING (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='bank' AND policyname='read_authenticated') THEN
    CREATE POLICY read_authenticated ON {schema}.bank FOR SELECT TO {read_role} USING (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='net' AND policyname='read_authenticated') THEN
    CREATE POLICY read_authenticated ON {schema}.net FOR SELECT TO {read_role} USING (true);
  END IF;
END $$;

-- Política de escritura para el rol del pooler (p.e. 'postgres')
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='persons' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.persons FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='personal' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.personal FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='location' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.location FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='professional' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.professional FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='bank' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.bank FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='{schema}' AND tablename='net' AND policyname='write_pooler') THEN
    CREATE POLICY write_pooler ON {schema}.net FOR ALL TO {write_role} USING (true) WITH CHECK (true);
  END IF;
END $$;
"""

def ensure_pg_schema_tables_rls(engine, schema: str, enable_rls: bool, read_role: str, write_role: str):
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL_SQL.format(schema=schema))
        if enable_rls:
            conn.exec_driver_sql(RLS_ENABLE_SQL.format(schema=schema, read_role=read_role, write_role=write_role))
    log.info("🧱 DDL ok (schema/tablas/índices%s)",
             " y RLS asegurados" if enable_rls else "")


# =============================================================================
# Extract (Mongo)
# =============================================================================

def project_fields() -> Dict[str, int]:
    # Solo traemos lo necesario
    return {
        "_id": 1,
        "_grouping_key": 1,
        "identifiers": 1,
        "types_received": 1,
        "is_complete": 1,
        "created_at": 1,
        "updated_at": 1,
        "data.personal": 1,
        "data.location": 1,
        "data.professional": 1,
        "data.bank": 1,
        "data.net": 1,
    }

def fetch_incremental_complete(db, coll_name: str, ts_field: str, since: Optional[datetime],
                               required_types: List[str], limit: int) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """
    Devuelve documentos de personas COMPLETAS (is_complete:true y types_received contiene todos required_types),
    con updated_at > since (si since no es None). Orden ascendente por ts_field.
    """
    filt: Dict[str, Any] = {
        "is_complete": True,
        "types_received": {"$all": required_types},
    }
    if since is not None:
        filt[ts_field] = {"$gt": since}

    cur = (
        db[coll_name]
        .find(filt, project_fields())
        .sort(ts_field, 1)
        .limit(limit)
    )
    docs = list(cur)

    max_ts = None
    for d in docs:
        ts = d.get(ts_field)
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if max_ts is None or ts > max_ts:
                max_ts = ts

    return docs, max_ts


# =============================================================================
# Transform helpers
# =============================================================================

def _s(val: Any) -> Optional[str]:
    """A texto o None (solo almacenamos strings en tablas de dominio)."""
    if val is None:
        return None
    return str(val)

def flatten_person(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Fila para persons."""
    person_id = doc.get("_grouping_key") or str(doc.get("_id"))
    identifiers = doc.get("identifiers", {}) or {}
    types = doc.get("types_received", []) or []

    created_at = doc.get("created_at")
    updated_at = doc.get("updated_at")

    # normaliza a aware UTC
    if isinstance(created_at, datetime) and created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    if isinstance(updated_at, datetime) and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    return {
        "person_id": person_id,
        "grouping_key": _s(doc.get("_grouping_key")),
        "passport": _s(identifiers.get("passport")),
        "email": _s(identifiers.get("email")),
        "phone": _s(identifiers.get("phone")),
        "tax_id": _s(identifiers.get("tax_id")),
        "ssn": _s(identifiers.get("ssn")),
        "created_at": created_at,
        "updated_at": updated_at,
        "is_complete": True,
        "types_received": types,
    }

def flatten_block(doc: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
    data = (doc.get("data") or {}).get(kind) or {}
    if not isinstance(data, dict):
        return None
    person_id = doc.get("_grouping_key") or str(doc.get("_id"))

    if kind == "personal":
        return {
            "person_id": person_id,
            "fullname": _s(data.get("fullname")),
            "first_name": _s(data.get("first_name")),
            "last_name": _s(data.get("last_name")),
            "name": _s(data.get("name")),
            "surname": _s(data.get("surname")),
            "email": _s(data.get("email")),
            "phone": _s(data.get("phone")),
            "dob": _s(data.get("dob")),
            "birthdate": _s(data.get("birthdate")),
        }
    if kind == "location":
        return {
            "person_id": person_id,
            "address": _s(data.get("address")),
            "street": _s(data.get("street")),
            "city": _s(data.get("city")),
            "state": _s(data.get("state")),
            "province": _s(data.get("province")),
            "postal_code": _s(data.get("postal_code")),
            "zip": _s(data.get("zip")),
            "country": _s(data.get("country")),
            "lat": _s(data.get("lat")),
            "lon": _s(data.get("lon")),
            "latitude": _s(data.get("latitude")),
            "longitude": _s(data.get("longitude")),
        }
    if kind == "professional":
        return {
            "person_id": person_id,
            "company": _s(data.get("company")),
            "position": _s(data.get("position")),
            "job_title": _s(data.get("job_title")),
            "department": _s(data.get("department")),
            "salary": _s(data.get("salary")),
            "contract_type": _s(data.get("contract_type")),
            "experience_years": _s(data.get("experience_years")),
        }
    if kind == "bank":
        return {
            "person_id": person_id,
            "iban": _s(data.get("iban")),
            "swift": _s(data.get("swift")),
            "bic": _s(data.get("bic")),
            "account_number": _s(data.get("account_number")),
            "bank_name": _s(data.get("bank_name")),
            "card_number": _s(data.get("card_number")),
            "card_brand": _s(data.get("card_brand")),
            "card_exp": _s(data.get("card_exp")),
            "card_holder": _s(data.get("card_holder")),
        }
    if kind == "net":
        return {
            "person_id": person_id,
            "ip": _s(data.get("ip")),
            "ipv4": _s(data.get("ipv4")),
            "ipv6": _s(data.get("ipv6")),
            "mac": _s(data.get("mac")),
            "hostname": _s(data.get("hostname")),
            "domain": _s(data.get("domain")),
            "url": _s(data.get("url")),
            "ssid": _s(data.get("ssid")),
            "user_agent": _s(data.get("user_agent")),
            "browser": _s(data.get("browser")),
            "os": _s(data.get("os")),
        }
    return None


# =============================================================================
# Load (Postgres) - upserts
# =============================================================================

def upsert_persons(conn, schema: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = text(f"""
        INSERT INTO {schema}.persons
          (person_id, grouping_key, passport, email, phone, tax_id, ssn,
           created_at, updated_at, is_complete, types_received)
        VALUES
          (:person_id, :grouping_key, :passport, :email, :phone, :tax_id, :ssn,
           :created_at, :updated_at, :is_complete, :types_received)
        ON CONFLICT (person_id) DO UPDATE SET
          grouping_key = EXCLUDED.grouping_key,
          passport = EXCLUDED.passport,
          email = EXCLUDED.email,
          phone = EXCLUDED.phone,
          tax_id = EXCLUDED.tax_id,
          ssn = EXCLUDED.ssn,
          created_at = COALESCE({schema}.persons.created_at, EXCLUDED.created_at),
          updated_at = EXCLUDED.updated_at,
          is_complete = EXCLUDED.is_complete,
          types_received = EXCLUDED.types_received
    """)
    conn.execute(sql, rows)
    return len(rows)

def upsert_block(conn, schema: str, table: str, pk: str, cols: List[str], rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    col_list = ", ".join(cols)
    params = ", ".join([f":{c}" for c in cols])
    update = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != pk])
    sql = text(f"""
        INSERT INTO {schema}.{table} ({col_list})
        VALUES ({params})
        ON CONFLICT ({pk}) DO UPDATE SET
          {update}
    """)
    conn.execute(sql, rows)
    return len(rows)


# =============================================================================
# Estado incremental en Mongo (colección de estado)
# =============================================================================

STATE_COLL = "_sql_sync_state"

def get_last_ts(db, ts_field: str) -> Optional[datetime]:
    doc = db.get_collection(STATE_COLL).find_one({"_id": "last_ts"})
    if not doc:
        return None
    val = doc.get("value")
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None

def set_last_ts(db, ts: datetime):
    db.get_collection(STATE_COLL).update_one(
        {"_id": "last_ts"},
        {"$set": {"value": ts.isoformat()}},
        upsert=True
    )


# =============================================================================
# Main loop
# =============================================================================

def main():
    settings = load_settings()

    log.info("🚀 Iniciando SQL Persister | poll=%.1fs batch=%d req_types=%s",
             settings["SQL_POLL_SECONDS"], settings["SQL_BATCH_SIZE"], ",".join(settings["REQUIRED_TYPES"]))

    # Conexiones
    mongo = make_mongo(settings)
    db = mongo[settings["MONGO_DATABASE"]]
    ensure_mongo_indexes_for_persister(db, settings["AGGREGATED_COLLECTION"], settings["AGGREGATED_TS_FIELD"])

    engine = make_pg_engine(settings)
    ensure_pg_schema_tables_rls(
        engine,
        schema=settings["PG_SCHEMA"],
        enable_rls=settings["ENABLE_RLS"],
        read_role=settings["RLS_READ_ROLE"],
        write_role=settings["RLS_WRITER_ROLE"],
    )

    ts_field = settings["AGGREGATED_TS_FIELD"]
    batch = settings["SQL_BATCH_SIZE"]
    sleep_s = settings["SQL_POLL_SECONDS"]
    required = settings["REQUIRED_TYPES"]

    while True:
        try:
            last_ts = get_last_ts(db, ts_field)
            docs, max_ts = fetch_incremental_complete(
                db, settings["AGGREGATED_COLLECTION"], ts_field, last_ts, required, batch
            )

            if not docs:
                log.debug("⏳ Sin novedades (>= %s). Dormimos %.1fs", last_ts, sleep_s)
                time.sleep(sleep_s)
                continue

            # Transform
            persons_rows: List[Dict[str, Any]] = []
            personal_rows: List[Dict[str, Any]] = []
            location_rows: List[Dict[str, Any]] = []
            professional_rows: List[Dict[str, Any]] = []
            bank_rows: List[Dict[str, Any]] = []
            net_rows: List[Dict[str, Any]] = []

            for d in docs:
                persons_rows.append(flatten_person(d))

                pb = flatten_block(d, "personal")
                if pb: personal_rows.append(pb)

                lb = flatten_block(d, "location")
                if lb: location_rows.append(lb)

                prb = flatten_block(d, "professional")
                if prb: professional_rows.append(prb)

                bb = flatten_block(d, "bank")
                if bb: bank_rows.append(bb)

                nb = flatten_block(d, "net")
                if nb: net_rows.append(nb)

            # Load
            with engine.begin() as conn:
                n_persons = upsert_persons(conn, settings["PG_SCHEMA"], persons_rows)
                n_pers = upsert_block(conn, settings["PG_SCHEMA"], "personal", "person_id",
                                      ["person_id","fullname","first_name","last_name","name","surname","email","phone","dob","birthdate"],
                                      personal_rows)
                n_loc = upsert_block(conn, settings["PG_SCHEMA"], "location", "person_id",
                                     ["person_id","address","street","city","state","province","postal_code","zip","country","lat","lon","latitude","longitude"],
                                     location_rows)
                n_prof = upsert_block(conn, settings["PG_SCHEMA"], "professional", "person_id",
                                      ["person_id","company","position","job_title","department","salary","contract_type","experience_years"],
                                      professional_rows)
                n_bank = upsert_block(conn, settings["PG_SCHEMA"], "bank", "person_id",
                                      ["person_id","iban","swift","bic","account_number","bank_name","card_number","card_brand","card_exp","card_holder"],
                                      bank_rows)
                n_net = upsert_block(conn, settings["PG_SCHEMA"], "net", "person_id",
                                     ["person_id","ip","ipv4","ipv6","mac","hostname","domain","url","ssid","user_agent","browser","os"],
                                     net_rows)

            # checkpoint
            if max_ts:
                set_last_ts(db, max_ts)

            log.info("✅ Lote Postgres: persons=%d | pers=%d loc=%d prof=%d bank=%d net=%d | checkpoint=%s",
                     len(persons_rows), n_pers, n_loc, n_prof, n_bank, n_net,
                     max_ts.isoformat() if max_ts else None)

        except (SQLAlchemyError, PyMongoError) as e:
            log.error("❌ Error en ciclo ETL: %s", str(e).strip())
            time.sleep(max(2.0, sleep_s))
        except KeyboardInterrupt:
            log.info("🛑 Interrumpido por el usuario.")
            break
        except Exception as e:
            log.exception("❌ Error inesperado: %s", e)
            time.sleep(max(2.0, sleep_s))


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except OperationalError as e:
        log.error("❌ Error fatal de conexión a Postgres: %s", str(e).strip())
        sys.exit(2)
    except Exception as e:
        log.exception("❌ Falló la ejecución principal: %s", e)
        sys.exit(3)
