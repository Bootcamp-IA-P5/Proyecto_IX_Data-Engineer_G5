#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Persister
-------------
Lee documentos "agregados" en MongoDB (colección aggregated_data),
filtrando solo los que están COMPLETOS (tienen todos los tipos requeridos),
y los vuelca en Postgres/Supabase con upserts idempotentes.

FIJOS EN ESTA VERSIÓN:
- Checkpoint robusto con (updated_at, _id) para evitar perder docs cuando
  hay empates de timestamp.
- Índices Mongo creados solo donde procede (sin tocar _id).
- Métricas de rendimiento por lote (tiempo y throughput).
- DDL autocontenida: crea tablas si no existen e instala RLS básica
  (SELECT para authenticated, ALL para postgres).
- Tablas hijas usan JSONB para datos (flexible ante cambios de esquema).

ENV por defecto (ajusta según tu despliegue):
- MONGO_URI=mongodb://mongo:27017/
- MONGO_DB=hrpro_db
- AGGREGATED_COLLECTION=aggregated_data
- STATE_COLLECTION=sql_sync_state
- REQUIRED_TYPES=personal,location,professional,bank,net
- AGGREGATED_TS_FIELD=updated_at
- AGGREGATED_GROUPING_FIELD=_grouping_key
- SQL_POLL_SECONDS=2
- SQL_BATCH_SIZE=1000
- SQL_SKEW_SECONDS=0           # retroceder un poco el reloj para tolerar clocks
- PG_HOST=aws-1-eu-west-1.pooler.supabase.com
- PG_PORT=5432
- PG_DATABASE=postgres
- PG_USER=
- PG_PASSWORD=
- PG_SSLMODE=require
- LOG_LEVEL=INFO
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch, Json
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from bson import ObjectId

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - sql-persister - %(levelname)s - %(message)s",
)
log = logging.getLogger("sql-persister")

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

def env_csv(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

settings: Dict[str, Any] = {
    # Mongo
    "MONGO_URI": os.getenv("MONGO_URI", "mongodb://mongo:27017/"),
    "MONGO_DB": os.getenv("MONGO_DB", "hrpro_db"),
    "AGGREGATED_COLLECTION": os.getenv("AGGREGATED_COLLECTION", "aggregated_data"),
    "STATE_COLLECTION": os.getenv("STATE_COLLECTION", "sql_sync_state"),

    # Gating y ordenación
    "REQUIRED_TYPES": env_csv("REQUIRED_TYPES", "personal,location,professional,bank,net"),
    "AGGREGATED_TS_FIELD": os.getenv("AGGREGATED_TS_FIELD", "updated_at"),
    "GROUPING_FIELD": os.getenv("AGGREGATED_GROUPING_FIELD", "_grouping_key"),

    # Batches / polling
    "SQL_POLL_SECONDS": float(os.getenv("SQL_POLL_SECONDS", "2")),
    "SQL_BATCH_SIZE": int(os.getenv("SQL_BATCH_SIZE", "1000")),
    "SQL_SKEW_SECONDS": int(os.getenv("SQL_SKEW_SECONDS", "0")),

    # Postgres
    "PG_HOST": os.getenv("PG_HOST", "aws-1-eu-west-1.pooler.supabase.com"),
    "PG_PORT": int(os.getenv("PG_PORT", "5432")),
    "PG_DATABASE": os.getenv("PG_DATABASE", "postgres"),
    "PG_USER": os.getenv("PG_USER", ""),
    "PG_PASSWORD": os.getenv("PG_PASSWORD", ""),
    "PG_SSLMODE": os.getenv("PG_SSLMODE", "require"),
}

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

UTC = timezone.utc


def ensure_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    # intentamos parsear ISO
    try:
        x = datetime.fromisoformat(str(dt))
        return x if x.tzinfo else x.replace(tzinfo=UTC)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Conexiones
# -----------------------------------------------------------------------------

def mongo_connect() -> Tuple[MongoClient, Collection, Collection]:
    log.info("🔗 MongoDB conectar…")
    mc = MongoClient(settings["MONGO_URI"], tz_aware=True)
    db = mc[settings["MONGO_DB"]]
    agg = db[settings["AGGREGATED_COLLECTION"]]
    state = db[settings["STATE_COLLECTION"]]

    # Índices en aggregated_data
    try:
        agg.create_index([(settings["GROUPING_FIELD"], ASCENDING)], name="idx_grouping_key", background=True)
        agg.create_index([(settings["AGGREGATED_TS_FIELD"], ASCENDING)], name="idx_updated_at", background=True)
        agg.create_index(
            [(settings["GROUPING_FIELD"], ASCENDING), (settings["AGGREGATED_TS_FIELD"], ASCENDING)],
            name="idx_grouping_updated",
            background=True,
        )
        log.debug("Índices Mongo verificados/creados")
    except Exception as e:
        log.warning("No se pudieron crear índices Mongo: %s", e)

    # No crear índice en _id (Mongo ya lo tiene y da error si se fuerza opciones)
    return mc, agg, state


def pg_connect():
    log.info(
        "🔗 Postgres conectar | host=%s db=%s sslmode=%s",
        settings["PG_HOST"], settings["PG_DATABASE"], settings["PG_SSLMODE"],
    )
    conn = psycopg2.connect(
        host=settings["PG_HOST"],
        port=settings["PG_PORT"],
        dbname=settings["PG_DATABASE"],
        user=settings["PG_USER"],
        password=settings["PG_PASSWORD"],
        sslmode=settings["PG_SSLMODE"],
    )
    conn.autocommit = False
    log.info("✅ Postgres OK")
    return conn


# -----------------------------------------------------------------------------
# Checkpoint (ts, _id)
# -----------------------------------------------------------------------------

def get_checkpoint(state_col: Collection) -> Tuple[Optional[datetime], Optional[str]]:
    doc = state_col.find_one({"_id": "checkpoint"})
    if not doc:
        return None, None
    ts = ensure_tz(doc.get("ts"))
    last_id = doc.get("last_id")
    return ts, last_id


def set_checkpoint(state_col: Collection, ts: Optional[datetime], last_id: Optional[str]) -> None:
    state_col.update_one(
        {"_id": "checkpoint"},
        {"$set": {"ts": ts.isoformat() if ts else None, "last_id": last_id}},
        upsert=True,
    )


# -----------------------------------------------------------------------------
# Fetch batch
# -----------------------------------------------------------------------------

def fetch_batch(
    agg: Collection,
    since_ts: Optional[datetime],
    since_id: Optional[str],
    limit: int,
    required_types: Iterable[str],
    ts_field: str,
) -> List[Dict[str, Any]]:
    filt: Dict[str, Any] = {"types_received": {"$all": list(required_types)}}

    if since_ts is not None:
        # Filtro robusto: (ts > last_ts) OR (ts == last_ts AND _id > last_id)
        or_cond: List[Dict[str, Any]] = [{ts_field: {"$gt": since_ts}}]
        if since_id:
            try:
                or_cond.append({ts_field: since_ts, "_id": {"$gt": ObjectId(since_id)}})
            except Exception:
                or_cond.append({ts_field: since_ts})
        else:
            or_cond.append({ts_field: since_ts})
        filt["$or"] = or_cond

    cur = (
        agg.find(filt)
        .sort([(ts_field, 1), ("_id", 1)])
        .limit(limit)
    )

    return list(cur)


# -----------------------------------------------------------------------------
# DDL y RLS en Postgres
# -----------------------------------------------------------------------------

DDL_SQL = r"""
CREATE TABLE IF NOT EXISTS public.persons (
  grouping_key TEXT PRIMARY KEY,
  passport     TEXT,
  email        TEXT,
  phone        TEXT,
  tax_id       TEXT,
  ssn          TEXT,
  created_at   TIMESTAMPTZ,
  updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.personal_data (
  grouping_key TEXT PRIMARY KEY REFERENCES public.persons(grouping_key) ON DELETE CASCADE,
  data         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.location_data (
  grouping_key TEXT PRIMARY KEY REFERENCES public.persons(grouping_key) ON DELETE CASCADE,
  data         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.professional_data (
  grouping_key TEXT PRIMARY KEY REFERENCES public.persons(grouping_key) ON DELETE CASCADE,
  data         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.bank_data (
  grouping_key TEXT PRIMARY KEY REFERENCES public.persons(grouping_key) ON DELETE CASCADE,
  data         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.net_data (
  grouping_key TEXT PRIMARY KEY REFERENCES public.persons(grouping_key) ON DELETE CASCADE,
  data         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_persons_updated_at      ON public.persons(updated_at);
CREATE INDEX IF NOT EXISTS idx_personal_data_updated   ON public.personal_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_location_data_updated   ON public.location_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_professional_data_updated ON public.professional_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_bank_data_updated       ON public.bank_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_net_data_updated        ON public.net_data(updated_at);
"""

RLS_SQL = r"""
DO $$ BEGIN
  EXECUTE 'ALTER TABLE public.persons ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'persons_select_authenticated') THEN
    CREATE POLICY persons_select_authenticated ON public.persons
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'persons_all_postgres') THEN
    CREATE POLICY persons_all_postgres ON public.persons
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;

  EXECUTE 'ALTER TABLE public.personal_data ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'personal_select_authenticated') THEN
    CREATE POLICY personal_select_authenticated ON public.personal_data
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'personal_all_postgres') THEN
    CREATE POLICY personal_all_postgres ON public.personal_data
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;

  EXECUTE 'ALTER TABLE public.location_data ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'location_select_authenticated') THEN
    CREATE POLICY location_select_authenticated ON public.location_data
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'location_all_postgres') THEN
    CREATE POLICY location_all_postgres ON public.location_data
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;

  EXECUTE 'ALTER TABLE public.professional_data ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'professional_select_authenticated') THEN
    CREATE POLICY professional_select_authenticated ON public.professional_data
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'professional_all_postgres') THEN
    CREATE POLICY professional_all_postgres ON public.professional_data
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;

  EXECUTE 'ALTER TABLE public.bank_data ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'bank_select_authenticated') THEN
    CREATE POLICY bank_select_authenticated ON public.bank_data
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'bank_all_postgres') THEN
    CREATE POLICY bank_all_postgres ON public.bank_data
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;

  EXECUTE 'ALTER TABLE public.net_data ENABLE ROW LEVEL SECURITY';
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'net_select_authenticated') THEN
    CREATE POLICY net_select_authenticated ON public.net_data
      FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE polname = 'net_all_postgres') THEN
    CREATE POLICY net_all_postgres ON public.net_data
      FOR ALL TO postgres USING (true) WITH CHECK (true);
  END IF;
END $$;
"""


def ensure_tables_and_rls(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_SQL)
        conn.commit()
    # RLS puede fallar si el rol no tiene permisos → lo registramos pero no abortamos
    try:
        with conn.cursor() as cur:
            cur.execute(RLS_SQL)
            conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning("RLS no aplicada (permiso/entorno): %s", e)


# -----------------------------------------------------------------------------
# Extracción de campos
# -----------------------------------------------------------------------------

def _get_grouping_key(doc: Dict[str, Any]) -> Optional[str]:
    for k in (settings["GROUPING_FIELD"], "grouping_key", "id", "_id"):
        v = doc.get(k)
        if v:
            return str(v)
    return None


def _get_identifiers(doc: Dict[str, Any]) -> Dict[str, Optional[str]]:
    ident = doc.get("identifiers", {}) or {}
    return {
        "passport": (ident.get("passport") or doc.get("passport")) or None,
        "email": (ident.get("email") or doc.get("email")) or None,
        "phone": (ident.get("phone") or doc.get("phone")) or None,
        "tax_id": (ident.get("tax_id") or doc.get("tax_id")) or None,
        "ssn": (ident.get("ssn") or doc.get("ssn")) or None,
    }


def _get_payload(doc: Dict[str, Any], name: str) -> Any:
    payloads = doc.get("payloads") or {}
    v = payloads.get(name)
    if v is None:
        v = doc.get(name)
    return v


def _is_empty_payload(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, (str, bytes)):
        return len(v) == 0
    if isinstance(v, dict):
        # dict vacío o todos sus valores vacíos
        return all(_is_empty_payload(x) for x in v.values()) if v else True
    if isinstance(v, list):
        return all(_is_empty_payload(x) for x in v) if v else True
    return False


# -----------------------------------------------------------------------------
# Upsert en Postgres
# -----------------------------------------------------------------------------

PERSONS_SQL = (
    """
    INSERT INTO public.persons (
        grouping_key, passport, email, phone, tax_id, ssn, created_at, updated_at
    ) VALUES (%(grouping_key)s, %(passport)s, %(email)s, %(phone)s, %(tax_id)s, %(ssn)s, %(created_at)s, %(updated_at)s)
    ON CONFLICT (grouping_key) DO UPDATE SET
      passport   = EXCLUDED.passport,
      email      = EXCLUDED.email,
      phone      = EXCLUDED.phone,
      tax_id     = EXCLUDED.tax_id,
      ssn        = EXCLUDED.ssn,
      created_at = COALESCE(public.persons.created_at, EXCLUDED.created_at),
      updated_at = EXCLUDED.updated_at
    """
)

CHILD_SQL_TMPL = (
    """
    INSERT INTO {table} (
        grouping_key, data, updated_at
    ) VALUES (%(grouping_key)s, %(data)s, %(updated_at)s)
    ON CONFLICT (grouping_key) DO UPDATE SET
      data      = EXCLUDED.data,
      updated_at = EXCLUDED.updated_at
    """
)

CHILD_TABLES = {
    "personal": "public.personal_data",
    "location": "public.location_data",
    "professional": "public.professional_data",
    "bank": "public.bank_data",
    "net": "public.net_data",
}


def upsert_batch_pg(conn, docs: List[Dict[str, Any]], ts_field: str) -> Tuple[int, Dict[str, int], Optional[datetime]]:
    counts = {"persons": 0, "personal": 0, "location": 0, "professional": 0, "bank": 0, "net": 0}
    total = 0
    max_ts: Optional[datetime] = None

    persons_rows: List[Dict[str, Any]] = []
    child_rows: Dict[str, List[Dict[str, Any]]] = {k: [] for k in CHILD_TABLES}

    for d in docs:
        grouping_key = _get_grouping_key(d)
        if not grouping_key:
            continue  # sin clave agrupadora, no volcamos

        created_at = ensure_tz(d.get("created_at"))
        updated_at = ensure_tz(d.get(ts_field))
        if updated_at is None:
            # si falta, usamos ahora para no dejar nulo
            updated_at = datetime.now(tz=UTC)
        if created_at is None:
            created_at = updated_at

        if (max_ts is None) or (updated_at > max_ts):
            max_ts = updated_at

        ident = _get_identifiers(d)
        persons_rows.append(
            {
                "grouping_key": grouping_key,
                "passport": ident.get("passport"),
                "email": ident.get("email"),
                "phone": ident.get("phone"),
                "tax_id": ident.get("tax_id"),
                "ssn": ident.get("ssn"),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

        # Hijas (si hay payloads no vacíos)
        for name in CHILD_TABLES:
            payload = _get_payload(d, name)
            if _is_empty_payload(payload):
                continue
            child_rows[name].append(
                {
                    "grouping_key": grouping_key,
                    "data": Json(payload),
                    "updated_at": updated_at,
                }
            )

    with conn.cursor() as cur:
        # persons
        if persons_rows:
            execute_batch(cur, PERSONS_SQL, persons_rows, page_size=1000)
            counts["persons"] += len(persons_rows)
            total += len(persons_rows)

        # hijas
        for name, rows in child_rows.items():
            if not rows:
                continue
            sql = CHILD_SQL_TMPL.format(table=CHILD_TABLES[name])
            execute_batch(cur, sql, rows, page_size=1000)
            counts[name] += len(rows)
            total += len(rows)

    conn.commit()
    return total, counts, max_ts


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------

def main() -> None:
    req_types = settings["REQUIRED_TYPES"]
    log.info(
        "🚀 Iniciando SQL Persister | poll=%.1fs batch=%d req_types=%s",
        settings["SQL_POLL_SECONDS"], settings["SQL_BATCH_SIZE"], ",".join(req_types)
    )

    mc, agg, state = mongo_connect()

    # Conexión a Postgres (si falla, registramos y reintentamos)
    conn: Optional[psycopg2.extensions.connection] = None
    while conn is None:
        try:
            conn = pg_connect()
        except Exception as e:
            log.error("❌ No se pudo conectar a Postgres: %s", e)
            time.sleep(3)

    ensure_tables_and_rls(conn)

    ts_field = settings["AGGREGATED_TS_FIELD"]

    while True:
        try:
            last_ts, last_id = get_checkpoint(state)

            # tolerancia a relojes (opcional)
            skew = int(settings["SQL_SKEW_SECONDS"]) or 0
            query_ts = last_ts
            query_id = last_id
            if last_ts and skew > 0:
                query_ts = last_ts - timedelta(seconds=skew)
                query_id = None  # al retroceder, reiniciamos desempate para ese ts

            docs = fetch_batch(
                agg,
                query_ts,
                query_id,
                settings["SQL_BATCH_SIZE"],
                req_types,
                ts_field,
            )

            if not docs:
                msg_ts = last_ts.isoformat() if last_ts else "None"
                log.info("⏳ Sin novedades (>= %s). Dormimos %.1fs", msg_ts, settings["SQL_POLL_SECONDS"])
                time.sleep(settings["SQL_POLL_SECONDS"])
                continue

            tic = time.perf_counter()
            total, counts, max_ts = upsert_batch_pg(conn, docs, ts_field)
            toc = time.perf_counter()
            dur = toc - tic
            rps = (total / dur) if dur > 0 else float(total)

            # Avanzamos checkpoint al ÚLTIMO documento del lote (ordenado por ts,_id)
            last_doc = docs[-1]
            new_ts = ensure_tz(last_doc.get(ts_field)) or max_ts
            new_last_id = str(last_doc.get("_id")) if last_doc.get("_id") is not None else None
            set_checkpoint(state, new_ts, new_last_id)

            log.info(
                "✅ Lote Postgres: persons=%d | pers=%d loc=%d prof=%d bank=%d net=%d | filas=%d | ⏱️ %.3fs ~%.1f filas/s | checkpoint=%s / %s",
                counts["persons"], counts["personal"], counts["location"], counts["professional"], counts["bank"], counts["net"],
                total, dur, rps,
                new_ts.isoformat() if new_ts else None, new_last_id,
            )

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            log.error("❌ Error de conexión Postgres: %s", e)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            # reintento
            while conn is None:
                try:
                    conn = pg_connect()
                    ensure_tables_and_rls(conn)
                except Exception as e2:
                    log.error("❌ Reintento Postgres fallido: %s", e2)
                    time.sleep(3)

        except KeyboardInterrupt:
            log.info("🛑 Señal de cierre recibida. Finalizando…")
            break
        except Exception as e:
            log.exception("❌ Error ETL: %s", e)
            time.sleep(2)

    try:
        if conn:
            conn.close()
        mc.close()
    except Exception:
        pass
    log.info("👋 SQL Persister finalizado")


if __name__ == "__main__":
    main()
