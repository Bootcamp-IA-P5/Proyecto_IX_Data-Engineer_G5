# -*- coding: utf-8 -*-
"""
sql_persister_metrics.py
Resumen de:
- Totales en Mongo (raw_total, raw_pending, persons_total, ALL5)
- Totales en Postgres por tabla (persons/personal/location/professional/bank/net)
- Max(updated_at) en persons (para ver "recientez")
Lee .env. No imprime secretos.

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
- SQL_SKEW_SECONDS=0 # retroceder un poco el reloj para tolerar clocks
- PG_HOST=aws-1-eu-west-1.pooler.supabase.com
- PG_PORT=5432
- PG_DATABASE=postgres
- PG_USER=
- PG_PASSWORD=
- PG_SSLMODE=require
- LOG_LEVEL=INFO
"""

import os
import sys
import time
import re
import logging
from datetime import datetime, date
from typing import Dict, Any, Tuple, List

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
import psycopg2
import psycopg2.extras
from psycopg2 import sql

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "hrpro_db")
AGG_COLLECTION = os.getenv("AGGREGATED_COLLECTION", "aggregated_data")
STATE_COLLECTION = os.getenv("STATE_COLLECTION", "sql_sync_state")
AGG_TS_FIELD = os.getenv("AGGREGATED_TS_FIELD", "updated_at")
AGG_GROUP_FIELD = os.getenv("AGGREGATED_GROUPING_FIELD", "_grouping_key")

REQUIRED_TYPES = set(
    [t.strip() for t in os.getenv("REQUIRED_TYPES", "personal,location,professional,bank,net").split(",") if t.strip()]
)

POLL_SECONDS = float(os.getenv("SQL_POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", "1000"))
SKEW_SECONDS = int(os.getenv("SQL_SKEW_SECONDS", "0"))  # tolerancia reloj

PG_HOST = os.getenv("PG_HOST", "aws-1-eu-west-1.pooler.supabase.com")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

ENABLE_RLS = os.getenv("ENABLE_RLS", "true").lower() in {"1", "true", "yes"}
RLS_ROLE_READ = os.getenv("RLS_ROLE_READ", "authenticated")    # rol cliente supabase
RLS_ROLE_ADMIN = os.getenv("RLS_ROLE_ADMIN", "postgres")       # rol admin interno

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Map de tablas por tipo
TYPE_TABLES = {
    "personal": "personal_data",
    "location": "location_data",
    "professional": "professional_data",
    "bank": "bank_data",
    "net": "net_data",
}

# Campos "identificadores" típicos que solemos extraer hacia persons
PERSON_KEYS = ["passport", "email", "phone", "tax_id", "ssn"]

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - sql-persister - %(levelname)s - %(message)s",
)
log = logging.getLogger("sql-persister")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

_SNAKE_RE_1 = re.compile(r"[^a-zA-Z0-9_]+")
_SNAKE_RE_2 = re.compile(r"__+")


def snake(name: str) -> str:
    """Sanitiza a snake_case apto para columnas."""
    if not name:
        return "col"
    s = name.strip()
    s = _SNAKE_RE_1.sub("_", s)
    s = s.strip("_").lower()
    s = _SNAKE_RE_2.sub("_", s)
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def flatten(d: Dict[str, Any], parent: str = "", sep: str = "__") -> Dict[str, Any]:
    """Aplana diccionarios anidados en una capa con sep."""
    out = {}
    for k, v in (d or {}).items():
        if v is None:
            continue
        kk = snake(f"{parent}{sep}{k}" if parent else k)
        if isinstance(v, dict):
            out.update(flatten(v, kk, sep=sep))
        elif isinstance(v, list):
            # representamos lista como texto simple (CSV) para mantener 100% columnas
            out[kk] = ",".join([str(x) for x in v])
        else:
            out[kk] = v
    return out


def infer_pg_type(value: Any) -> str:
    """Inferencia simple y segura de tipo PG."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    if isinstance(value, (datetime, )):
        return "TIMESTAMPTZ"
    if isinstance(value, (date, )):
        return "DATE"
    # default seguro
    return "TEXT"


def pg_connect():
    """Conexión Postgres."""
    log.info("🔗 Postgres conectar | host=%s db=%s schema=%s", PG_HOST, PG_DB, PG_SCHEMA)
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
    )
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(PG_SCHEMA)))
    log.info("✅ Postgres OK")
    return conn


def mongo_connect() -> Tuple[MongoClient, Collection, Collection]:
    """Conecta a Mongo y devuelve colecciones (agg, state). Crea índices donde toque."""
    log.info("🔗 MongoDB conectar…")
    mc = MongoClient(MONGO_URI)
    db = mc[MONGO_DB]
    agg = db[AGG_COLLECTION]
    state = db[STATE_COLLECTION]

    # Índices útiles en aggregated_data
    agg.create_index([(AGG_GROUP_FIELD, ASCENDING)], name="idx_grouping_key", background=True)
    agg.create_index([(AGG_TS_FIELD, ASCENDING)], name="idx_updated_at", background=True)
    agg.create_index([(AGG_GROUP_FIELD, ASCENDING), (AGG_TS_FIELD, ASCENDING)], name="idx_grouping_updated", background=True)

    # NO crear índice sobre _id (Mongo ya lo tiene y daría error si forzamos opciones)
    log.debug("Mongo índices verificados")

    return mc, agg, state


# ------------------------------------------------------------------------------
# DDL base (sin JSONB) + evolución de esquema
# ------------------------------------------------------------------------------

def ensure_base_tables(conn):
    """Crea tablas base si no existen (sin JSONB) e índices básicos."""
    with conn.cursor() as cur:
        # persons
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.persons (
            id TEXT PRIMARY KEY,
            grouping_key TEXT NOT NULL,
            passport TEXT,
            email TEXT,
            phone TEXT,
            tax_id TEXT,
            ssn TEXT,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
        );
        """)
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS persons_grouping_key_uniq ON {PG_SCHEMA}.persons(grouping_key);")

        # hijas (1 fila por persona y tipo). PK = person_id
        for tbl in TYPE_TABLES.values():
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{tbl} (
                person_id TEXT PRIMARY KEY REFERENCES {PG_SCHEMA}.persons(id) ON DELETE CASCADE,
                grouping_key TEXT NOT NULL,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS {tbl}_grouping_key_idx ON {PG_SCHEMA}.{tbl}(grouping_key);")

    conn.commit()


def ensure_rls(conn):
    """Activa RLS (opcional) y crea políticas razonables."""
    if not ENABLE_RLS:
        return
    try:
        with conn.cursor() as cur:
            # RLS ON
            for tbl in ["persons"] + list(TYPE_TABLES.values()):
                cur.execute(sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;").format(sql.Identifier(PG_SCHEMA, tbl)))

            # Política admin (postgres) acceso total
            for tbl in ["persons"] + list(TYPE_TABLES.values()):
                cur.execute(sql.SQL("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_policies
                             WHERE schemaname = %(schema)s AND tablename = %(table)s AND policyname = %(pname)s
                        ) THEN
                            EXECUTE format('CREATE POLICY %I ON %I.%I FOR ALL TO %I USING (true) WITH CHECK (true);',
                                %(pname)s, %(schema)s, %(table)s, %(role)s);
                        END IF;
                    END$$;
                """), {
                    "schema": PG_SCHEMA,
                    "table": tbl,
                    "pname": f"{tbl}_admin_all",
                    "role": RLS_ROLE_ADMIN,
                })

            # Política lectura para authenticated
            for tbl in ["persons"] + list(TYPE_TABLES.values()):
                cur.execute(sql.SQL("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_policies
                             WHERE schemaname = %(schema)s AND tablename = %(table)s AND policyname = %(pname)s
                        ) THEN
                            EXECUTE format('CREATE POLICY %I ON %I.%I FOR SELECT TO %I USING (true);',
                                %(pname)s, %(schema)s, %(table)s, %(role)s);
                        END IF;
                    END IF;
                    END$$;
                """), {
                    "schema": PG_SCHEMA,
                    "table": tbl,
                    "pname": f"{tbl}_read_auth",
                    "role": RLS_ROLE_READ,
                })
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning("⚠️ No se pudo configurar RLS automáticamente: %s", e)


def current_columns(conn, table: str) -> Dict[str, str]:
    """Devuelve columnas existentes {nombre: tipo} del esquema destino."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s;
        """, (PG_SCHEMA, table))
        return {row["column_name"]: row["data_type"] for row in cur.fetchall()}


def ensure_columns(conn, table: str, new_cols: Dict[str, Any]):
    """Añade columnas que falten basadas en los valores a insertar."""
    if not new_cols:
        return
    cols_now = current_columns(conn, table)
    to_add: List[Tuple[str, str]] = []

    for k, v in new_cols.items():
        col = snake(k)
        if col in cols_now:
            continue
        pg_type = infer_pg_type(v)
        to_add.append((col, pg_type))

    if not to_add:
        return

    with conn.cursor() as cur:
        for col, t in to_add:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {};")
                .format(sql.Identifier(PG_SCHEMA), sql.Identifier(table), sql.Identifier(col), sql.SQL(t))
            )
    conn.commit()


# ------------------------------------------------------------------------------
# Upserts
# ------------------------------------------------------------------------------

def pick_identifiers(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extrae identificadores conocidos desde top-level o subdocumentos típicos."""
    out = {}
    for k in PERSON_KEYS:
        v = doc.get(k)
        if v is None:
            # buscar en 'personal' si existiese
            v = (doc.get("personal") or {}).get(k)
        out[k] = v
    return out


def compute_grouping_key(doc: Dict[str, Any], ids: Dict[str, Any]) -> str:
    """Usa _grouping_key si viene, sino compone a partir de identificadores; si nada, cae a _id."""
    g = doc.get(AGG_GROUP_FIELD)
    if g:
        return str(g)

    # prioridad por identificadores
    if ids.get("passport"):
        return f"passport::{ids['passport']}"
    if ids.get("email"):
        return f"email::{ids['email']}"
    if ids.get("phone"):
        return f"phone::{ids['phone']}"
    if ids.get("tax_id"):
        return f"tax::{ids['tax_id']}"
    if ids.get("ssn"):
        return f"ssn::{ids['ssn']}"

    # fallback
    return str(doc.get("_id"))


def upsert_person(conn, doc: Dict[str, Any]):
    ids = pick_identifiers(doc)
    grouping_key = compute_grouping_key(doc, ids)
    person_id = grouping_key  # decidimos usar el grouping como id estable

    created_at = doc.get("created_at")
    updated_at = doc.get("updated_at")

    # Asegurar tabla creada
    ensure_base_tables(conn)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                INSERT INTO {}.persons (id, grouping_key, passport, email, phone, tax_id, ssn, created_at, updated_at)
                VALUES (%(id)s, %(grouping_key)s, %(passport)s, %(email)s, %(phone)s, %(tax_id)s, %(ssn)s, %(created_at)s, %(updated_at)s)
                ON CONFLICT (id) DO UPDATE SET
                    grouping_key = EXCLUDED.grouping_key,
                    passport     = COALESCE(EXCLUDED.passport, {}.persons.passport),
                    email        = COALESCE(EXCLUDED.email, {}.persons.email),
                    phone        = COALESCE(EXCLUDED.phone, {}.persons.phone),
                    tax_id       = COALESCE(EXCLUDED.tax_id, {}.persons.tax_id),
                    ssn          = COALESCE(EXCLUDED.ssn, {}.persons.ssn),
                    created_at   = COALESCE({}.persons.created_at, EXCLUDED.created_at),
                    updated_at   = EXCLUDED.updated_at;
            """).format(
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(PG_SCHEMA),
            ),
            {
                "id": person_id,
                "grouping_key": grouping_key,
                "passport": ids.get("passport"),
                "email": ids.get("email"),
                "phone": ids.get("phone"),
                "tax_id": ids.get("tax_id"),
                "ssn": ids.get("ssn"),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
    conn.commit()
    return person_id, grouping_key


def upsert_type_table(conn, table: str, person_id: str, grouping_key: str,
                      payload: Dict[str, Any], created_at: Any, updated_at: Any):
    """Upsert en una tabla de tipo. Añade columnas nuevas si aparecen campos nuevos."""
    if payload is None:
        payload = {}
    flat = flatten(payload)

    # columnas base + dinámicas
    fixed = {
        "person_id": person_id,
        "grouping_key": grouping_key,
        "created_at": created_at,
        "updated_at": updated_at,
    }

    # Asegurar tabla base
    ensure_base_tables(conn)

    # Añadir columnas nuevas si faltan
    ensure_columns(conn, table, flat)

    # Construir SQL dinámico
    cols = list(fixed.keys()) + list(flat.keys())
    cols_sql = [sql.Identifier(c) for c in cols]
    vals_sql = [sql.Placeholder(c) for c in cols]

    updates = []
    for c in cols:
        if c == "person_id":
            continue
        if c == "created_at":
            # mantener el created_at previo
            updates.append(sql.SQL("{} = COALESCE({table}.{col}, EXCLUDED.{col})").format(
                sql.Identifier(c), table=sql.Identifier(PG_SCHEMA, table), col=sql.Identifier(c)
            ))
        else:
            updates.append(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)))

    params = {**fixed, **flat}

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT (person_id) DO UPDATE SET {};")
            .format(
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(table),
                sql.SQL(',').join(cols_sql),
                sql.SQL(',').join(vals_sql),
                sql.SQL(',').join(updates),
            ),
            params
        )
    conn.commit()


# ------------------------------------------------------------------------------
# Ingest loop
# ------------------------------------------------------------------------------

def load_checkpoint(state: Collection) -> Tuple[datetime | None, Any | None]:
    doc = state.find_one({"_id": "checkpoint"})
    if not doc:
        return None, None
    return doc.get("last_updated_at"), doc.get("last_id")


def save_checkpoint(state: Collection, last_updated_at: Any, last_id: Any):
    state.update_one(
        {"_id": "checkpoint"},
        {"$set": {"last_updated_at": last_updated_at, "last_id": last_id, "saved_at": datetime.utcnow()}},
        upsert=True,
    )


def query_batch(agg: Collection, since_ts: datetime | None, since_id: Any | None) -> List[Dict[str, Any]]:
    q: Dict[str, Any] = {}
    if since_ts is not None:
        # (updated_at > ts) OR (updated_at == ts AND _id > since_id)
        q = {
            "$or": [
                {AGG_TS_FIELD: {"$gt": since_ts}},
                {AGG_TS_FIELD: since_ts, "_id": {"$gt": since_id}},
            ]
        }
    # pequeño skew si se configuró
    sort = [(AGG_TS_FIELD, 1), ("_id", 1)]
    cursor = agg.find(q).sort(sort).limit(BATCH_SIZE)
    return list(cursor)


def gate_required_types(doc: Dict[str, Any]) -> bool:
    got = set(doc.get("types_received") or [])
    return REQUIRED_TYPES.issubset(got)


def process_doc(conn, doc: Dict[str, Any]):
    # Upsert de persona primero
    person_id, grouping_key = upsert_person(conn, doc)

    created_at = doc.get("created_at")
    updated_at = doc.get("updated_at")

    # Upsert por tipo (si vienen bloques)
    for req in REQUIRED_TYPES:
        block = doc.get(req)
        # Aunque falte algún bloque, insertamos fila vacía? Mejor sólo si viene contenido:
        if block:
            upsert_type_table(conn, TYPE_TABLES[req], person_id, grouping_key, block, created_at, updated_at)


def main():
    log.info("🚀 Iniciando SQL Persister | poll=%.1fs batch=%d req_types=%s",
             POLL_SECONDS, BATCH_SIZE, ",".join(sorted(REQUIRED_TYPES)))

    # Conexiones
    mc, agg, state = mongo_connect()
    conn = pg_connect()

    # DDL base + RLS
    ensure_base_tables(conn)
    ensure_rls(conn)

    since_ts, since_id = load_checkpoint(state)
    if since_ts:
        log.info("⏱️ Reanudando desde ts=%s id=%s", since_ts, since_id)
    else:
        log.info("⏱️ Sin checkpoint previo; empezamos desde el inicio.")

    while True:
        t0 = time.time()
        batch = query_batch(agg, since_ts, since_id)
        if not batch:
            log.info("⏳ Sin novedades%s. Dormimos %.1fs",
                     f" (>= {since_ts.isoformat()} id>{since_id})" if since_ts else "", POLL_SECONDS)
            time.sleep(POLL_SECONDS)
            continue

        # Filtrado por required types
        batch_ok = [d for d in batch if gate_required_types(d)]
        skipped = len(batch) - len(batch_ok)

        ok = 0
        for d in batch_ok:
            try:
                process_doc(conn, d)
                ok += 1
                since_ts = d.get(AGG_TS_FIELD)
                since_id = d.get("_id")
            except Exception as e:
                conn.rollback()
                log.exception("❌ Error procesando _id=%s: %s", d.get("_id"), e)

        # Guardar checkpoint del último bien procesado
        if ok:
            try:
                save_checkpoint(state, since_ts, since_id)
            except Exception:
                log.exception("⚠️ No pude guardar checkpoint en Mongo (continuará igualmente)")

        dt = time.time() - t0
        if ok:
            rps = ok / dt if dt > 0 else ok
            log.info("✅ Lote: %d ok | %d saltados (sin tipos requeridos) | %.2fs (%.1f rows/s) | checkpoint: %s / %s",
                     ok, skipped, dt, rps, since_ts, since_id)
        else:
            log.info("ℹ️ Lote sin filas válidas (%d saltados).", skipped)

        # pequeño sleep para no aporrear
        time.sleep(max(0.0, POLL_SECONDS / 2.0))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("👋 Finalizado por señal.")
    except psycopg2.OperationalError as e:
        log.error("❌ Error de conexión Postgres: %s", e)
        sys.exit(2)
    except Exception as e:
        log.exception("❌ Error fatal: %s", e)
        sys.exit(1)
