# Estructura/shared/utils/mongo_aggregated_tools.py
# -*- coding: utf-8 -*-
"""
Herramientas para aggregated_data / raw_messages (menu interactivo)
- Lee variables de .env (MONGO_URI o MONGO_HOST/MONGO_PORT/MONGO_DATABASE, etc.)
- Conexión robusta: intenta varios hosts si falla (localhost, 127.0.0.1, mongo, hrpro-mongodb).
- Operaciones:
  1) Monitor: métricas rápidas y checks útiles.
  2) Reconciliar: NO inventa datos. Sólo:
      - Alinea types_received con presencia de data.<tipo>
      - Recalcula is_complete con $setIsSubset
  3) Borrar aggregated_data (confirmación)
  4) Reset processed en raw_messages (set false o unset)
  5) Stats raw/agg
"""

import os
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
except Exception:
    load_dotenv = None
    find_dotenv = None

ALL_TYPES = ["personal", "location", "professional", "bank", "net"]


# -----------------------------------------------------------------------------
# Utilidades .env y conexión
# -----------------------------------------------------------------------------

def _load_root_env_if_possible() -> None:
    """Carga el .env de la RAÍZ del repo si está disponible, sin fallar si no existe."""
    try:
        if load_dotenv and find_dotenv:
            env_path = find_dotenv(usecwd=True)
            if env_path:
                load_dotenv(env_path)
    except Exception:
        pass


def _mask_uri(uri: str) -> str:
    """Enmascara user:password en una URI para logs seguros."""
    try:
        if "@" in uri and "://" in uri:
            scheme, rest = uri.split("://", 1)
            creds, tail = rest.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
                return f"{scheme}://{user}:***@{tail}"
    except Exception:
        pass
    return uri


def _try_connect(uri: str, dbname: str, timeout_ms: int = 5000) -> Optional[MongoClient]:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        client.admin.command("ping")
        # devolvemos client listo
        return client
    except Exception:
        return None


def get_mongo_handles() -> Dict[str, Any]:
    """
    Intenta conectar usando:
    - MONGO_URI (si existe)
    - Si no, construye URI con MONGO_HOST/MONGO_PORT
    Fallback hosts si falla: localhost, 127.0.0.1, mongo, hrpro-mongodb
    """
    _load_root_env_if_possible()

    uri_env = os.getenv("MONGO_URI", "").strip()
    dbname = os.getenv("MONGO_DATABASE", "hrpro_db")
    raw_name = os.getenv("MONGO_COLLECTION", os.getenv("MONGO_COLLATION", "raw_messages"))
    agg_name = os.getenv("AGGREGATED_COLLECTION", "aggregated_data")

    tried: List[str] = []
    client: Optional[MongoClient] = None

    # 1) Intento directo con MONGO_URI
    if uri_env:
        tried.append(_mask_uri(uri_env))
        client = _try_connect(uri_env, dbname)
        if client:
            db = client[dbname]
            return {
                "client": client,
                "db": db,
                "raw": db[raw_name],
                "agg": db[agg_name],
                "uri_used": _mask_uri(uri_env)
            }

    # 2) Host/port + fallbacks
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    base = f"mongodb://{host}:{port}/"
    tried.append(base)

    # orden de hosts candidatos si falla el principal
    candidates = [base]
    for h in ["localhost", "127.0.0.1", "mongo", "hrpro-mongodb"]:
        candidates.append(f"mongodb://{h}:{port}/")

    seen = set()
    uniq_candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    for uri in uniq_candidates:
        client = _try_connect(uri, dbname)
        tried.append(uri)
        if client:
            db = client[dbname]
            return {
                "client": client,
                "db": db,
                "raw": db[raw_name],
                "agg": db[agg_name],
                "uri_used": _mask_uri(uri)
            }

    raise RuntimeError(
        "❌ No se pudo conectar a MongoDB. Intentos: \n  - " + "\n  - ".join(tried)
        + "\nRevisa tu .env (MONGO_URI ó MONGO_HOST/MONGO_PORT/MONGO_DATABASE)."
    )


# -----------------------------------------------------------------------------
# Operaciones
# -----------------------------------------------------------------------------

def fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def op_stats_raw_agg(agg, raw) -> None:
    """Estadísticas rápidas de raw y aggregated."""
    try:
        raw_total = raw.count_documents({})
        raw_pending = raw.count_documents({"$or": [{"processed": False}, {"processed": {"$exists": False}}]})
        raw_processed = raw.count_documents({"processed": True})
        agg_total = agg.count_documents({})
        agg_complete = agg.count_documents({"is_complete": True})

        print("\n📊 Estadísticas rápidas")
        print("──────────────────────")
        print(f" RAW total        : {fmt(raw_total)}")
        print(f" RAW pendientes   : {fmt(raw_pending)}")
        print(f" RAW procesados   : {fmt(raw_processed)}")
        print(f" AGG total        : {fmt(agg_total)}")
        print(f" AGG is_complete  : {fmt(agg_complete)}")

    except PyMongoError as e:
        print(f"❌ Error en op_stats_raw_agg: {e}")


def op_monitor(agg, raw) -> None:
    """Monitor: varias métricas interesantes de aggregated_data."""
    try:
        print("\n🩺 Monitor de aggregated_data")
        print("─────────────────────────────")

        # Distribución de tamaños de types_received
        dist = list(agg.aggregate([
            {"$project": {"k": {"$size": {"$ifNull": ["$types_received", []]}}}},
            {"$group": {"_id": "$k", "n": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]))
        print(" • Tamaño de types_received → cantidad de docs:")
        for row in dist:
            print(f"   - {row['_id']}: {fmt(row['n'])}")

        # Frecuencia por tipo
        freq = list(agg.aggregate([
            {"$unwind": "$types_received"},
            {"$group": {"_id": "$types_received", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}}
        ]))
        print(" • Frecuencia por tipo:")
        for row in freq:
            print(f"   - {row['_id']}: {fmt(row['n'])}")

        # Cuántos con los 5 tipos (no fuerza nada; solo mira estado actual)
        all5 = agg.count_documents({"types_received": {"$all": ALL_TYPES}})
        print(f" • Con los 5 tipos (ALL)   : {fmt(all5)}")

        # Algunos ejemplos "net" y "bank" reales (si existen)
        sample_net = list(agg.find(
            {
                "types_received": "net",
                "data.net": {"$type": "object"},
                "$expr": {"$gt": [{"$size": {"$objectToArray": "$data.net"}}, 0]}
            },
            {"_id": 0, "_grouping_key": 1, "types_received": 1, "data.net": 1}
        ).limit(2))
        if sample_net:
            print(" • Ejemplos con NET (2):")
            for s in sample_net:
                print(f"   - {s}")

        sample_bank = list(agg.find(
            {
                "types_received": "bank",
                "data.bank": {"$type": "object"},
                "$expr": {"$gt": [{"$size": {"$objectToArray": "$data.bank"}}, 0]}
            },
            {"_id": 0, "_grouping_key": 1, "types_received": 1, "data.bank": 1}
        ).limit(2))
        if sample_bank:
            print(" • Ejemplos con BANK (2):")
            for s in sample_bank:
                print(f"   - {s}")

        print("")
        op_stats_raw_agg(agg, raw)

    except PyMongoError as e:
        print(f"❌ Error en op_monitor: {e}")


def op_reconcile(agg) -> None:
    """
    Reconciliación (refine):
      - NO inventa datos ni mete placeholders.
      - A partir de la presencia real de data.<tipo>, recalcula types_received.
      - Vuelve a calcular is_complete = $setIsSubset(ALL_TYPES, types_received).
    """
    print("\n🛠  Reconciliación / Refine")
    print("──────────────────────────")
    confirm = input("¿Proceder? (escribe 'SI' para continuar): ").strip().upper()
    if confirm != "SI":
        print("Cancelado.")
        return

    start = time.time()

    # Helper: array ["tipo"] si data.<tipo> existe con al menos un campo; si no, []
    def arr_if_data_object_has_fields(path: str) -> Dict[str, Any]:
        return {
            "$cond": [
                {"$gt": [{"$size": {"$objectToArray": {"$ifNull": [f"$data.{path}", {}]}}}, 0]},
                [path.split(".")[-1]],  # ["personal"] / ["location"] / ...
                []
            ]
        }

    # Union acumulada de arrays para los 5 tipos basados en data.*
    types_from_data = {
        "$setUnion": [
            {"$setUnion": [
                {"$setUnion": [
                    {"$setUnion": [
                        arr_if_data_object_has_fields("personal"),
                        arr_if_data_object_has_fields("location")
                    ]},
                    arr_if_data_object_has_fields("professional")
                ]},
                arr_if_data_object_has_fields("bank")
            ]},
            arr_if_data_object_has_fields("net")
        ]
    }

    coalesced_types = {"$cond": [{"$isArray": "$types_received"}, "$types_received", []]}

    try:
        res = agg.update_many(
            {},
            [
                {
                    "$set": {
                        "types_received": {"$setUnion": [coalesced_types, types_from_data]}
                    }
                },
                {
                    "$set": {
                        "is_complete": {
                            "$setIsSubset": [
                                ALL_TYPES,
                                {"$cond": [{"$isArray": "$types_received"}, "$types_received", []]}
                            ]
                        }
                    }
                }
            ]
        )
        dur = time.time() - start
        print(f"✔ Reconciliación aplicada a {fmt(res.modified_count)} documentos en {dur:0.1f}s.")

        # Resumen post-acción
        all5 = agg.count_documents({"types_received": {"$all": ALL_TYPES}})
        complete = agg.count_documents({"is_complete": True})
        print(f"   → Con ALL 5 tipos: {fmt(all5)}")
        print(f"   → is_complete   : {fmt(complete)}")

    except PyMongoError as e:
        print(f"❌ Error en op_reconcile: {e}")


def op_clear_aggregated(agg) -> None:
    """Borra todos los documentos de aggregated_data (confirmación fuerte)."""
    print("\n🧨 Borrado de aggregated_data")
    print("─────────────────────────────")
    confirm = input("¿Estás seguro? Esto borrará TODO aggregated_data. Escribe 'BORRAR': ").strip().upper()
    if confirm != "BORRAR":
        print("Cancelado.")
        return
    try:
        res = agg.delete_many({})
        print(f"✔ Eliminados {fmt(res.deleted_count)} documentos de aggregated_data.")
    except PyMongoError as e:
        print(f"❌ Error en op_clear_aggregated: {e}")


def op_reset_processed(raw) -> None:
    """
    Resetea processed en raw_messages:
      1) set processed=false donde esté true
      2) unset processed y processed_at (elimina ambos campos)
    """
    print("\n♻ Reset de 'processed' en raw_messages")
    print("──────────────────────────────────────")
    print("  1) Marcar processed=false donde esté true")
    print("  2) Unset de processed y processed_at")
    choice = input("Elige opción (1/2): ").strip()
    try:
        if choice == "1":
            res = raw.update_many({"processed": True}, {"$set": {"processed": False}})
            print(f"✔ Modificados {fmt(res.modified_count)} documentos (processed=false).")
        elif choice == "2":
            res = raw.update_many({"processed": {"$exists": True}}, {"$unset": {"processed": "", "processed_at": ""}})
            print(f"✔ Unset en {fmt(res.modified_count)} documentos (processed, processed_at).")
        else:
            print("Opción no válida.")
    except PyMongoError as e:
        print(f"❌ Error en op_reset_processed: {e}")


# -----------------------------------------------------------------------------
# Menú interactivo
# -----------------------------------------------------------------------------

def show_mongo_tools_menu() -> None:
    """Menú interactivo para usar desde tu menú principal o standalone."""
    try:
        handles = get_mongo_handles()
    except Exception as e:
        print(f"❌ Error conectando a MongoDB (revisa tu .env): {e}")
        return

    agg = handles["agg"]
    raw = handles["raw"]
    print(f"\nConectado a: {handles['uri_used']}")
    print("Colecciones → raw: %s | agg: %s" % (raw.name, agg.name))

    while True:
        print("\n=== Herramientas Mongo (aggregated_data/raw_messages) ===")
        print("  1) Monitor aggregated_data")
        print("  2) Reconciliar / Refine aggregated_data (recalcular tipos y is_complete)")
        print("  3) Borrar aggregated_data (¡destructivo!)")
        print("  4) Reset 'processed' en raw_messages")
        print("  5) Stats rápidas raw/agg")
        print("  0) Volver / Salir")
        op = input("Elige opción: ").strip()

        if op == "1":
            op_monitor(agg, raw)
        elif op == "2":
            op_reconcile(agg)
        elif op == "3":
            op_clear_aggregated(agg)
        elif op == "4":
            op_reset_processed(raw)
        elif op == "5":
            op_stats_raw_agg(agg, raw)
        elif op == "0":
            break
        else:
            print("Opción no válida.")


# -----------------------------------------------------------------------------
# Modo CLI directo (por si quieres ejecutarlo solo)
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Si lo ejecutas directamente: muestra el menú
    show_mongo_tools_menu()
