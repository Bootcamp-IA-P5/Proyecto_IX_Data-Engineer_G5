# Estructura/services/data-processor/src/mongo_client.py
# -*- coding: utf-8 -*-
"""
Cliente MongoDB para Data Processor
Lee de raw_messages y agrega en aggregated_data con soporte de índices
para el matching y la reconciliación.

- Variables del entorno en la RAÍZ del proyecto (no passwords en claro en el código).
- Soporta config.py y/o fallback directo a variables de entorno.
"""

import logging
import os
from typing import Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError

try:
    from . import config  # type: ignore
    _HAVE_CONFIG = True
except Exception:
    _HAVE_CONFIG = False

logger = logging.getLogger(__name__)

def _load_root_env_if_possible() -> None:
    try:
        from dotenv import load_dotenv, find_dotenv  # type: ignore
        env_path = find_dotenv(usecwd=True)
        if env_path:
            load_dotenv(env_path)
    except Exception:
        pass

def _mask_uri(uri: str) -> str:
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

class MongoDBClient:
    def __init__(self) -> None:
        _load_root_env_if_possible()

        if _HAVE_CONFIG:
            self.uri = getattr(config, "MONGO_URI", os.getenv("MONGO_URI"))
            self.database_name = getattr(config, "MONGO_DATABASE", os.getenv("MONGO_DATABASE", "hrpro_db"))
            self.raw_collection_name = getattr(
                config, "MONGO_COLLECTION",
                os.getenv("MONGO_COLLECTION", os.getenv("MONGO_COLLATION", "raw_messages"))
            )
            self.aggregated_collection_name = getattr(
                config, "AGGREGATED_COLLECTION",
                os.getenv("AGGREGATED_COLLECTION", "aggregated_data")
            )
            self.host = os.getenv("MONGO_HOST", "localhost")
            self.port = os.getenv("MONGO_PORT", "27017")
        else:
            self.uri = os.getenv("MONGO_URI")
            self.database_name = os.getenv("MONGO_DATABASE", "hrpro_db")
            self.raw_collection_name = os.getenv("MONGO_COLLECTION", os.getenv("MONGO_COLLATION", "raw_messages"))
            self.aggregated_collection_name = os.getenv("AGGREGATED_COLLECTION", "aggregated_data")
            self.host = os.getenv("MONGO_HOST", "localhost")
            self.port = os.getenv("MONGO_PORT", "27017")

        self.client: Optional[MongoClient] = None
        self.db = None
        self.raw_collection = None
        self.aggregated_collection = None

    def connect(self) -> None:
        try:
            if self.uri:
                logger.info(f"📡 Conectando a MongoDB (URI): {_mask_uri(self.uri)}")
                self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            else:
                logger.info(f"📡 Conectando a MongoDB (host/port): {self.host}:{self.port}")
                self.client = MongoClient(f"mongodb://{self.host}:{self.port}/", serverSelectionTimeoutMS=5000)

            self.client.admin.command("ping")

            self.db = self.client[self.database_name]
            self.raw_collection = self.db[self.raw_collection_name]
            self.aggregated_collection = self.db[self.aggregated_collection_name]

            logger.info(f"✅ Conectado a MongoDB: {self.database_name}")
            logger.info(f"   - Colección origen (raw): {self.raw_collection_name}")
            logger.info(f"   - Colección destino (agg): {self.aggregated_collection_name}")

            self._create_indexes()

        except PyMongoError as e:
            logger.error(f"❌ Error conectando a MongoDB: {e}")
            raise

    def _create_indexes(self) -> None:
        from pymongo.errors import PyMongoError

        def _as_tuple(spec):
            return tuple(spec)

        def _find_index_by_keys(coll, keys_tuple):
            for ix in coll.list_indexes():
                if tuple(ix["key"].items()) == keys_tuple:
                    return ix
            return None

        def _ensure_index(coll, keys, name: str, unique: bool = False, sparse: bool = False):
            kt = _as_tuple(keys)
            ix = _find_index_by_keys(coll, kt)
            if ix:
                ix_unique = bool(ix.get("unique", False))
                ix_sparse = bool(ix.get("sparse", False))
                if ix_unique != unique or ix_sparse != sparse:
                    try:
                        coll.drop_index(ix["name"])
                    except PyMongoError as e:
                        logger.warning(f"⚠️  No se pudo borrar índice existente {ix.get('name')}: {e}")
                    try:
                        coll.create_index(keys, name=name, unique=unique, sparse=sparse)
                        logger.info(f"🔁 Índice recreado: {name}")
                    except PyMongoError as e:
                        logger.warning(f"⚠️  Error creando índice {name}: {e}")
                return
            else:
                try:
                    coll.create_index(keys, name=name, unique=unique, sparse=sparse)
                    logger.info(f"🆕 Índice creado: {name}")
                except PyMongoError as e:
                    logger.warning(f"⚠️  Error creando índice {name}: {e}")

        try:
            # RAW
            _ensure_index(self.raw_collection, [("processed", 1)], name="idx_processed", unique=False, sparse=False)

            # AGGREGATED
            _ensure_index(self.aggregated_collection, [("_grouping_key", 1)], name="uniq_grouping_key", unique=True)
            _ensure_index(self.aggregated_collection, [("grouping_status", 1)], name="idx_grouping_status")
            _ensure_index(self.aggregated_collection, [("types_received", 1)], name="idx_types_received")
            _ensure_index(self.aggregated_collection, [("identifiers.passport", 1)], name="idx_id_passport", sparse=True)
            _ensure_index(self.aggregated_collection, [("identifiers.email", 1)], name="idx_id_email", sparse=True)
            _ensure_index(self.aggregated_collection, [("identifiers.phone", 1)], name="idx_id_phone", sparse=True)
            _ensure_index(self.aggregated_collection, [("identifiers.tax_id", 1)], name="idx_id_tax", sparse=True)
            _ensure_index(self.aggregated_collection, [("identifiers.ssn", 1)], name="idx_id_ssn", sparse=True)
            _ensure_index(self.aggregated_collection, [("data.location.address", 1)], name="idx_loc_address", sparse=True)
            _ensure_index(self.aggregated_collection, [("data.personal.email", 1)], name="idx_personal_email", sparse=True)
            _ensure_index(self.aggregated_collection, [("data.bank.iban", 1)], name="idx_bank_iban", sparse=True)
            _ensure_index(self.aggregated_collection, [("is_complete", 1)], name="idx_is_complete")
            _ensure_index(self.aggregated_collection, [("is_complete", 1), ("updated_at", 1)], name="idx_complete_updated")


            logger.info("✅ Índices creados/verificados correctamente")
        except PyMongoError as e:
            logger.warning(f"⚠️  No se pudieron crear índices: {e}")

    def get_unprocessed_documents(self, limit: int) -> List[Dict]:
        try:
            query = {"$or": [{"processed": False}, {"processed": {"$exists": False}}]}
            docs = list(self.raw_collection.find(query).limit(limit))
            if docs:
                logger.info(f"📥 Obtenidos {len(docs)} documentos sin procesar")
            return docs
        except PyMongoError as e:
            logger.error(f"❌ Error obteniendo documentos: {e}")
            return []

    def mark_as_processed(self, doc_ids: List) -> int:
        if not doc_ids:
            return 0
        try:
            result = self.raw_collection.update_many(
                {"_id": {"$in": doc_ids}},
                {"$set": {"processed": True}}
            )
            count = result.modified_count
            if count > 0:
                logger.info(f"✅ Marcados {count} documentos como procesados")
            return count
        except PyMongoError as e:
            logger.error(f"❌ Error marcando documentos: {e}")
            return 0

    def get_stats(self) -> Dict:
        try:
            raw_total = self.raw_collection.count_documents({})
            raw_processed = self.raw_collection.count_documents({"processed": True})
            raw_pending = self.raw_collection.count_documents({"$or": [{"processed": False}, {"processed": {"$exists": False}}]})
            aggregated_total = self.aggregated_collection.count_documents({})
            aggregated_complete = self.aggregated_collection.count_documents({"is_complete": True})
            aggregated_pending = self.aggregated_collection.count_documents({"grouping_status": "pending_reconciliation"})
            return {
                "raw_total": raw_total,
                "raw_processed": raw_processed,
                "raw_pending": raw_pending,
                "aggregated_total": aggregated_total,
                "aggregated_complete": aggregated_complete,
                "aggregated_pending": aggregated_pending,
            }
        except PyMongoError as e:
            logger.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}

    def close(self) -> None:
        if self.client:
            self.client.close()
            logger.info("🔌 Conexión a MongoDB cerrada")

_mongo_client_instance: Optional[MongoDBClient] = None

def get_mongo_client() -> MongoDBClient:
    global _mongo_client_instance
    if _mongo_client_instance is None:
        _mongo_client_instance = MongoDBClient()
        _mongo_client_instance.connect()
    return _mongo_client_instance
