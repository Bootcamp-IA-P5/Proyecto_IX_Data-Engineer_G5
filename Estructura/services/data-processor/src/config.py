"""
Configuración central del data-processor.

- Lee variables desde .env (en la RAÍZ del proyecto) y variables de entorno.
- Construye MONGO_URI si no se proporciona uno completo.
- NO se guardan credenciales en claro; siempre vienen de variables.
- Expone parámetros operativos (batch size, polling, logging).

Comentarios en español por coherencia con el resto del proyecto.
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

ROOT_ENV = Path("/app/.env")
if load_dotenv and ROOT_ENV.exists():
    load_dotenv(ROOT_ENV)

def _bool_env(var: str, default: bool = False) -> bool:
    val = os.getenv(var)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

MONGO_DATABASE: str = os.getenv("MONGO_DATABASE", "hrpro_db").strip()

_raw_from_collection_env = os.getenv("MONGO_COLLECTION")
_raw_from_collation_env = os.getenv("MONGO_COLLATION")
if _raw_from_collection_env:
    MONGO_COLLECTION: str = _raw_from_collection_env.strip()
elif _raw_from_collation_env:
    MONGO_COLLECTION = _raw_from_collation_env.strip()
else:
    MONGO_COLLECTION = "raw_messages"

AGGREGATED_COLLECTION: str = os.getenv("AGGREGATED_COLLECTION", "aggregated_data").strip()

MONGO_URI: Optional[str] = os.getenv("MONGO_URI")
if not MONGO_URI:
    MONGO_HOST = os.getenv("MONGO_HOST", "mongo").strip()
    MONGO_PORT = os.getenv("MONGO_PORT", "27017").strip()
    MONGO_USERNAME = os.getenv("MONGO_USERNAME", os.getenv("MONGO_USER", "")).strip()
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "").strip()
    MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "admin").strip()
    MONGO_TLS = _bool_env("MONGO_TLS", False)
    MONGO_REPLICA_SET = os.getenv("MONGO_REPLICA_SET", "").strip()

    creds = f"{MONGO_USERNAME}:{MONGO_PASSWORD}@" if MONGO_USERNAME else ""
    params = []
    if MONGO_AUTH_SOURCE:
        params.append(f"authSource={MONGO_AUTH_SOURCE}")
    if MONGO_TLS:
        params.append("tls=true")
    if MONGO_REPLICA_SET:
        params.append(f"replicaSet={MONGO_REPLICA_SET}")
    query = "?" + "&".join(params) if params else ""
    MONGO_URI = f"mongodb://{creds}{MONGO_HOST}:{MONGO_PORT}/{query}"

def safe_mongo_uri(uri: str) -> str:
    try:
        if "@" not in uri or "://" not in uri:
            return uri
        head, tail = uri.split("://", 1)
        auth_host = tail.split("@", 1)
        if len(auth_host) == 1:
            return uri
        creds, rest = auth_host
        if ":" in creds:
            user, _pwd = creds.split(":", 1)
            return f"{head}://{user}:***@{rest}"
        return f"{head}://***@{rest}"
    except Exception:
        return uri

BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
POLL_SECONDS: float = float(os.getenv("POLL_SECONDS", "1"))

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

def describe_config_for_log() -> str:
    return (
        f"DB={MONGO_DATABASE} "
        f"raw={MONGO_COLLECTION} "
        f"agg={AGGREGATED_COLLECTION} "
        f"batch={BATCH_SIZE} poll={POLL_SECONDS}s"
    )
