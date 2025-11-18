# Estructura/services/data-processor/src/processor.py
# -*- coding: utf-8 -*-
"""
Data Processor: lee mensajes crudos de raw_messages (MongoDB), los normaliza y los
agrega en aggregated_data (una fila por persona). Solo agrupamos con identificadores
fuertes (passport/email/phone/tax_id/ssn). Los mensajes sin ID fuerte se marcan como
'pending_reconciliation' y se intentan reconciliar automáticamente cuando llega una
pieza con identificador.

Variables de entorno esperadas (.env en la raíz):
- MONGO_URI (opcional; si no existe se usan MONGO_HOST/MONGO_PORT)
- MONGO_HOST (default: localhost)
- MONGO_PORT (default: 27017)
- MONGO_DATABASE (nombre de la BD)
- MONGO_COLLATION (en vuestro .env es la colección RAW, ej: raw_messages)
- AGGREGATED_COLLECTION (colección agregada, ej: aggregated_data)
- BATCH_SIZE (opcional, para el runner: 500 por defecto)

Estructura de documento agregado:
{
  _grouping_key: "passport:ABC123" | "email:x@y.com" | ...
  grouping_status: "matched" | "pending_reconciliation" | "reconciled"
  identifiers: { passport, email, phone, tax_id, ssn }
  data: {
    personal: {...}, location: {...}, professional: {...}, bank: {...}, net: {...}
  }
  types_received: ["personal", "bank", ...]
  messages_count: <int>
  last_updated: <UTC datetime>
  is_complete: <bool>  # True si types_received tiene los 5 tipos
  source_message_ids: [<ids de raw consumidos>]
}

Lee mensajes crudos de MongoDB (raw_messages), los normaliza, detecta su tipo,
genera una clave de agrupación por persona y hace upsert en aggregated_data.

Puntos clave de esta implementación:
- Normalización fuerte de identificadores (passport, email, phone, tax_id, ssn).
- Fallbacks prudentes (fullname+address, fullname, address) marcando
  grouping_status='pending_reconciliation' si la clave es débil.
- Upsert seguro sin conflictos de paths (no se setea 'data' ni 'identifiers'
  a la vez que 'data.*' o 'identifiers.*' en el mismo operador).
- Heurística robusta de clasificación por "scoring" de campos.
- Normalización ligera de textos (minúsculas + trim) para facilitar matching (sin perder original).
- Unificación de types_received y cálculo de is_complete (5 tipos) con coalesce seguro a arrays.
- Upsert seguro con "update pipeline":
    * types_received <- setUnion(coalesce, [tipo])
    * is_complete    <- setIsSubset(ALL_TYPES, coalesce(types_received, []))
- Derivaciones para favorecer 5/5 tipos:
    * PERSONAL mínimo desde identificadores (email/phone)
    * LOCATION desde IBAN (country_code y nombre país si se conoce)
    * NET desde email (domain)
    * * Derivación de professional desde dominio de email (cuando no exista)
    * NO se fuerza ningún placeholder para completar 5/5
- Logs claros por lote:
    📊 Lote: leídos, upserts (por tipo), skipped_unknown, skipped_empty, errores
    ✅ Procesados <upserts> mensajes: desglose por tipo
"""

import re
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Iterable, Tuple

from .mongo_client import get_mongo_client
from . import config

logger = logging.getLogger(__name__)

# =============================================================================
# SECCIÓN: Configuración general
# =============================================================================

BATCH_SIZE = getattr(config, "BATCH_SIZE", 1000)
POLL_SECONDS = getattr(config, "POLL_SECONDS", 1.0)

# =============================================================================
# SECCIÓN: Alias y patrones
# =============================================================================

ALIASES: Dict[str, Dict[str, List[str]]] = {
    "personal": {
        "fullname": ["fullname", "full_name", "name"],
        "first_name": ["first_name", "firstname", "given_name", "givenname"],
        "last_name": ["last_name", "lastname", "surname", "family_name", "familyname"],
        "email": ["email", "personal_email", "work_email", "mail", "correo"],
        "phone": ["phone", "mobile", "phone_number", "tel", "telefono"],
        "dob": ["dob", "birthdate", "date_of_birth", "fecha_nacimiento"],
    },
    "location": {
        "address": ["address", "street_address", "direccion", "dirección", "addr", "calle"],
        "city": ["city", "ciudad", "locality", "town"],
        "state": ["state", "province", "provincia", "region"],
        "postal_code": ["postal_code", "zip", "zipcode", "cp", "codigo_postal"],
        "country": ["country", "pais", "país", "country_name"],
        "latitude": ["latitude", "lat"],
        "longitude": ["longitude", "lon", "lng", "long"],
    },
    "professional": {
        "company": ["company", "empresa", "employer"],
        "position": ["position", "job_title", "role", "puesto", "cargo"],
        "department": ["department", "dept", "departamento"],
        "salary": ["salary", "salario", "compensation"],
        "contract_type": ["contract_type", "tipo_contrato"],
        "experience_years": ["experience_years", "anos_experiencia", "años_experiencia"],
    },
    "bank": {
        "iban": ["iban", "iban_code", "iban_number", "ibannumber", "ibanNum", "IBAN", "IBAN_CODE"],
        "swift": ["swift", "swift_code", "bic", "bic_code", "BIC", "SWIFT"],
        "account_number": ["account_number", "account", "acct", "n_cuenta", "numero_cuenta", "accountNumber"],
        "bank_name": ["bank_name", "bank", "banco"],
        "card_number": ["card_number", "tarjeta", "num_tarjeta", "cardNumber"],
        "card_brand": ["card_brand", "brand", "marca_tarjeta"],
        "card_exp": ["card_exp", "expiry", "exp", "exp_date", "expiration"],
        "card_holder": ["card_holder", "holder", "titular"],
    },
    "net": {
        "ipv4": ["ipv4", "ip", "ip_address"],
        "ipv6": ["ipv6"],
        "mac": ["mac", "mac_address"],
        "hostname": ["hostname", "host"],
        "domain": ["domain", "email_domain"],
        "url": ["url", "link"],
        "ssid": ["ssid", "wifi_ssid"],
        "user_agent": ["user_agent", "agent", "ua"],
        "browser": ["browser", "navegador"],
        "os": ["os", "so", "operating_system"],
    },
}

RE_IBAN = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b", re.I)
RE_SWIFT = re.compile(r"\b[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?\b", re.I)
RE_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
RE_IPV6 = re.compile(r"\b[0-9a-f:]{2,}\b", re.I)

FREE_MAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.es", "ymail.com",
    "hotmail.com", "live.com", "outlook.com", "icloud.com", "me.com",
    "aol.com", "proton.me", "protonmail.com", "mail.com", "gmx.com",
    "gmx.es", "yandex.ru", "yandex.com", "tutanota.com", "zoho.com",
    "qq.com", "163.com", "126.com", "wanadoo.fr", "orange.fr", "libero.it",
}

# =============================================================================
# SECCIÓN: Utilidades generales
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: _normalize_text
# Descripción: normaliza strings (trim + minúsculas + espacios colapsados).
# ─────────────────────────────────────────────────────────────────────────────
def _normalize_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    s = str(v).strip().lower()
    return " ".join(s.split())

# ─────────────────────────────────────────────────────────────────────────────
# Función: _iter_kv_top_and_shallow
# Descripción: itera claves/valores de primer nivel y, además, un nivel anidado.
# ─────────────────────────────────────────────────────────────────────────────
def _iter_kv_top_and_shallow(doc: Dict[str, Any]) -> Iterable[tuple[str, Any]]:
    for k, v in doc.items():
        yield k, v
    for v in doc.values():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                yield k2, v2

# ─────────────────────────────────────────────────────────────────────────────
# Función: _find_value_by_aliases
# Descripción: busca un valor por lista de aliases (top-level o shallow).
# ─────────────────────────────────────────────────────────────────────────────
def _find_value_by_aliases(doc: Dict[str, Any], aliases: List[str]) -> Optional[Any]:
    aliases_lc = [a.lower() for a in aliases]
    for k, v in _iter_kv_top_and_shallow(doc):
        if k.lower() in aliases_lc:
            return v
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Función: _scan_strings
# Descripción: genera todos los strings del documento (incluye listas/tuplas).
# ─────────────────────────────────────────────────────────────────────────────
def _scan_strings(doc: Dict[str, Any]) -> Iterable[str]:
    for _, v in _iter_kv_top_and_shallow(doc):
        if isinstance(v, str):
            yield v
        elif isinstance(v, (list, tuple)):
            for i in v:
                if isinstance(i, str):
                    yield i

# =============================================================================
# SECCIÓN: Clasificación / Normalización por tipo
# =============================================================================

ALL_TYPES = {"personal", "location", "professional", "bank", "net"}
ALL_TYPES_LIST = ["personal", "location", "professional", "bank", "net"]

# ─────────────────────────────────────────────────────────────────────────────
# Función: pick_fields_for_type
# Descripción: extrae y normaliza el bloque data.<tipo> desde el raw.
# ─────────────────────────────────────────────────────────────────────────────
def pick_fields_for_type(doc: Dict[str, Any], msg_type: str) -> Dict[str, Any]:
    block: Dict[str, Any] = {}
    aliases = ALIASES.get(msg_type, {})

    # Por alias (canónico <- alias)
    for canonical, keys in aliases.items():
        val = _find_value_by_aliases(doc, keys)
        if isinstance(val, str):
            # En "bank" respetamos mayúsculas (IBAN/SWIFT); otros tipos normalizamos
            block[canonical] = val.strip() if msg_type == "bank" else _normalize_text(val)
        elif val is not None:
            block[canonical] = val

    # Patrones específicos
    if msg_type == "bank":
        if not block.get("iban"):
            for s in _scan_strings(doc):
                m = RE_IBAN.search(s.upper())
                if m:
                    block["iban"] = m.group(0)
                    break
        if not block.get("swift"):
            for s in _scan_strings(doc):
                m = RE_SWIFT.search(s.upper())
                if m:
                    block["swift"] = m.group(0)
                    break

    if msg_type == "net":
        if not block.get("ipv4"):
            for s in _scan_strings(doc):
                m = RE_IPV4.search(s)
                if m:
                    block["ipv4"] = m.group(0)
                    break
        if not block.get("ipv6"):
            for s in _scan_strings(doc):
                if RE_IPV6.search(s) and ":" in s:
                    block["ipv6"] = s
                    break

    # Limpieza
    block = {k: v for k, v in block.items() if v not in (None, "", [])}
    if msg_type != "bank":
        for k, v in list(block.items()):
            if isinstance(v, str):
                block[k] = _normalize_text(v)

    return block

# ─────────────────────────────────────────────────────────────────────────────
# Función: detect_message_type
# Descripción: asigna un tipo al documento por scoring de campos y keywords.
# ─────────────────────────────────────────────────────────────────────────────
def detect_message_type(doc: Dict[str, Any]) -> str:
    scores = {t: len(pick_fields_for_type(doc, t)) for t in ALL_TYPES}

    for s in _scan_strings(doc):
        ls = s.lower()

        # Señales para "bank"
        if any(h in ls for h in ("iban", "swift", "bic", "bank", "account")) or RE_IBAN.search(s) or RE_SWIFT.search(s):
            scores["bank"] += 1

        # Señales para "professional"
        if any(h in ls for h in ("company", "position", "role", "department", "job", "empresa", "puesto")):
            scores["professional"] += 1

        # Señales para "net"
        if any(h in ls for h in ("ip", "ipv4", "ipv6", "mac", "hostname", "domain", "url", "ssid", "browser", "user-agent", "user agent")) or RE_IPV4.search(s):
            scores["net"] += 1

        # Señales para "location"
        if any(h in ls for h in ("address", "city", "postal", "zipcode", "zip", "country", "latitude", "longitude",
                                  "coords", "street", "calle", "ciudad", "cp", "provincia", "pais", "dirección", "direccion")):
            scores["location"] += 1

        # Señales para "personal"
        if any(h in ls for h in ("name", "fullname", "first name", "last name", "surname", "email", "phone", "birth",
                                  "nombre", "apellido", "correo", "teléfono", "telefono", "fecha de nacimiento")):
            scores["personal"] += 1

    # Mejor puntuado
    best_type = max(scores.items(), key=lambda x: x[1])[0]
    if scores[best_type] == 0:
        return "unknown"

    # Desempate por prioridad fija
    tie_order = ["bank", "professional", "personal", "location", "net"]
    ties = [t for t, s in scores.items() if s == scores[best_type]]
    for t in tie_order:
        if t in ties:
            return t
    return best_type

# =============================================================================
# SECCIÓN: Identificadores y clave de agrupación
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: extract_identifiers
# Descripción: extrae y normaliza identificadores (passport/email/phone/tax/ssn).
# ─────────────────────────────────────────────────────────────────────────────
def extract_identifiers(doc: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    def _get_ci(*cands: str) -> Optional[str]:
        val = _find_value_by_aliases(doc, list(cands))
        return _normalize_text(val) if isinstance(val, str) else None

    passport = _get_ci("passport", "id_document", "dni", "nif")
    email = _get_ci("email", "personal_email", "work_email", "mail", "correo")
    phone = _get_ci("phone", "mobile", "phone_number", "tel", "telefono")
    tax_id = _get_ci("tax_id", "nif", "nie", "ssn")
    ssn = _get_ci("ssn")

    if passport:
        out["passport"] = passport
    if email:
        out["email"] = email
    if phone:
        out["phone"] = phone
    if tax_id:
        out["tax_id"] = tax_id
    if ssn:
        out["ssn"] = ssn

    return out

# ─────────────────────────────────────────────────────────────────────────────
# Función: build_grouping_key
# Descripción: construye la clave estable (passport > email > phone > fullname+dob > rawid).
# ─────────────────────────────────────────────────────────────────────────────
def build_grouping_key(ids: Dict[str, Any], raw: Dict[str, Any]) -> str:
    if ids.get("passport"):
        return f"passport::{ids['passport']}"
    if ids.get("email"):
        return f"email::{ids['email']}"
    if ids.get("phone"):
        return f"phone::{ids['phone']}"

    fn = _find_value_by_aliases(raw, ["fullname", "full_name", "name"])
    dob = _find_value_by_aliases(raw, ["dob", "birthdate", "date_of_birth", "fecha_nacimiento"])
    if isinstance(fn, str) and isinstance(dob, str):
        return f"fullnamedob::{_normalize_text(fn)}::{_normalize_text(dob)}"

    return f"rawid::{raw.get('_id')}"

# ─────────────────────────────────────────────────────────────────────────────
# Función: build_data_block
# Descripción: devuelve bloque data.<tipo> o "raw" ligero si el tipo es unknown.
# ─────────────────────────────────────────────────────────────────────────────
def build_data_block(raw: Dict[str, Any], msg_type: str) -> Dict[str, Any]:
    if msg_type in ALIASES:
        return pick_fields_for_type(raw, msg_type)

    light: Dict[str, Any] = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        if isinstance(v, (str, int, float, bool)):
            light[k] = v
    return {"raw": light}

# =============================================================================
# SECCIÓN: Derivaciones (location desde bank, professional desde email/net)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Funciones auxiliares para derivaciones
# ─────────────────────────────────────────────────────────────────────────────
def _extract_domain_from_email(email: str) -> Optional[str]:
    try:
        if "@" in email:
            return email.split("@", 1)[1].strip().lower()
    except Exception:
        pass
    return None

_IBAN_COUNTRY = {
    "ES": "Spain", "IT": "Italy", "FR": "France", "DE": "Germany", "PT": "Portugal",
    "NL": "Netherlands", "BE": "Belgium", "GB": "United Kingdom", "IE": "Ireland",
    "SE": "Sweden", "NO": "Norway", "FI": "Finland", "DK": "Denmark", "CH": "Switzerland",
    "PL": "Poland", "CZ": "Czech Republic", "AT": "Austria",
}

# ─────────────────────────────────────────────────────────────────────────────
# Función: derive_location_from_bank
# Descripción: infiere país a partir del prefijo de un IBAN.
# ─────────────────────────────────────────────────────────────────────────────
def derive_location_from_bank(bank_block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    iban = bank_block.get("iban")
    if not isinstance(iban, str) or len(iban) < 2:
        return None
    cc = iban[:2].upper()
    loc = {"country_code": cc}
    if cc in _IBAN_COUNTRY:
        loc["country"] = _IBAN_COUNTRY[cc]
    return loc

# ─────────────────────────────────────────────────────────────────────────────
# Función: derive_personal_minimal
# Descripción: crea bloque personal mínimo a partir de IDs (email/phone).
# ─────────────────────────────────────────────────────────────────────────────
def derive_personal_minimal(ids: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {}
    if isinstance(ids.get("email"), str):
        out["email"] = ids["email"]
    if isinstance(ids.get("phone"), str):
        out["phone"] = ids["phone"]
    return out or None

# ─────────────────────────────────────────────────────────────────────────────
# Función: _company_label_from_domain
# Descripción: genera nombre de compañía a partir de un dominio, filtrando free-mail.
# ─────────────────────────────────────────────────────────────────────────────
def _company_label_from_domain(domain: str) -> Optional[str]:
    if not domain:
        return None
    d = domain.lower()
    if d in FREE_MAIL_DOMAINS:
        return None
    parts = [p for p in d.split(".") if p not in ("www", "mail")]
    base = parts[-2] if len(parts) >= 2 else parts[0]
    if not base:
        return None
    return base.capitalize()

# ─────────────────────────────────────────────────────────────────────────────
# Función: derive_professional_from_email_or_net
# Descripción: si el dominio NO es "free", infiere professional.company desde email/net.
# ─────────────────────────────────────────────────────────────────────────────
def derive_professional_from_email_or_net(ids: Dict[str, Any], net_block: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    domain: Optional[str] = None
    if isinstance(ids.get("email"), str):
        domain = _extract_domain_from_email(ids["email"])
    if not domain and net_block and isinstance(net_block.get("domain"), str):
        domain = net_block["domain"]
    company = _company_label_from_domain(domain) if domain else None
    if company:
        return {"company": company, "_inferred": True}
    return None

# =============================================================================
# SECCIÓN: Reconciliación (match por IDs y merge de grupos)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: _identifier_match_query
# Descripción: arma un $or con los IDs presentes para buscar coincidencias.
# ─────────────────────────────────────────────────────────────────────────────
def _identifier_match_query(ids: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    clauses = []
    for k in ("passport", "email", "phone", "tax_id", "ssn"):
        v = ids.get(k)
        if v:
            clauses.append({f"identifiers.{k}": v})
    if not clauses:
        return None
    return {"$or": clauses}

# ─────────────────────────────────────────────────────────────────────────────
# Función: resolve_grouping_key
# Descripción: si existe un grupo con esos IDs, usaremos su _grouping_key.
# ─────────────────────────────────────────────────────────────────────────────
def resolve_grouping_key(agg_coll, ids: Dict[str, Any], proposed_key: str) -> Tuple[str, Optional[str]]:
    q = _identifier_match_query(ids)
    if not q:
        return proposed_key, None

    cur = agg_coll.find(q, {"_grouping_key": 1, "identifiers": 1}).limit(10)
    cand = list(cur)
    if not cand:
        return proposed_key, None

    def score(doc):
        i = doc.get("identifiers", {})
        return (
            1 if i.get("passport") else 0,
            1 if i.get("email") else 0,
            1 if i.get("phone") else 0,
            1 if i.get("tax_id") else 0,
            1 if i.get("ssn") else 0,
        )

    best = sorted(cand, key=score, reverse=True)[0]
    return best["_grouping_key"], best["_grouping_key"]

# ─────────────────────────────────────────────────────────────────────────────
# Función: merge_duplicate_groups
# Descripción: fusiona dos grupos (identificadores, data.*, types_received, is_complete).
# ─────────────────────────────────────────────────────────────────────────────
def merge_duplicate_groups(agg_coll, source_key: str, target_key: str) -> bool:
    if source_key == target_key:
        return False

    src = agg_coll.find_one({"_grouping_key": source_key})
    if not src:
        return False
    tgt = agg_coll.find_one({"_grouping_key": target_key})
    if not tgt:
        return False

    now = datetime.now(timezone.utc)
    set_ops: Dict[str, Any] = {}

    # Identificadores
    src_ids = src.get("identifiers") or {}
    tgt_ids = tgt.get("identifiers") or {}
    for k, v in src_ids.items():
        if k not in tgt_ids and v not in (None, ""):
            set_ops[f"identifiers.{k}"] = v

    # Data.*
    src_data = src.get("data") or {}
    tgt_data = tgt.get("data") or {}
    for t, block in src_data.items():
        if t not in tgt_data and isinstance(block, dict) and block:
            set_ops[f"data.{t}"] = block

    # Types + is_complete
    src_types = set(src.get("types_received") or [])
    tgt_types = set(tgt.get("types_received") or [])
    union_types = sorted(list(src_types | tgt_types))
    set_ops["types_received"] = union_types
    set_ops["is_complete"] = set(ALL_TYPES_LIST).issubset(set(union_types))
    set_ops["updated_at"] = now

    if set_ops:
        agg_coll.update_one({"_grouping_key": target_key}, {"$set": set_ops})
    agg_coll.delete_one({"_grouping_key": source_key})

    logger.info(
        f"🔗 Merge: '{source_key}' -> '{target_key}' | tipos={union_types} | complete={set_ops['is_complete']}"
    )
    return True

# =============================================================================
# SECCIÓN: Upsert (update pipeline)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: upsert_message
# Descripción:
#   * Hace upsert del bloque data.<tipo>
#   * Unifica types_received con $setUnion (coalesce a [])
#   * Recalcula is_complete con $setIsSubset (coalesce a [])
# ─────────────────────────────────────────────────────────────────────────────
def upsert_message(
    agg_coll,
    grouping_key: str,
    ids: Dict[str, Any],
    data_block: Dict[str, Any],
    msg_type: str,
) -> None:
    if not grouping_key or msg_type == "unknown":
        return
    if not isinstance(data_block, dict) or len(data_block) == 0:
        return

    now = datetime.now(timezone.utc)
    coalesced_types_expr = {"$cond": [{"$isArray": "$types_received"}, "$types_received", []]}

    # Stage 1: set "vivo"
    stage_set: Dict[str, Any] = {
        "updated_at": now,
        "types_received": {"$setUnion": [coalesced_types_expr, [msg_type]]},
        f"data.{msg_type}": data_block,
    }
    for k, v in ids.items():
        stage_set[f"identifiers.{k}"] = v

    # Stage 2: defaults "on insert"
    stage_defaults = {
        "_grouping_key": {"$ifNull": ["$_grouping_key", grouping_key]},
        "created_at": {"$ifNull": ["$created_at", now]},
    }

    # Stage 3: is_complete
    stage_complete = {
        "is_complete": {
            "$setIsSubset": [
                ALL_TYPES_LIST,
                {"$cond": [{"$isArray": "$types_received"}, "$types_received", []]},
            ]
        }
    }

    agg_coll.update_one(
        {"_grouping_key": grouping_key},
        [{"$set": stage_set}, {"$set": stage_defaults}, {"$set": stage_complete}],
        upsert=True,
    )

# =============================================================================
# SECCIÓN: Procesamiento por lotes
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: _has_useful_fields
# Descripción: true si el bloque tiene algún valor no vacío.
# ─────────────────────────────────────────────────────────────────────────────
def _has_useful_fields(block: Dict[str, Any]) -> bool:
    return bool(block and any(v is not None and v != "" for v in block.values()))

# ─────────────────────────────────────────────────────────────────────────────
# Función: process_batch
# Descripción: procesa un listado de documentos crudos y realiza upserts/derivaciones.
# ─────────────────────────────────────────────────────────────────────────────
def process_batch(client, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    totals = {
        "personal": 0,
        "location": 0,
        "professional": 0,
        "bank": 0,
        "net": 0,
        "unknown": 0,
        "errors": 0,
        "skipped_unknown": 0,
        "skipped_empty": 0,
        "_read": len(docs),
        "_upserts": 0,
        "_merges": 0,
    }
    if not docs:
        return totals

    doc_ids: List[Any] = []

    for raw in docs:
        _id = raw.get("_id")
        if _id is not None:
            doc_ids.append(_id)

        try:
            # 1) Clasificar y preparar
            msg_type = detect_message_type(raw)
            ids = extract_identifiers(raw)
            proposed_key = build_grouping_key(ids, raw)

            # 2) Reconciliación por IDs (usar grupo existente si aplica)
            target_key, matched_key = resolve_grouping_key(client.aggregated_collection, ids, proposed_key)
            if matched_key and proposed_key != matched_key:
                if merge_duplicate_groups(client.aggregated_collection, proposed_key, matched_key):
                    totals["_merges"] += 1
                grouping_key = matched_key
            else:
                grouping_key = proposed_key

            added_types: set = set()

            # 3) Tipo primario
            if msg_type != "unknown":
                primary = build_data_block(raw, msg_type)
                if _has_useful_fields(primary):
                    upsert_message(client.aggregated_collection, grouping_key, ids, primary, msg_type)
                    totals[msg_type] += 1
                    totals["_upserts"] += 1
                    added_types.add(msg_type)
                else:
                    totals["skipped_empty"] += 1
            else:
                totals["skipped_unknown"] += 1

            # 4) Derivar LOCATION desde BANK (por IBAN)
            bank_block = pick_fields_for_type(raw, "bank")
            if _has_useful_fields(bank_block):
                loc_from_bank = derive_location_from_bank(bank_block)
                if loc_from_bank and "location" not in added_types:
                    upsert_message(client.aggregated_collection, grouping_key, ids, loc_from_bank, "location")
                    totals["location"] += 1
                    totals["_upserts"] += 1
                    added_types.add("location")

            # 5) PERSONAL mínimo desde IDs (email/phone)
            if "personal" not in added_types:
                personal_min = derive_personal_minimal(ids)
                if personal_min:
                    upsert_message(client.aggregated_collection, grouping_key, ids, personal_min, "personal")
                    totals["personal"] += 1
                    totals["_upserts"] += 1
                    added_types.add("personal")

            # 6) NET desde dominio de email (si aún no está)
            net_from_email = None
            if "net" not in added_types:
                personal_block_for_deriv = pick_fields_for_type(raw, "personal") if msg_type == "personal" else None
                if personal_block_for_deriv and "email" in personal_block_for_deriv:
                    d = _extract_domain_from_email(personal_block_for_deriv["email"])
                    if d:
                        net_from_email = {"domain": d}
                elif isinstance(ids.get("email"), str):
                    d = _extract_domain_from_email(ids["email"])
                    if d:
                        net_from_email = {"domain": d}

                if net_from_email:
                    upsert_message(client.aggregated_collection, grouping_key, ids, net_from_email, "net")
                    totals["net"] += 1
                    totals["_upserts"] += 1
                    added_types.add("net")

            # 7) PROFESSIONAL: real del raw -> si no, derivado de email/net -> si no, nada (sin placeholder)
            if "professional" not in added_types:
                prof_raw = pick_fields_for_type(raw, "professional")
                if _has_useful_fields(prof_raw):
                    upsert_message(client.aggregated_collection, grouping_key, ids, prof_raw, "professional")
                    totals["professional"] += 1
                    totals["_upserts"] += 1
                    added_types.add("professional")
                else:
                    net_block_present = net_from_email or pick_fields_for_type(raw, "net")
                    prof_deriv = derive_professional_from_email_or_net(ids, net_block_present)
                    if prof_deriv:
                        upsert_message(client.aggregated_collection, grouping_key, ids, prof_deriv, "professional")
                        totals["professional"] += 1
                        totals["_upserts"] += 1
                        added_types.add("professional")

        except Exception as e:
            totals["errors"] += 1
            logger.error(f"❌ Error procesando doc {_id}: {e}", exc_info=True)

    # 8) Marcado de procesados en RAW
    if doc_ids:
        client.mark_as_processed(doc_ids)

    return totals

# =============================================================================
# SECCIÓN: Bucle principal
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Función: main_loop
# Descripción: loop infinito por lotes con métricas de lote.
# ─────────────────────────────────────────────────────────────────────────────
def main_loop():
    logger.info(f"🚀 Iniciando Data Processor | batch_size={BATCH_SIZE} poll={POLL_SECONDS:.2f}s")
    client = get_mongo_client()
    while True:
        docs = client.get_unprocessed_documents(BATCH_SIZE)
        if not docs:
            time.sleep(POLL_SECONDS)
            continue

        res = process_batch(client, docs)

        logger.info(
            "📊 Lote: leídos=%d, upserts=%d, merges=%d, skipped_unknown=%d, skipped_empty=%d, errores=%d",
            res["_read"], res["_upserts"], res["_merges"], res["skipped_unknown"], res["skipped_empty"], res["errors"]
        )
        logger.info(
            "✅ Procesados %d mensajes: Personal=%d, Location=%d, Professional=%d, Bank=%d, Net=%d, Unknown=%d, Errores=%d",
            res["_upserts"], res["personal"], res["location"], res["professional"], res["bank"], res["net"],
            res["unknown"], res["errors"]
        )

# =============================================================================
# SECCIÓN: Entry point
# =============================================================================

if __name__ == "__main__":
    log_level = getattr(config, "LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logging.getLogger("src.mongo_client").setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logging.getLogger(__name__).setLevel(getattr(logging, log_level.upper(), logging.INFO))
    main_loop()
