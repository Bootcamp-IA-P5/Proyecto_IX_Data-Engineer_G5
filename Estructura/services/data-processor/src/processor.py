"""
Data Processor - Agrupa 5 tipos de mensajes en un único registro por persona
"""
import time
import logging
from datetime import datetime
from typing import Dict, Optional, List
from . import config
from .mongo_client import get_mongo_client

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


# ============================================================
# DETECCIÓN DE TIPO DE MENSAJE
# ============================================================

def detect_message_type(message: Dict) -> str:
    """
    Detecta el tipo de mensaje basándose en los campos presentes
    
    Returns:
        'personal' | 'location' | 'professional' | 'bank' | 'net' | 'unknown'
    """
    fields = set(message.keys())
    
    # Tipo 1: Personal Data (name, last_name, sex, telfnumber, passport, email)
    if 'name' in fields and 'last_name' in fields:
        return 'personal'
    
    # Tipo 2: Location (fullname, city, address)
    if 'city' in fields:
        return 'location'
    
    # Tipo 3: Professional Data (fullname, company, job)
    if 'company' in fields and 'job' in fields:
        return 'professional'
    
    # Tipo 4: Bank Data (passport, IBAN, salary)
    if 'IBAN' in fields and 'salary' in fields:
        return 'bank'
    
    # Tipo 5: Net Data (address, IPv4)
    if 'IPv4' in fields or 'ipv4' in fields:
        return 'net'
    
    return 'unknown'


# ============================================================
# EXTRACCIÓN Y LIMPIEZA DE CAMPOS
# ============================================================

def clean_string(value) -> Optional[str]:
    """Limpia y valida strings"""
    if value is None:
        return None
    if isinstance(value, list):
        value = value[0] if value else None
    return str(value).strip() if value else None


def extract_passport(message: Dict) -> Optional[str]:
    """Extrae y valida el passport del mensaje"""
    passport = message.get('passport')
    if passport:
        passport = clean_string(passport)
        # Validar formato básico (al menos 5 caracteres alfanuméricos)
        if passport and len(passport) >= 5:
            return passport
    return None


def extract_fullname(message: Dict) -> Optional[str]:
    """
    Extrae fullname del mensaje
    Puede estar en 'fullname' o construirse de 'name' + 'last_name'
    """
    # Opción 1: Campo fullname directo
    if 'fullname' in message:
        return clean_string(message['fullname'])
    
    # Opción 2: Construir de name + last_name
    name = clean_string(message.get('name'))
    last_name = clean_string(message.get('last_name'))
    
    if name and last_name:
        return f"{name} {last_name}"
    elif name:
        return name
    elif last_name:
        return last_name
    
    return None


def extract_email(message: Dict) -> Optional[str]:
    """Extrae y valida email"""
    email = clean_string(message.get('email'))
    if email and '@' in email:
        return email.lower()
    return None


def extract_address(message: Dict) -> Optional[str]:
    """Extrae address (normalizado)"""
    address = clean_string(message.get('address'))
    return address.lower() if address else None


# ============================================================
# GENERACIÓN DE CLAVES DE AGRUPACIÓN
# ============================================================

def generate_grouping_key(message: Dict) -> Optional[str]:
    """
    Genera clave única para agrupar mensajes de la misma persona
    
    Prioridad:
    1. passport (más confiable)
    2. email (único por persona)
    3. fullname + address (combinación)
    4. fullname solo (menos confiable)
    5. FALLBACK: address sola (para Net Data sin otros identificadores)
    
    Returns:
        Clave única o None si no se puede determinar
    """
    # Prioridad 1: Passport
    passport = extract_passport(message)
    if passport:
        return f"passport:{passport}"
    
    # Prioridad 2: Email
    email = extract_email(message)
    if email:
        return f"email:{email}"
    
    # Prioridad 3: Fullname + Address
    fullname = extract_fullname(message)
    address = extract_address(message)
    
    if fullname and address:
        # Normalizar para agrupación
        key = f"{fullname.lower()}|{address[:50]}"  # Limitar address a 50 chars
        return f"name_addr:{key}"
    
    # Prioridad 4: Solo fullname (menos confiable)
    if fullname:
        return f"name:{fullname.lower()}"
    
    # Prioridad 5: FALLBACK - Solo address (para Net Data)
    # Esto es arriesgado pero necesario para Net Data que viene sin identificadores
    if address:
        return f"addr:{address[:50]}"
    
    # No se puede agrupar
    logger.warning(f"⚠️  No se pudo generar clave de agrupación para mensaje: {message.get('_id')}")
    return None



# ============================================================
# PROCESAMIENTO DE CADA TIPO DE MENSAJE
# ============================================================

def process_personal_data(message: Dict) -> Dict:
    """Procesa mensaje tipo Personal Data"""
    return {
        'name': clean_string(message.get('name')),
        'last_name': clean_string(message.get('last_name')),
        'sex': message.get('sex'),  # Ya es lista
        'telfnumber': clean_string(message.get('telfnumber')),
        'email': extract_email(message),
        'passport': extract_passport(message),
    }


def process_location_data(message: Dict) -> Dict:
    """Procesa mensaje tipo Location"""
    return {
        'fullname': extract_fullname(message),
        'city': clean_string(message.get('city')),
        'address': clean_string(message.get('address')),
    }


def process_professional_data(message: Dict) -> Dict:
    """Procesa mensaje tipo Professional Data"""
    return {
        'company': clean_string(message.get('company')),
        'company_address': clean_string(message.get('company address')),
        'company_telfnumber': clean_string(message.get('company_telfnumber')),
        'company_email': clean_string(message.get('company_email')),
        'job': clean_string(message.get('job')),
    }


def process_bank_data(message: Dict) -> Dict:
    """Procesa mensaje tipo Bank Data"""
    return {
        'passport': extract_passport(message),
        'iban': clean_string(message.get('IBAN')),
        'salary': clean_string(message.get('salary')),
    }


def process_net_data(message: Dict) -> Dict:
    """Procesa mensaje tipo Net Data"""
    # Manejar ambos casos: 'IPv4' o 'ipv4'
    ipv4 = clean_string(message.get('IPv4') or message.get('ipv4'))
    
    return {
        'ipv4': ipv4,
        'net_address': clean_string(message.get('address')),
    }


# ============================================================
# AGRUPACIÓN Y MERGE DE DATOS
# ============================================================

def merge_data(existing: Dict, new_data: Dict, msg_type: str) -> Dict:
    """
    Merge inteligente: solo actualiza campos que no existen o están vacíos
    
    Args:
        existing: Datos existentes del documento agregado
        new_data: Nuevos datos a incorporar
        msg_type: Tipo de mensaje ('personal', 'location', etc.)
    
    Returns:
        Documento actualizado
    """
    result = existing.copy()
    
    # Actualizar campos solo si no existen o están vacíos
    for key, value in new_data.items():
        if value is not None and value != '':
            # Si el campo no existe o está vacío, actualizar
            if key not in result or result[key] is None or result[key] == '':
                result[key] = value
            # Si existe pero es diferente, loguear inconsistencia
            elif result[key] != value:
                logger.debug(f"⚠️  Inconsistencia en '{key}': existe='{result[key]}', nuevo='{value}'")
    
    # Actualizar metadata
    if 'types_received' not in result:
        result['types_received'] = []
    
    if msg_type not in result['types_received']:
        result['types_received'].append(msg_type)
    
    result['messages_count'] = result.get('messages_count', 0) + 1
    result['last_updated'] = datetime.utcnow()
    
    # Verificar si está completo (tiene los 5 tipos)
    result['is_complete'] = len(result['types_received']) == 5
    
    return result


def process_message(message: Dict, mongo_client) -> bool:
    """
    Procesa un mensaje y lo agrega al registro correspondiente
    
    Args:
        message: Mensaje raw de MongoDB
        mongo_client: Cliente MongoDB
        
    Returns:
        True si se procesó correctamente
    """
    # Detectar tipo
    msg_type = detect_message_type(message)
    
    if msg_type == 'unknown':
        logger.warning(f"⚠️  Mensaje desconocido: {message.get('_id')} - Campos: {list(message.keys())}")
        return False
    
    # Generar clave de agrupación
    grouping_key = generate_grouping_key(message)
    if not grouping_key:
        return False
    
    # Procesar según tipo
    processors = {
        'personal': process_personal_data,
        'location': process_location_data,
        'professional': process_professional_data,
        'bank': process_bank_data,
        'net': process_net_data,
    }
    
    try:
        new_data = processors[msg_type](message)
        
        # Obtener documento existente (si existe)
        existing = mongo_client.aggregated_collection.find_one({'_grouping_key': grouping_key}) or {}
        
        # Merge de datos
        merged = merge_data(existing, new_data, msg_type)
        
        # Asegurar que tiene la clave de agrupación
        merged['_grouping_key'] = grouping_key
        
        # Extraer passport si está disponible (clave preferida)
        if 'passport' in merged and merged['passport']:
            merged['_main_key'] = f"passport:{merged['passport']}"
        else:
            merged['_main_key'] = grouping_key
        
        # Agregar ID del mensaje fuente
        if 'source_message_ids' not in merged:
            merged['source_message_ids'] = []
        merged['source_message_ids'].append(str(message['_id']))
        
        # Guardar/actualizar
        mongo_client.aggregated_collection.update_one(
            {'_grouping_key': grouping_key},
            {'$set': merged},
            upsert=True
        )
        
        logger.debug(f"✅ {msg_type.upper()}: {grouping_key}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error procesando {msg_type}: {e}")
        return False


# ============================================================
# PROCESAMIENTO POR LOTES
# ============================================================

def process_batch(mongo_client) -> int:
    """
    Procesa un lote de mensajes
    
    Returns:
        Cantidad de mensajes procesados
    """
    # Obtener mensajes sin procesar
    messages = mongo_client.get_unprocessed_documents(config.BATCH_SIZE)
    
    if not messages:
        return 0
    
    stats = {
        'personal': 0,
        'location': 0,
        'professional': 0,
        'bank': 0,
        'net': 0,
        'unknown': 0,
        'errors': 0
    }
    
    processed_ids = []
    
    for msg in messages:
        msg_type = detect_message_type(msg)
        
        if process_message(msg, mongo_client):
            stats[msg_type] += 1
            processed_ids.append(msg['_id'])
        else:
            if msg_type == 'unknown':
                stats['unknown'] += 1
            else:
                stats['errors'] += 1
            # Marcar como procesado de todos modos para no bloquearse
            processed_ids.append(msg['_id'])
    
    # Marcar como procesados
    if processed_ids:
        mongo_client.mark_as_processed(processed_ids)
    
    logger.info(
        f"✅ Procesados {len(messages)} mensajes: "
        f"Personal={stats['personal']}, Location={stats['location']}, "
        f"Professional={stats['professional']}, Bank={stats['bank']}, "
        f"Net={stats['net']}, Unknown={stats['unknown']}, Errors={stats['errors']}"
    )
    
    return len(messages)


# ============================================================
# ESTADÍSTICAS
# ============================================================

def print_stats(mongo_client):
    """Imprime estadísticas detalladas"""
    stats = mongo_client.get_stats()
    
    logger.info("=" * 80)
    logger.info("📊 ESTADÍSTICAS DE PROCESAMIENTO")
    logger.info("=" * 80)
    logger.info(f"Raw Messages:")
    logger.info(f"  - Total: {stats.get('raw_total', 0):,}")
    logger.info(f"  - Procesados: {stats.get('raw_processed', 0):,}")
    logger.info(f"  - Pendientes: {stats.get('raw_pending', 0):,}")
    logger.info(f"Datos Agregados:")
    logger.info(f"  - Total personas: {stats.get('aggregated_total', 0):,}")
    logger.info(f"  - Registros completos (5 tipos): {stats.get('aggregated_complete', 0):,}")
    
    # Calcular porcentaje de completitud
    if stats.get('aggregated_total', 0) > 0:
        completeness = (stats.get('aggregated_complete', 0) / stats.get('aggregated_total', 0)) * 100
        logger.info(f"  - Completitud: {completeness:.2f}%")
    
    logger.info("=" * 80)


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    """Loop principal del procesador"""
    logger.info("🚀 Iniciando Data Processor...")
    config.print_config()
    
    # Obtener cliente MongoDB
    mongo_client = get_mongo_client()
    
    # Contador para estadísticas periódicas
    batch_count = 0
    
    while True:
        try:
            processed = process_batch(mongo_client)
            
            if processed == 0:
                logger.info(f"⏳ No hay mensajes. Esperando {config.POLL_INTERVAL}s...")
                time.sleep(config.POLL_INTERVAL)
            else:
                batch_count += 1
                time.sleep(1)
                
                # Mostrar estadísticas cada STATS_INTERVAL batches
                if batch_count % config.STATS_INTERVAL == 0:
                    print_stats(mongo_client)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Deteniendo procesador...")
            print_stats(mongo_client)
            mongo_client.close()
            break
            
        except Exception as e:
            logger.error(f"❌ Error en loop principal: {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()