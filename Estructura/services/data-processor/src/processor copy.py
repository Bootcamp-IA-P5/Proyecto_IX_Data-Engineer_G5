import os
import time
from pymongo import MongoClient, UpdateOne
from datetime import datetime

# Configuración desde variables de entorno (sin valores por defecto)
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
RAW_COLLECTION = os.getenv('MONGO_COLLECTION')
PERSONAL_COLLECTION = 'personal_data'
FINANCIAL_COLLECTION = 'financial_data'
EMPLOYMENT_COLLECTION = 'employment_data'
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))

def connect_mongo():
    """Conectar a MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print(f"✅ Conectado a MongoDB: {MONGO_URI}")
        print(f"📁 Base de datos: {DB_NAME}")
        print(f"📋 Colección raw: {RAW_COLLECTION}")
        return client[DB_NAME]
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {e}")
        raise

def detect_message_type(message):
    """Detectar tipo de mensaje basándose en campos presentes"""
    fields = set(message.keys())
    
    # Personal: address, IPv4
    if 'address' in fields or 'IPv4' in fields:
        return 'personal'
    
    # Financial: passport, IBAN, salary
    if 'passport' in fields or 'IBAN' in fields or 'salary' in fields:
        return 'financial'
    
    # Employment: fullname, company, job
    if 'fullname' in fields or 'company' in fields or 'job' in fields:
        return 'employment'
    
    return 'unknown'

def process_personal(message):
    """Procesar datos personales"""
    return {
        '_id': message['_id'],
        'address': message.get('address'),
        'ipv4': message.get('IPv4'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }

def process_financial(message):
    """Procesar datos financieros"""
    return {
        '_id': message['_id'],
        'passport': message.get('passport'),
        'iban': message.get('IBAN'),
        'salary': message.get('salary'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }

def process_employment(message):
    """Procesar datos laborales"""
    return {
        '_id': message['_id'],
        'fullname': message.get('fullname'),
        'company': message.get('company'),
        'company_address': message.get('company address'),
        'company_phone': message.get('company_telfnumber'),
        'company_email': message.get('company_email'),
        'job': message.get('job'),
        'processed_at': datetime.utcnow(),
        '_kafka_metadata': message.get('_kafka_metadata', {}),
        '_inserted_at': message.get('_inserted_at')
    }

def process_batch(db, batch_size=BATCH_SIZE):
    """Procesar un lote de mensajes"""
    raw_coll = db[RAW_COLLECTION]
    
    # Obtener mensajes sin procesar
    messages = list(raw_coll.find({'processed': {'$ne': True}}).limit(batch_size))
    
    if not messages:
        return 0
    
    personal_ops = []
    financial_ops = []
    employment_ops = []
    update_ops = []
    stats = {'personal': 0, 'financial': 0, 'employment': 0, 'unknown': 0}
    
    for msg in messages:
        msg_type = detect_message_type(msg)
        
        try:
            if msg_type == 'personal':
                processed = process_personal(msg)
                personal_ops.append(UpdateOne(
                    {'_id': processed['_id']},
                    {'$set': processed},
                    upsert=True
                ))
                stats['personal'] += 1
                
            elif msg_type == 'financial':
                processed = process_financial(msg)
                financial_ops.append(UpdateOne(
                    {'_id': processed['_id']},
                    {'$set': processed},
                    upsert=True
                ))
                stats['financial'] += 1
                
            elif msg_type == 'employment':
                processed = process_employment(msg)
                employment_ops.append(UpdateOne(
                    {'_id': processed['_id']},
                    {'$set': processed},
                    upsert=True
                ))
                stats['employment'] += 1
                
            else:
                stats['unknown'] += 1
                print(f"⚠️  Mensaje desconocido: {msg['_id']} - Campos: {list(msg.keys())}")
            
            # Marcar como procesado
            update_ops.append(UpdateOne(
                {'_id': msg['_id']},
                {'$set': {'processed': True, 'processed_at': datetime.utcnow()}}
            ))
            
        except Exception as e:
            print(f"❌ Error procesando mensaje {msg['_id']}: {e}")
    
    # Ejecutar operaciones bulk
    if personal_ops:
        db[PERSONAL_COLLECTION].bulk_write(personal_ops, ordered=False)
    if financial_ops:
        db[FINANCIAL_COLLECTION].bulk_write(financial_ops, ordered=False)
    if employment_ops:
        db[EMPLOYMENT_COLLECTION].bulk_write(employment_ops, ordered=False)
    if update_ops:
        raw_coll.bulk_write(update_ops, ordered=False)
    
    print(f"✅ Procesados {len(messages)} mensajes: "
          f"Personal={stats['personal']}, "
          f"Financial={stats['financial']}, "
          f"Employment={stats['employment']}, "
          f"Unknown={stats['unknown']}")
    
    return len(messages)

def main():
    """Loop principal del procesador"""
    print("🚀 Iniciando Data Processor...")
    db = connect_mongo()
    
    while True:
        try:
            processed = process_batch(db)
            
            if processed == 0:
                print(f"⏳ No hay mensajes. Esperando {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
            else:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Deteniendo procesador...")
            break
        except Exception as e:
            print(f"❌ Error en loop principal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()