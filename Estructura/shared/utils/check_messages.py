"""
Analiza patrones y estadísticas de los mensajes en raw_messages

Este script analiza los primeros 100 mensajes de la colección raw_messages en MongoDB,
mostrando los campos más frecuentes y ejemplos reales. 
Sirve para entender la estructura y patrones de los datos recibidos, 
ayudando a ajustar el procesamiento y la lógica de agrupación.
"""

import os
import sys
import json
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

def get_message_patterns():
    try:
        result = os.popen(
            f'docker exec {MONGO_CONTAINER} mongosh {MONGO_DATABASE} '
            f'--username {MONGO_USER} --password {MONGO_PASS} --authenticationDatabase admin --quiet --eval "' +
            "const msgs = db.raw_messages.find({}).limit(100).toArray();"
            "const patterns = {};"
            "msgs.forEach(msg => {"
            "  Object.keys(msg).forEach(k => { patterns[k] = (patterns[k] || 0) + 1; });"
            "});"
            "print(JSON.stringify({patterns: patterns, examples: msgs.slice(0,5)}));"
            '"'
        ).read().strip()
        return json.loads(result)
    except Exception as e:
        print(f"❌ Error obteniendo patrones: {e}")
        return None

if __name__ == "__main__":
    print("=" * 80)
    print(" " * 25 + "📊 ANÁLISIS DE PATRONES EN RAW_MESSAGES")
    print("=" * 80)

    stats = get_message_patterns()
    if not stats:
        print("No se pudieron obtener patrones.")
        sys.exit(1)

    print("\n🔑 Campos más frecuentes en los primeros 100 mensajes:")
    for k, v in sorted(stats['patterns'].items(), key=lambda x: -x[1]):
        print(f"   {k}: {v}")

    print("\n🔎 Ejemplos de mensajes:")
    for msg in stats['examples']:
        print(f"   {msg}")

    print("\n" + "=" * 80)