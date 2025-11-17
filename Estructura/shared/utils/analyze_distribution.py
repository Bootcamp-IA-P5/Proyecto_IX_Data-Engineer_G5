"""
Este script analiza la distribución de tipos de datos agregados por persona en MongoDB.
Muestra cuántas personas tienen 1, 2, 3, 4 o 5 tipos, calcula la completitud global
y presenta ejemplos de registros con diferentes niveles de agregación.
Es útil para evaluar la calidad del matching y el avance del proceso de integración.
"""
import subprocess
import json
import os
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))

MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_USER = os.getenv('MONGO_USERNAME')
MONGO_PASS = os.getenv('MONGO_PASSWORD')
MONGO_CONTAINER = os.getenv('MONGO_CONTAINER')

print("=" * 80)
print(" " * 20 + "📊 ANÁLISIS DE DISTRIBUCIÓN DE TIPOS")
print("=" * 80)

try:
    result = subprocess.run([
        'docker', 'exec', MONGO_CONTAINER, 'mongosh', MONGO_DATABASE,
        '--username', MONGO_USER,
        '--password', MONGO_PASS,
        '--authenticationDatabase', 'admin',
        '--quiet', '--eval',
        '''
        const distribution = db.aggregated_data.aggregate([
            {$project: {num_types: {$size: "$types_received"}}},
            {$group: {_id: "$num_types", count: {$sum: 1}}},
            {$sort: {_id: 1}}
        ]).toArray();
        
        const sample_3_types = db.aggregated_data.findOne(
            {types_received: {$size: 3}},
            {person_id: 1, types_received: 1, data: 1}
        );
        
        const sample_4_types = db.aggregated_data.findOne(
            {types_received: {$size: 4}},
            {person_id: 1, types_received: 1, data: 1}
        );
        
        const sample_5_types = db.aggregated_data.findOne(
            {types_received: {$size: 5}},
            {person_id: 1, types_received: 1, data: 1}
        );
        
        print(JSON.stringify({
            distribution: distribution,
            sample_3: sample_3_types,
            sample_4: sample_4_types,
            sample_5: sample_5_types
        }));
        '''
    ], capture_output=True, text=True, check=True, timeout=30)
    
    data = json.loads(result.stdout.strip())
    
    print("\n📈 DISTRIBUCIÓN POR CANTIDAD DE TIPOS:")
    print("─" * 80)
    
    total_personas = 0
    for item in data['distribution']:
        num_types = item['_id']
        count = item['count']
        total_personas += count
        
        bar_width = int(count / 100000)  # Escala: 1 char = 100k personas
        bar = '█' * bar_width
        
        print(f"   {num_types} tipo{'s' if num_types > 1 else ' '}: {count:>10,} personas {bar}")
    
    print("─" * 80)
    print(f"   TOTAL:  {total_personas:>10,} personas\n")
    
    # Análisis de completitud
    total_types_needed = total_personas * 5
    if data['distribution']:
        total_types_received = sum(item['_id'] * item['count'] for item in data['distribution'])
        completitud_global = (total_types_received / total_types_needed * 100) if total_types_needed > 0 else 0
        
        print(f"📊 ANÁLISIS DE COMPLETITUD:")
        print(f"   Tipos recibidos:  {total_types_received:>12,}")
        print(f"   Tipos necesarios: {total_types_needed:>12,}")
        print(f"   Completitud:      {completitud_global:>11.2f}%")
    
    # Mostrar muestras
    print("\n" + "=" * 80)
    print("📝 MUESTRAS DE REGISTROS:")
    print("=" * 80)
    
    if data['sample_3']:
        print("\n🔸 Ejemplo con 3 tipos:")
        print(f"   person_id: {data['sample_3'].get('person_id', 'N/A')}")
        print(f"   tipos: {data['sample_3'].get('types_received', [])}")
        print(f"   campos en data: {list(data['sample_3'].get('data', {}).keys())}")
    
    if data['sample_4']:
        print("\n🔸 Ejemplo con 4 tipos:")
        print(f"   person_id: {data['sample_4'].get('person_id', 'N/A')}")
        print(f"   tipos: {data['sample_4'].get('types_received', [])}")
        print(f"   campos en data: {list(data['sample_4'].get('data', {}).keys())}")
    
    if data['sample_5']:
        print("\n🎉 Ejemplo COMPLETO (5 tipos):")
        print(f"   person_id: {data['sample_5'].get('person_id', 'N/A')}")
        print(f"   tipos: {data['sample_5'].get('types_received', [])}")
        print(f"   campos en data: {list(data['sample_5'].get('data', {}).keys())}")
    else:
        print("\n⚠️  No hay registros completos aún (5 tipos)")
    
    print("\n" + "=" * 80)
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()