#!/usr/bin/env python3
"""
Script de testing para verificar el consumer de Kafka
sin necesidad de tener Kafka corriendo.
Simula mensajes para probar la lógica de parseo.
"""

import json
import sys
sys.path.append('.')

from models import (
    PersonalData, Location, ProfessionalData,
    BankData, NetData, identify_message_type
)

# Datos de prueba
test_messages = [
    {
        "type": "Personal Data",
        "data": {
            "Name": "Juan",
            "Lastname": "García",
            "Sex": "M",
            "Telfnumber": "+34612345678",
            "Passport": "ABC123456",
            "E-Mail": "juan.garcia@email.com"
        }
    },
    {
        "type": "Location",
        "data": {
            "Fullname": "Juan García",
            "City": "Madrid",
            "Address": "Calle Mayor 123"
        }
    },
    {
        "type": "Professional Data",
        "data": {
            "Fullname": "Juan García",
            "Company": "Tech Corp",
            "Company Adress": "Paseo de la Castellana 100",
            "Company Telfnumber": "+34912345678",
            "Company E-Mail": "info@techcorp.com",
            "Job": "Software Engineer"
        }
    },
    {
        "type": "Bank Data",
        "data": {
            "Passport": "ABC123456",
            "IBAN": "ES1234567890123456789012",
            "Salary": "45000"
        }
    },
    {
        "type": "Net Data",
        "data": {
            "Address": "192.168.1.100",
            "IPv4": "192.168.1.100"
        }
    }
]


def test_message_identification():
    """Prueba la identificación de tipos de mensajes"""
    print("=" * 60)
    print("🧪 TEST: Identificación de tipos de mensajes")
    print("=" * 60)
    
    for test in test_messages:
        msg_type = identify_message_type(test["data"])
        expected = test["type"].lower().replace(" data", "").replace(" ", "")
        
        status = "✅" if msg_type == expected else "❌"
        print(f"{status} {test['type']}: identificado como '{msg_type}'")
    
    print()


def test_data_parsing():
    """Prueba el parseo de datos a objetos"""
    print("=" * 60)
    print("🧪 TEST: Parseo de datos a objetos")
    print("=" * 60)
    
    for test in test_messages:
        data = test["data"]
        msg_type = identify_message_type(data)
        
        try:
            if msg_type == 'personal':
                obj = PersonalData.from_dict(data)
                print(f"✅ Personal: {obj.Name} {obj.Lastname}")
                
            elif msg_type == 'location':
                obj = Location.from_dict(data)
                print(f"✅ Location: {obj.Fullname} - {obj.City}")
                
            elif msg_type == 'professional':
                obj = ProfessionalData.from_dict(data)
                print(f"✅ Professional: {obj.Fullname} @ {obj.Company}")
                
            elif msg_type == 'bank':
                obj = BankData.from_dict(data)
                print(f"✅ Bank: {obj.Passport} - {obj.IBAN}")
                
            elif msg_type == 'net':
                obj = NetData.from_dict(data)
                print(f"✅ Net: {obj.IPv4}")
                
        except Exception as e:
            print(f"❌ Error parseando {test['type']}: {e}")
    
    print()


def test_json_parsing():
    """Prueba el parseo de JSON"""
    print("=" * 60)
    print("🧪 TEST: Parseo de JSON")
    print("=" * 60)
    
    for test in test_messages:
        try:
            json_str = json.dumps(test["data"])
            parsed = json.loads(json_str)
            print(f"✅ {test['type']}: JSON válido")
        except Exception as e:
            print(f"❌ {test['type']}: Error - {e}")
    
    print()


def main():
    """Ejecuta todos los tests"""
    print("\n" + "🎯" * 30)
    print("KAFKA CONSUMER - TEST SUITE")
    print("🎯" * 30 + "\n")
    
    test_message_identification()
    test_data_parsing()
    test_json_parsing()
    
    print("=" * 60)
    print("✅ TODOS LOS TESTS COMPLETADOS")
    print("=" * 60)


if __name__ == "__main__":
    main()
