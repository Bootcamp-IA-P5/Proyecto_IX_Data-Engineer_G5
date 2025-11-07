"""
Modelos de datos para los mensajes de Kafka.
Basados en los schemas descritos en el proyecto:
- Personal data
- Location
- Professional data
- Bank Data
- Net Data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PersonalData:
    """Datos personales de una persona"""
    name: str
    last_name: str
    sex: str
    telfnumber: str
    passport: str
    email: str
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea una instancia desde un diccionario"""
        # Los datos vienen con minúsculas y guiones bajos
        return cls(
            name=data.get('name', ''),
            last_name=data.get('last_name', ''),
            sex=data.get('sex', ''),
            telfnumber=data.get('telfnumber', ''),
            passport=data.get('passport', ''),
            email=data.get('email', '')
        )


@dataclass
class Location:
    """Datos de ubicación"""
    fullname: str
    city: str
    address: str
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea una instancia desde un diccionario"""
        return cls(
            fullname=data.get('fullname', ''),
            city=data.get('city', ''),
            address=data.get('address', '')
        )


@dataclass
class ProfessionalData:
    """Datos profesionales"""
    fullname: str
    company: str
    company_address: str
    company_telfnumber: str
    company_email: str
    job: str
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea una instancia desde un diccionario"""
        # El campo puede venir como 'company address' con espacio
        return cls(
            fullname=data.get('fullname', ''),
            company=data.get('company', ''),
            company_address=data.get('company address', ''),
            company_telfnumber=data.get('company_telfnumber', ''),
            company_email=data.get('company_email', ''),
            job=data.get('job', '')
        )


@dataclass
class BankData:
    """Datos bancarios"""
    passport: str
    IBAN: str
    salary: str
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea una instancia desde un diccionario"""
        return cls(
            passport=data.get('passport', ''),
            IBAN=data.get('IBAN', ''),
            salary=data.get('salary', '')
        )


@dataclass
class NetData:
    """Datos de red"""
    address: str
    IPv4: str
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea una instancia desde un diccionario"""
        return cls(
            address=data.get('address', ''),
            IPv4=data.get('IPv4', '')
        )


def identify_message_type(data: dict) -> Optional[str]:
    """
    Identifica el tipo de mensaje basándose en las claves presentes.
    
    Returns:
        str: Tipo de mensaje ('personal', 'location', 'professional', 'bank', 'net')
        None: Si no se puede identificar
    """
    keys = set(data.keys())
    
    # Personal data tiene: name, last_name, sex, telfnumber, passport, email
    if 'name' in keys and 'last_name' in keys and 'sex' in keys:
        return 'personal'
    
    # Location tiene: fullname, city, address
    if 'fullname' in keys and 'city' in keys and 'address' in keys:
        # Verificar que no sea Professional data (que también tiene fullname)
        if 'company' not in keys:
            return 'location'
    
    # Professional data tiene: fullname, company, job, company address
    if 'fullname' in keys and 'company' in keys and 'job' in keys:
        return 'professional'
    
    # Bank data tiene: passport, IBAN, salary
    if 'passport' in keys and 'IBAN' in keys and 'salary' in keys:
        return 'bank'
    
    # Net data tiene: address, IPv4
    if 'IPv4' in keys and 'address' in keys:
        return 'net'
    
    return None
