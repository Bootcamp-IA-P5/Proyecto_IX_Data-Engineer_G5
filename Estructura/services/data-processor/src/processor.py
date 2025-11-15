"""
Data Processor - Agrupa y procesa datos crudos de MongoDB

Responsabilidades (Issue #6):
1. Leer mensajes crudos de raw_messages
2. Agrupar por persona (Passport, Fullname, Address)
3. Unificar 5 tipos de datos en 1 registro
4. Validar integridad de datos
5. Guardar en aggregated_data (MongoDB temporal)
6. SQL Persister (Issue #8) se encargará de mover a SQL
"""
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import config
from mongo_client import get_mongo_client

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

# Control de cierre graceful
shutdown_requested = False


def signal_handler(sig, frame):
    """Maneja señales de cierre (Ctrl+C, SIGTERM)"""
    global shutdown_requested
    logger.info("🛑 Señal de cierre recibida. Finalizando...")
    shutdown_requested = True


class DataProcessor:
    """
    Procesador de Datos - Agrupa información por persona
    
    Estrategia de agrupación:
    1. Lee documentos crudos (raw_messages)
    2. Agrupa por passport (clave principal)
    3. Unifica 5 tipos de datos:
       - Personal Info
       - Contact Info
       - Address
       - Employment
       - Benefits
    4. Valida que estén completos
    5. Guarda en aggregated_data
    """
    
    def __init__(self):
        """Inicializa el procesador"""
        self.mongo_client = None
        self.stats = {
            'total_processed': 0,
            'total_grouped': 0,
            'total_incomplete': 0,
            'errors': 0,
            'start_time': time.time()
        }
        # Buffer temporal para acumular datos por persona
        self.person_buffer: Dict[str, Dict] = defaultdict(dict)
    
    def setup(self):
        """Configura conexión a MongoDB"""
        logger.info("🚀 Iniciando Data Processor")
        config.print_config()
        
        logger.info("📦 Conectando a MongoDB...")
        self.mongo_client = get_mongo_client()
        
        logger.info("✅ Data Processor listo. Esperando datos...")
    
    def validate_document(self, doc: Dict) -> bool:
        """
        Valida que un documento tenga estructura mínima
        
        Args:
            doc: Documento a validar
            
        Returns:
            True si es válido
        """
        # Campos obligatorios
        required_fields = ['_kafka_metadata', '_inserted_at']
        
        for field in required_fields:
            if field not in doc:
                logger.warning(f"⚠️ Documento inválido: falta campo '{field}'")
                return False
        
        return True
    
    def extract_person_key(self, doc: Dict) -> Optional[str]:
        """
        Extrae clave única de la persona (passport)
        
        Args:
            doc: Documento con datos
            
        Returns:
            Passport de la persona o None
        """
        # TODO: Adaptar según estructura real de tus mensajes
        # Por ahora asume que el campo 'passport' está en el root
        passport = doc.get('passport')
        
        if not passport:
            # Intentar extraer de otros campos
            passport = doc.get('data', {}).get('passport')
        
        if not passport:
            logger.warning(f"⚠️ Documento sin passport: {doc.get('_id')}")
        
        return passport
    
    def categorize_message(self, doc: Dict) -> Optional[str]:
        """
        Categoriza el tipo de mensaje
        
        Args:
            doc: Documento con datos
            
        Returns:
            Tipo de mensaje: 'personal', 'contact', 'address', 'employment', 'benefits'
        """
        # TODO: Adaptar según tu estructura de mensajes
        # Ejemplo: si tienes un campo 'message_type' o 'event_type'
        msg_type = doc.get('message_type') or doc.get('event_type')
        
        # Mapeo de tipos (adaptar según tus datos)
        type_mapping = {
            'personal_info': 'personal',
            'contact_info': 'contact',
            'address_info': 'address',
            'employment_info': 'employment',
            'benefits_info': 'benefits'
        }
        
        return type_mapping.get(msg_type)
    
    def accumulate_person_data(self, doc: Dict):
        """
        Acumula datos de una persona en el buffer temporal
        
        Args:
            doc: Documento con datos de un tipo específico
        """
        try:
            # Extraer passport
            passport = self.extract_person_key(doc)
            if not passport:
                self.stats['errors'] += 1
                return
            
            # Categorizar mensaje
            category = self.categorize_message(doc)
            if not category:
                logger.warning(f"⚠️ Tipo de mensaje no reconocido: {doc.get('message_type')}")
                self.stats['errors'] += 1
                return
            
            # Inicializar estructura de persona si no existe
            if passport not in self.person_buffer:
                self.person_buffer[passport] = {
                    'passport': passport,
                    'personal': None,
                    'contact': None,
                    'address': None,
                    'employment': None,
                    'benefits': None,
                    'first_seen': datetime.utcnow(),
                    'last_updated': datetime.utcnow(),
                    'source_ids': []  # IDs de documentos crudos usados
                }
            
            # Actualizar categoría específica
            self.person_buffer[passport][category] = doc
            self.person_buffer[passport]['last_updated'] = datetime.utcnow()
            self.person_buffer[passport]['source_ids'].append(doc.get('_id'))
            
            logger.debug(f"📥 Acumulado: {passport} - {category}")
            
        except Exception as e:
            logger.error(f"❌ Error acumulando datos: {e}")
            self.stats['errors'] += 1
    
    def is_person_complete(self, person_data: Dict) -> bool:
        """
        Verifica si una persona tiene los 5 tipos de datos
        
        Args:
            person_data: Diccionario con datos de la persona
            
        Returns:
            True si tiene los 5 tipos completos
        """
        required_types = ['personal', 'contact', 'address', 'employment', 'benefits']
        return all(person_data.get(t) is not None for t in required_types)
    
    def should_flush_person(self, person_data: Dict) -> bool:
        """
        Determina si se debe guardar una persona (completa o por timeout)
        
        Args:
            person_data: Datos de la persona
            
        Returns:
            True si se debe guardar
        """
        # Si está completa, siempre flush
        if self.is_person_complete(person_data):
            return True
        
        # Si pasó el tiempo de ventana, flush aunque esté incompleta
        elapsed = (datetime.utcnow() - person_data['first_seen']).total_seconds()
        if elapsed > config.GROUPING_WINDOW:
            logger.info(f"⏰ Timeout: guardando persona incompleta (passport: {person_data['passport']})")
            return True
        
        return False
    
    def create_aggregated_document(self, person_data: Dict) -> Dict:
        """
        Crea documento agregado con validación de integridad
        
        Args:
            person_data: Datos acumulados de la persona
            
        Returns:
            Documento agregado listo para MongoDB
        """
        # Validar integridad
        is_complete = self.is_person_complete(person_data)
        
        # Extraer datos limpios de cada categoría
        aggregated = {
            'passport': person_data['passport'],
            'is_complete': is_complete,
            'missing_data': [],
            'processed_at': datetime.utcnow(),
            'first_seen': person_data['first_seen'],
            'last_updated': person_data['last_updated'],
            'source_count': len(person_data['source_ids']),
            'source_ids': person_data['source_ids']
        }
        
        # Agregar cada tipo de dato
        for data_type in ['personal', 'contact', 'address', 'employment', 'benefits']:
            if person_data[data_type]:
                aggregated[data_type] = person_data[data_type]
            else:
                aggregated['missing_data'].append(data_type)
        
        return aggregated
    
    def flush_person_buffer(self):
        """
        Procesa personas del buffer que estén listas
        """
        passports_to_remove = []
        
        for passport, person_data in self.person_buffer.items():
            if self.should_flush_person(person_data):
                # Crear documento agregado
                agg_doc = self.create_aggregated_document(person_data)
                
                # Guardar en MongoDB
                if self.mongo_client.upsert_aggregated(agg_doc):
                    self.stats['total_grouped'] += 1
                    
                    if not agg_doc['is_complete']:
                        self.stats['total_incomplete'] += 1
                        logger.info(
                            f"⚠️ Persona incompleta guardada: {passport} "
                            f"(falta: {', '.join(agg_doc['missing_data'])})"
                        )
                    else:
                        logger.info(f"✅ Persona completa: {passport}")
                    
                    # Marcar documentos crudos como procesados
                    self.mongo_client.mark_as_processed(person_data['source_ids'])
                    
                    # Preparar para eliminación del buffer
                    passports_to_remove.append(passport)
                else:
                    self.stats['errors'] += 1
        
        # Limpiar buffer
        for passport in passports_to_remove:
            del self.person_buffer[passport]
    
    def process_batch(self):
        """
        Procesa un lote de documentos crudos
        
        Flujo:
        1. Lee BATCH_SIZE documentos de raw_messages
        2. Valida cada documento
        3. Acumula en buffer por persona
        4. Flush personas completas o con timeout
        """
        try:
            # Leer documentos sin procesar
            logger.info(f"📥 Leyendo hasta {config.BATCH_SIZE} documentos crudos...")
            docs = self.mongo_client.get_unprocessed_documents(config.BATCH_SIZE)
            
            if not docs:
                logger.info("ℹ️  No hay documentos nuevos para procesar")
                # Flush buffer por timeout
                self.flush_person_buffer()
                return
            
            logger.info(f"✅ Se encontraron {len(docs)} documentos para procesar")
            
            # Procesar cada documento
            for doc in docs:
                if self.validate_document(doc):
                    self.accumulate_person_data(doc)
                    self.stats['total_processed'] += 1
                else:
                    self.stats['errors'] += 1
            
            # Flush personas listas
            self.flush_person_buffer()
            
        except Exception as e:
            logger.error(f"❌ Error en process_batch: {e}")
            self.stats['errors'] += 1
    
    def print_stats(self):
        """Muestra estadísticas del procesador"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 60)
        logger.info("📊 ESTADÍSTICAS DEL DATA PROCESSOR")
        logger.info("=" * 60)
        logger.info(f"Total procesados: {self.stats['total_processed']:,}")
        logger.info(f"Personas agrupadas: {self.stats['total_grouped']:,}")
        logger.info(f"Personas incompletas: {self.stats['total_incomplete']:,}")
        logger.info(f"Errores: {self.stats['errors']}")
        logger.info(f"En buffer: {len(self.person_buffer)}")
        logger.info(f"Rate: {rate:.2f} mensajes/segundo")
        logger.info(f"Tiempo: {elapsed:.2f} segundos")
        
        # Estadísticas de MongoDB
        mongo_stats = self.mongo_client.get_stats()
        logger.info(f"MongoDB raw_messages: {mongo_stats.get('raw_total', 0):,}")
        logger.info(f"MongoDB pendientes: {mongo_stats.get('raw_pending', 0):,}")
        logger.info(f"MongoDB agregados: {mongo_stats.get('aggregated_total', 0):,}")
        logger.info("=" * 60)
    
    def run(self):
        """Loop principal del procesador"""
        try:
            self.setup()
            
            last_stats_time = time.time()
            
            while not shutdown_requested:
                # Procesar lote
                self.process_batch()
                
                # Mostrar estadísticas periódicamente
                if time.time() - last_stats_time >= config.STATS_INTERVAL:
                    self.print_stats()
                    last_stats_time = time.time()
                
                # Esperar antes del siguiente lote
                logger.info(f"⏳ Esperando {config.PROCESSING_INTERVAL}s antes del siguiente lote...")
                time.sleep(config.PROCESSING_INTERVAL)
            
            # Flush final
            logger.info("🔄 Flush final de buffer...")
            self.flush_person_buffer()
            self.print_stats()
            
        except KeyboardInterrupt:
            logger.info("⌨️  Interrupción por teclado")
        except Exception as e:
            logger.error(f"❌ Error en run(): {e}", exc_info=True)
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos antes de cerrar"""
        logger.info("🧹 Limpiando recursos...")
        
        if self.mongo_client:
            self.mongo_client.close()
        
        logger.info("👋 Data Processor finalizado")


def main():
    """Punto de entrada principal"""
    # Registrar handlers de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Crear y ejecutar processor
    processor = DataProcessor()
    processor.run()


if __name__ == "__main__":
    main()