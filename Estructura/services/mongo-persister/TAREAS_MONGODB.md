# 📋 TAREAS: MongoDB Persister - Persistir Datos Crudos

## 🎯 Objetivo: Configurar MongoDB y persistir los mensajes de Kafka en crudo

---

## ✅ TAREAS A REALIZAR

### **PASO 1: Configurar MongoDB con Docker**

#### 1.1 Crear/Modificar `docker-compose.yml`
📍 Ubicación: Raíz del proyecto

```yaml
version: '3.8'

services:
  # ... servicios existentes de Kafka ...
  
  mongodb:
    image: mongo:7.0
    container_name: hrpro-mongodb
    restart: always
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin123
      MONGO_INITDB_DATABASE: hrpro_db
    volumes:
      - mongodb_data:/data/db
      - ./mongo-init:/docker-entrypoint-initdb.d
    networks:
      - hrpro-network

  mongo-express:  # Interfaz web para ver MongoDB (opcional pero útil)
    image: mongo-express:latest
    container_name: hrpro-mongo-express
    restart: always
    ports:
      - "8081:8081"
    environment:
      ME_CONFIG_MONGODB_ADMINUSERNAME: admin
      ME_CONFIG_MONGODB_ADMINPASSWORD: admin123
      ME_CONFIG_MONGODB_URL: mongodb://admin:admin123@mongodb:27017/
      ME_CONFIG_BASICAUTH: false
    depends_on:
      - mongodb
    networks:
      - hrpro-network

volumes:
  mongodb_data:

networks:
  hrpro-network:
    driver: bridge
```

#### 1.2 Levantar MongoDB
```bash
docker-compose up -d mongodb mongo-express
```

#### 1.3 Verificar que MongoDB funciona
```bash
docker exec -it hrpro-mongodb mongosh --username admin --password admin123
```

---

### **PASO 2: Crear el servicio `mongo-persister`**

#### 2.1 Estructura de carpetas
```
mongo-persister/
├── src/
│   ├── __init__.py
│   ├── mongo_client.py      # Conexión a MongoDB
│   ├── persister.py          # Servicio que guarda datos
│   └── config.py             # Configuración
├── tests/
│   ├── __init__.py
│   └── test_mongo_client.py
├── requirements.txt
├── .env
├── Dockerfile
└── README.md
```

#### 2.2 Crear `requirements.txt`
📍 Ubicación: `mongo-persister/requirements.txt`

```txt
pymongo==4.6.1
python-dotenv==1.0.0
confluent-kafka==2.3.0
```

#### 2.3 Crear `.env`
📍 Ubicación: `mongo-persister/.env`

```env
# MongoDB Configuration
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USERNAME=admin
MONGO_PASSWORD=admin123
MONGO_DATABASE=hrpro_db
MONGO_COLLECTION=raw_messages

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=probando
KAFKA_GROUP_ID=mongo-persister-group
```

---

### **PASO 3: Implementar el código**

#### 3.1 Crear `mongo_client.py`
📍 Ubicación: `mongo-persister/src/mongo_client.py`

```python
"""
Cliente de MongoDB para persistir datos.
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class MongoDBClient:
    """Cliente para conectarse y operar con MongoDB"""
    
    def __init__(self):
        self.host = os.getenv('MONGO_HOST', 'localhost')
        self.port = int(os.getenv('MONGO_PORT', 27017))
        self.username = os.getenv('MONGO_USERNAME', 'admin')
        self.password = os.getenv('MONGO_PASSWORD', 'admin123')
        self.database_name = os.getenv('MONGO_DATABASE', 'hrpro_db')
        self.collection_name = os.getenv('MONGO_COLLECTION', 'raw_messages')
        
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self):
        """Establece conexión con MongoDB"""
        try:
            # Crear URI de conexión
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
            
            # Conectar
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión
            self.client.admin.command('ping')
            
            # Obtener database y collection
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            logger.info(f"✅ Conectado a MongoDB: {self.host}:{self.port}")
            logger.info(f"📦 Database: {self.database_name}")
            logger.info(f"📋 Collection: {self.collection_name}")
            
            return True
            
        except ConnectionFailure as e:
            logger.error(f"❌ Error conectando a MongoDB: {e}")
            return False
    
    def insert_one(self, document: dict) -> str:
        """
        Inserta un documento en la colección.
        
        Args:
            document: Diccionario con los datos a insertar
            
        Returns:
            str: ID del documento insertado
        """
        try:
            result = self.collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error insertando documento: {e}")
            raise
    
    def insert_many(self, documents: list) -> list:
        """
        Inserta múltiples documentos.
        
        Args:
            documents: Lista de diccionarios a insertar
            
        Returns:
            list: Lista de IDs insertados
        """
        try:
            result = self.collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Error insertando documentos: {e}")
            raise
    
    def count_documents(self) -> int:
        """Retorna el número total de documentos en la colección"""
        return self.collection.count_documents({})
    
    def close(self):
        """Cierra la conexión"""
        if self.client:
            self.client.close()
            logger.info("🔌 Conexión a MongoDB cerrada")
```

#### 3.2 Crear `persister.py`
📍 Ubicación: `mongo-persister/src/persister.py`

```python
"""
Servicio que consume mensajes de Kafka y los persiste en MongoDB.
"""
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging
import signal
import sys
from datetime import datetime
from mongo_client import MongoDBClient
import os
from dotenv import load_dotenv

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Variable global para shutdown graceful
running = True

def signal_handler(signum, frame):
    """Maneja la señal de interrupción (Ctrl+C)"""
    global running
    logger.info("\n⚠️  Señal de interrupción recibida. Cerrando...")
    running = False

# Registrar el handler de señales
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class KafkaToMongoDBPersister:
    """Consume mensajes de Kafka y los guarda en MongoDB"""
    
    def __init__(self):
        # Configuración Kafka
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
            'group.id': os.getenv('KAFKA_GROUP_ID', 'mongo-persister-group'),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.topic = os.getenv('KAFKA_TOPIC', 'probando')
        
        # Cliente MongoDB
        self.mongo_client = MongoDBClient()
        
        # Consumer Kafka
        self.consumer = None
        
        # Estadísticas
        self.stats = {
            'total_processed': 0,
            'total_saved': 0,
            'errors': 0
        }
    
    def connect(self):
        """Establece conexiones a Kafka y MongoDB"""
        # Conectar a MongoDB
        if not self.mongo_client.connect():
            logger.error("No se pudo conectar a MongoDB")
            return False
        
        # Crear consumer de Kafka
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe([self.topic])
            logger.info(f"✅ Suscrito a Kafka topic: {self.topic}")
            return True
        except Exception as e:
            logger.error(f"Error conectando a Kafka: {e}")
            return False
    
    def process_and_save(self, message_data: dict) -> bool:
        """
        Procesa y guarda un mensaje en MongoDB.
        
        Args:
            message_data: Datos del mensaje de Kafka
            
        Returns:
            bool: True si se guardó exitosamente
        """
        try:
            # Agregar metadata
            document = {
                'data': message_data,
                'timestamp': datetime.utcnow(),
                'source': 'kafka',
                'topic': self.topic,
                'processed': False  # Para el siguiente servicio que procese
            }
            
            # Guardar en MongoDB
            doc_id = self.mongo_client.insert_one(document)
            logger.debug(f"💾 Guardado documento ID: {doc_id}")
            
            self.stats['total_saved'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error guardando en MongoDB: {e}")
            self.stats['errors'] += 1
            return False
    
    def run(self):
        """Loop principal que consume y persiste mensajes"""
        global running
        
        logger.info("🚀 Iniciando Kafka to MongoDB Persister...")
        logger.info("-" * 60)
        
        try:
            while running:
                # Poll mensaje de Kafka
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Error en mensaje: {msg.error()}")
                        continue
                
                # Parsear mensaje
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    self.stats['total_processed'] += 1
                    
                    # Guardar en MongoDB
                    self.process_and_save(message_data)
                    
                    # Commit manual
                    self.consumer.commit(msg)
                    
                    # Mostrar estadísticas cada 100 mensajes
                    if self.stats['total_processed'] % 100 == 0:
                        self.print_stats()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error parseando JSON: {e}")
                    self.stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    self.stats['errors'] += 1
        
        finally:
            self.cleanup()
    
    def print_stats(self):
        """Imprime estadísticas"""
        total_in_db = self.mongo_client.count_documents()
        logger.info("=" * 60)
        logger.info("📊 ESTADÍSTICAS")
        logger.info(f"  📨 Mensajes procesados: {self.stats['total_processed']}")
        logger.info(f"  💾 Mensajes guardados: {self.stats['total_saved']}")
        logger.info(f"  🗄️  Total en MongoDB: {total_in_db}")
        logger.info(f"  ❌ Errores: {self.stats['errors']}")
        logger.info("=" * 60)
    
    def cleanup(self):
        """Limpieza al cerrar"""
        logger.info("\n🧹 Limpiando recursos...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Consumer de Kafka cerrado")
        
        self.mongo_client.close()
        self.print_stats()
        logger.info("✅ Persister cerrado correctamente")


def main():
    """Función principal"""
    persister = KafkaToMongoDBPersister()
    
    if persister.connect():
        persister.run()
    else:
        logger.error("❌ No se pudo iniciar el persister")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

---

### **PASO 4: Testing**

#### 4.1 Instalar dependencias
```bash
cd mongo-persister
pip install -r requirements.txt
```

#### 4.2 Ejecutar el persister
```bash
python src/persister.py
```

#### 4.3 Verificar datos en MongoDB

**Opción 1: Usar Mongo Express (navegador)**
```
http://localhost:8081
```

**Opción 2: Usar mongosh (terminal)**
```bash
docker exec -it hrpro-mongodb mongosh --username admin --password admin123

use hrpro_db
db.raw_messages.countDocuments()
db.raw_messages.find().limit(5).pretty()
```

**Opción 3: Crear script de verificación**
```python
# verify_data.py
from pymongo import MongoClient

client = MongoClient('mongodb://admin:admin123@localhost:27017/')
db = client['hrpro_db']
collection = db['raw_messages']

print(f"Total documentos: {collection.count_documents({})}")
print("\nPrimeros 3 documentos:")
for doc in collection.find().limit(3):
    print(doc)
```

---

## 📊 CRITERIOS DE ACEPTACIÓN

✅ MongoDB está corriendo en Docker  
✅ El servicio `mongo-persister` se conecta a MongoDB  
✅ El servicio consume mensajes de Kafka  
✅ Los mensajes se guardan en MongoDB en formato crudo (raw)  
✅ Cada documento tiene metadata (timestamp, source, etc.)  
✅ Se pueden visualizar los datos guardados  
✅ Las estadísticas muestran mensajes procesados vs guardados  
✅ No hay pérdida de mensajes  

---

## 🔗 INTEGRACIÓN CON EL EQUIPO

### Dependencias:
- ✅ Necesita que Kafka esté funcionando (ya lo tienes)
- ✅ Necesita el topic `probando` (ya existe)

### Outputs:
- 📦 Datos crudos en MongoDB listos para ser procesados
- 📊 Collection: `raw_messages` con todos los mensajes

### Siguiente paso:
- El equipo de `data-processor` tomará estos datos crudos y los agrupará por persona

---

## 📚 RECURSOS ÚTILES

- [Documentación PyMongo](https://pymongo.readthedocs.io/)
- [MongoDB Docker Hub](https://hub.docker.com/_/mongo)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)

---

## ❓ PREGUNTAS FRECUENTES

**P: ¿Dónde guardo los datos crudos?**  
R: En la collection `raw_messages` de MongoDB, tal cual vienen de Kafka.

**P: ¿Necesito procesar/limpiar los datos?**  
R: NO. Solo guárdalos tal cual (raw/crudos). El procesamiento lo hace otro servicio.

**P: ¿Qué metadata debo agregar?**  
R: timestamp, source, topic, processed (boolean).

**P: ¿Cómo coordino con el consumer de Kafka?**  
R: Usan diferentes `group.id`, así que ambos pueden leer del mismo topic.

---

## 🎯 RESULTADO ESPERADO

Al final deberías tener:
```
MongoDB corriendo → Persister corriendo → Datos guardándose en tiempo real
```

Y poder hacer queries como:
```javascript
db.raw_messages.find({ "data.name": { $exists: true } }).count()
```

---

**Fecha de inicio**: [A completar]  
**Fecha estimada de finalización**: [A completar]  
**Estado**: ⏳ Pendiente
