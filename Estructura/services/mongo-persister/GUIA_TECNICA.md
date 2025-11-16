# 🗄️ MongoDB Persister - Guía Técnica Consolidada

## 🎯 Propósito del Servicio

**Función:** Consumir mensajes crudos de Kafka y persistirlos en MongoDB sin procesarlos.

**Filosofía:** *Store first, process later* - Guardamos TODO, procesamos después.

---

## 📊 Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                    FLUJO DE DATOS COMPLETO                      │
└─────────────────────────────────────────────────────────────────┘

  Random Generator        Kafka             Mongo Persister         MongoDB
  (Simulador externo)   (Message Broker)   (Este servicio)         (Persistencia)
┌─────────────────┐   ┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│ Python Script   │   │ Topic:      │    │ Python Service  │    │ Database:    │
│ (datagen/)      │──→│ probando    │───→│ - mongo_client  │───→│ hrpro_db     │
│                 │   │             │    │ - persister     │    │              │
│ Genera ~100 msg │   │ Partitions: │    │ - config        │    │ Collection:  │
│ y se detiene    │   │ 3           │    │                 │    │ transactions │
└─────────────────┘   │             │    └─────────────────┘    └──────────────┘
        │             │ Port:       │             │                      │
        │             │ 9092 (int)  │             │                      │
        │             │ 29092 (ext) │             │                      │
        │             └─────────────┘             │                      │
        │                    │                    │                      │
        │                    ▼                    │                      ▼
        │             ┌─────────────┐             │              ┌──────────────┐
        │             │  Kafdrop    │             │              │ Mongo Express│
        └────────────→│  (UI Kafka) │             │              │ (UI MongoDB) │
                      │  Port: 9000 │             │              │ Port: 8081   │
                      └─────────────┘             │              └──────────────┘
                                                  │
                                         ┌────────┴────────┐
                                         │   Tests Suite   │
                                         │ 56 tests, 71%   │
                                         └─────────────────┘
```

---

## 🏗️ Componentes del Sistema

### **1. Random Generator (Simulador de Datos Externos)**

**Repositorio origen:** https://github.com/Factoria-F5-madrid/data-engineering-educational-project

**Ubicación:** `Estructura/datagen/`

**Función:**
- ✅ Simula una fuente externa de datos (ej: sistema bancario PIX)
- ✅ Genera ~100 mensajes de transacciones ficticias
- ✅ Los envía al topic `probando` de Kafka
- ✅ Se detiene automáticamente después de 2 minutos

**Características:**
```python
# Genera mensajes con esta estructura:
{
  "transaction_id": "TX123456",
  "amount": 150.75,
  "currency": "USD",
  "timestamp": 1700000000000,
  "sender": "Alice",
  "receiver": "Bob"
}
```

**Cómo funciona:**
1. Se levanta con `docker-compose -f docker-compose-kafka.yml up -d random_generator`
2. Genera mensajes durante ~2 minutos
3. Se detiene automáticamente (`restart: "no"` en docker-compose)
4. Para más mensajes: `docker-compose -f docker-compose-kafka.yml restart random_generator`

**⚠️ NO está en nuestro repositorio:**
- Es un componente externo (repositorio de Factoría F5)
- Lo implementamos desde ese repo educativo
- Simula datos de fuentes externas (bancos, APIs, etc.)
- En producción, esto sería un sistema real

**💡 Control del generador:**
```bash
# Levantar el generador
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Ver logs en tiempo real
docker logs -f random_generator

# Detener manualmente (si no quieres esperar 2 minutos)
docker stop random_generator

# Reiniciar para generar más mensajes
docker-compose -f docker-compose-kafka.yml restart random_generator

# Verificar si está corriendo
docker ps | grep random_generator
```

---

### **2. Kafka (Message Broker)**

**Imagen:** `confluentinc/cp-kafka:7.5.0`

**Puertos:**
- `9092` → Comunicación interna (Docker network)
- `29092` → Comunicación externa (localhost)

**Topic principal:** `probando`
- Particiones: 3
- Replication factor: 1
- Auto-create: enabled

**Configuración de retención:**
```yaml
KAFKA_LOG_RETENTION_HOURS: 1          # Retiene mensajes 1 hora
KAFKA_LOG_RETENTION_BYTES: 524288000  # Máximo 500MB por partición
```

**💡 Por qué retención corta:**
- Desarrollo local: no necesitamos días de datos
- Evita saturar el disco
- Kafka es una "cola temporal", MongoDB es la persistencia definitiva

---

### **3. Zookeeper (Coordinador de Kafka)**

**Imagen:** `confluentinc/cp-zookeeper:7.5.0`

**Puerto:** `22181` (externo) → `2181` (interno)

**Función:**
- Coordina el cluster de Kafka
- Gestiona metadata de topics y particiones
- Necesario para que Kafka funcione

---

### **4. Kafdrop (UI para Kafka)**

**Imagen:** `obsidiandynamics/kafdrop:latest`

**Puerto:** `9000`

**Acceso:** http://localhost:9000

**Funcionalidades:**
- 📊 Ver topics y sus mensajes
- 📈 Monitorear consumer groups y LAG
- 🔍 Buscar mensajes específicos
- ⚙️ Ver configuración de brokers

**💡 Uso recomendado:**
```bash
# 1. Abrir Kafdrop
http://localhost:9000

# 2. Ver el topic "probando"
http://localhost:9000/topic/probando

# 3. Ver consumer group "mongo-persister-group"
http://localhost:9000/consumer/mongo-persister-group

# 4. Ver LAG (mensajes pendientes)
# LAG = 0 → Todo procesado
# LAG > 0 → Hay mensajes pendientes
```

---

### **5. MongoDB (Base de Datos)**

**Imagen:** `mongo:7.0`

**Puerto:** `27017`

**Base de datos:** `hrpro_db`

**Colección:** `transactions`

**Autenticación:**
```bash
# Usuario: <user>
# Password: <passwd>
# Auth DB: admin
```

**Verificar datos:**
```bash
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.countDocuments()"
```

---

### **6. Mongo Express (UI para MongoDB)**

**Imagen:** `mongo-express:latest`

**Puerto:** `8081`

**Acceso:** http://localhost:8081

**Autenticación web:**
```bash
# Usuario: <user>
# Password: <passwd>
```

**Funcionalidades:**
- 📊 Ver bases de datos y colecciones
- 🔍 Buscar y filtrar documentos
- ✏️ Editar documentos
- 📈 Estadísticas de colecciones

---

### **7. Mongo Persister (Este Servicio)**

**Código:** `Estructura/services/mongo-persister/`

**Componentes:**

#### **`src/config.py`** - Configuración centralizada
```python
# Variables de entorno (desde .env o Docker)
- KAFKA_BOOTSTRAP_SERVERS
- KAFKA_TOPIC
- KAFKA_GROUP_ID
- MONGO_URI
- MONGO_DB
- MONGO_COLLECTION
```

#### **`src/mongo_client.py`** - Cliente MongoDB (82% coverage)
```python
class MongoDBClient:
    ✅ connect()              # Conectar a MongoDB
    ✅ insert_message(dict)   # Insertar un mensaje
    ✅ insert_batch(list)     # Insertar lote (bulk insert)
    ✅ get_stats()            # Estadísticas de BD
    ✅ close()                # Cerrar conexión
```

#### **`src/persister.py`** - Servicio principal (59% coverage)
```python
class MongoPersister:
    ✅ setup()                # Configurar Kafka + MongoDB
    ✅ run()                  # Loop principal
    ✅ _process_message()     # Procesar mensaje individual
    ✅ _process_batch()       # Procesar lote
    ✅ close()                # Limpieza y cierre
```

---

## 🐳 Arquitectura de Contenedores

### **Docker Compose Files:**

```
PIX_G5_DataEngineer/
├── docker-compose-kafka.yml        # Kafka + Zookeeper + Kafdrop + Generator
└── docker-compose-services.yml     # MongoDB + Mongo Express + Persister
```

### **Servicios por archivo:**

| docker-compose-kafka.yml | docker-compose-services.yml |
|--------------------------|----------------------------|
| ✅ zookeeper | ✅ hrpro-mongodb |
| ✅ kafka | ✅ hrpro-mongo-viewer |
| ✅ kafdrop | ✅ mongo-persister |
| ✅ random_generator | |

---

## 🚀 Flujo de Arranque Completo

### **Opción 1: Desarrollo Local (Python fuera de Docker)**

```bash
# ============================================================
# PASO 1: Levantar infraestructura (Kafka + MongoDB)
# ============================================================
cd PIX_G5_DataEngineer

# Levantar Kafka + Zookeeper + Kafdrop (SIN generador aún)
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop

# Levantar MongoDB + Mongo Express
docker-compose -f docker-compose-services.yml up -d hrpro-mongodb hrpro-mongo-viewer

# Verificar que están corriendo
docker ps

# ============================================================
# PASO 2: Configurar entorno Python
# ============================================================
cd Estructura/services/mongo-persister

# Crear virtualenv
python -m venv venv
source venv/bin/activate  # Linux/macOS
# o
venv\Scripts\activate  # Windows

# Instalar dependencias
pip install -r requirements.txt

# Configurar .env
cp .env.example .env
nano .env

# ============================================================
# PASO 3: Ejecutar el persister (verás warning de topic no disponible)
# ============================================================
python src/persister.py

# ✅ Output esperado:
# ⏳ Topic 'probando' aún no disponible, reintentando...
# (Esto es normal, el topic se crea cuando el generador envía el primer mensaje)

# ============================================================
# PASO 4: Levantar el generador (en otra terminal)
# ============================================================
cd PIX_G5_DataEngineer
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Ver logs del generador
docker logs -f random_generator

# ✅ Verás mensajes como:
# Enviando mensaje 1/100...
# Enviando mensaje 2/100...
# ...

# ============================================================
# PASO 5: Ver el persister procesando mensajes
# ============================================================
# Volver a la terminal del persister
# ✅ Verás:
# ✅ Conectado a MongoDB: localhost:27017
# ✅ Suscrito a Kafka topic: probando
# 📊 Procesando mensajes...
# ✅ Insertado: TX123456
# ✅ Insertado: TX123457
# ...

# ============================================================
# PASO 6: Verificar datos en MongoDB
# ============================================================
# Opción 1: Mongo Express (UI web)
http://localhost:8081

# Opción 2: Línea de comandos
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.countDocuments()"

# ============================================================
# PASO 7: Detener el generador (después de 2 minutos se detiene solo)
# ============================================================
docker stop random_generator

# ============================================================
# PASO 8: Parar el persister
# ============================================================
# Ctrl+C en la terminal del persister

# ============================================================
# PASO 9: Limpiar (opcional)
# ============================================================
docker-compose -f docker-compose-kafka.yml down
docker-compose -f docker-compose-services.yml down
```

---

### **Opción 2: Producción (Todo en Docker)**

```bash
# ============================================================
# PASO 1: Levantar Kafka
# ============================================================
cd PIX_G5_DataEngineer
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop

# ============================================================
# PASO 2: Levantar MongoDB + Persister
# ============================================================
docker-compose -f docker-compose-services.yml up -d

# Verificar que están corriendo
docker ps

# ============================================================
# PASO 3: Ver logs del persister (verás warning de topic no disponible)
# ============================================================
docker logs -f mongo-persister

# ✅ Output esperado:
# ⏳ Topic 'probando' aún no disponible, reintentando...

# ============================================================
# PASO 4: Levantar el generador
# ============================================================
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Ver logs
docker logs -f random_generator

# ============================================================
# PASO 5: Ver logs del persister procesando
# ============================================================
docker logs -f mongo-persister

# ✅ Verás:
# ✅ Conectado a MongoDB: hrpro_db.transactions
# ✅ Suscrito al topic: probando
# ✅ Insertado: TX123456
# ...

# ============================================================
# PASO 6: Verificar en Kafdrop y Mongo Express
# ============================================================
# Kafka: http://localhost:9000
# MongoDB: http://localhost:8081

# ============================================================
# PASO 7: Parada ordenada
# ============================================================
# Detener generador
docker stop random_generator

# Esperar a que LAG = 0 (verificar en Kafdrop)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# Detener todo
docker-compose -f docker-compose-services.yml down
docker-compose -f docker-compose-kafka.yml down
```

---

## 🔍 Verificación del Sistema

### **1. Verificar que TODO está corriendo:**

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**✅ Output esperado:**
```
NAMES                  STATUS              PORTS
mongo-persister        Up X minutes        
hrpro-mongodb          Up X minutes        0.0.0.0:27017->27017/tcp
hrpro-mongo-viewer     Up X minutes        0.0.0.0:8081->8081/tcp
kafka                  Up X minutes        0.0.0.0:29092->29092/tcp
kafdrop                Up X minutes        0.0.0.0:9000->9000/tcp
zookeeper              Up X minutes        0.0.0.0:22181->22181/tcp
random_generator       Up X minutes        (se detiene después de 2 min)
```

---

### **2. Verificar logs del persister:**

```bash
docker logs mongo-persister | tail -30
```

**✅ Output esperado:**
```
============================================================
CONFIGURACIÓN DEL MONGO PERSISTER
============================================================
Kafka Bootstrap Servers: kafka:9092
Kafka Topic: probando
MongoDB URI: mongodb://mongo:27017/
MongoDB Database: hrpro_db
============================================================
✅ Conectado a MongoDB: hrpro_db.transactions
✅ Suscrito al topic: probando
✅ Mongo Persister listo. Esperando mensajes...
============================================================
📊 ESTADÍSTICAS DEL MONGO PERSISTER
============================================================
Mensajes consumidos: 100
Mensajes insertados: 100
Errores: 0
Rate: 123.45 mensajes/segundo
============================================================
```

---

### **3. Verificar mensajes en Kafka:**

```bash
# Ver mensajes en el topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic probando \
  --from-beginning \
  --max-messages 5

# Ver offset del topic (cuántos mensajes hay)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando
```

---

### **4. Verificar datos en MongoDB:**

```bash
# Contar documentos
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.countDocuments()" \
  --quiet

# Ver un documento de ejemplo
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.findOne()" \
  --quiet
```

---

### **5. Verificar LAG del consumer:**

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe
```

**✅ LAG = 0** → Consumer está al día  
**⚠️ LAG > 0** → Hay mensajes pendientes (normal durante procesamiento)

---

## 🔄 Gestión del Generador de Datos

### **¿Por qué detener el generador?**

1. **Desarrollo:** No necesitamos millones de mensajes de prueba
2. **Disco:** Kafka retiene datos (aunque solo 1 hora con nuestra config)
3. **Recursos:** El generador consume CPU/memoria mientras corre

### **¿Cuándo levantar el generador?**

```bash
# Escenario 1: Primera vez (para crear el topic y tener datos iniciales)
docker-compose -f docker-compose-kafka.yml up -d random_generator
# → Esperar 2 minutos (se detiene automáticamente)
# → Deberías tener ~100 mensajes en Kafka

# Escenario 2: Necesitas más datos para pruebas
docker-compose -f docker-compose-kafka.yml restart random_generator
# → Genera otros ~100 mensajes

# Escenario 3: Tests de carga (muchos mensajes)
for i in {1..10}; do
  docker-compose -f docker-compose-kafka.yml restart random_generator
  sleep 120  # Esperar 2 minutos entre ejecuciones
done
# → Genera ~1000 mensajes en total
```

### **Control del generador:**

```bash
# Ver si está corriendo
docker ps | grep random_generator

# Ver logs en tiempo real
docker logs -f random_generator

# Detener manualmente
docker stop random_generator

# Ver cuántos mensajes generó
docker logs random_generator | grep "Enviando mensaje" | wc -l

# Limpiar logs del generador
docker-compose -f docker-compose-kafka.yml up -d --force-recreate random_generator
```

---

## 📊 Formato de Datos

### **Mensaje de entrada (Kafka - generado por random_generator):**
```json
{
  "transaction_id": "TX123456",
  "amount": 150.75,
  "currency": "USD",
  "timestamp": 1700000000000,
  "sender": "Alice",
  "receiver": "Bob",
  "status": "completed"
}
```

### **Documento guardado (MongoDB - por el persister):**
```json
{
  "_id": ObjectId("..."),
  "transaction_id": "TX123456",
  "amount": 150.75,
  "currency": "USD",
  "timestamp": 1700000000000,
  "sender": "Alice",
  "receiver": "Bob",
  "status": "completed",
  "_kafka_metadata": {
    "topic": "probando",
    "partition": 0,
    "offset": 12345,
    "timestamp": 1700000000000
  },
  "_persisted_at": ISODate("2025-01-15T10:30:00Z")
}
```

**⚠️ IMPORTANTE:** El persister NO modifica los datos originales, solo añade metadata.

---

## 🧪 Testing

### **Tests sin contenedores (más rápidos):**

```bash
cd Estructura/services/mongo-persister

# Tests unitarios (usan mongomock)
python run_tests_persister.py -m unit -c
# ✅ 33 tests en ~0.57s

# Tests de performance (usan mongomock)
python run_tests_persister.py -m performance -v
# ✅ 5 tests en ~0.50s
```

### **Tests con contenedores (requieren infraestructura):**

```bash
# 1. Levantar infraestructura
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka
docker-compose -f docker-compose-services.yml up -d hrpro-mongodb

# 2. Ejecutar tests de integración
python run_tests_persister.py -m integration -v
# ✅ 10 tests en ~18s

# 3. Todos los tests con coverage
python run_tests_persister.py -c -o
# ✅ 56 tests, 71% coverage
```

### **Coverage HTML:**

```bash
# Generar y abrir reporte
python run_tests_persister.py -c -o

# O abrir manualmente
start htmlcov/index.html  # Windows
open htmlcov/index.html   # macOS
xdg-open htmlcov/index.html  # Linux
```

---

## ⚠️ Troubleshooting

### **Problema 1: "Topic 'probando' aún no disponible"**

**Mensaje:**
```
⏳ Topic 'probando' aún no disponible, reintentando en 5 segundos...
```

**✅ Solución:**  
Esto es **normal** cuando el generador aún no ha enviado datos. El topic se crea automáticamente cuando el generador envía el primer mensaje.

```bash
# Levantar el generador
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Esperar ~10 segundos
# El persister detectará el topic automáticamente
```

---

### **Problema 2: "Error conectando a MongoDB"**

```bash
# Verificar que MongoDB está corriendo
docker ps | grep mongodb

# Ver logs de MongoDB
docker logs hrpro-mongodb

# Verificar conectividad
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.runCommand({ping: 1})"

# Reiniciar MongoDB
docker-compose -f docker-compose-services.yml restart hrpro-mongodb
```

---

### **Problema 3: "Error conectando a Kafka"**

```bash
# Verificar que Kafka está corriendo
docker ps | grep kafka

# Ver logs de Kafka
docker logs kafka | tail -50

# Verificar topics disponibles
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Reiniciar Kafka
docker-compose -f docker-compose-kafka.yml restart kafka zookeeper
```

---

### **Problema 4: LAG Alto (Mensajes sin procesar)**

```bash
# Ver LAG actual
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# Si LAG > 1000:
# 1. Verificar logs del persister
docker logs mongo-persister | tail -50

# 2. Verificar errores
docker logs mongo-persister | grep ERROR

# 3. Reiniciar persister
docker restart mongo-persister
```

---

### **Problema 5: Generador no envía datos**

```bash
# Ver logs del generador
docker logs random_generator

# Si no hay logs o hay errores:
# 1. Verificar que Kafka está corriendo
docker ps | grep kafka

# 2. Reiniciar el generador
docker-compose -f docker-compose-kafka.yml restart random_generator

# 3. Ver logs en tiempo real
docker logs -f random_generator
```

---

### **Problema 6: Disco lleno (Kafka retiene muchos datos)**

```bash
# Ver tamaño de datos de Kafka
docker exec kafka du -sh /var/lib/kafka/data

# Solución 1: Limpiar datos de Kafka (borra TODO)
docker-compose -f docker-compose-kafka.yml down -v
docker-compose -f docker-compose-kafka.yml up -d

# Solución 2: Cambiar retención en docker-compose-kafka.yml
# KAFKA_LOG_RETENTION_HOURS: 1  # De 1 hora a menos
# KAFKA_LOG_RETENTION_BYTES: 104857600  # De 500MB a 100MB
```

---

## 📌 Variables de Entorno Clave

### **Para producción (Docker):**

| Variable | Ejemplo Docker | Descripción |
|----------|----------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Servidor Kafka (nombre del contenedor) |
| `KAFKA_TOPIC` | `probando` | Topic a consumir |
| `KAFKA_GROUP_ID` | `mongo-persister-group` | ID único del consumer group |
| `MONGO_URI` | `mongodb://mongo:27017/` | URI de MongoDB (nombre del contenedor) |
| `MONGO_DB` | `hrpro_db` | Base de datos |
| `MONGO_COLLECTION` | `transactions` | Colección donde guardar |

### **Para desarrollo local (Python fuera de Docker):**

| Variable | Ejemplo Local | Descripción |
|----------|---------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Puerto externo de Kafka |
| `KAFKA_TOPIC` | `probando` | Topic a consumir |
| `KAFKA_GROUP_ID` | `mongo-persister-local` | ID único (diferente a Docker) |
| `MONGO_URI` | `mongodb://localhost:27017/` | MongoDB en localhost |
| `MONGO_DB` | `hrpro_db` | Base de datos |
| `MONGO_COLLECTION` | `transactions` | Colección donde guardar |

---

## 🔗 Integración con Otros Servicios

### **Flujo completo de datos:**

```
random_generator → Kafka → mongo-persister → MongoDB → data-processor
   (Simulador)   (Queue)   (Este servicio)  (Storage)   (Procesamiento)
```

### **Upstream (consume de):**
- 📨 **Kafka** (topic: `probando`)
  - Mensajes crudos generados por `random_generator`
  - Sin procesar, sin filtrar
  - ~100 mensajes por ejecución del generador

### **Downstream (provee a):**
- 📦 **MongoDB** (collection: `transactions`)
  - Datos listos para ser procesados por `data-processor`
  - Incluye metadata de Kafka y timestamp de persistencia
  - Búsqueda por `_kafka_metadata.offset` o `_persisted_at`

---

## 📊 Monitoreo y Métricas

### **Estadísticas en tiempo real (del persister):**

```
============================================================
📊 ESTADÍSTICAS DEL MONGO PERSISTER
============================================================
Total consumidos: 12,345
Total insertados: 12,345
Errores: 0
Rate: 123.45 mensajes/segundo
Tiempo: 100.00 segundos
Documentos en MongoDB: 12,345
============================================================
```

### **Métricas clave a monitorear:**

| Métrica | Valor Ideal | Cómo Verificar |
|---------|-------------|----------------|
| **LAG** | 0 | Kafdrop o `kafka-consumer-groups` |
| **Errores** | 0 | Logs del persister |
| **Rate** | > 100 msg/s | Logs del persister |
| **Consumidos = Insertados** | ✅ | Logs del persister |
| **MongoDB docs** | ≈ Kafka offset | Mongo Express o mongosh |

### **Herramientas de monitoreo:**

1. **Kafdrop** (http://localhost:9000)
   - Ver topics y mensajes
   - Monitorear consumer groups y LAG
   - Ver configuración de brokers

2. **Mongo Express** (http://localhost:8081)
   - Ver documentos guardados
   - Buscar por campos específicos
   - Estadísticas de colecciones

3. **Docker logs**
   ```bash
   # Ver logs en tiempo real
   docker logs -f mongo-persister
   docker logs -f random_generator
   docker logs -f kafka
   ```

---

## 🎯 Checklist de Verificación

### **Antes de desarrollar:**
- [ ] Kafka está corriendo
- [ ] MongoDB está corriendo
- [ ] Dependencias instaladas (`pip install -r requirements.txt`)
- [ ] `.env` configurado (si desarrollo local)

### **Durante desarrollo:**
- [ ] Tests unitarios pasan (`python run_tests_persister.py -m unit -c`)
- [ ] Coverage > 50%
- [ ] Código formateado y limpio
- [ ] Sin errores en logs

### **Antes de producción:**
- [ ] Tests de integración pasan (`python run_tests_persister.py -m integration -v`)
- [ ] Dockerfile funciona (`docker build -t mongo-persister:latest .`)
- [ ] Variables de entorno configuradas en `docker-compose-services.yml`
- [ ] Documentación actualizada

### **Checklist del generador:**
- [ ] Generador envía ~100 mensajes
- [ ] Se detiene automáticamente después de 2 minutos
- [ ] Topic `probando` se crea en Kafka
- [ ] Mensajes visibles en Kafdrop

---

## 🚀 Quick Commands Cheatsheet

```bash
# ============================================================
# LEVANTAR INFRAESTRUCTURA
# ============================================================
# Kafka + Kafdrop (sin generador)
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop

# MongoDB + Mongo Express
docker-compose -f docker-compose-services.yml up -d hrpro-mongodb hrpro-mongo-viewer

# Generador (opcional, se detiene después de 2 min)
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Persister (en Docker)
docker-compose -f docker-compose-services.yml up -d mongo-persister

# ============================================================
# DESARROLLO LOCAL (Python fuera de Docker)
# ============================================================
cd Estructura/services/mongo-persister
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python src/persister.py

# ============================================================
# TESTING
# ============================================================
# Tests unitarios (rápidos, sin contenedores)
python run_tests_persister.py -m unit -c

# Tests de integración (requieren contenedores)
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka
docker-compose -f docker-compose-services.yml up -d hrpro-mongodb
python run_tests_persister.py -m integration -v

# Todos los tests con coverage
python run_tests_persister.py -c -o

# ============================================================
# MONITOREO
# ============================================================
# Ver logs del persister
docker logs -f mongo-persister

# Ver logs del generador
docker logs -f random_generator

# Ver LAG del consumer
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# Ver mensajes en Kafka
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic probando \
  --from-beginning \
  --max-messages 10

# Ver datos en MongoDB
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.find().limit(5).pretty()"

# Contar documentos en MongoDB
docker exec hrpro-mongodb mongosh \
  --username <user> \
  --password <passwd> \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').transactions.countDocuments()"

# ============================================================
# REINICIAR SERVICIOS
# ============================================================
# Reiniciar persister
docker restart mongo-persister

# Reiniciar generador (para generar más mensajes)
docker-compose -f docker-compose-kafka.yml restart random_generator

# Reiniciar Kafka
docker-compose -f docker-compose-kafka.yml restart kafka zookeeper

# Reiniciar MongoDB
docker-compose -f docker-compose-services.yml restart hrpro-mongodb

# ============================================================
# LIMPIAR Y DETENER
# ============================================================
# Detener persister
docker stop mongo-persister

# Detener generador
docker stop random_generator

# Detener todo
docker-compose -f docker-compose-kafka.yml down
docker-compose -f docker-compose-services.yml down

# Limpiar volúmenes (BORRA TODOS LOS DATOS)
docker-compose -f docker-compose-kafka.yml down -v
docker-compose -f docker-compose-services.yml down -v

# ============================================================
# UIs WEB
# ============================================================
# Kafdrop (Kafka UI)
http://localhost:9000

# Mongo Express (MongoDB UI)
http://localhost:8081
```

---

## 📚 Documentación Relacionada

| Archivo | Descripción |
|---------|-------------|
| [`README.md`](README.md) | Documentación técnica detallada del servicio |
| [`OPERATIONS_GUIDE.md`](../../../OPERATIONS_GUIDE.md) | Guía de operación del proyecto completo |
| [`docker-compose-kafka.yml`](../../../docker-compose-kafka.yml) | Configuración de Kafka + Generador |
| [`docker-compose-services.yml`](../../../docker-compose-services.yml) | Configuración de MongoDB + Persister |
| [`tests/test_*.py`](tests/) | Tests con documentación inline |
| `htmlcov/index.html` | Reporte de coverage (generado con `-c`) |

---

## 🔗 Recursos Externos

### **Repositorio del generador:**
- https://github.com/Factoria-F5-madrid/data-engineering-educational-project
- Carpeta: `datagen/`
- **Nota:** NO lo subimos a nuestro repo, solo lo implementamos desde ese repo educativo

### **Documentación oficial:**
- [PyMongo Docs](https://pymongo.readthedocs.io/)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [MongoDB Docker](https://hub.docker.com/_/mongo)
- [Kafdrop](https://github.com/obsidiandynamics/kafdrop)
- [Pytest Docs](https://docs.pytest.org/)

---

**Última actualización:** Noviembre 2024  
**Versión:** 2.0  
**Autores:** Equipo G5 DataEngineer