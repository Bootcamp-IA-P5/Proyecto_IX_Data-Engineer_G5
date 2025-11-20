# 🚀 Guía de Operación - Proyecto G5 DataEngineer

**Versión:** 1.0  
**Fecha:** 15 de Noviembre de 2025  
**Autor:** Equipo PIX G5

---

## 📋 Índice

1. Arquitectura del Sistema
2. Prerrequisitos
3. Inicio del Sistema
4. Monitoreo y Verificación
5. Gestión del Generador de Datos
6. Acceso a las Interfaces Web
7. Consultas y Análisis de Datos
8. Detención del Sistema
9. Troubleshooting
10. Comandos de Referencia Rápida

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                     FLUJO DE DATOS                              │
└─────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐
  │ Data Generator  │  ⚠️ IMPORTANTE: Detener manualmente
  │ (Fuente Externa)│     Genera ~1M msg/min
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  Kafka Topic    │  Topic: "probando"
  │  (3 particiones)│  Retención: 7 días
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ Mongo Persister │  DB: hrpro_db
  │ (raw_messages)  │  Collection: raw_messages
  └────────┬────────┘  ontainer Consumer: "mongo-persister"
           │
           ▼
  ┌─────────────────┐
  │    MongoDB      │  DB: hrpro_db
  │(aggregated_data)│  Collection: aggregated_data
  └─────────────────┘  Container: "data-processor"
           │
           ▼
  ┌─────────────────┐
  │    PostgresSQL  │  DB: hrpto_db  
  │    (Supabase)   │  
  └─────────────────┘   
```

---

## ✅ Prerrequisitos

Antes de comenzar, verifica que tienes instalado:

```bash
# Verificar Docker
docker --version
# Esperado: Docker version 20.10.x o superior

# Verificar Docker Compose
docker-compose --version
# Esperado: Docker Compose version 2.x.x o superior

# Verificar que Docker está corriendo
docker ps
# Esperado: Lista de contenedores (puede estar vacía)

# Verificar espacio en disco (necesitas al menos 10GB libres)
df -h
```

---

## 🚀 Inicio del Sistema

### **Paso 1: Levantar la Infraestructura de Kafka**

⏱️ **Tiempo estimado:** 30-60 segundos

```bash
# Navegar al directorio del proyecto
cd /ruta/al/proyecto/PIX_G5_DataEngineer

# Levantar Zookeeper, Kafka y Kafdrop
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop

# ✅ Deberías ver:
# [+] Running 3/3
#  ✔ Container zookeeper  Started
#  ✔ Container kafka      Started
#  ✔ Container kafdrop    Started
```

**⏳ Esperar a que Kafka esté listo:**

```bash
# Esperar 30 segundos para que Kafka inicie completamente
echo "⏳ Esperando 30 segundos a que Kafka inicie..."
sleep 30

# Verificar que Kafka está listo
docker logs kafka | tail -20

# ✅ Buscar en los logs: "Kafka Server started"
# ✅ Si no lo ves, espera otros 10 segundos y vuelve a revisar
```

**📊 Verificación:**

```bash
# Ver contenedores activos
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ✅ Deberías ver:
# NAME        STATUS              PORTS
# kafdrop     Up (healthy)        0.0.0.0:9000->9000/tcp
# kafka       Up (healthy)        0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
# zookeeper   Up                  0.0.0.0:22181->2181/tcp
```

---

### **Paso 2: Levantar MongoDB y Servicios**

⏱️ **Tiempo estimado:** 10-15 segundos

```bash
# Levantar MongoDB y mongo-persister
docker-compose -f docker-compose-services.yml up -d

# ✅ Deberías ver:
# [+] Running 4/4
#  ✔ Container hrpro-mongodb       Started
#  ✔ Container hrpro-mongo-viewer  Started
#  ✔ Container mongo-persister     Started
#  ✔ Container kafka-consumer      Started
```

**📊 Verificación del mongo-persister:**

```bash
# Ver logs del mongo-persister
docker logs -f mongo-persister

# ✅ Deberías ver:
# ============================================================
# CONFIGURACIÓN DEL MONGO PERSISTER
# ============================================================
# Kafka Bootstrap Servers: kafka:9092
# Kafka Topic: probando
# ...
# 2025-11-15 XX:XX:XX - INFO - ✅ Mongo Persister listo. Esperando mensajes...
# 2025-11-15 XX:XX:XX - WARNING - ⏳ Topic 'probando' aún no disponible. 
#                                     Esperando a que el generador de datos lo cree...

# ⚠️ Es NORMAL ver este warning. El topic se creará cuando el generador envíe datos.
# ✅ IMPORTANTE: Esto NO es un error, es el comportamiento esperado.

# Presiona Ctrl+C para salir de los logs (el servicio sigue corriendo)
```

---

### **Paso 3: Levantar el Generador de Datos**

⚠️ **MUY IMPORTANTE:** El generador NO se detiene solo. **Debes detenerlo manualmente**.

⏱️ **Tiempo recomendado de ejecución:** 30-60 segundos (genera ~500K-1M mensajes)

```bash
# En una NUEVA terminal (deja los logs de mongo-persister en la otra)
cd /ruta/al/proyecto/PIX_G5_DataEngineer

# Levantar el generador
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Ver logs del generador
docker logs -f random_generator

# ✅ Deberías ver:
# Sent
# Sent
# Sent
# ... (muchos "Sent" continuos)

# ⚠️ IMPORTANTE: 
# El generador genera ~1,000,000 mensajes por minuto
# Déjalo correr 30-60 segundos y luego DETENLO (ver siguiente sección)
```

**📊 En la terminal de mongo-persister:**

Vuelve a la terminal donde tenías los logs de `mongo-persister`. Deberías ver:

```
✅ Topic 'probando' ahora disponible. Procesando mensajes...
============================================================
📊 ESTADÍSTICAS DEL MONGO PERSISTER
============================================================
Total consumidos: 1,234
Total insertados: 1,234
Errores: 0
Rate: 123.45 mensajes/segundo
Tiempo: 10.00 segundos
Documentos en MongoDB: 1,234
============================================================
```

---

## ⚠️ Gestión del Generador de Datos

### **¿Cuándo detener el generador?**

El generador genera aproximadamente **1,000,000 mensajes por minuto**. Para evitar saturar el sistema:

| Tiempo de ejecución | Mensajes aprox. | Uso recomendado |
|---------------------|----------------|-----------------|
| 30 segundos | ~500,000 | ✅ Testing rápido |
| 60 segundos | ~1,000,000 | ✅ Demo/desarrollo |
| 2 minutos | ~2,000,000 | ⚠️ Solo si necesitas muchos datos |
| 5+ minutos | ~5,000,000+ | ❌ No recomendado (saturación) |

### **Detener el generador:**

```bash
# ⛔ DETENER el generador (IMPORTANTE hacerlo manualmente)
docker stop random_generator

# Verificar que se detuvo
docker ps -a | grep random_generator

# ✅ Deberías ver:
# random_generator   Exited (137) X seconds ago

# 📊 Ver cuántos mensajes generó
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando 2>/dev/null | \
  awk -F ":" '{sum += $3} END {print "Total generado: " sum " mensajes"}'
```

### **Reiniciar el generador (generar más datos):**

```bash
# Si necesitas más datos, puedes reiniciar el generador
docker-compose -f docker-compose-kafka.yml restart random_generator

# Ver logs
docker logs -f random_generator

# ⚠️ RECUERDA detenerlo después de 30-60 segundos:
docker stop random_generator
```

---

## 📊 Monitoreo y Verificación

### **1. Verificar todos los contenedores:**

```bash
# Ver estado de todos los contenedores
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ✅ Deberías ver 7 contenedores activos:
# NAME                  STATUS              PORTS
# random_generator      Up / Exited         (depende si lo detuviste)
# kafdrop               Up (healthy)        9000/tcp
# kafka-consumer        Up                  
# mongo-persister       Up                  
# hrpro-mongo-viewer    Up                  8081/tcp
# kafka                 Up (healthy)        9092/tcp, 29092/tcp
# hrpro-mongodb         Up                  27017/tcp
# zookeeper             Up                  2181/tcp
```

### **2. Verificar mensajes en Kafka:**

```bash
# Ver cuántos mensajes hay en el topic "probando"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando 2>/dev/null | \
  awk -F ":" '{sum += $3} END {print "Total en Kafka: " sum " mensajes"}'

# Ejemplo de salida:
# Total en Kafka: 1,234,567 mensajes
```

### **3. Verificar LAG del consumer (mensajes pendientes):**

```bash
# Ver si el consumer está al día
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# ✅ Interpretación:
# LAG = 0     → El consumer está al día ✅
# LAG > 0     → El consumer está procesando, espera unos segundos ⏳
# LAG grande  → Posible problema, revisar logs ⚠️
```

**Ejemplo de salida esperada:**

```
GROUP                 TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
mongo-persister-group probando 0          411,789         411,789         0
mongo-persister-group probando 1          412,345         412,345         0
mongo-persister-group probando 2          410,433         410,433         0
```

✅ **LAG = 0 en todas las particiones significa que todo está procesado.**

### **4. Verificar datos en MongoDB:**

```bash
# Contar documentos en MongoDB
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.countDocuments()" \
  --quiet

# Ejemplo de salida:
# 1234567

# ✅ Este número debería coincidir con el total en Kafka
```

### **5. Ver logs de los servicios:**

```bash
# Logs de mongo-persister (consumer)
docker logs -f mongo-persister

# Logs del generador
docker logs -f random_generator

# Logs de Kafka
docker logs kafka | tail -50

# Logs de MongoDB
docker logs hrpro-mongodb | tail -50
```

---

## 🌐 Acceso a las Interfaces Web

### **1. Kafdrop (Interfaz web de Kafka)**

**URL:** http://localhost:9000

**Funcionalidades:**
- 📁 **Topics:** Ver el topic "probando" y sus mensajes
- 👥 **Consumers:** Ver el consumer group "mongo-persister-group" y su LAG
- 🖥️ **Brokers:** Ver estado del broker de Kafka
- 🔍 **Messages:** Buscar y ver mensajes específicos

**Navegación recomendada:**

```
1. Abrir http://localhost:9000

2. Click en "Topics" (menú superior)
   → Verás el topic "probando"

3. Click en "probando"
   → Ver detalles:
     - Partitions: 3
     - Messages: X,XXX,XXX
     - Size: XXX MB

4. Click en "View Messages"
   → Ver los últimos mensajes en formato JSON

5. Click en "Consumers" (menú superior)
   → Ver "mongo-persister-group"
   → Ver LAG por partición
```

---

### **2. Mongo Express (Interfaz web de MongoDB)**

**URL:** http://localhost:8081

**Credenciales:**
- **Usuario:** `admin`
- **Contraseña:** `admin123`

**Navegación recomendada:**

```
1. Abrir http://localhost:8081

2. Login con admin/admin123

3. Click en "hrpro_db" (base de datos)

4. Click en "raw_messages" (colección)

5. Ver los documentos almacenados:
   - Datos originales de Kafka
   - Metadata de Kafka (_kafka_metadata)
   - Timestamp de inserción
```

---

## 🔍 Consultas y Análisis de Datos

### **1. Ver ejemplos de datos en MongoDB:**

```bash
# Ver un documento de ejemplo (formato bonito)
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.findOne()" \
  --quiet

# Ver 5 documentos
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.find().limit(5).pretty()" \
  --quiet
```

**Ejemplo de documento:**

```json
{
  "_id": ObjectId("..."),
  "name": "Juan",
  "last_name": "García",
  "sex": ["M"],
  "telfnumber": "+34 123 456 789",
  "passport": "ABC123456",
  "email": "juan@example.com",
  "_kafka_metadata": {
    "topic": "probando",
    "partition": 0,
    "offset": 12345,
    "timestamp": 1700000000000
  }
}
```

### **2. Consultas útiles en MongoDB:**

```bash
# Contar documentos por tipo (si aplica)
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.aggregate([
    { \$group: { _id: '\$type', count: { \$sum: 1 } } }
  ])" \
  --quiet

# Ver documentos recientes (últimos 10)
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.find().sort({_id:-1}).limit(10).pretty()" \
  --quiet

# Buscar por campo específico
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.find({passport: 'ABC123456'}).pretty()" \
  --quiet
```

### **3. Verificar índices en MongoDB:**

```bash
# Ver índices creados
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.getIndexes()" \
  --quiet

# ✅ Deberías ver índices en:
# - _kafka_metadata.topic
# - _kafka_metadata.partition
# - _kafka_metadata.offset
```

---

## 🛑 Detención del Sistema

### **Orden recomendado de detención:**

⚠️ **IMPORTANTE:** Siempre detén en este orden para evitar pérdida de datos.



GHalfbbt: El kafka se podria dejar en una retención menor.
Déjamelo en un fichero con formato md, para que pueda descargármelo y añadirlo al proyecto. Gracias. y en qué lugar debería ubicarlo.


GitHub Copilot: # 📚 Guía de Operación - Pipeline de Datos PIX G5

> **Versión:** 1.0  
> **Fecha:** 2025-11-15  
> **Autor:** Equipo PIX G5 DataEngineer

---

## 📋 Índice

1. Descripción General
2. Arquitectura del Sistema
3. Pre-requisitos
4. Orden de Arranque
5. Verificación del Sistema
6. Monitoreo y Análisis
7. Orden de Parada
8. Limpieza y Mantenimiento
9. Troubleshooting
10. Configuración Avanzada

---

## 🎯 Descripción General

Este proyecto implementa un pipeline de datos en tiempo real que:

1. **Genera** datos simulados de RRHH (Generador de Datos)
2. **Transmite** los datos a través de Apache Kafka
3. **Persiste** los datos en MongoDB sin procesarlos (Mongo Persister)
4. **Visualiza** los datos mediante interfaces web

### Componentes Principales

| Componente | Puerto | Descripción |
|------------|--------|-------------|
| **Zookeeper** | 22181 | Coordinación de Kafka |
| **Kafka** | 9092, 29092 | Message broker |
| **Kafdrop** | 9000 | UI web para Kafka |
| **MongoDB** | 27017 | Base de datos NoSQL |
| **Mongo Express** | 8081 | UI web para MongoDB |
| **Mongo Persister** | - | Servicio consumer de Kafka |
| **Data Generator** | - | Generador de datos sintéticos |

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                    CAPA DE GENERACIÓN                       │
├─────────────────────────────────────────────────────────────┤
│  Data Generator (Loop Infinito)                            │
│  ├─ Genera ~100 mensajes/segundo                           │
│  ├─ 5 tipos de documentos por iteración                    │
│  └─ ⚠️  IMPORTANTE: Parar manualmente                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  CAPA DE STREAMING (KAFKA)                  │
├─────────────────────────────────────────────────────────────┤
│  Topic: probando                                            │
│  ├─ 3 Particiones                                           │
│  ├─ Retención: 1 hora (configurable)                       │
│  └─ Auto-create: enabled                                    │
│                                                             │
│  Monitoreo: Kafdrop (http://localhost:9000)                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  CAPA DE PERSISTENCIA                       │
├─────────────────────────────────────────────────────────────┤
│  Mongo Persister                                            │
│  ├─ Consumer Group: mongo-persister-group                  │
│  ├─ Batch Size: 100 mensajes                               │
│  └─ Inserta en MongoDB sin procesar                        │
│                                                             │
│  MongoDB (hrpro_db.raw_messages)                            │
│  └─ Monitoreo: Mongo Express (http://localhost:8081)       │
└─────────────────────────────────────────────────────────────┘
```

---

## ✅ Pre-requisitos

### Software Necesario

- **Docker:** >= 20.10
- **Docker Compose:** >= 2.0
- **Git:** >= 2.30
- **RAM disponible:** >= 4GB
- **Disco disponible:** >= 10GB

### Verificar Instalación

```bash
# Verificar Docker
docker --version
docker compose version

# Verificar recursos disponibles
docker system df

# Verificar que no hay contenedores huérfanos
docker ps -a
```

---

## 🚀 Orden de Arranque

### ⚠️ IMPORTANTE: Secuencia Correcta

El orden de arranque **ES CRÍTICO** para evitar errores. Sigue estos pasos en orden:

---

### **PASO 1: Levantar Infraestructura Base (Kafka + Zookeeper + Kafdrop)**

```bash
# Ir al directorio del proyecto
cd /ruta/a/PIX_G5_DataEngineer

# Levantar infraestructura Kafka
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop

# ⏳ Esperar 30 segundos a que Kafka esté completamente operativo
echo "⏳ Esperando a que Kafka se inicialice..."
sleep 30
```

**¿Por qué esperar?** Kafka necesita tiempo para:
- Conectarse a Zookeeper
- Inicializar brokers
- Crear listeners
- Estar listo para recibir conexiones

---

### **PASO 2: Verificar que Kafka está Listo**

```bash
# Ver logs de Kafka (últimas 20 líneas)
docker logs kafka | tail -20

# ✅ Deberías ver algo como:
# [KafkaServer id=1] started (kafka.server.KafkaServer)
```

Si no ves el mensaje de inicio, espera 10 segundos más y vuelve a verificar.

---

### **PASO 3: Levantar Base de Datos (MongoDB)**

```bash
# Levantar MongoDB y su interfaz web
docker-compose -f docker-compose-services.yml up -d hrpro-mongodb hrpro-mongo-viewer

# Esperar 10 segundos
sleep 10

# Verificar que MongoDB está corriendo
docker logs hrpro-mongodb | tail -10

# ✅ Deberías ver:
# Waiting for connections on port 27017
```

---

### **PASO 4: Levantar Mongo Persister (Consumer)**

```bash
# Levantar el servicio consumer
docker-compose -f docker-compose-services.yml up -d mongo-persister

# Ver logs en tiempo real
docker logs -f mongo-persister

# ✅ Deberías ver:
# ⏳ Topic 'probando' aún no disponible. Esperando a que el generador de datos lo cree...
# (Esto es NORMAL - el topic se creará cuando arranque el generador)
```

**Presiona `Ctrl+C`** para salir de los logs (el contenedor sigue corriendo).

---

### **PASO 5: Levantar Data Generator (⚠️ IMPORTANTE)**

> **⚠️ ADVERTENCIA CRÍTICA:**  
> El generador corre en **LOOP INFINITO**.  
> **DEBES** pararlo manualmente cuando tengas suficientes datos.  
> Si lo dejas corriendo, llenará Kafka y MongoDB indefinidamente.

```bash
# Levantar el generador de datos
docker-compose -f docker-compose-kafka.yml up -d random_generator

# Ver logs en tiempo real
docker logs -f random_generator

# Deberías ver:
# Sent
# Sent
# Sent
# ... (uno por cada mensaje enviado)
```

**🎯 Recomendación:**
- **Para pruebas:** Déjalo correr **30-60 segundos** (generará ~3,000-6,000 mensajes)
- **Para desarrollo:** Déjalo correr **2-3 minutos** (generará ~12,000-18,000 mensajes)
- **Para testing:** Déjalo correr **5 minutos máximo** (generará ~30,000 mensajes)

---

### **PASO 6: Parar el Generador (⚠️ MUY IMPORTANTE)**

```bash
# ⚠️ DESPUÉS DEL TIEMPO DESEADO, PARAR EL GENERADOR:
docker stop random_generator

# Verificar que se detuvo
docker ps -a | grep random_generator
# Debería decir: Exited (137) o Exited (143)
```

**⏰ Configura una alarma** en tu teléfono para recordar parar el generador.

---

### **PASO 7: Verificar Logs de Mongo Persister**

```bash
# Volver a ver los logs del persister
docker logs mongo-persister | tail -50

# ✅ Ahora deberías ver:
# ✅ Topic 'probando' ahora disponible. Procesando mensajes...
# ============================================================
# 📊 ESTADÍSTICAS DEL MONGO PERSISTER
# ============================================================
# Total consumidos: 12,345
# Total insertados: 12,345
# Errores: 0
# Rate: 100.00 mensajes/segundo
# Documentos en MongoDB: 12,345
# ============================================================
```

**✅ Si ves `Errores: 0` → Todo está funcionando correctamente**

---

## 🔍 Verificación del Sistema

### Comando Rápido de Verificación

```bash
# Ver estado de todos los contenedores
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ✅ Deberías ver algo como:
# NAME                 STATUS              PORTS
# random_generator     Exited (137)        
# mongo-persister      Up 5 minutes        
# hrpro-mongodb        Up 5 minutes        0.0.0.0:27017->27017/tcp
# hrpro-mongo-viewer   Up 5 minutes        0.0.0.0:8081->8081/tcp
# kafdrop              Up 6 minutes        0.0.0.0:9000->9000/tcp
# kafka                Up 6 minutes        0.0.0.0:9092->9092/tcp
# zookeeper            Up 6 minutes        0.0.0.0:22181->2181/tcp
```

---

### Verificación Detallada por Componente

#### 1️⃣ **Verificar Kafka**

```bash
# Ver topics disponibles
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# ✅ Deberías ver:
# probando

# Ver detalles del topic
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic probando

# ✅ Deberías ver:
# Topic: probando
#   PartitionCount: 3
#   ReplicationFactor: 1
```

---

#### 2️⃣ **Verificar Mensajes en Kafka**

```bash
# Contar mensajes totales en el topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando 2>/dev/null | \
  awk -F ":" '{sum += $3} END {print "Total: " sum " mensajes"}'

# Ejemplo de salida:
# Total: 12345 mensajes
```

---

#### 3️⃣ **Verificar Consumer Group (LAG)**

```bash
# Ver estado del consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# ✅ Salida esperada:
# GROUP                 TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# mongo-persister-group probando 0          4115            4115            0
# mongo-persister-group probando 1          4115            4115            0
# mongo-persister-group probando 2          4115            4115            0

# ✅ LAG = 0 significa que el consumer está al día
# ⚠️ LAG > 0 significa que hay mensajes pendientes de procesar
```

---

#### 4️⃣ **Verificar MongoDB**

```bash
# Contar documentos en MongoDB
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.countDocuments()" \
  --quiet

# Ejemplo de salida:
# 12345

# Ver un documento de ejemplo
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.findOne()" \
  --quiet
```

---

#### 5️⃣ **Verificar Mongo Persister**

```bash
# Ver estadísticas del persister
docker logs mongo-persister | grep "📊 ESTADÍSTICAS"

# Ver errores (debería ser 0)
docker logs mongo-persister | grep "Errores:"

# Ver últimas 50 líneas de logs
docker logs mongo-persister | tail -50
```

---

## 📊 Monitoreo y Análisis

### Interfaces Web Disponibles

#### 🌐 **Kafdrop (Monitoreo de Kafka)**

- **URL:** http://localhost:9000
- **Función:** Visualizar y gestionar Kafka

**Navegación:**

1. **Dashboard Principal:**
   - Número de brokers
   - Lista de topics
   - Consumer groups activos

2. **Ver Topic "probando":**
   - Click en "Topics" (menú superior)
   - Click en "probando"
   - Verás: Particiones, Total de mensajes, Tamaño

3. **Ver Mensajes:**
   - Dentro del topic "probando"
   - Click en "View Messages"
   - Selecciona "Partition: All"
   - Click en "View Messages"
   - Verás los últimos mensajes en formato JSON

4. **Monitorear Consumer Groups:**
   - Click en "Consumers" (menú superior)
   - Click en "mongo-persister-group"
   - Verás el LAG por partición en tiempo real

---

#### 🌐 **Mongo Express (Explorador de MongoDB)**

- **URL:** http://localhost:8081
- **Usuario:** `admin`
- **Password:** `admin123`

**Navegación:**

1. **Ver Base de Datos:**
   - Click en "hrpro_db"

2. **Ver Colección:**
   - Click en "raw_messages"

3. **Explorar Datos:**
   - Verás todos los documentos insertados
   - Puedes buscar, filtrar, editar y eliminar

4. **Estadísticas:**
   - Click en "Stats" para ver tamaño, índices, etc.

---

### Comandos de Monitoreo en Tiempo Real

#### Monitoreo Continuo del Sistema

```bash
# Ver logs en tiempo real de todos los servicios
docker-compose -f docker-compose-kafka.yml logs -f
docker-compose -f docker-compose-services.yml logs -f

# Ver logs de un servicio específico
docker logs -f mongo-persister
docker logs -f random_generator
docker logs -f kafka
```

---

#### Dashboard en Terminal (Actualización Automática)

```bash
# Monitoreo en tiempo real (actualiza cada 5 segundos)
watch -n 5 '
echo "=========================================="
echo "📊 DASHBOARD DEL SISTEMA"
echo "=========================================="
echo ""
echo "📦 KAFKA - Topic \"probando\":"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando 2>/dev/null | \
  awk -F ":" "{sum += \$3} END {printf \"   Total: %'"'"'d mensajes\n\", sum}"
echo ""
echo "👥 CONSUMER GROUP - mongo-persister-group:"
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe 2>/dev/null | \
  tail -n +3 | \
  awk "{lag += \$6} END {printf \"   LAG: %d mensajes pendientes\n\", lag}"
echo ""
echo "💾 MONGODB - hrpro_db.raw_messages:"
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB(\"hrpro_db\").raw_messages.countDocuments()" \
  --quiet 2>/dev/null | \
  awk "{printf \"   Documentos: %'"'"'d\n\", \$1}"
echo ""
echo "=========================================="
echo "Presiona Ctrl+C para salir"
'
```

**Presiona `Ctrl+C`** para salir del monitoreo.

---

#### Script de Verificación Completa

Crea un archivo `check_system.sh`:

```bash
#!/bin/bash

echo "=========================================="
echo "🔍 VERIFICACIÓN COMPLETA DEL SISTEMA"
echo "=========================================="
echo ""

# Verificar contenedores
echo "1️⃣  Estado de Contenedores:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "NAME|kafka|zookeeper|mongo|persister|generator|kafdrop"
echo ""

# Verificar Kafka
echo "2️⃣  Kafka - Mensajes en topic 'probando':"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic probando 2>/dev/null | \
  awk -F ":" '{sum += $3} END {printf "   Total: %'"'"'d mensajes\n", sum}'
echo ""

# Verificar Consumer Group
echo "3️⃣  Consumer Group - LAG:"
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe 2>/dev/null | \
  tail -n +3 | \
  awk '{lag += $6} END {printf "   LAG: %d mensajes\n", lag}'
echo ""

# Verificar MongoDB
echo "4️⃣  MongoDB - Documentos guardados:"
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.countDocuments()" \
  --quiet 2>/dev/null | \
  awk '{printf "   Documentos: %'"'"'d\n", $1}'
echo ""

# Verificar Errores del Persister
echo "5️⃣  Mongo Persister - Errores:"
docker logs mongo-persister 2>/dev/null | grep "Errores:" | tail -1
echo ""

# Resumen
echo "=========================================="
echo "✅ Verificación completada"
echo "=========================================="
```

**Uso:**
```bash
chmod +x check_system.sh
./check_system.sh
```

---

## 🛑 Orden de Parada

### ⚠️ IMPORTANTE: Secuencia Correcta de Parada

El orden de parada **también es importante** para evitar pérdida de datos.

---

### **PASO 1: Parar el Generador (Si aún está corriendo)**

```bash
# ⚠️ MUY IMPORTANTE: Parar el generador primero
docker stop random_generator

# Verificar que se detuvo
docker ps -a | grep random_generator
# Debería decir: Exited
```

---

### **PASO 2: Esperar a que Mongo Persister Procese Todo**

```bash
# Verificar el LAG
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# ✅ Esperar hasta que LAG = 0 en todas las particiones
# Puede tardar unos segundos o minutos dependiendo de cuántos mensajes haya

# Ver progreso en tiempo real
watch -n 2 'docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe 2>/dev/null | tail -n +3'
```

**Presiona `Ctrl+C`** cuando veas LAG = 0 en todas las particiones.

---

### **PASO 3: Parar Mongo Persister**

```bash
# Ver estadísticas finales
docker logs mongo-persister | grep "📊 ESTADÍSTICAS" | tail -1

# Parar el servicio
docker stop mongo-persister

# Verificar logs finales
docker logs mongo-persister | tail -20
```

---

### **PASO 4: Parar Servicios de Aplicación**

```bash
# Parar todos los servicios de aplicación
docker-compose -f docker-compose-services.yml down

# Verificar que se detuvieron
docker ps | grep -E "mongo-persister|hrpro-mongodb|hrpro-mongo-viewer"
# No debería mostrar nada
```

---

### **PASO 5: Parar Infraestructura Kafka**

```bash
# Parar infraestructura Kafka
docker-compose -f docker-compose-kafka.yml down

# Verificar que se detuvieron
docker ps | grep -E "kafka|zookeeper|kafdrop|random_generator"
# No debería mostrar nada
```

---

### **PASO 6: Verificación Final**

```bash
# Verificar que no hay contenedores del proyecto corriendo
docker ps --format "table {{.Names}}\t{{.Status}}"

# ✅ No debería aparecer ningún contenedor del proyecto
```

---

### Parada Completa (Un Solo Comando)

Si necesitas parar todo rápidamente (⚠️ sin esperar a procesar mensajes pendientes):

```bash
# Parar todo en orden
docker stop random_generator
sleep 5
docker-compose -f docker-compose-services.yml down
docker-compose -f docker-compose-kafka.yml down

# Verificar
docker ps -a | grep -E "kafka|mongo|persister|generator"
```

---

## 🧹 Limpieza y Mantenimiento

### Limpiar Datos (Mantener Configuración)

```bash
# Parar todo
docker-compose -f docker-compose-services.yml down
docker-compose -f docker-compose-kafka.yml down

# Eliminar volúmenes de datos (⚠️ BORRA TODOS LOS DATOS)
docker-compose -f docker-compose-services.yml down -v
docker-compose -f docker-compose-kafka.yml down -v

# Verificar que se eliminaron
docker volume ls | grep -E "kafka|mongo|zookeeper"
# No debería mostrar nada
```

---

### Limpiar Todo (Incluir Imágenes)

```bash
# Parar y eliminar todo
docker-compose -f docker-compose-services.yml down -v --rmi all
docker-compose -f docker-compose-kafka.yml down -v --rmi all

# Limpiar imágenes huérfanas
docker image prune -a -f

# Limpiar redes huérfanas
docker network prune -f

# Verificar espacio liberado
docker system df
```

---

### Reconstruir Servicios (Después de Cambios en Código)

```bash
# Reconstruir mongo-persister
docker-compose -f docker-compose-services.yml build --no-cache mongo-persister

# Reconstruir generador
docker-compose -f docker-compose-kafka.yml build --no-cache random_generator

# Reconstruir todo
docker-compose -f docker-compose-services.yml build --no-cache
docker-compose -f docker-compose-kafka.yml build --no-cache
```

---

### Mantenimiento de MongoDB

```bash
# Compactar base de datos (liberar espacio)
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').runCommand({compact: 'raw_messages'})"

# Ver estadísticas de espacio
docker exec hrpro-mongodb mongosh \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.stats()" \
  --quiet
```

---

### Mantenimiento de Kafka

```bash
# Ver tamaño del topic
docker exec kafka kafka-log-dirs \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic-list probando

# Borrar topic completo (⚠️ BORRA TODOS LOS MENSAJES)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic probando

# El topic se recreará automáticamente cuando el generador envíe datos
```

---

## 🔧 Troubleshooting

### Problema 1: "Topic no disponible" (Error al inicio)

**Síntoma:**
```
⏳ Topic 'probando' aún no disponible. Esperando a que el generador de datos lo cree...
```

**Solución:**
✅ **Esto es NORMAL** si el generador aún no ha arrancado.

1. Verificar que el generador esté corriendo:
   ```bash
   docker ps | grep random_generator
   ```

2. Si no está corriendo, arrancarlo:
   ```bash
   docker-compose -f docker-compose-kafka.yml up -d random_generator
   ```

3. Esperar 10 segundos y verificar logs de mongo-persister:
   ```bash
   docker logs mongo-persister | tail -20
   ```

4. ✅ Deberías ver: "Topic 'probando' ahora disponible. Procesando mensajes..."

---

### Problema 2: LAG Alto (Mensajes sin procesar)

**Síntoma:**
```
LAG: 50000 mensajes pendientes
```

**Causas posibles:**
- Generador demasiado rápido
- Mongo Persister sobrecargado
- MongoDB lento

**Solución:**

1. **Parar el generador inmediatamente:**
   ```bash
   docker stop random_generator
   ```

2. **Esperar a que el LAG baje:**
   ```bash
   watch -n 2 'docker exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group mongo-persister-group \
     --describe 2>/dev/null | tail -n +3'
   ```

3. **Si el LAG no baja, verificar logs:**
   ```bash
   docker logs mongo-persister | tail -50
   ```

4. **Reiniciar mongo-persister si es necesario:**
   ```bash
   docker restart mongo-persister
   ```

---

### Problema 3: Errores en Mongo Persister

**Síntoma:**
```
Errores: 123
```

**Solución:**

1. **Ver logs detallados:**
   ```bash
   docker logs mongo-persister | grep "ERROR"
   ```

2. **Errores comunes y soluciones:**

   **a) Error de conexión a MongoDB:**
   ```
   Error: Connection refused
   ```
   **Solución:**
   ```bash
   # Verificar que MongoDB está corriendo
   docker ps | grep hrpro-mongodb
   
   # Si no está corriendo, levantarlo
   docker-compose -f docker-compose-services.yml up -d hrpro-mongodb
   ```

   **b) Error de conexión a Kafka:**
   ```
   Error: Broker not available
   ```
   **Solución:**
   ```bash
   # Verificar que Kafka está corriendo
   docker ps | grep kafka
   
   # Ver logs de Kafka
   docker logs kafka | tail -20
   
   # Reiniciar Kafka si es necesario
   docker restart kafka
   sleep 30
   docker restart mongo-persister
   ```

   **c) Error de parsing JSON:**
   ```
   Error parseando JSON
   ```
   **Solución:**
   ```bash
   # Ver mensajes problemáticos en Kafka
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic probando \
     --from-beginning \
     --max-messages 10
   
   # Verificar formato del generador
   docker logs random_generator | tail -20
   ```

---

### Problema 4: MongoDB Lleno

**Síntoma:**
```
Error: No space left on device
```

**Solución:**

1. **Verificar espacio:**
   ```bash
   docker exec hrpro-mongodb df -h
   ```

2. **Eliminar documentos antiguos:**
   ```bash
   # Eliminar documentos más viejos de 1 hora
   docker exec hrpro-mongodb mongosh \
     --username admin \
     --password admin123 \
     --authenticationDatabase admin \
     --eval "
       db.getSiblingDB('hrpro_db').raw_messages.deleteMany({
         '_kafka_metadata.timestamp': {
           \$lt: new Date(Date.now() - 3600000)
         }
       })
     "
   ```

3. **Compactar base de datos:**
   ```bash
   docker exec hrpro-mongodb mongosh \
     --username admin \
     --password admin123 \
     --authenticationDatabase admin \
     --eval "db.getSiblingDB('hrpro_db').runCommand({compact: 'raw_messages'})"
   ```

---

### Problema 5: Kafka Lleno

**Síntoma:**
```
Error: Log is too large
```

**Solución:**

1. **Configurar retención más agresiva** (ver sección "Configuración Avanzada")

2. **Borrar topic y empezar de cero:**
   ```bash
   # Parar servicios
   docker stop mongo-persister random_generator
   
   # Borrar topic
   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --delete \
     --topic probando
   
   # Esperar 10 segundos
   sleep 10
   
   # Reiniciar servicios
   docker start mongo-persister
   docker start random_generator
   ```

---

### Problema 6: Contenedores No Arrancan

**Síntoma:**
```
Error: Conflict. The container name "..." is already in use
```

**Solución:**

1. **Ver contenedores huérfanos:**
   ```bash
   docker ps -a | grep -E "kafka|mongo|persister|generator"
   ```

2. **Eliminar contenedores huérfanos:**
   ```bash
   docker rm -f random_generator
   docker rm -f mongo-persister
   docker rm -f kafka
   docker rm -f zookeeper
   docker rm -f kafdrop
   docker rm -f hrpro-mongodb
   docker rm -f hrpro-mongo-viewer
   ```

3. **Reintentar:**
   ```bash
   # Seguir el orden de arranque desde el PASO 1
   docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop
   # ...etc
   ```

---

### Problema 7: Puertos Ya en Uso

**Síntoma:**
```
Error: Bind for 0.0.0.0:9092 failed: port is already allocated
```

**Solución:**

1. **Ver qué está usando el puerto:**
   ```bash
   # Windows
   netstat -ano | findstr :9092
   
   # Linux/Mac
   lsof -i :9092
   ```

2. **Opciones:**
   - **a) Detener el proceso que usa el puerto**
   - **b) Cambiar el puerto en docker-compose-kafka.yml:**
     ```yaml
     kafka:
       ports:
         - "29093:29092"  # Cambiar 29092 por 29093
         - "9093:9092"     # Cambiar 9092 por 9093
     ```

---

## ⚙️ Configuración Avanzada

### Ajustar Retención de Kafka (Reducir Espacio)

Por defecto, Kafka retiene mensajes durante **7 días**. Para desarrollo, puedes reducirlo a **1 hora**:

#### **Opción 1: Configurar al Crear el Topic**

Edita docker-compose-kafka.yml:

```yaml
kafka:
  environment:
    # ...existing code...
    KAFKA_LOG_RETENTION_HOURS: 1  # ← Cambiar de 168 (7 días) a 1 (1 hora)
    KAFKA_LOG_RETENTION_BYTES: 1073741824  # ← Límite de 1GB por partición
```

Luego:
```bash
# Reiniciar Kafka
docker-compose -f docker-compose-kafka.yml restart kafka
```

---

#### **Opción 2: Cambiar Retención de un Topic Existente**

```bash
# Reducir retención a 1 hora (3600000 ms)
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name probando \
  --alter \
  --add-config retention.ms=3600000

# Limitar tamaño a 500MB por partición
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name probando \
  --alter \
  --add-config retention.bytes=524288000

# Verificar configuración
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name probando \
  --describe
```

---

### Ajustar Batch Size del Mongo Persister

Si el persister va muy lento, aumenta el batch size:

Edita docker-compose-services.yml:

```yaml
mongo-persister:
  environment:
    MONGO_BATCH_SIZE: 500  # ← Cambiar de 100 a 500 (inserta más rápido)
```

Luego:
```bash
# Reconstruir y reiniciar
docker-compose -f docker-compose-services.yml up -d --build mongo-persister
```

---

### Ajustar Velocidad del Generador

Si quieres controlar cuántos mensajes genera por segundo:

**⚠️ NOTA:** El generador legacy no tiene esta configuración por defecto. Necesitarías modificar el código o limitar con un wrapper script.

**Script wrapper para controlar ejecución:**

```bash
#!/bin/bash
# run_generator_controlled.sh
# Ejecuta el generador durante X segundos y lo para

DURATION=${1:-60}  # Default: 60 segundos

echo "🚀 Iniciando generador por $DURATION segundos..."
docker-compose -f docker-compose-kafka.yml up -d random_generator

echo "⏳ Esperando..."
sleep $DURATION

echo "🛑 Parando generador..."
docker stop random_generator

echo "✅ Completado"
./check_system.sh
```

**Uso:**
```bash
chmod +x run_generator_controlled.sh

# Generar durante 30 segundos
./run_generator_controlled.sh 30

# Generar durante 2 minutos
./run_generator_controlled.sh 120
```

---

### Escalar Horizontalmente (Múltiples Consumers)

Para procesar más rápido, puedes levantar múltiples consumers:

Edita docker-compose-services.yml:

```yaml
services:
  mongo-persister:
    # ...existing code...
    deploy:
      replicas: 3  # ← Levantar 3 instancias en paralelo
```

Luego:
```bash
docker-compose -f docker-compose-services.yml up -d --scale mongo-persister=3
```

**Resultado:** Kafka distribuirá las 3 particiones entre los 3 consumers (1 por consumer).

---

### Añadir Autenticación a Kafka (Producción)

Para producción, deberías añadir autenticación SASL/SSL. Consulta la documentación oficial de Kafka.

---

## 📚 Recursos Adicionales

### Documentación Oficial

- **Apache Kafka:** https://kafka.apache.org/documentation/
- **MongoDB:** https://docs.mongodb.com/
- **Docker Compose:** https://docs.docker.com/compose/
- **Kafdrop:** https://github.com/obsidiandynamics/kafdrop
- **Confluent Kafka Python:** https://docs.confluent.io/kafka-clients/python/current/overview.html

---

### Comandos Útiles de Referencia Rápida

```bash
# ============================================================
# ARRANQUE RÁPIDO
# ============================================================
docker-compose -f docker-compose-kafka.yml up -d zookeeper kafka kafdrop
sleep 30
docker-compose -f docker-compose-services.yml up -d
docker-compose -f docker-compose-kafka.yml up -d random_generator

# ⏰ PARAR GENERADOR DESPUÉS DE X TIEMPO
sleep 60 && docker stop random_generator

# ============================================================
# VERIFICACIÓN
# ============================================================
# Mensajes en Kafka
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic probando 2>/dev/null | \
  awk -F ":" '{sum += $3} END {print sum}'

# LAG del consumer
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group mongo-persister-group \
  --describe

# Documentos en MongoDB
docker exec hrpro-mongodb mongosh \
  --username xxxxxx --password xxxxxxx \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('hrpro_db').raw_messages.countDocuments()" \
  --quiet

# ============================================================
# PARADA ORDENADA
# ============================================================
docker stop random_generator
# Esperar hasta alcanzar LAG = 0
docker-compose -f docker-compose-services.yml down
docker-compose -f docker-compose-kafka.yml down

# ============================================================
# LIMPIEZA COMPLETA
# ============================================================
docker-compose -f docker-compose-services.yml down -v
docker-compose -f docker-compose-kafka.yml down -v
```

---

## 🆘 Soporte

Si encuentras problemas no documentados aquí:

1. **Revisar logs detallados:**
   ```bash
   docker logs <nombre_contenedor> --tail 100
   ```

2. **Verificar estado del sistema:**
   ```bash
   docker ps -a
   docker network ls
   docker volume ls
   ```

3. **Consultar documentación oficial** de cada componente

4. **Contactar al equipo de desarrollo**

---

## 📝 Notas Finales

### ⚠️ Recordatorios Importantes

1. **SIEMPRE** parar el generador de datos manualmente
2. **NUNCA** bajar Kafka antes que el Mongo Persister (esperar LAG = 0)
3. **Monitorear** el espacio en disco regularmente
4. **Ajustar** la retención de Kafka según tus necesidades
5. **Verificar** que no hay errores en los logs después de arrancar

### ✅ Checklist de Operación Diaria

- [ ] Arrancar infraestructura en el orden correcto
- [ ] Verificar que Kafka está listo antes de continuar
- [ ] Levantar Mongo Persister ANTES del generador
- [ ] Configurar alarma para parar el generador
- [ ] Verificar que LAG = 0 y Errores = 0
- [ ] Monitorear espacio en disco
- [ ] Parar el generador cuando tengas suficientes datos
- [ ] Esperar a que LAG = 0 antes de apagar
- [ ] Detener servicios en el orden correcto

---

**Versión del documento:** 1.0  
**Última actualización:** 2025-11-15  
**Mantenido por:** Equipo PIX G5 DataEngineer

---

## 📍 Ubicación de Este Documento

Este archivo debe ubicarse en la **raíz del proyecto**:

```
PIX_G5_DataEngineer/
├── README.md                    ← Descripción general del proyecto
├── OPERATIONS_GUIDE.md          ← 📄 ESTE DOCUMENTO
├── docker-compose-kafka.yml
├── docker-compose-services.yml
├── Estructura/
│   ├── datagen/
│   └── services/
└── ...
```

