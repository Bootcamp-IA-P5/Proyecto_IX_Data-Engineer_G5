# MongoDB Persister - Servicio de Persistencia de Datos Crudos

## 🎯 Objetivo
Consumir mensajes de Kafka y guardarlos en MongoDB en formato crudo (sin procesar).

## 🏗️ Arquitectura

```
Kafka (topic: probando)
        ↓
   Mongo Persister
        ↓
   MongoDB (raw_messages)
```

## 🚀 Quick Start

### 1. Levantar MongoDB
```bash
docker-compose up -d mongodb mongo-express
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno
Copiar `.env.example` a `.env` y ajustar si es necesario.

### 4. Ejecutar el persister
```bash
python src/persister.py
```

### 5. Verificar datos
Ir a http://localhost:8081 (Mongo Express)

## 📁 Estructura

```
mongo-persister/
├── src/
│   ├── mongo_client.py    # Cliente MongoDB
│   ├── persister.py        # Servicio principal
│   └── config.py           # Configuración
├── requirements.txt
├── .env
└── README.md
```

## 🔧 Configuración

Variables en `.env`:
- `MONGO_HOST`: Host de MongoDB (default: localhost)
- `MONGO_PORT`: Puerto (default: 27017)
- `MONGO_DATABASE`: Base de datos (default: hrpro_db)
- `MONGO_COLLECTION`: Colección (default: raw_messages)
- `KAFKA_BOOTSTRAP_SERVERS`: Servidor Kafka
- `KAFKA_TOPIC`: Topic a consumir

## 📊 Verificar datos guardados

```bash
# Conectar a MongoDB
docker exec -it hrpro-mongodb mongosh --username admin --password admin123

# Ver datos
use hrpro_db
db.raw_messages.countDocuments()
db.raw_messages.find().limit(5).pretty()
```

## 🧪 Tests

### Ejecutar tests unitarios

```bash
# Desde el directorio del servicio
cd Estructura/services/mongo-persister

# Ejecutar todos los tests
python -m unittest discover tests/

# Ejecutar test específico
python -m unittest tests.test_persister.TestDataPersister.test_init_successful

# Con pytest (si está instalado)
pytest tests/
```

### Cobertura de tests

```bash
# Instalar coverage
pip install coverage

# Ejecutar con cobertura
coverage run -m unittest discover tests/
coverage report
coverage html  # Genera reporte HTML en htmlcov/
```

## ✅ Checklist

- [ ] MongoDB corriendo en Docker
- [ ] Persister conecta a MongoDB
- [ ] Persister consume de Kafka
- [ ] Datos se guardan correctamente
- [ ] Metadata incluida (timestamp, source, etc.)
- [ ] Sin pérdida de mensajes

## 📝 Notas

- Los datos se guardan TAL CUAL vienen de Kafka (raw/crudos)
- NO se procesan ni transforman en este servicio
- El procesamiento lo hará el servicio `data-processor`
- Cada mensaje incluye metadata adicional

## 🔗 Dependencias

- Kafka corriendo (puerto 29092)
- Topic `probando` existente
- MongoDB (puerto 27017)

## 📚 Documentación Completa

Ver `TAREAS_MONGODB.md` para instrucciones detalladas paso a paso.
