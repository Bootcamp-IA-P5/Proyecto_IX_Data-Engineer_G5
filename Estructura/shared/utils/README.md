# 🛠️ Utilidades de Desarrollo

Scripts auxiliares para monitoreo, debugging y testing del sistema para el servicio data-procesor (lee datos en crudo en MongoDB 'raw_messages' y los inserta en MongoDB en 'aggregated-data')

## 📊 Scripts Disponibles


### `menu_monitoring_data_processor.py`
Menú interactivo para lanzar los principales scripts de monitorización y utilidades.

```bash
python Estructura/shared/utils/menu_monitoring_data_processor.py
```

### `mongodb_stats.py`
Muestra estadísticas rápidas de MongoDB.

```bash
python Estructura/shared/utils/mongodb_stats.py
```

**Muestra:**
- Total de mensajes raw
- Mensajes procesados vs pendientes
- Total de personas agregadas
- Registros completos (5 tipos)
- Distribución de tipos de datos

---

### `monitor_processing.py`
Dashboard en tiempo real del procesamiento de datos.

```bash
python Estructura/shared/utils/monitor_processing.py
```

**Características:**
- Actualización cada 5 segundos
- Barra de progreso visual
- Cálculo de ETA (tiempo estimado)
- Velocidad de procesamiento
- Distribución de tipos y completitud

---

### `check_messages.py`
Analiza patrones en los mensajes raw para entender la estructura de datos.

```bash
python Estructura/shared/utils/check_messages.py
```

**Muestra:**
- Patrones de campos encontrados
- Ejemplos de cada tipo de mensaje
- Estadísticas generales

---

### `analyze_distribution.py`
Analiza la distribución de tipos agregados por persona y la completitud del matching.

```bash
python Estructura/shared/utils/analyze_distribution.py
```

**Muestra:**
- Personas agrupadas por cantidad de tipos recibidos (1 a 5)
- Completitud global del matching
- Ejemplos de registros con 3, 4 y 5 tipos

---

### `test_connection.py`
Testea la conexión a MongoDB usando distintas configuraciones y variables de entorno.

```bash
python Estructura/shared/utils/test_connection.py
```

**Características:**
- Prueba varias formas de conexión (sin auth, con authSource, directConnection)
- Diagnóstico de errores de red y autenticación

---

### `reset_and_test.py`
Resetea el sistema completo y genera datos de prueba.

```bash
python Estructura/shared/utils/reset_and_test.py
```

**⚠️ ADVERTENCIA:** Elimina todos los datos existentes.

**Proceso:**
1. Detiene todos los servicios
2. Elimina volumen de MongoDB
3. Reconstruye infraestructura
4. Genera ~120k mensajes de prueba (2 minutos)
5. Inicia el data-processor y muestra estadísticas

---

## 🔧 Configuración

Todos los scripts usan variables de entorno del archivo `.env`:

```bash
MONGO_USERTEST=hrpro_user
MONGO_PASSWORDTEST=<passwd>
MONGO_DATABASE=hrpro_db
```

Ver `.env.example` para la plantilla completa.

---

## 📦 Dependencias

```bash
pip install pymongo python-dotenv
```

---

## 🐛 Troubleshooting

### Error de autenticación

Verificar que el usuario existe:
```bash
docker exec hrpro-mongodb mongosh hrpro_db --username hrpro_user --password <passwd> --eval "db.raw_messages.countDocuments({})"
```

Recrear usuario si es necesario:
```bash
docker exec hrpro-mongodb mongosh admin --username admin --password <passwd> --eval "db.getSiblingDB('hrpro_db').createUser({user: 'hrpro_user', pwd: '<passwd', roles: [{role: 'readWrite', db: 'hrpro_db'}, {role: 'dbAdmin', db: 'hrpro_db'}]})"
```

### MongoDB no responde

Verificar que está corriendo:
```bash
docker ps | grep mongo
```

Ver logs:
```bash
docker logs hrpro-mongodb
```

### Recrear todo el entorno

```bash
docker-compose -f docker-compose-kafka.yml down -v
docker-compose -f docker-compose-services.yml down -v
docker-compose -f docker-compose-kafka.yml up -d
```