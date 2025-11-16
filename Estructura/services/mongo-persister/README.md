# 🗄️ MongoDB Persister - Servicio de Persistencia de Datos Crudos

## 🎯 Objetivo
Consumir mensajes de Kafka y guardarlos en MongoDB en formato crudo (sin procesar).

---

## 🏗️ Arquitectura

```
Kafka (topic: probando)
        ↓
   Mongo Persister
        ↓
   MongoDB (raw_messages)
```

---

## 🚀 Quick Start

### 1. Levantar MongoDB
```bash
docker-compose up -d mongodb mongo-express
```

### 2. Instalar dependencias
```bash
# Dependencias de producción
pip install -r requirements.txt

# Dependencias de desarrollo (tests)
pip install -r requirements-dev.txt
```

### 3. Configurar variables de entorno
Copiar `.env.example` a `.env` y ajustar si es necesario.

### 4. Ejecutar el persister
```bash
python src/persister.py
```

### 5. Verificar datos
Ir a http://localhost:8081 (Mongo Express)

---

## 📁 Estructura del Proyecto

```
mongo-persister/
├── src/
│   ├── __init__.py
│   ├── config.py                    # ✅ Configuración centralizada
│   ├── mongo_client.py              # ✅ Cliente MongoDB (82% coverage)
│   └── persister.py                 # ✅ Servicio principal (59% coverage)
├── tests/
│   ├── conftest.py                  # ✅ Fixtures compartidas + carga .env.tests
│   ├── pytest.ini                   # ✅ Configuración pytest
│   ├── test_config.py               # ✅ Tests configuración (2 tests)
│   ├── test_persister.py            # ✅ Tests unitarios MongoDB (31 tests)
│   ├── test_persister_complete.py   # ✅ Tests unitarios Persister (15 tests)
│   ├── test_persister_stats.py      # ✅ Tests estadísticas (3 tests)
│   ├── test_performance.py          # ✅ Tests rendimiento (5 tests)
│   ├── test_integration.py          # ✅ Tests integración (9 tests)
│   └── test_persister_integration.py # ✅ Tests integración Persister (3 tests)
├── run_tests_persister.py           # 🎯 Script ejecutar tests (con menú)
├── requirements.txt                 # Dependencias producción
├── requirements-dev.txt             # Dependencias desarrollo
├── .env                             # Variables entorno (producción/Docker)
├── .env.tests                       # Variables entorno (tests locales)
├── README.md                        # Este archivo
├── RESUMEN_VISUAL.md                # Guía visual rápida
└── TAREAS_MONGODB.md                # Tutorial paso a paso
```

**📊 Total: 56 tests | 71% coverage** 🎉

---

## 🔧 Configuración

### **Variables de entorno**

#### **Producción (`.env`)**
```env
# MongoDB
MONGO_URI=mongodb://<user>:<passwd>@localhost:27017/
MONGO_DB=pix_transactions

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=probando
KAFKA_GROUP_ID=mongo-persister-group
```

#### **Tests locales (`.env.tests`)**
```env
# MongoDB
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=pix_test

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=probando
KAFKA_GROUP_ID=mongo-persister-test-group
```

**💡 Nota:** El archivo `conftest.py` carga automáticamente `.env.tests` para todos los tests.

---

## 📊 Verificar datos guardados

### **Opción 1: Mongo Express (UI web)**
```
http://localhost:8081
```

### **Opción 2: MongoDB Shell**
```bash
docker exec -it hrpro-mongodb mongosh --username <user> --password <passwd>

use pix_transactions
db.raw_messages.countDocuments()
db.raw_messages.find().limit(5).pretty()
```

### **Opción 3: Script Python**
```python
from pymongo import MongoClient

client = MongoClient('mongodb://<user>:<passwd>@localhost:27017/')
db = client['pix_transactions']
collection = db['raw_messages']

print(f"Total documentos: {collection.count_documents({})}")
for doc in collection.find().limit(3):
    print(doc)
```

---

## 🧪 Testing

### **🎯 Script principal: `run_tests_persister.py`**

```bash
# Tests básicos (todos)
python run_tests_persister.py

# Con coverage + reporte HTML
python run_tests_persister.py -c -o

# Solo tests unitarios (más rápido, sin contenedores)
python run_tests_persister.py -m unit -c

# Solo tests de integración (requiere contenedores)
python run_tests_persister.py -m integration -v

# Solo tests de performance
python run_tests_persister.py -m performance -v

# Ver ayuda completa
python run_tests_persister.py --help
```

---

### **📋 Menú interactivo del script**

Al ejecutar `python run_tests_persister.py`, verás:

```
========================================================================
  🧪 MONGO PERSISTER - TEST SUITE
========================================================================

📋 OPCIONES RÁPIDAS:

  Básicos:
    python run_tests_persister.py                    → Tests básicos
    python run_tests_persister.py -c                 → Con coverage
    python run_tests_persister.py -c -o              → Coverage + abrir HTML

  Filtros:
    python run_tests_persister.py -m unit            → Solo tests unitarios
    python run_tests_persister.py -m integration     → Solo tests integración
    python run_tests_persister.py -m performance     → Solo tests performance

  Debug:
    python run_tests_persister.py -v                 → Ver prints de tests
    python run_tests_persister.py -d 10              → Top 10 tests lentos
    python run_tests_persister.py -x                 → Parar en primer fallo

  Utilidades:
    python run_tests_persister.py --clean            → Limpiar archivos
    python run_tests_persister.py --open-only        → Solo abrir reporte
    python run_tests_persister.py --list-markers     → Listar markers
```

---

### **📊 Tipos de tests**

#### **1️⃣ Tests Unitarios** (33 tests - 0.57s ⚡)

**Qué testean:**
- ✅ Configuración del servicio
- ✅ Cliente MongoDB (con `mongomock`)
- ✅ Persister (con mocks de Kafka)
- ✅ Manejo de errores y edge cases
- ✅ Estadísticas y logging

**Archivos:**
- `test_config.py` (2 tests)
- `test_persister.py` (31 tests)

**Ejecutar:**
```bash
python run_tests_persister.py -m unit -c
```

**⚡ Ventaja:** NO requieren contenedores (usan mocks)

**Resultado esperado:**
```
Results (0.57s):
      33 passed
Coverage: 54.44%
```

---

#### **2️⃣ Tests de Performance** (5 tests - 0.50s ⚡)

**Qué testean:**
- ⚡ Throughput de inserción (mensajes/segundo)
- ⚡ Latencia de procesamiento (milisegundos)
- ⚡ Comparación batch vs individual
- ⚡ Performance de `get_stats()`
- ⚡ Manejo de concurrencia

**Archivo:**
- `test_performance.py`

**Ejecutar:**
```bash
python run_tests_persister.py -m performance -v
```

**Umbrales:**
- `insert_message`: > 200 msg/s
- `insert_batch`: > 500 msg/s
- Latencia: < 100ms
- `get_stats`: < 100ms

**⚡ Ventaja:** NO requieren contenedores (usan `mongomock`)

**Resultado esperado:**
```
⚡ Performance (insert_message):
   1000 mensajes insertados en: 0.079s
   Throughput: 12705 msg/s
   ✅ 1 mensajes verificados en BD

Results (0.50s):
       4 passed
       1 skipped
```

---

#### **3️⃣ Tests de Integración** (10 tests - 18.85s)

**Qué testean:**
- 🔌 Conexión a MongoDB real
- 🔌 Conexión a Kafka real
- 🔌 Inserción/recuperación MongoDB real
- 🔌 Producción/consumo Kafka real
- 🔌 Flujo end-to-end: Kafka → Persister → MongoDB

**Archivos:**
- `test_integration.py` (9 tests)
- `test_persister_integration.py` (3 tests)

**Ejecutar:**
```bash
# 1. Levantar contenedores
docker-compose up -d

# 2. Ejecutar tests
python run_tests_persister.py -m integration -v
```

**⚠️ Requiere:** Contenedores Kafka + MongoDB corriendo

**Skip automático:** Si los contenedores NO están disponibles:
```
⚠️  MongoDB no disponible
   Ejecuta: docker-compose up -d mongodb
```

**Resultado esperado:**
```
Results (18.85s):
      10 passed
Coverage: 64.09%
```

---

#### **4️⃣ Tests de Estadísticas y Logging** (3 tests)

**Qué testean:**
- 📊 Tracking de estadísticas (mensajes procesados/guardados)
- 📝 Logging correcto al iniciar
- 📈 Batch statistics tracking

**Archivo:**
- `test_persister_stats.py`

**Ejecutar:**
```bash
python run_tests_persister.py -f tests/test_persister_stats.py -v
```

---

### **📊 Cobertura de tests (Coverage)**

| Módulo | Statements | Coverage | Estado | Objetivo |
|--------|-----------|----------|--------|----------|
| `config.py` | 29 | **93%** | ✅ Excelente | 95% |
| `mongo_client.py` | 95 | **82%** | ✅ Muy bien | 80% |
| `persister.py` | 135 | **59%** | ✅ Bien | 60% |
| **TOTAL** | 259 | **71%** | ✅ **Meta alcanzada** | 70% |

```bash
# Ver reporte HTML de coverage
python run_tests_persister.py -c -o

# O abrir manualmente
start htmlcov/index.html       # Windows
open htmlcov/index.html        # macOS
xdg-open htmlcov/index.html    # Linux
```

---

### **🎯 Ejemplos de uso del script**

#### **Desarrollo diario (rápido)**
```bash
# Tests unitarios (sin contenedores)
python run_tests_persister.py -m unit -c
# ✅ 33 passed en 0.57s
```

#### **Antes de commit (completo)**
```bash
# Levantar contenedores
docker-compose up -d

# Todos los tests con coverage
python run_tests_persister.py -c -o
# ✅ 55 passed, 1 skipped en ~19s
```

#### **Solo tests de integración**
```bash
docker-compose up -d
python run_tests_persister.py -m integration -v
# ✅ 10 passed en 18.85s
```

#### **Solo tests de performance**
```bash
python run_tests_persister.py -m performance -v
# ✅ 4 passed, 1 skipped en 0.50s
```

#### **Test específico con debug**
```bash
python run_tests_persister.py -f tests/test_config.py -v -x
```

#### **Top 10 tests más lentos**
```bash
python run_tests_persister.py -d 10
```

#### **Limpiar archivos generados**
```bash
python run_tests_persister.py --clean
# Elimina: .coverage, htmlcov/, .pytest_cache/, __pycache__/
```

---

### **🔧 Comandos pytest tradicionales**

```bash
# Tests básicos
pytest tests/ -v --emoji

# Con coverage
pytest tests/ --cov=src --cov-report=html

# Solo tests unitarios
pytest tests/ -m unit -v

# Solo tests de performance
pytest tests/ -m performance -v

# Solo tests de integración
pytest tests/ -m integration -v

# Test específico
pytest tests/test_config.py::TestConfigPrintFunction::test_print_config_does_not_crash -v
```

---

## 🐳 Contenedores Docker

### **Para el servicio (producción)**

```bash
# Levantar todo el stack
docker-compose up -d

# Solo MongoDB + Mongo Express
docker-compose up -d mongodb mongo-express

# Ver logs
docker-compose logs -f mongo-persister

# Parar todo
docker-compose down
```

**Servicios:**
- ✅ `kafka` - Broker Kafka (puerto 9092 interno, 29092 externo)
- ✅ `zookeeper` - Coordinación Kafka
- ✅ `mongodb` - Base de datos (puerto 27017)
- ✅ `mongo-express` - UI web (puerto 8081)
- ✅ `mongo-persister` - Este servicio

---

### **Para los tests**

| Tipo de test | Requiere contenedores | Usa mocks | Tiempo |
|--------------|----------------------|-----------|---------|
| **Unitarios** (`unit`) | ❌ NO | ✅ `mongomock`, `MagicMock` | 0.57s ⚡ |
| **Performance** (`performance`) | ❌ NO | ✅ `mongomock` | 0.50s ⚡ |
| **Integración** (`integration`) | ✅ SÍ | ❌ MongoDB y Kafka reales | 18.85s |

#### **Levantar solo lo necesario para tests de integración:**
```bash
# Opción 1: Levantar solo MongoDB y Kafka
docker-compose up -d kafka zookeeper mongodb

# Opción 2: Levantar todo menos el persister
docker-compose up -d kafka zookeeper mongodb mongo-express
```

#### **Verificar que están corriendo:**
```bash
docker ps

# Deberías ver:
# - hrpro-mongodb (puerto 27017)
# - hrpro-mongo-express (puerto 8081)
# - kafka (puerto 29092)
# - zookeeper (puerto 2181)
```

---

## 🧰 Dependencias

### **Instalación**

```bash
# Producción
pip install -r requirements.txt

# Desarrollo (incluye testing)
pip install -r requirements-dev.txt
```

### **Producción (`requirements.txt`)**
```txt
pymongo>=4.6.0
confluent-kafka>=2.3.0
python-dotenv>=1.0.0
```

### **Desarrollo (`requirements-dev.txt`)**
```txt
# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-emoji>=0.2.0
pytest-sugar>=0.9.7
pytest-dotenv>=0.5.2
pytest-timeout>=2.2.0
pytest-mock>=3.12.0
pytest-asyncio>=0.21.0

# MongoDB mock
mongomock>=4.1.0

# Datos fake
faker>=20.0.0

# Calidad de código
pylint>=3.0.0
black>=23.0.0
```

---

## 🔍 Troubleshooting

### **Tests**

#### **Error: "pytest no está instalado"**
```bash
pip install -r requirements-dev.txt
```

#### **Error: "No se encontró htmlcov"**
```bash
# Ejecutar con coverage primero
python run_tests_persister.py -c
```

#### **Error: "ModuleNotFoundError: No module named 'src'"**
```bash
# Ejecutar desde el directorio correcto
cd Estructura/services/mongo-persister
python run_tests_persister.py
```

#### **Tests de integración se saltan (skip)**
```bash
# 1. Levantar contenedores
docker-compose up -d kafka zookeeper mongodb

# 2. Verificar que están corriendo
docker ps

# 3. Ejecutar tests
python run_tests_persister.py -m integration -v
```

---

### **Servicio**

#### **Error: "No se puede conectar a MongoDB"**
```bash
# Verificar que MongoDB está corriendo
docker ps | grep mongodb

# Ver logs
docker logs hrpro-mongodb

# Reiniciar MongoDB
docker-compose restart mongodb
```

#### **Error: "No se puede conectar a Kafka"**
```bash
# Verificar Kafka
docker ps | grep kafka

# Ver logs
docker logs kafka

# Reiniciar Kafka
docker-compose restart kafka zookeeper
```

#### **Error: "Colección no se crea"**
```bash
# Verificar variables de entorno
cat .env

# Verificar que el persister está leyendo el .env correcto
python -c "from src.config import config; config.print_config()"
```

---

## 📈 Workflow de desarrollo recomendado

```bash
# 1. Crear rama de feature
git checkout -b feature/nueva-funcionalidad

# 2. Hacer cambios en el código

# 3. Ejecutar tests unitarios (rápido)
python run_tests_persister.py -m unit -c

# 4. Ver reporte HTML
# (El script preguntará si quieres abrirlo)

# 5. Si algo falla, ejecutar test específico con -v
python run_tests_persister.py -f tests/test_persister.py -v -x

# 6. Tests de performance (opcional)
python run_tests_persister.py -m performance -v

# 7. Tests de integración (antes de commit)
docker-compose up -d
python run_tests_persister.py -m integration -v

# 8. Limpiar archivos generados
python run_tests_persister.py --clean

# 9. Commit y push
git add .
git commit -m "feat: nueva funcionalidad"
git push origin feature/nueva-funcionalidad
```

---

## ✅ Checklist de Calidad

### **Servicio:**
- [x] MongoDB corriendo en Docker
- [x] Persister conecta a MongoDB
- [x] Persister consume de Kafka
- [x] Datos se guardan correctamente
- [x] Metadata incluida (timestamp, source, topic, processed)
- [x] Sin pérdida de mensajes
- [x] Manejo de errores robusto
- [x] Logging detallado

### **Tests:**
- [x] Tests unitarios (33 tests, 54% coverage)
- [x] Tests de integración (10 tests, 64% coverage)
- [x] Tests de performance (5 tests)
- [x] Tests de estadísticas (3 tests)
- [x] Coverage total > 70% ✅ (71%)
- [x] Script de ejecución (`run_tests_persister.py`)
- [x] Reporte HTML de coverage
- [x] Tests de errores y edge cases
- [x] Documentación inline en tests

### **Documentación:**
- [x] README.md completo
- [x] RESUMEN_VISUAL.md
- [x] TAREAS_MONGODB.md
- [x] Docstrings en código
- [x] Comentarios en tests

---

## 📊 Métricas de Calidad

| Métrica | Valor | Estado |
|---------|-------|--------|
| **Coverage total** | 71% | ✅ Meta alcanzada |
| **Tests totales** | 56 | ✅ Muy completo |
| **Tests unitarios** | 33 | ✅ Amplia cobertura |
| **Tests integración** | 10 | ✅ E2E testing |
| **Tests performance** | 5 | ✅ Benchmarking |
| **Tiempo tests unit** | 0.57s | ✅ Muy rápido |
| **Tiempo tests all** | ~19s | ✅ Rápido |
| **Líneas de código** | 259 | ✅ Conciso |
| **Complejidad** | Baja | ✅ Mantenible |

---

## 📝 Notas Importantes

### **Sobre los datos:**
- ✅ Los datos se guardan **TAL CUAL** vienen de Kafka (raw/crudos)
- ❌ **NO** se procesan ni transforman en este servicio
- ✅ El procesamiento lo hará el servicio `data-processor`
- ✅ Cada mensaje incluye metadata adicional

### **Sobre los tests:**
- ✅ Los tests **unitarios y de performance NO requieren** contenedores (usan mocks)
- ⚠️ Los tests de integración **SÍ requieren** contenedores (Kafka + MongoDB reales)
- ✅ El reporte HTML de coverage **NO** se sube al repositorio (ver `.gitignore`)
- ✅ El archivo `.env.tests` se carga automáticamente por `conftest.py`

### **Sobre el coverage:**
- ✅ `config.py`: 93% (casi perfecto)
- ✅ `mongo_client.py`: 82% (muy bien)
- ✅ `persister.py`: 59% (bien, código con threading/loops complejos)
- ✅ **Total**: 71% (meta alcanzada)

---

## 🔗 Dependencias del Sistema

### **Para el servicio:**
- ✅ Kafka corriendo (puerto 29092)
- ✅ Topic `probando` existente
- ✅ MongoDB (puerto 27017)

### **Para los tests:**
- **Unitarios + Performance**: ✅ Ningún contenedor (solo mocks)
- **Integración**: ⚠️ Kafka + MongoDB (contenedores reales)

---

## 📚 Documentación Adicional

- 📄 `README.md` - Este archivo (documentación principal)
- 📄 `RESUMEN_VISUAL.md` - Guía visual rápida (diagramas y ejemplos)
- 📄 `TAREAS_MONGODB.md` - Tutorial paso a paso (setup completo)
- 📄 `tests/test_*.py` - Documentación inline en cada test
- 📄 `htmlcov/index.html` - Reporte coverage (generado con `-c`)

---

## 🚀 CI/CD (GitHub Actions)

Ejemplo de workflow para GitHub Actions:

```yaml
# .github/workflows/mongo-persister-tests.yml
name: MongoDB Persister Tests

on:
  push:
    branches: [ main, develop ]
    paths:
      - 'Estructura/services/mongo-persister/**'
  pull_request:
    branches: [ main, develop ]
    paths:
      - 'Estructura/services/mongo-persister/**'

jobs:
  test-unit:
    name: Tests Unitarios
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          cd Estructura/services/mongo-persister
          pip install -r requirements-dev.txt
      
      - name: Run unit tests
        run: |
          cd Estructura/services/mongo-persister
          python run_tests_persister.py -m unit -c
      
      - name: Upload coverage report
        uses: actions/upload-artifact@v3
        with:
          name: coverage-report-unit
          path: Estructura/services/mongo-persister/htmlcov/

  test-integration:
    name: Tests Integración
    runs-on: ubuntu-latest
    services:
      mongodb:
        image: mongo:7.0
        ports:
          - 27017:27017
        env:
          MONGO_INITDB_ROOT_USERNAME: admin
          MONGO_INITDB_ROOT_PASSWORD: admin123
      
      zookeeper:
        image: confluentinc/cp-zookeeper:latest
        ports:
          - 2181:2181
        env:
          ZOOKEEPER_CLIENT_PORT: 2181
      
      kafka:
        image: confluentinc/cp-kafka:latest
        ports:
          - 29092:29092
        env:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:29092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          cd Estructura/services/mongo-persister
          pip install -r requirements-dev.txt
      
      - name: Run integration tests
        run: |
          cd Estructura/services/mongo-persister
          python run_tests_persister.py -m integration -v
      
      - name: Upload coverage report
        uses: actions/upload-artifact@v3
        with:
          name: coverage-report-integration
          path: Estructura/services/mongo-persister/htmlcov/
```

---

## 🎓 Para Nuevos Desarrolladores

### **Primeros pasos:**

1. **Clonar el repositorio**
   ```bash
   git clone <repo>
   cd Estructura/services/mongo-persister
   ```

2. **Crear entorno virtual**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

3. **Instalar dependencias**
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Ejecutar tests unitarios**
   ```bash
   python run_tests_persister.py -m unit -c -o
   ```

5. **Explorar el código**
   - Empieza por `src/config.py` (configuración)
   - Luego `src/mongo_client.py` (cliente MongoDB)
   - Finalmente `src/persister.py` (servicio principal)

6. **Leer la documentación**
   - `RESUMEN_VISUAL.md` → Visión general
   - `TAREAS_MONGODB.md` → Tutorial completo
   - Este archivo → Documentación técnica

---

## 💡 Tips y Mejores Prácticas

### **Testing:**
- ✅ Ejecuta tests unitarios frecuentemente (son rápidos)
- ✅ Ejecuta tests de integración antes de cada commit
- ✅ Usa `-v` para ver prints cuando debuggees
- ✅ Usa `-x` para parar en el primer fallo
- ✅ Revisa el reporte HTML de coverage

### **Desarrollo:**
- ✅ Usa `config.print_config()` para verificar variables de entorno
- ✅ Lee los logs del persister para entender el flujo
- ✅ Usa Mongo Express para ver los datos guardados
- ✅ Mantén el coverage > 70%

### **Git:**
- ✅ Crea ramas de feature para cada cambio
- ✅ Ejecuta tests antes de cada commit
- ✅ Usa commits atómicos con mensajes descriptivos
- ✅ No subas `.coverage`, `htmlcov/`, `__pycache__/`

---

## 📞 Soporte

¿Problemas? ¿Dudas?

1. **Revisa la documentación:**
   - `README.md` (este archivo)
   - `RESUMEN_VISUAL.md`
   - `TAREAS_MONGODB.md`

2. **Ejecuta los tests:**
   ```bash
   python run_tests_persister.py -v
   ```

3. **Revisa los logs:**
   ```bash
   docker logs hrpro-mongodb
   docker logs kafka
   python src/persister.py  # Ver logs del servicio
   ```

4. **Contacta al equipo:**
   - Issues en GitHub
   - Slack: #pix-data-engineering
   - Email: team@example.com

---

**¿Listo para empezar?** 🚀

```bash
# Quick start
python run_tests_persister.py -c -o
```

**✨ Happy coding! ✨**