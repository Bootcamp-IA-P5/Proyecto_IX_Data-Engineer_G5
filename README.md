# Proyecto_IX_Data-Engineer_G5

## 🚀 Setup Rápido

**IMPORTANTE**: Este proyecto requiere un generador de datos externo.

### 📥 Para empezar:

1. **Configurar Kafka + Generador de Datos**
   ```bash
   # Ver instrucciones detalladas en:
   cat SETUP_KAFKA_GENERATOR.md
   ```

2. **Ejecutar nuestros servicios**
   ```bash
   cd Estructura/services/kafka-consumer
   cp .env.example .env
   pip install -r requirements.txt
   python src/consumer.py
   ```

**📖 Documentación completa**: Ver `SETUP_KAFKA_GENERATOR.md`

---

## 📁 Estructura del Proyecto

Estructura/
│
├── 📂 services/                      # Microservicios separados
│   ├── 📂 kafka-consumer/           # Servicio de consumo de Kafka
│   │   ├── 📂 src/
│   │   │   ├── __init__.py
│   │   │   ├── consumer.py          # Lógica principal del consumer
│   │   │   ├── config.py            # Configuración del servicio
│   │   │   └── models.py            # Modelos de datos
│   │   ├── 📂 tests/
│   │   │   ├── __init__.py
│   │   │   └── test_consumer.py
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── .env.example
│   │
│   ├── 📂 data-processor/           # Servicio de procesamiento ETL
│   │   ├── 📂 src/
│   │   │   ├── __init__.py
│   │   │   ├── processor.py         # Lógica de transformación
│   │   │   ├── aggregator.py        # Agrupación de datos
│   │   │   ├── validators.py        # Validaciones de datos
│   │   │   └── config.py
│   │   ├── 📂 tests/
│   │   │   ├── __init__.py
│   │   │   ├── test_processor.py
│   │   │   └── test_validators.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── 📂 mongo-persister/          # Servicio de persistencia MongoDB
│   │   ├── 📂 src/
│   │   │   ├── __init__.py
│   │   │   ├── persister.py         # Lógica de guardado en Mongo
│   │   │   ├── schemas.py           # Esquemas de documentos
│   │   │   └── config.py
│   │   ├── 📂 tests/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── 📂 sql-persister/            # Servicio de persistencia SQL
│   │   ├── 📂 src/
│   │   │   ├── __init__.py
│   │   │   ├── persister.py
│   │   │   ├── models.py            # ORM models
│   │   │   ├── migrations/          # Migraciones de BD
│   │   │   │   ├── 001_initial.sql
│   │   │   │   └── 002_add_indexes.sql
│   │   │   └── config.py
│   │   ├── 📂 tests/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── 📂 api/                      # API REST (Mejora Nivel 2)
│       ├── 📂 src/
│       │   ├── __init__.py
│       │   ├── main.py              # FastAPI app
│       │   ├── 📂 routers/
│       │   │   ├── __init__.py
│       │   │   ├── persons.py
│       │   │   └── stats.py
│       │   ├── 📂 schemas/
│       │   │   ├── __init__.py
│       │   │   └── person.py
│       │   └── config.py
│       ├── 📂 tests/
│       ├── Dockerfile
│       └── requirements.txt
│
├── 📂 shared/                        # Código compartido entre servicios
│   ├── __init__.py
│   ├── 📂 database/
│   │   ├── __init__.py
│   │   ├── mongo_client.py          # Cliente MongoDB reutilizable
│   │   ├── sql_client.py            # Cliente SQL reutilizable
│   │   └── redis_client.py          # Cliente Redis reutilizable
│   ├── 📂 utils/
│   │   ├── __init__.py
│   │   ├── logger.py                # Configuración de logs
│   │   ├── metrics.py               # Métricas de Prometheus
│   │   └── validators.py            # Validadores comunes
│   └── 📂 models/
│       ├── __init__.py
│       └── schemas.py               # Esquemas de datos compartidos
│
├── 📂 config/                        # Configuraciones generales
│   ├── kafka/
│   │   └── topics.yaml
│   ├── mongodb/
│   │   └── init-mongo.js
│   ├── postgres/
│   │   └── init.sql
│   └── prometheus/
│       └── prometheus.yml
│
├── 📂 scripts/                       # Scripts útiles
│   ├── setup.sh                     # Script de setup inicial
│   ├── start-dev.sh                 # Iniciar entorno desarrollo
│   ├── run-tests.sh                 # Ejecutar todos los tests
│   └── seed-data.sh                 # Datos de prueba
│
├── 📂 docs/                          # Documentación
│   ├── architecture.md              # Arquitectura del sistema
│   ├── api-docs.md                  # Documentación API
│   ├── deployment.md                # Guía de despliegue
│   └── diagrams/
│       ├── etl-flow.png
│       └── architecture.png
│
├── 📂 monitoring/                    # Monitoreo y observabilidad
│   ├── grafana/
│   │   └── dashboards/
│   │       └── etl-dashboard.json
│   └── prometheus/
│       └── rules.yml
│
├── 📂 tests/                         # Tests de integración
│   ├── integration/
│   │   ├── test_full_pipeline.py
│   │   └── test_end_to_end.py
│   └── fixtures/
│       └── sample_data.json
│
├── 📂 .github/                       # GitHub Actions CI/CD
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
│
├── docker-compose.yml                # Orquestación de servicios
├── docker-compose.dev.yml            # Configuración de desarrollo
├── docker-compose.prod.yml           # Configuración de producción
├── .gitignore
├── .env.example                      # Variables de entorno ejemplo
├── README.md