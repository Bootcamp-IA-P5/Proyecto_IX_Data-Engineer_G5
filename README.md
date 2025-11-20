# Proyecto IX Data Engineer G5 : Proceso ETL

Bienvenido al proyecto **PIX_G5_DataEngineer**: una arquitectura moderna para procesamiento de datos en tiempo real usando Kafka, MongoDB y PostgreSQL.

Proyecto grupal de Data Engineer dentro del bootcamp de IA de [Factoría F5 – Web Oficial](https://factoriaf5.org/)  
Nuestro objetivo es diseñar e implementar un sistema de gestión de datos eficiente para HR Pro, que les permita organizar y analizar grandes volúmenes de datos procedentes de diversas fuentes, como solicitudes de empleo, registros de nómina, encuestas de empleados, entre otros. Cada uno de ustedes aporta habilidades únicas en extracción, transformación y carga de datos (ETL), así como experiencia en el uso de tecnologías como Apache Kafka, MongoDB y bases de datos SQL. Deberán diseñar un proceso ETL robusto para integrar datos en un sistema unificado, organizar y almacenar datos de manera eficiente tanto en una base de datos MongoDB como en un almacén de datos SQL, implementar el sistema en un entorno Dockerizado para garantizar la escalabilidad y la portabilidad, y trabajar con una amplia variedad de datos, desde información personal hasta registros financieros y métricas de rendimiento.


---

👥 Autores

    
- [Umit Gungor](https://github.com/GungorUmit) — Data Engineer & Team Developer  
- [Anthony Caceda](https://github.com/Anthonycpcode) — Scrum Master & Data Engineer  
- [Jimena Flores](https://github.com/JIMENA-ft) — Data Engineer & Team Developer  
- [Alfonso Bermúdez Torres](https://github.com/GHalfbbt) — Data Engineer & Product Owner

---

## 📚 Documentación

- `README.md` — Este documento
- `ANALISIS_PROYECTO_COMPLETO.md` — Análisis técnico y arquitectura
- `GUIA_IMPLEMENTACION.md` — Guía paso a paso
- `SETUP_RAPIDO.md` — Instrucciones rápidas de despliegue
```
## 🏗️ ESTRUCTURA DEL PROYECTO

```
Proyecto_IX_Data-Engineer_G5/
│
├── 📄 docker-compose-kafka.yml          # ✅ Infraestructura Kafka  (dockerizados F5)
├── 📄 docker-compose-services.yml       # ✅ Servicios procesos ETL (dockerizados)
├── 📄 README.md                         # 📚 Documentación principal
├── 📄 .env.example                      # 🔧 Ejemplo de configuración
│
└── 📂 Estructura/
    ├── 📂 services/                     # 🎯 Microservicios
    │   ├── 📂 kafka-consumer/           # ✅ SERVICIO PRINCIPAL ANALIZADO
    │   │   ├── 📂 src/
    │   │   │   ├── consumer.py          # ✅ Lógica de consumo de Kafka
    │   │   │   ├── models.py            # ✅ Modelos de datos (5 tipos)
    │   │   │   └── __init__.py
    │   │   ├── 📂 tests/
    │   │   │   └── test_consumer.py
    │   │   ├── .env                     # ✅ Configuración (KAFKA_BOOTSTRAP_SERVERS=kafka:9092)
    │   │   ├── .env.example
    │   │   ├── requirements.txt         # ✅ Dependencias Python
    │   │   ├── README.md
    │   │   ├── GUIA_IMPLEMENTACION.md
    │   │   └── SETUP_RAPIDO.md
    │   │ 
    │   ├── 📂 data-processor/           # 🔜 Consumer Kafka --> MongoDB raw_messages (collection)
    │   │   └── 📂 src
    │   │        └── processor.py
    │   ├── 📂 mongo-persister/          # 🔜 MongoDB raw_messages --> datos agregados 
    │   │    └── 📂 src
    │   │    │    └── persister.py
    │   │    └── 📂 tests/               # 🔧 Test unitarios (pytest)
    │   │           └── pytest.ini
    │   └── 📂 sql-persister/            # 🔜 ETL (datos agregados --> Supabase PostgresSQL)
    │       └── 📂 src
    │           └── persister.py
    │
    └── 📂 shared/                       
        ├── database/
        ├── models/
        └── utils/                        # 🔜 Scripts management dockers, estadísticas, monitorización
```

- **Kafka**: Ingesta y distribución de mensajes.
- **MongoDB**: Almacenamiento de datos crudos y agregados.
- **PostgreSQL (Supabase)**: Persistencia final para análisis SQL.
- **Microservicios**: 
  - `kafka-consumer`: Consume mensajes de Kafka.
  - `data-processor`: Normaliza y agrega datos en MongoDB.
  - `mongo-persister`: Persiste datos agregados.
  - `sql-persister`: ETL hacia PostgreSQL.
- **Scripts de utilidades**: Monitorización, limpieza y gestión de contenedores.

---

## 🚦 Cómo empezar

1. **Configura las variables de entorno** en `.env` (ver `.env.example`).
2. **Instala dependencias**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Levanta la infraestructura**:
   ```bash
   docker-compose -f docker-compose-kafka.yml up -d
   docker-compose -f docker-compose-services.yml up -d
   ```
4. **Consulta los logs**:
   ```bash
   docker logs -f kafka-consumer
   ```

## 🛠️ Principales tecnologías

- **Python 3.11+**
- **Kafka**
- **MongoDB**
- **PostgreSQL (Supabase)**
- **Docker & Docker Compose**

## 📊 Monitorización y utilidades

Scripts en `Estructura/shared/utils/` para:
- Estadísticas rápidas de MongoDB
- Limpieza de colecciones
- Monitorización en tiempo real
- Gestión de contenedores Docker

## 🤝 Contribuir

¿Quieres colaborar?  
Abre un issue o pull request.  
Toda mejora en documentación, tests o código es bienvenida.

---

**Proyecto desarrollado por el Grupo 5 de Data Engineer (PIX_G5) de Factoria F5**  
Fecha de inicio: 2025  

