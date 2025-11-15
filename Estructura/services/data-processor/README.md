# Data Processor Service

Servicio que agrupa y procesa datos crudos desde MongoDB (Issue #6).

## 📋 Descripción

Este servicio implementa la lógica de agrupación de datos:

1. **Lee** mensajes crudos de `raw_messages` (generados por mongo-persister)
2. **Agrupa** por persona usando passport como clave
3. **Unifica** 5 tipos de datos en un único registro:
   - Personal Info
   - Contact Info
   - Address
   - Employment
   - Benefits
4. **Valida** integridad de datos
5. **Guarda** en `aggregated_data` (MongoDB temporal)
6. **SQL Persister** (Issue #8) se encargará de mover a PostgreSQL/MySQL

## ⚙️ Configuración

Variables de entorno en el `.env` de la raíz:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `MONGO_URI` | - | URI de conexión a MongoDB |
| `MONGO_DATABASE` | - | Nombre de la base de datos |
| `RAW_COLLECTION` | `raw_messages` | Colección de datos crudos |
| `AGGREGATED_COLLECTION` | `aggregated_data` | Colección de datos agregados |
| `BATCH_SIZE` | `1000` | Documentos por lote |
| `PROCESSING_INTERVAL` | `10` | Segundos entre procesamiento |
| `GROUPING_WINDOW` | `60` | Segundos de timeout para datos incompletos |
| `STATS_INTERVAL` | `10` | Intervalo para mostrar estadísticas |
| `LOG_LEVEL` | `INFO` | Nivel de logging |

## 🚀 Uso

### Con Docker Compose

```bash
docker-compose -f docker-compose-services.yml up data-processor
```

### Desarrollo Local

```bash
# Desde el directorio del servicio
python -m src.processor
```

## 📊 Estrategia de Agrupación

### Buffer Temporal
- Acumula mensajes por persona (passport)
- Espera a tener los 5 tipos completos
- Timeout de 60s si datos incompletos

### Validación
- Verifica campos obligatorios
- Detecta datos faltantes
- Maneja casos edge (duplicados, datos parciales)

### Salida
- Documento agregado con estructura unificada
- Flag `is_complete` indica si tiene todos los datos
- Campo `missing_data` lista qué falta

## 🔧 Relacionado

- **Issue #6**: Implementar lógica de agrupación de datos (ESTE SERVICIO)
- **Issue #8**: SQL Persister leerá `aggregated_data` y lo moverá a SQL

## 🧪 Tests

```bash
# Ejecutar tests unitarios
python -m pytest tests/

# O con unittest
python -m unittest discover tests/
```