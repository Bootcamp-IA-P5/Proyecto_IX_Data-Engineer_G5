# Kafka Consumer - HR Pro Project

## 📋 Descripción
Consumer de Kafka que lee mensajes en tiempo real del topic `user-tracker` y los procesa identificando diferentes tipos de datos:
- 📋 Personal Data
- 📍 Location
- 💼 Professional Data
- 💰 Bank Data
- 🌐 Net Data

## 🚀 Instalación

### 1. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar variables de entorno (opcional)
```bash
cp .env.example .env
# Editar .env si necesitas cambiar la configuración
```

## ▶️ Ejecución

### Ejecutar el consumer
```bash
cd src
python consumer.py
```

### Detener el consumer
Presiona `Ctrl+C` para detener el consumer de forma segura.

## 📊 Funcionalidades

### ✅ Características implementadas
- ✅ Conexión al broker de Kafka (localhost:9092)
- ✅ Lectura de mensajes en tiempo real
- ✅ Parseo de JSON
- ✅ Identificación automática del tipo de mensaje
- ✅ Logging con formato claro y emojis
- ✅ Estadísticas de consumo cada 100 mensajes
- ✅ Manejo de errores robusto
- ✅ Commit manual de offsets
- ✅ Cierre seguro del consumer

### 📈 Estadísticas
El consumer muestra estadísticas cada 100 mensajes procesados:
- Total de mensajes procesados
- Cantidad por tipo de mensaje
- Mensajes desconocidos
- Errores encontrados

## 🔧 Configuración

### Parámetros del consumer
- **bootstrap.servers**: `localhost:9092` - Dirección del broker Kafka
- **group.id**: `hr-pro-consumer-group` - ID del grupo de consumidores
- **auto.offset.reset**: `earliest` - Leer desde el principio
- **enable.auto.commit**: `False` - Control manual de commits
- **topic**: `user-tracker` - Topic a consumir

## 📝 Estructura del código

```
kafka-consumer/
├── src/
│   ├── __init__.py
│   ├── consumer.py      # Lógica principal del consumer
│   └── models.py        # Modelos de datos y parseo
├── requirements.txt     # Dependencias Python
├── .env.example        # Ejemplo de configuración
└── README.md           # Este archivo
```

## 🐛 Troubleshooting

### Error: "Failed to connect to Kafka"
- Verifica que el servidor Kafka esté ejecutándose
- Comprueba que el puerto 9092 esté disponible
- Revisa la configuración en `bootstrap.servers`

### Error: "No module named 'confluent_kafka'"
```bash
pip install confluent-kafka
```

### Los mensajes no se procesan
- Verifica que el topic `user-tracker` existe
- Comprueba que hay mensajes siendo enviados al topic
- Revisa los logs para identificar errores

## 🎯 Criterios de aceptación cumplidos
- ✅ El consumer se conecta exitosamente a Kafka
- ✅ Los mensajes se leen en tiempo real
- ✅ JSON se parsea correctamente
- ✅ Los mensajes se imprimen en consola para verificar
- ✅ Se identifica el tipo de cada mensaje
- ✅ Se manejan errores adecuadamente

## 📚 Próximos pasos
1. Persistir mensajes en MongoDB (Data Lake)
2. Agrupar datos por persona usando identificadores comunes
3. Procesar y transformar datos
4. Persistir datos agregados en base de datos SQL

## 👥 Autor
Equipo Data Engineer - HR Pro Project
