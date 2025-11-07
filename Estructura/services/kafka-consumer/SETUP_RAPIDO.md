# 🚀 Configuración Rápida del Kafka Consumer

## Para tu compañero:

### 1. Copiar la configuración
```bash
cd Estructura/services/kafka-consumer
cp .env.example .env
```

### 2. Verificar la configuración
Abre el archivo `.env` y verifica que tenga estos valores:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=probando
KAFKA_GROUP_ID=hr-insights-consumer
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4. Ejecutar el consumer
```bash
python src/consumer.py
```

O usar el script:
```bash
bash run_consumer.sh
```

## ✅ Debería ver:
```
🚀 Consumer iniciado. Escuchando topic: probando
📡 Broker: localhost:29092
👥 Group ID: hr-insights-consumer
------------------------------------------------------------
📋 PERSONAL DATA: Juan Pérez | Passport: 12345678
📍 LOCATION: María García | City: Madrid
💼 PROFESSIONAL: Pedro López | Company: Tech SA
...
```

## 🆘 Problemas comunes:

### Error: "No module named 'confluent_kafka'"
```bash
pip install -r requirements.txt
```

### Error: "Connection refused"
Verifica que Kafka esté corriendo:
```bash
cd ../../Kafka/data-engineering-educational-project
docker compose ps
```

Si no está corriendo:
```bash
docker compose up -d
```

### Cambiar el puerto o topic
Edita el archivo `.env` con tus valores.

## 📝 Notas importantes:
- NO subas el archivo `.env` a Git (contiene configuración local)
- El `.env.example` SÍ se sube como referencia
- Cada compañero puede tener su propio `.env` con configuración diferente
