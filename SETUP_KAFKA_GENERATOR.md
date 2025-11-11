# 🚀 Setup del Generador de Datos y Kafka

## 📌 Requisitos Previos
- Docker y Docker Compose instalados
- Git

---

## 📥 PASO 1: Clonar el Proyecto Educativo

El generador de datos viene de un repositorio educativo separado:

```bash
# Ir a la carpeta de proyectos
cd ~/Desktop

# Crear carpeta para Kafka (si no existe)
mkdir -p Kafka
cd Kafka

# Clonar el repositorio educativo
git clone https://github.com/Factoria-F5-dev/data-engineering-educational-project.git

cd data-engineering-educational-project
```

---

## 🐳 PASO 2: Levantar Kafka + Generador

```bash
# Dentro de data-engineering-educational-project/
docker compose up -d

# Verificar que esté corriendo
docker compose ps
```

Deberías ver algo como:
```
NAME               STATUS
datagen-kafka-1    Up
datagen-zookeeper-1 Up
random_generator   Up
```

---

## ✅ PASO 3: Verificar que Funciona

```bash
# Ver logs del generador
docker compose logs -f random_generator

# Probar consumir mensajes
docker exec -it datagen-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic probando \
  --max-messages 5
```

---

## 🔌 PASO 4: Conectar Nuestro Consumer

Ahora puedes ejecutar nuestro consumer:

```bash
# Volver a nuestro proyecto
cd ~/Desktop/Proyecto_IX_Data-Engineer_G5

# Ir al consumer
cd Estructura/services/kafka-consumer

# Crear .env si no existe
cp .env.example .env

# Ejecutar consumer
python src/consumer.py
```

---

## 📊 Puertos Importantes

- **Kafka (interno)**: `localhost:9092`
- **Kafka (externo)**: `localhost:29092` ← **USAR ESTE**
- **Zookeeper**: `localhost:2181`
- **Topic**: `probando`

---

## 🆘 Problemas Comunes

### Error: "Connection refused"
```bash
# Verificar que Kafka esté corriendo
docker compose ps

# Si no está, levantarlo
docker compose up -d
```

### Error: "No module named 'confluent_kafka'"
```bash
# Instalar dependencias
pip install -r requirements.txt
```

### Kafka lento o no responde
```bash
# Reiniciar servicios
docker compose restart

# O parar y volver a levantar
docker compose down
docker compose up -d
```

---

## 🛑 Detener Todo

```bash
# Detener Kafka y generador
cd ~/Desktop/Kafka/data-engineering-educational-project
docker compose down

# Si quieres borrar datos también
docker compose down -v
```

---

## 📝 Estructura Final

```
~/Desktop/
├── Kafka/
│   └── data-engineering-educational-project/  ← Generador + Kafka
│       ├── datagen/
│       ├── docker-compose.yml
│       └── README.md
│
└── Proyecto_IX_Data-Engineer_G5/              ← Nuestro proyecto
    └── Estructura/
        └── services/
            ├── kafka-consumer/                 ← Nuestro consumer
            ├── mongo-persister/
            └── ...
```

---

## 🎯 Resumen para tu Compañero

1. Clonar repo educativo en `~/Desktop/Kafka/`
2. Ejecutar `docker compose up -d`
3. Verificar con `docker compose ps`
4. Ejecutar nuestro consumer con `python src/consumer.py`

---

**Fecha de actualización**: 11 noviembre 2025
**Autor**: Equipo Proyecto IX
