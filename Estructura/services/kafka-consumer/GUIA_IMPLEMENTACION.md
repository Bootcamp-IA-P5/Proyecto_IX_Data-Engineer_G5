# 🚀 GUÍA PASO A PASO - IMPLEMENTACIÓN KAFKA CONSUMER

## ✅ ARCHIVOS CREADOS

```
kafka-consumer/
├── src/
│   ├── __init__.py          ✅ (ya existía)
│   ├── consumer.py          ✅ COMPLETADO
│   ├── models.py            ✅ COMPLETADO
│   └── test_consumer.py     ✅ NUEVO (para testing)
├── requirements.txt         ✅ NUEVO
├── .env.example            ✅ NUEVO
└── README.md               ✅ NUEVO
```

---

## 📋 PASOS PARA EJECUTAR

### PASO 1: Instalar dependencias

```bash
cd "/Users/umitgungor/Desktop/Factoria F5 IA-2025/Proyectos/Proyecto_IX_Data-Engineer_G5/Proyecto_IX_Data-Engineer_G5/Estructura/services/kafka-consumer"

pip install -r requirements.txt
```

### PASO 2: Probar el código SIN Kafka (testing)

```bash
cd src
python test_consumer.py
```

Esto ejecutará tests para verificar que:
- ✅ La identificación de tipos de mensajes funciona
- ✅ El parseo de datos a objetos funciona
- ✅ El parseo de JSON funciona

### PASO 3: Iniciar Docker Desktop

1. Abre Docker Desktop en tu Mac
2. Espera a que Docker esté corriendo (icono verde)

### PASO 4: Levantar el servidor Kafka

```bash
cd "/Users/umitgungor/Desktop/Factoria F5 IA-2025/Proyectos/Kafka/data-engineering-educational-project/datagen"

docker compose up --build
```

Espera a ver mensajes como:
```
✅ Kafka is ready
✅ Data generator started
```

### PASO 5: Ejecutar el consumer (en otra terminal)

Abre una NUEVA terminal y ejecuta:

```bash
cd "/Users/umitgungor/Desktop/Factoria F5 IA-2025/Proyectos/Proyecto_IX_Data-Engineer_G5/Proyecto_IX_Data-Engineer_G5/Estructura/services/kafka-consumer/src"

python consumer.py
```

### PASO 6: Verificar que funciona

Deberías ver mensajes como:
```
🚀 Consumer iniciado. Escuchando topic: user-tracker
📋 PERSONAL DATA: Juan García | Passport: ABC123456
📍 LOCATION: María López | City: Madrid
💼 PROFESSIONAL: Pedro Sánchez | Company: TechCorp | Job: Engineer
💰 BANK DATA: Passport: XYZ789 | IBAN: ES1234567...
🌐 NET DATA: 192.168.1.1 | Address: example.com...

📊 ESTADÍSTICAS DE CONSUMO
Total mensajes: 100
  📋 Personal Data: 20
  📍 Location: 20
  💼 Professional: 20
  💰 Bank Data: 20
  🌐 Net Data: 20
```

### PASO 7: Detener el consumer

Presiona `Ctrl+C` para detener el consumer de forma segura.

Verás:
```
⚠️  Interrupción recibida. Cerrando consumer...
📊 ESTADÍSTICAS FINALES
✅ Consumer cerrado correctamente
```

---

## 🎯 CRITERIOS DE ACEPTACIÓN - STATUS

| Criterio | Status |
|----------|--------|
| ✅ Instalar librería kafka-python o confluent-kafka | ✅ COMPLETADO (confluent-kafka) |
| ✅ Configurar conexión al broker de Kafka | ✅ COMPLETADO (localhost:9092) |
| ✅ Implementar consumer básico que lea mensajes | ✅ COMPLETADO |
| ✅ Parsear JSON de los mensajes | ✅ COMPLETADO |
| ✅ Imprimir mensajes en consola para verificar | ✅ COMPLETADO |
| **EXTRA:** Identificar tipos de mensajes | ✅ BONUS |
| **EXTRA:** Logging profesional | ✅ BONUS |
| **EXTRA:** Estadísticas de consumo | ✅ BONUS |
| **EXTRA:** Manejo de errores robusto | ✅ BONUS |

---

## 🔧 CONFIGURACIÓN IMPLEMENTADA

### Consumer Config
```python
{
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'hr-pro-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'max.poll.interval.ms': 300000
}
```

### Topic
- **Nombre:** `user-tracker`

### Tipos de mensajes identificados
1. 📋 **Personal Data**: Name, Lastname, Sex, Telfnumber, Passport, E-Mail
2. 📍 **Location**: Fullname, City, Address
3. 💼 **Professional Data**: Fullname, Company, Job, etc.
4. 💰 **Bank Data**: Passport, IBAN, Salary
5. 🌐 **Net Data**: IPv4, Address

---

## 🐛 TROUBLESHOOTING

### Error: "No module named 'confluent_kafka'"
```bash
pip install confluent-kafka
```

### Error: "Cannot connect to Docker daemon"
1. Abre Docker Desktop
2. Espera a que esté completamente iniciado
3. Verifica con: `docker ps`

### Error: "Failed to connect to Kafka"
1. Verifica que Kafka esté corriendo: `docker compose ps`
2. Verifica que el puerto 9092 esté libre: `lsof -i :9092`
3. Revisa los logs de Kafka: `docker compose logs kafka`

### No se reciben mensajes
1. Verifica que el generador de datos esté corriendo
2. Comprueba el topic existe: 
   ```bash
   docker exec -it <kafka-container> kafka-topics --list --bootstrap-server localhost:9092
   ```

---

## 📚 SIGUIENTES PASOS DEL PROYECTO

### Nivel Esencial (continuación):
1. ✅ ~~Configurar consumer de Kafka~~ COMPLETADO
2. ⏭️ Persistir mensajes RAW en MongoDB (Data Lake)
3. ⏭️ Procesar y agrupar datos por persona
4. ⏭️ Persistir datos agrupados en SQL (Data Warehouse)

### Recomendaciones:
- **MongoDB**: Guardar TODOS los mensajes tal cual llegan (sin procesar)
- **SQL**: Guardar datos AGRUPADOS y PROCESADOS por persona
- **Identificador común**: Usar campos como `Passport`, `Fullname` para unir datos

---

## 💡 TIPS

1. **Testing primero**: Siempre prueba con `test_consumer.py` antes de conectar a Kafka
2. **Logs**: Los logs te ayudarán a debuggear problemas
3. **Estadísticas**: Monitorea las estadísticas cada 100 mensajes
4. **Commits**: Los offsets se guardan manualmente para control preciso
5. **Ctrl+C**: Siempre usa Ctrl+C para cerrar limpiamente el consumer

---

## 👥 DATOS DEL IMPLEMENTADOR

**Tarea:** Issue #2.2 - Implementar Kafka Consumer Básico
**Prioridad:** CRÍTICA
**Estado:** ✅ COMPLETADO
**Fecha:** 6 de noviembre de 2025

---

## ✅ CHECKLIST FINAL

- [ ] Instalar dependencias
- [ ] Ejecutar test_consumer.py y verificar que pasa
- [ ] Iniciar Docker Desktop
- [ ] Levantar servidor Kafka (docker compose up)
- [ ] Ejecutar consumer.py
- [ ] Verificar que se reciben mensajes
- [ ] Probar Ctrl+C para cerrar limpiamente
- [ ] Documentar cualquier problema encontrado
- [ ] Hacer commit de los cambios
- [ ] Crear Pull Request para revisión

---

¡Éxito con la implementación! 🚀
