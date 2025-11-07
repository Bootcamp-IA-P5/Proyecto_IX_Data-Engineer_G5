# 🎯 RESUMEN VISUAL - MongoDB Persister

## Tu Misión
Guardar TODOS los mensajes de Kafka en MongoDB tal cual (sin procesar).

## 📊 Diagrama de Flujo

```
┌─────────────┐
│   KAFKA     │
│ (probando)  │
└──────┬──────┘
       │ Mensajes en tiempo real
       ▼
┌─────────────────┐
│ MONGO PERSISTER │ ← TU CÓDIGO
│  (Python)       │
└──────┬──────────┘
       │ Guardar raw data
       ▼
┌─────────────┐
│  MongoDB    │
│raw_messages │
└─────────────┘
```

## ✅ Lo Que DEBES Hacer

### 1️⃣ CONFIGURAR MONGODB (15 min)
```bash
# Agregar al docker-compose.yml
docker-compose up -d mongodb mongo-express
```

### 2️⃣ CREAR EL CÓDIGO (30 min)
- `mongo_client.py` → Conecta a MongoDB
- `persister.py` → Consume Kafka y guarda

### 3️⃣ PROBAR (10 min)
```bash
python src/persister.py
# Ver datos en: http://localhost:8081
```

## 🎬 Ejemplo de Mensaje a Guardar

**Lo que recibes de Kafka:**
```json
{
  "name": "Juan",
  "last_name": "Pérez",
  "passport": "X12345678"
}
```

**Lo que guardas en MongoDB:**
```json
{
  "data": {
    "name": "Juan",
    "last_name": "Pérez", 
    "passport": "X12345678"
  },
  "timestamp": "2025-11-06T13:00:00Z",
  "source": "kafka",
  "topic": "probando",
  "processed": false
}
```

## 📝 Checklist Rápido

- [ ] MongoDB corriendo → `docker ps`
- [ ] Código creado → `mongo_client.py` + `persister.py`
- [ ] Dependencias instaladas → `pip install -r requirements.txt`
- [ ] Persister corriendo → `python src/persister.py`
- [ ] Datos guardándose → Ver en Mongo Express
- [ ] Estadísticas OK → Debe mostrar mensajes guardados

## ⚠️ IMPORTANTE: NO Procesarás los Datos

❌ NO agrupar por persona  
❌ NO limpiar datos  
❌ NO transformar  
✅ SÍ guardar TAL CUAL vienen  

El procesamiento lo hace OTRO servicio después.

## 🆘 Si Tienes Problemas

1. **MongoDB no conecta**
   ```bash
   docker logs hrpro-mongodb
   ```

2. **Kafka no conecta**
   - Verifica que el consumer de Kafka esté apagado
   - Usa un `group.id` diferente

3. **No guarda datos**
   - Revisa los logs del persister
   - Verifica el `.env`

## 📞 Coordinación con el Equipo

**Dependes de:**
- ✅ Kafka (ya está corriendo)
- ✅ Topic `probando` (ya existe)

**Entregas a:**
- MongoDB con datos crudos → Para el equipo de `data-processor`

## 🎯 Resultado Final Esperado

```bash
# Al ejecutar:
python src/persister.py

# Deberías ver:
✅ Conectado a MongoDB: localhost:27017
✅ Suscrito a Kafka topic: probando
📊 ESTADÍSTICAS
  📨 Mensajes procesados: 1000
  💾 Mensajes guardados: 1000
  🗄️ Total en MongoDB: 1000
  ❌ Errores: 0
```

## ⏱️ Tiempo Estimado

- Setup Docker: 15 min
- Código: 30 min
- Testing: 15 min
- **TOTAL: ~1 hora**

## 📚 Archivos que Tu Compañero Necesita

1. `TAREAS_MONGODB.md` → Instrucciones detalladas paso a paso
2. `README.md` → Resumen técnico
3. `RESUMEN_VISUAL.md` → Este archivo (vista rápida)

---

**¡Éxito! 🚀**
