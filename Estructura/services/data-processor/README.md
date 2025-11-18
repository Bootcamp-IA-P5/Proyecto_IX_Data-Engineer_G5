# Data Processor – Agregación inteligente sobre MongoDB

## 🎯 Objetivo

Leer documentos **crudos** desde `raw_messages`, **clasificarlos**, **normalizarlos** y **agregarlos por persona** en `aggregated_data` usando una clave de agrupación estable y un `update pipeline` atómico y eficiente.

---

## 🏗️ Arquitectura

```
Kafka → (mongo-persister) → MongoDB.raw_messages
                              │
                              ▼
                      (data-processor)
                              │
                              ▼
                      MongoDB.aggregated_data
```

* **Entrada:** `raw_messages` (documentos tal cual llegan).
* **Salida:** `aggregated_data` (1 documento por persona, consolidado).
* **Clave de agrupación:** prioriza identificadores fuertes (`passport` > `email` > `phone` > `fullname+dob` > `rawid`).

---

## 🚀 Quick Start

### 1) Variables de entorno

Copia `.env.example` a `.env` en la **raíz del repo** y revisa:

```env
# Mongo
MONGO_URI=mongodb://admin:admin123@mongo:27017/
MONGO_DATABASE=hrpro_db
MONGO_COLLECTION=raw_messages
AGGREGATED_COLLECTION=aggregated_data

# Servicio
BATCH_SIZE=1000
POLL_SECONDS=1.0
LOG_LEVEL=INFO
```

> Si no defines `MONGO_URI`, el cliente usa `MONGO_HOST`/`MONGO_PORT`.

### 2) Levantar servicios

```bash
docker compose up -d mongodb mongo-express
docker compose -f docker-compose-services.yml up -d data-processor
```

### 3) Logs

```bash
docker logs -f data-processor
```

Verás líneas como:

```
📥 Obtenidos 1000 documentos sin procesar
📊 Lote: leídos=1000, upserts=997, skipped_unknown=0, skipped_empty=3, errores=0
✅ Procesados 997 mensajes: Personal=212, Location=387, Professional=397, Bank=0, Net=1, Unknown=0, Errores=0
```

---

## 📁 Estructura del proyecto (servicio)

```
data-processor/
├── src/
│   ├── __init__.py
│   ├── config.py              # Config a partir de .env (raíz)
│   ├── mongo_client.py        # Cliente Mongo + índices idempotentes
│   └── processor.py           # Bucle principal + agregación
├── Dockerfile
└── README.md                  # (este archivo)
```

---

## 🧩 Lógica de agregación

### 1) Clasificación de tipo (`detect_message_type`)

Heurística por **puntuación**:

* +1 por cada **campo característico** presente.
* +1 por **palabras clave** en strings (ej. “iban”, “swift”, “ip”, “address”, “company”…).
* Empates resueltos con prioridad fija: **bank > professional > personal > location > net**.
* Si no hay señales → `unknown` (se **omite** en agregación para no “ensuciar”).

Tipos soportados:

* `personal`, `location`, `professional`, `bank`, `net`.

### 2) Identificadores (`extract_identifiers`)

Normalización suave (trim, minúsculas) sobre: `passport`, `email`, `phone`, `tax_id`, `ssn`.

### 3) Clave de agrupación (`build_grouping_key`)

Orden de fuerza:

1. `passport::<valor>`
2. `email::<valor>`
3. `phone::<valor>`
4. `fullnamedob::<fullname>::<dob>`
5. `rawid::<_id_raw>`

### 4) Bloque de datos (`build_data_block`)

* Si hay tipo conocido: **selecciona únicamente campos relevantes** para ese tipo.
* Si no, guarda un bloque **ligero** (`raw`) con pares clave/valor simples (sin metadatos internos `_...`).

### 5) Upsert atómico (`upsert_message`)

Uso de **update pipeline** con 3 stages:

1. **Set vivo**:

   * `updated_at`
   * `types_received` ← `setUnion( coalesce(types_received, []), [msg_type] )`
   * `data.<tipo>` ← bloque de datos del mensaje
   * `identifiers.*` (solo los presentes)
2. **Defaults**:

   * `_grouping_key` si no existe
   * `created_at` si no existe
3. **is_complete**:

   * `setIsSubset(["personal","location","professional","bank","net"], coalesce(types_received, []))`

> **Robustez:** Siempre **coalesce a array** antes de `$setIsSubset` para evitar el error *“both operands of $setIsSubset must be arrays“*.

---

## ✅ Campos y semántica de salida

Ejemplo de documento agregado:

```json
{
  "_grouping_key": "passport::375101412",
  "identifiers": { "passport": "375101412", "email": "dbrambilla@tele2.it" },
  "data": {
    "personal": { "name": "sr(a).", "last_name": "felipe", "email": "dbrambilla@tele2.it" },
    "location": { "address": "...", "city": "..." },
    "professional": { "company": "Tele2", "salary": "152947db", "_inferred": true },
    "bank": { "iban": "ES4579..." },
    "net": { "domain": "tele2.it" }
  },
  "types_received": ["bank","location","net","personal","professional"],
  "is_complete": true,
  "created_at": "...",
  "updated_at": "..."
}
```

* `types_received` → **set** de tipos observados (sin duplicados).
* `is_complete` → `true` cuando hay **los 5** tipos.
* `identifiers` → últimos identificadores conocidos para la persona.

---

## ⚙️ Índices (creados idempotentemente)

* `raw_messages`: `{ processed: 1 }`
* `aggregated_data`:

  * Único: `{ _grouping_key: 1 }`
  * Búsqueda: `types_received`, `grouping_status`, `is_complete`
  * Identificadores: `identifiers.passport|email|phone|tax_id|ssn` (sparse)
  * Campos útiles: `data.location.address`, `data.personal.email`, etc.

> Si detecta un índice con **mismas claves pero distinta configuración** (unique/sparse), lo **recrea** automáticamente.

---

## 🧾 Logging: cómo leer las métricas del lote

* **leídos**: nº de documentos traídos del raw.
* **upserts**: nº de documentos que **sí** se han agregado (se omiten `unknown` o vacíos).
* **skipped_unknown**: mensajes sin señales de tipo (no se agregan).
* **skipped_empty**: mensajes “vacíos” tras normalizar (p.ej. solo metadatos).
* **errores**: excepciones capturadas durante el upsert.

**Ejemplo:**

```
📊 Lote: leídos=1000, upserts=996, skipped_unknown=0, skipped_empty=4, errores=0
✅ Procesados 996 mensajes: Personal=212, Location=387, Professional=397, Bank=0, Net=0, Unknown=0, Errores=0
```

---

## 🔍 Consultas útiles (mongosh)

### Distribución por nº de tipos

```js
db.aggregated_data.aggregate([
  {$project:{k:{$size:{$ifNull:["$types_received",[]]}}}},
  {$group:{_id:"$k", n:{$sum:1}}},
  {$sort:{_id:1}}
]).toArray()
```

### Documentos completos (5/5)

```js
db.aggregated_data.countDocuments({
  types_received: {$all:["personal","location","professional","bank","net"]}
})
```

### Presencia por tipo

```js
db.aggregated_data.aggregate([
  {$unwind:"$types_received"},
  {$group:{_id:"$types_received", n:{$sum:1}}},
  {$sort:{n:-1}}
]).toArray()
```

### Muestras de `net` con contenido

```js
db.aggregated_data.find(
  {types_received:"net", "data.net":{$type:"object"},
   $expr: {$gt:[ {$size:{$objectToArray:"$data.net"}}, 0 ]}},
  {_id:0,_grouping_key:1,types_received:1,"data.net":1}
).limit(3).pretty()
```

---

## 🧰 Utilidades y mantenimiento

### Menú unificado

Desde `Estructura/shared/utils/menu_monitoring_data-processor.py`:

* **🧰 Herramientas Mongo (monitor/reconcile/limpiezas)**
  → abre el submenú `mongo_aggregated_tools.py` con:

  * **monitor**: métricas en caliente (`raw` vs `agg`, tipos, 5/5).
  * **reconcile**: tarea **offline** para **refinar** datos (ligera; idempotente).
  * **clear-agg**: limpiar `aggregated_data`.
  * **raw-processed set/unset**: marcar/unmarcar `processed` en `raw_messages`.

> Recomendación: ejecutar **reconcile** de forma programada (semanal) como tarea `cron`/pipeline en entornos productivos.

---

## 🧱 Eficiencia y robustez técnica

* **Update pipeline** atómico → una sola pasada por documento.
* **`$setUnion` + coalesce** → `types_received` como **set** sin duplicados.
* **`$isArray` / coalesce** → prevención de errores con operadores de arrays.
* **`$setIsSubset`** para `is_complete` (tras normalizar siempre a array).
* **Índices** solo se crean o **recrean** si las opciones no coinciden.
* **Omisión de `unknown`** → evita ruido y mantiene `agg` sólido.

---

## 🛠️ Troubleshooting

**Error `$setIsSubset` ambos operandos deben ser arrays**
→ asegurado en código con coalesce (`[]`) y chequeo `$isArray`.

**No aparecen tipos `net` o `bank`**
→ revisa que los **campos característicos** existan (p. ej. `iban`, `swift`, `ip`, `domain`, `url`, `ssid`…), o que la heurística de palabras clave aplique a tus datos.

**Muchos `rawid::...`**
→ indica **falta de identificadores fuertes**; revisa si puedes enriquecer upstream.

---

## 🧭 Workflow recomendado

1. **Arrancar Mongo y Data Processor**.
2. **Ver logs** y validar que suben `types_received`.
3. **Monitorizar** desde el **menú de utilidades**.
4. **Ejecutar `reconcile`** semanalmente (o tras grandes cargas).
5. **Dashboards** (futuro): usar los scripts de métricas como base.

---

## 📎 Notas finales

* El servicio **no reescribe** datos previos salvo para **añadir tipos** o **actualizar** campos del tipo en curso.
* El objetivo es **convergencia** a 5/5 de forma **natural**, sin “rellenos aleatorios”.
* La heurística `professional` ayuda a cerrar 5/5 cuando hay evidencias ligeras (palabras clave/campos).

---

**¿Listo para agregar con cariño y sin dolores?** 🚀
