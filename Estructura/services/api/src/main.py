"""
FastAPI Main Application - HR Pro API
Orquestador central para todos los microservicios
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from .config import get_settings
from .routers import health_router, kafka_router
from .services.kafka_service import KafkaService, get_kafka_service, set_kafka_service

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()

# Instancia global de KafkaService
kafka_service: KafkaService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestión del ciclo de vida de la aplicación
    Se ejecuta al iniciar y cerrar la aplicación
    """
    # Startup
    logger.info("🚀 Iniciando HR Pro API...")
    logger.info(f"📋 Ambiente: {settings.environment}")
    logger.info(f"🔗 Kafka: {settings.kafka_bootstrap_servers}")
    
    # Inicializar servicios
    global kafka_service
    kafka_service = KafkaService()
    await kafka_service.initialize()
    set_kafka_service(kafka_service)
    logger.info("✅ Kafka Service inicializado")
    
    # Aquí puedes inicializar otros servicios
    # await mongodb_service.initialize()
    # await redis_service.initialize()
    
    logger.info("✅ API completamente inicializada")
    
    yield  # La aplicación está corriendo
    
    # Shutdown
    logger.info("🛑 Cerrando HR Pro API...")
    await kafka_service.close()
    logger.info("✅ API cerrada correctamente")


# Crear aplicación FastAPI
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="API central para gestión de datos HR con Kafka y microservicios",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Manejador global de excepciones
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Error global: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Error interno del servidor",
            "error": str(exc) if settings.debug else "Internal Server Error"
        }
    )


# Incluir routers
app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(kafka_router, prefix="/kafka", tags=["Kafka"])


@app.get("/")
async def root():
    """Endpoint raíz con información de la API"""
    return {
        "message": f"Bienvenido a {settings.app_name}",
        "version": settings.app_version,
        "environment": settings.environment,
        "docs": "/docs",
        "health": "/health",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info"
    )
