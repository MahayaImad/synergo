# backend/main.py - VERSION CORRIGÉE
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime

# Imports Synergo
from app.api.v1.sync_status import router as sync_router
from app.sync.scheduler import SchedulerService, get_scheduler_instance
from app.core.config import settings
from loguru import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestionnaire de cycle de vie de l'application
    """
    # Startup
    logger.info("🚀 Démarrage Synergo API")

    try:
        # Optionnel: Démarrer le scheduler automatiquement
        # scheduler = get_scheduler_instance()
        # asyncio.create_task(SchedulerService.start_sync_service())
        # logger.info("📅 Scheduler de sync démarré automatiquement")

        yield

    finally:
        # Shutdown
        logger.info("🛑 Arrêt Synergo API")
        try:
            scheduler = get_scheduler_instance()
            if scheduler.is_running:
                await SchedulerService.stop_sync_service()
                logger.info("📅 Scheduler arrêté proprement")
        except Exception as e:
            logger.error(f"Erreur arrêt scheduler: {e}")


# Application FastAPI
app = FastAPI(
    title="Synergo Pharmacy Management",
    description="Système de gestion pharmacie avec intelligence artificielle",
    version="1.0.0",
    lifespan=lifespan
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],  # Frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclusion des routers
app.include_router(sync_router, prefix="/api/v1")


# Routes principales
@app.get("/")
async def root():
    """Page d'accueil de l'API"""
    return {
        "message": "🏥 Synergo Pharmacy Management API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "sync_dashboard": "/api/v1/sync/dashboard",
            "sync_manual": "/api/v1/sync/manual",
            "health": "/health",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    """Vérification de santé de l'API - VERSION CORRIGÉE"""
    try:
        # Test rapide base de données - CORRECTION ICI
        from app.core.database import get_async_session_context
        from sqlalchemy import text

        # Utiliser le context manager au lieu du générateur
        async with get_async_session_context() as session:
            await session.execute(text("SELECT 1"))

        # État du scheduler
        scheduler = get_scheduler_instance()
        scheduler_status = "running" if scheduler.is_running else "stopped"

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected",
            "scheduler": scheduler_status,
            "uptime": "active"
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy: {str(e)}"
        )


@app.get("/api/v1/info")
async def get_system_info():
    """Informations système"""
    scheduler = get_scheduler_instance()

    return {
        "system": "Synergo Pharmacy Management",
        "version": "1.0.0",
        "environment": "development",
        "sync": {
            "scheduler_running": scheduler.is_running,
            "sync_count": scheduler.sync_count,
            "error_count": scheduler.error_count,
            "interval_minutes": scheduler.sync_interval_minutes
        },
        "database": {
            "hfsql_server": settings.HFSQL_SERVER,
            "hfsql_database": settings.HFSQL_DATABASE,
            "postgresql_connected": True
        },
        "generated_at": datetime.now().isoformat()
    }


# Test de base de données simple pour debug
@app.get("/api/v1/db-test")
async def test_database():
    """Test simple de la base de données"""
    try:
        from app.core.database import get_async_session_context
        from sqlalchemy import text

        async with get_async_session_context() as session:
            result = await session.execute(text("SELECT version()"))
            version = result.fetchone()[0]

        return {
            "status": "success",
            "message": "Base de données connectée",
            "postgres_version": version.split(',')[0],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Test DB failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Test de connexion HFSQL
@app.get("/api/v1/hfsql-test")
async def test_hfsql():
    """Test simple de la connexion HFSQL"""
    try:
        from app.utils.hfsql_connector import HFSQLConnector

        connector = HFSQLConnector()
        test_result = await connector.test_connection_step_by_step()
        connector.close()

        return {
            "status": "success" if test_result.get("final_status") == "success" else "error",
            "details": test_result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Test HFSQL failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }


# Gestion d'erreurs globale
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Gestionnaire d'erreurs global"""
    logger.error(f"Erreur non gérée: {exc}")

    return JSONResponse(
        status_code=500,
        content={
            "error": "Erreur interne du serveur",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )


# Point d'entrée pour le développement
if __name__ == "__main__":
    import uvicorn

    logger.add("logs/synergo_api.log", rotation="10 MB")

    uvicorn.run(
        "main:app",
        host="localhost",
        port=8000,
        reload=True,
        log_level="info"
    )