# backend/app/api/v1/sync_status.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, List, Any, Optional
from datetime import datetime
from ...sync.scheduler import SchedulerService, get_scheduler_instance
from ...sync.sync_manager import SynergoSyncManager
from ...core.database import get_async_session_context
from sqlalchemy import text
from pydantic import BaseModel, Field


# Modèles Pydantic pour les réponses API
class SyncTableStatus(BaseModel):
    table_name: str
    last_sync_id: int
    last_sync_timestamp: Optional[str] = None
    total_records: int
    last_sync_status: str
    records_processed_last_sync: int
    error_message: Optional[str] = None


class SyncStats24h(BaseModel):
    total_syncs: int
    total_records_processed: int
    avg_processing_time: float


class SchedulerStatus(BaseModel):
    is_running: bool
    is_syncing: bool
    sync_interval_minutes: int
    sync_count: int
    error_count: int
    uptime_seconds: float
    next_sync_time: Optional[str] = None  # CORRIGÉ - Le champ peut être None
    last_sync_summary: Dict[str, Any]


class SyncDashboard(BaseModel):
    scheduler: SchedulerStatus
    tables: List[SyncTableStatus]
    stats_24h: SyncStats24h
    generated_at: str


class ManualSyncResponse(BaseModel):
    status: str
    message: str
    results_summary: Dict[str, Any]
    execution_time_seconds: float


# Router pour les APIs de synchronisation
router = APIRouter(prefix="/sync", tags=["Synchronisation"])


@router.get("/status", response_model=SchedulerStatus)
async def get_sync_status():
    """
    Récupère le statut actuel du planificateur de synchronisation
    """
    try:
        status = SchedulerService.get_sync_status()

        # S'assurer que next_sync_time est soit une chaîne soit None
        if status.get('next_sync_time') and not isinstance(status['next_sync_time'], str):
            if hasattr(status['next_sync_time'], 'isoformat'):
                status['next_sync_time'] = status['next_sync_time'].isoformat()
            else:
                status['next_sync_time'] = str(status['next_sync_time'])

        return SchedulerStatus(**status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur récupération statut: {str(e)}")


@router.get("/dashboard", response_model=SyncDashboard)
async def get_sync_dashboard():
    """
    Tableau de bord complet de la synchronisation
    """
    try:
        # Récupérer le rapport détaillé
        report = await SchedulerService.get_sync_report()

        # Nettoyer les données pour Pydantic
        scheduler_data = report['scheduler'].copy()

        # Gérer next_sync_time
        if scheduler_data.get('next_sync_time'):
            if hasattr(scheduler_data['next_sync_time'], 'isoformat'):
                scheduler_data['next_sync_time'] = scheduler_data['next_sync_time'].isoformat()
            elif not isinstance(scheduler_data['next_sync_time'], str):
                scheduler_data['next_sync_time'] = str(scheduler_data['next_sync_time'])
        else:
            scheduler_data['next_sync_time'] = None

        # Nettoyer les données des tables
        tables_data = []
        for table in report['sync_states']:
            table_data = table.copy()

            # Gérer last_sync_timestamp
            if table_data.get('last_sync_timestamp'):
                if hasattr(table_data['last_sync_timestamp'], 'isoformat'):
                    table_data['last_sync_timestamp'] = table_data['last_sync_timestamp'].isoformat()
                elif not isinstance(table_data['last_sync_timestamp'], str):
                    table_data['last_sync_timestamp'] = str(table_data['last_sync_timestamp'])
            else:
                table_data['last_sync_timestamp'] = None

            # S'assurer que les valeurs numériques sont correctes
            table_data['last_sync_id'] = table_data.get('last_sync_id', 0) or 0
            table_data['total_records'] = table_data.get('total_records', 0) or 0
            table_data['records_processed_last_sync'] = table_data.get('records_processed_last_sync', 0) or 0

            tables_data.append(table_data)

        # Transformer en format API
        dashboard = SyncDashboard(
            scheduler=SchedulerStatus(**scheduler_data),
            tables=[SyncTableStatus(**table) for table in tables_data],
            stats_24h=SyncStats24h(**report['stats_24h']),
            generated_at=report['generated_at']
        )

        return dashboard

    except Exception as e:
        # Retourner un dashboard minimal en cas d'erreur
        fallback_dashboard = {
            'scheduler': {
                'is_running': False,
                'is_syncing': False,
                'sync_interval_minutes': 30,
                'sync_count': 0,
                'error_count': 0,
                'uptime_seconds': 0.0,
                'next_sync_time': None,
                'last_sync_summary': {'status': 'error', 'message': str(e)}
            },
            'tables': [],
            'stats_24h': {
                'total_syncs': 0,
                'total_records_processed': 0,
                'avg_processing_time': 0.0
            },
            'generated_at': datetime.now().isoformat()
        }

        try:
            return SyncDashboard(**fallback_dashboard)
        except Exception:
            raise HTTPException(status_code=500, detail=f"Erreur génération dashboard: {str(e)}")


@router.post("/manual", response_model=ManualSyncResponse)
async def trigger_manual_sync(background_tasks: BackgroundTasks):
    """
    Déclenche une synchronisation manuelle
    """
    start_time = datetime.now()

    try:
        # Déclencher la sync
        results = await SchedulerService.trigger_manual_sync()

        execution_time = (datetime.now() - start_time).total_seconds()

        # Analyser les résultats
        total_records = sum(r.records_processed for r in results)
        success_count = sum(1 for r in results if r.status == 'SUCCESS')
        error_count = sum(1 for r in results if r.status == 'ERROR')

        # Déterminer le statut global
        if error_count == 0:
            status = "success"
            message = f"Synchronisation réussie: {total_records} enregistrements"
        elif success_count > 0:
            status = "partial"
            message = f"Synchronisation partielle: {success_count}/{len(results)} tables OK"
        else:
            status = "error"
            message = "Échec de la synchronisation"

        results_summary = {
            'total_tables': len(results),
            'successful_tables': success_count,
            'error_tables': error_count,
            'total_records_processed': total_records,
            'table_details': [
                {
                    'table_name': r.table_name,
                    'status': r.status,
                    'records_processed': r.records_processed,
                    'error_message': r.error_message
                }
                for r in results
            ]
        }

        return ManualSyncResponse(
            status=status,
            message=message,
            results_summary=results_summary,
            execution_time_seconds=execution_time
        )

    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        raise HTTPException(
            status_code=500,
            detail={
                "error": f"Erreur synchronisation manuelle: {str(e)}",
                "execution_time_seconds": execution_time
            }
        )


@router.get("/health")
async def sync_health_check():
    """
    Vérification de santé du système de synchronisation
    """
    try:
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "status": "healthy",
            "checks": {}
        }

        # 1. Test connecteur HFSQL
        try:
            from ...utils.hfsql_connector import HFSQLConnector
            hfsql = HFSQLConnector()
            hfsql_test = await hfsql.test_connection_step_by_step()

            health_status["checks"]["hfsql"] = {
                "status": "ok" if hfsql_test.get("final_status") == "success" else "error",
                "details": hfsql_test
            }
            hfsql.close()
        except Exception as e:
            health_status["checks"]["hfsql"] = {
                "status": "error",
                "error": str(e)
            }

        # 2. Test base PostgreSQL
        try:
            async with get_async_session_context() as session:
                await session.execute(text("SELECT 1"))
                health_status["checks"]["postgresql"] = {"status": "ok"}
        except Exception as e:
            health_status["checks"]["postgresql"] = {
                "status": "error",
                "error": str(e)
            }

        # 3. État du scheduler
        try:
            scheduler = get_scheduler_instance()
            scheduler_status = scheduler.get_status()
            health_status["checks"]["scheduler"] = {
                "status": "ok" if scheduler_status["is_running"] else "stopped",
                "details": {
                    "is_running": scheduler_status["is_running"],
                    "sync_count": scheduler_status["sync_count"],
                    "error_count": scheduler_status["error_count"]
                }
            }
        except Exception as e:
            health_status["checks"]["scheduler"] = {
                "status": "error",
                "error": str(e)
            }

        # 4. Vérification des tables de sync
        try:
            sync_manager = SynergoSyncManager()
            dashboard_data = await sync_manager.get_sync_dashboard_data()

            error_tables = [t for t in dashboard_data['sync_states'] if t.get('last_sync_status') == 'ERROR']

            health_status["checks"]["sync_tables"] = {
                "status": "ok" if len(error_tables) == 0 else "warning",
                "total_tables": len(dashboard_data['sync_states']),
                "error_tables": len(error_tables),
                "error_table_names": [t['table_name'] for t in error_tables]
            }
        except Exception as e:
            health_status["checks"]["sync_tables"] = {
                "status": "error",
                "error": str(e)
            }

        # Déterminer le statut global
        all_checks = health_status["checks"].values()
        error_checks = [c for c in all_checks if c["status"] == "error"]
        warning_checks = [c for c in all_checks if c["status"] == "warning"]

        if error_checks:
            health_status["status"] = "unhealthy"
        elif warning_checks:
            health_status["status"] = "degraded"
        else:
            health_status["status"] = "healthy"

        return health_status

    except Exception as e:
        return {
            "timestamp": datetime.now().isoformat(),
            "status": "error",
            "error": str(e)
        }


# Ajouter le router à l'application principale
def include_sync_router(app):
    """Helper pour inclure le router dans l'app FastAPI"""
    app.include_router(router)