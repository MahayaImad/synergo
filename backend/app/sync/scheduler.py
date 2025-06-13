# backend/app/sync/scheduler.py
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger
from .sync_manager import SynergoSyncManager, SyncResult


class SynergoSyncScheduler:
    """
    Planificateur de synchronisation Synergo

    Responsabilités:
    - Exécute la sync toutes les 30 minutes
    - Gère les erreurs et reprises automatiques
    - Monitoring et logs détaillés
    - Interface de contrôle (start/stop/status)
    """

    def __init__(self, sync_interval_minutes: int = 30):
        self.sync_manager = SynergoSyncManager()
        self.sync_interval_minutes = sync_interval_minutes
        self.is_running = False
        self.is_syncing = False
        self.last_sync_results: List[SyncResult] = []
        self.next_sync_time: Optional[datetime] = None
        self.sync_count = 0
        self.error_count = 0
        self.start_time: Optional[datetime] = None

    async def start_scheduler(self):
        """
        Démarre le planificateur de synchronisation
        """
        if self.is_running:
            logger.warning("⚠️ Planificateur déjà en cours d'exécution")
            return

        self.is_running = True
        self.start_time = datetime.now()

        logger.info(f"🚀 Démarrage Synergo Sync Scheduler")
        logger.info(f"   📅 Intervalle: {self.sync_interval_minutes} minutes")
        logger.info(f"   🕐 Première sync: immédiate")

        try:
            while self.is_running:
                # Calculer la prochaine synchronisation
                self.next_sync_time = datetime.now() + timedelta(minutes=self.sync_interval_minutes)

                # Exécuter la synchronisation
                await self._execute_sync_cycle()

                # Attendre le prochain cycle (seulement si on est toujours en marche)
                if self.is_running:
                    logger.info(f"⏰ Prochaine sync prévue: {self.next_sync_time.strftime('%H:%M:%S')}")
                    await self._wait_for_next_sync()

        except asyncio.CancelledError:
            logger.info("🛑 Planificateur arrêté par signal")
        except Exception as e:
            logger.error(f"❌ Erreur critique planificateur: {e}")
        finally:
            self.is_running = False
            logger.info("🔌 Planificateur Synergo arrêté")

    async def stop_scheduler(self):
        """
        Arrête le planificateur proprement
        """
        logger.info("🛑 Arrêt du planificateur demandé...")
        self.is_running = False

        # Attendre la fin de la sync en cours si nécessaire
        if self.is_syncing:
            logger.info("⏳ Attente de la fin de la synchronisation en cours...")
            timeout = 30  # secondes
            while self.is_syncing and timeout > 0:
                await asyncio.sleep(1)
                timeout -= 1

            if self.is_syncing:
                logger.warning("⚠️ Timeout: synchronisation toujours en cours")

    async def trigger_manual_sync(self) -> List[SyncResult]:
        """
        Déclenche une synchronisation manuelle
        """
        if self.is_syncing:
            logger.warning("⚠️ Synchronisation déjà en cours")
            return self.last_sync_results

        logger.info("🔄 Synchronisation manuelle déclenchée")
        return await self._execute_sync_cycle()

    async def _execute_sync_cycle(self) -> List[SyncResult]:
        """
        Exécute un cycle de synchronisation complet
        """
        if self.is_syncing:
            logger.warning("⚠️ Synchronisation déjà en cours, abandon")
            return []

        self.is_syncing = True
        cycle_start = datetime.now()

        try:
            logger.info(f"🔄 Début cycle de synchronisation #{self.sync_count + 1}")

            # Exécuter la synchronisation via le manager
            results = await self.sync_manager.sync_all_active_tables()

            # Analyser les résultats
            self.last_sync_results = results
            self.sync_count += 1

            # Statistiques du cycle
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            total_records = sum(r.records_processed for r in results)
            success_count = sum(1 for r in results if r.status == 'SUCCESS')
            error_count = sum(1 for r in results if r.status == 'ERROR')

            # Log résumé
            if error_count == 0:
                logger.info(f"✅ Cycle #{self.sync_count} terminé avec succès")
                logger.info(f"   📊 {total_records} enregistrements en {cycle_duration:.2f}s")
                logger.info(f"   🎯 {success_count}/{len(results)} tables synchronisées")
            else:
                self.error_count += 1
                logger.warning(f"⚠️ Cycle #{self.sync_count} terminé avec erreurs")
                logger.warning(f"   📊 {total_records} enregistrements en {cycle_duration:.2f}s")
                logger.warning(f"   🎯 {success_count}/{len(results)} tables OK, {error_count} erreurs")

            # Log détails des erreurs
            for result in results:
                if result.status == 'ERROR':
                    logger.error(f"   ❌ {result.table_name}: {result.error_message}")

            return results

        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ Erreur critique pendant le cycle de sync: {e}")

            # Créer un résultat d'erreur
            error_result = SyncResult(
                table_name="GLOBAL",
                status="ERROR",
                error_message=str(e)
            )
            self.last_sync_results = [error_result]
            return [error_result]

        finally:
            self.is_syncing = False

    async def _wait_for_next_sync(self):
        """
        Attente intelligente jusqu'à la prochaine synchronisation
        """
        wait_seconds = self.sync_interval_minutes * 60

        # Attente par petits intervalles pour permettre l'arrêt propre
        interval = 10  # secondes
        elapsed = 0

        while elapsed < wait_seconds and self.is_running:
            await asyncio.sleep(min(interval, wait_seconds - elapsed))
            elapsed += interval



    def get_status(self) -> Dict[str, any]:
        """
        Retourne le statut actuel du planificateur - VERSION CORRIGÉE
        """
        now = datetime.now()
        uptime = (now - self.start_time).total_seconds() if self.start_time else 0

        # Formater next_sync_time correctement
        next_sync_formatted = None
        if self.next_sync_time:
            if hasattr(self.next_sync_time, 'isoformat'):
                next_sync_formatted = self.next_sync_time.isoformat()
            else:
                next_sync_formatted = str(self.next_sync_time)

        status = {
            'is_running': self.is_running,
            'is_syncing': self.is_syncing,
            'sync_interval_minutes': self.sync_interval_minutes,
            'sync_count': self.sync_count,
            'error_count': self.error_count,
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'next_sync_time': next_sync_formatted,  # CORRIGÉ
            'last_sync_summary': self._get_last_sync_summary()
        }

        return status

    def _get_last_sync_summary(self) -> Dict[str, any]:
        """
        Résumé de la dernière synchronisation
        """
        if not self.last_sync_results:
            return {'status': 'never_synced'}

        total_records = sum(r.records_processed for r in self.last_sync_results)
        success_count = sum(1 for r in self.last_sync_results if r.status == 'SUCCESS')
        error_count = sum(1 for r in self.last_sync_results if r.status == 'ERROR')
        no_changes_count = sum(1 for r in self.last_sync_results if r.status == 'NO_CHANGES')

        return {
            'timestamp': max(r.timestamp for r in self.last_sync_results).isoformat(),
            'total_tables': len(self.last_sync_results),
            'successful_tables': success_count,
            'error_tables': error_count,
            'no_changes_tables': no_changes_count,
            'total_records_processed': total_records,
            'overall_status': 'success' if error_count == 0 else 'partial' if success_count > 0 else 'error'
        }

    async def get_detailed_sync_report(self) -> Dict[str, any]:
        """
        Rapport détaillé incluant les données du dashboard
        """
        # Récupérer les données du dashboard
        dashboard_data = await self.sync_manager.get_sync_dashboard_data()

        # Ajouter les infos du scheduler
        scheduler_status = self.get_status()

        return {
            'scheduler': scheduler_status,
            'sync_states': dashboard_data.get('sync_states', []),
            'stats_24h': dashboard_data.get('stats_24h', {}),
            'generated_at': datetime.now().isoformat()
        }


# Singleton pour gestion globale
_scheduler_instance: Optional[SynergoSyncScheduler] = None


def get_scheduler_instance() -> SynergoSyncScheduler:
    """Retourne l'instance globale du scheduler"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = SynergoSyncScheduler()
    return _scheduler_instance


# Service de gestion du scheduler
class SchedulerService:
    """
    Service de haut niveau pour contrôler le scheduler
    """

    @staticmethod
    async def start_sync_service():
        """Démarre le service de synchronisation"""
        scheduler = get_scheduler_instance()
        await scheduler.start_scheduler()

    @staticmethod
    async def stop_sync_service():
        """Arrête le service de synchronisation"""
        scheduler = get_scheduler_instance()
        await scheduler.stop_scheduler()

    @staticmethod
    async def trigger_manual_sync() -> List[SyncResult]:
        """Déclenche une synchronisation manuelle"""
        scheduler = get_scheduler_instance()
        return await scheduler.trigger_manual_sync()

    @staticmethod
    def get_sync_status() -> Dict[str, any]:
        """Récupère le statut de la synchronisation"""
        scheduler = get_scheduler_instance()
        return scheduler.get_status()

    @staticmethod
    async def get_sync_report() -> Dict[str, any]:
        """Récupère un rapport détaillé"""
        scheduler = get_scheduler_instance()
        return await scheduler.get_detailed_sync_report()


# Script de démarrage pour utilisation standalone
async def run_scheduler_standalone():
    """
    Lance le scheduler en mode standalone pour tests
    """
    logger.info("🚀 Démarrage Synergo Scheduler (mode standalone)")

    scheduler = SynergoSyncScheduler(sync_interval_minutes=5)  # Test avec 5 minutes

    try:
        await scheduler.start_scheduler()
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt demandé par utilisateur")
        await scheduler.stop_scheduler()


if __name__ == "__main__":
    # Pour tester le scheduler
    asyncio.run(run_scheduler_standalone())