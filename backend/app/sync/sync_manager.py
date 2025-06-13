# backend/app/sync/sync_manager.py
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ..core.database import get_async_session_context
from ..utils.hfsql_connector import HFSQLConnector
from .strategies.id_based_sync import IdBasedSyncStrategy
from .transformers.sales_transformer import SalesTransformer
from .transformers.product_transformer import ProductTransformer


class SyncResult:
    def __init__(self, table_name: str, status: str, records_processed: int = 0,
                 error_message: str = None, duration_ms: int = 0):
        self.table_name = table_name
        self.status = status  # 'SUCCESS', 'ERROR', 'NO_CHANGES'
        self.records_processed = records_processed
        self.error_message = error_message
        self.duration_ms = duration_ms
        self.timestamp = datetime.now()


class SynergoSyncManager:
    """
    Chef d'orchestre de la synchronisation Synergo
    Gère la sync incrémentale entre HFSQL et PostgreSQL
    """

    def __init__(self):
        self.hfsql_connector = HFSQLConnector()
        self.sync_tables_config = self._load_sync_config()

    def _load_sync_config(self) -> Dict[str, Dict]:
        """Configuration des tables à synchroniser"""
        return {
            'sales_summary': {
                'table_name': 'sales_summary',
                'hfsql_table': 'sorties',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': SalesTransformer,
                'sync_interval_minutes': 30,
                'batch_size': 1000,
                'schema': 'synergo_core'
            },
            'products_catalog': {
                'table_name': 'products_catalog',
                'hfsql_table': 'nomenclature',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': ProductTransformer,
                'sync_interval_minutes': 60,
                'batch_size': 500,
                'schema': 'synergo_core'
            }
        }

    async def sync_all_active_tables(self) -> List[SyncResult]:
        """Synchronise toutes les tables actives"""
        results = []

        logger.info("🔄 Début synchronisation globale Synergo")
        start_time = datetime.now()

        for table_key, config in self.sync_tables_config.items():
            try:
                logger.info(f"📊 Sync table: {config['table_name']}")
                result = await self.sync_single_table(config)
                results.append(result)

                # Log résultat
                if result.status == 'SUCCESS':
                    logger.info(
                        f"✅ {config['table_name']}: {result.records_processed} enregistrements en {result.duration_ms}ms")
                elif result.status == 'NO_CHANGES':
                    logger.info(f"📌 {config['table_name']}: Aucun nouveau enregistrement")
                else:
                    logger.error(f"❌ {config['table_name']}: {result.error_message}")

            except Exception as e:
                logger.error(f"❌ Erreur sync {config['table_name']}: {e}")
                results.append(SyncResult(
                    table_name=config['table_name'],
                    status='ERROR',
                    error_message=str(e)
                ))

        # Résumé global
        total_duration = (datetime.now() - start_time).total_seconds()
        total_records = sum(r.records_processed for r in results)
        success_count = sum(1 for r in results if r.status == 'SUCCESS')

        logger.info(f"🎯 Sync globale terminée: {total_records} enregistrements, "
                    f"{success_count}/{len(results)} tables OK en {total_duration:.2f}s")

        # Enregistrer le résumé en base
        await self._log_sync_summary(results, total_duration)

        return results

    async def sync_single_table(self, config: Dict[str, Any]) -> SyncResult:
        """Synchronise une table spécifique"""
        start_time = datetime.now()

        try:
            # 1. Récupérer l'état de sync actuel
            async with get_async_session_context() as session:  # CHANGEMENT ICI
                sync_state = await self._get_sync_state(session, config['table_name'])
                last_sync_id = sync_state.get('last_sync_id', 0) if sync_state else 0

            # 2. Créer la stratégie de sync appropriée
            if config['strategy'] == 'ID_BASED':
                strategy = IdBasedSyncStrategy(config, self.hfsql_connector)
            else:
                raise ValueError(f"Stratégie non supportée: {config['strategy']}")

            # 3. Récupérer les nouveaux enregistrements HFSQL
            new_records = await strategy.get_new_records(last_sync_id)

            if not new_records:
                return SyncResult(
                    table_name=config['table_name'],
                    status='NO_CHANGES',
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            # 4. Transformer les données
            transformer = config['transformer']()
            transformed_records = await transformer.transform_batch(new_records)

            # 5. Insérer en PostgreSQL + Mettre à jour l'état de sync
            async with get_async_session_context() as session:  # CHANGEMENT ICI
                inserted_count = await strategy.insert_records(session, transformed_records)

                # 6. Mettre à jour last_sync_id
                new_last_id = max(record[config['id_field']] for record in new_records)
                await self._update_sync_state(session, config['table_name'], {
                    'last_sync_id': new_last_id,
                    'last_sync_timestamp': datetime.now(),
                    'total_records': sync_state.get('total_records',
                                                    0) + inserted_count if sync_state else inserted_count,
                    'last_sync_status': 'SUCCESS',
                    'records_processed_last_sync': inserted_count
                })

                await session.commit()

            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            return SyncResult(
                table_name=config['table_name'],
                status='SUCCESS',
                records_processed=inserted_count,
                duration_ms=duration_ms
            )

        except Exception as e:
            # En cas d'erreur, log et retourner l'erreur
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            async with get_async_session_context() as session:  # CHANGEMENT ICI
                await self._update_sync_state(session, config['table_name'], {
                    'last_sync_status': 'ERROR',
                    'error_message': str(e),
                    'last_sync_timestamp': datetime.now()
                })
                await session.commit()

            return SyncResult(
                table_name=config['table_name'],
                status='ERROR',
                error_message=str(e),
                duration_ms=duration_ms
            )

    async def _get_sync_state(self, session: AsyncSession, table_name: str) -> Optional[Dict]:
        """Récupère l'état de synchronisation d'une table"""
        query = """
        SELECT last_sync_id, last_sync_timestamp, total_records, last_sync_status
        FROM synergo_sync.sync_state 
        WHERE table_name = :table_name
        """
        result = await session.execute(text(query), {'table_name': table_name})
        row = result.fetchone()

        if row:
            return {
                'last_sync_id': row[0],
                'last_sync_timestamp': row[1],
                'total_records': row[2],
                'last_sync_status': row[3]
            }
        return None

    async def get_sync_dashboard_data(self) -> Dict[str, Any]:
        """Données pour le dashboard de monitoring - VERSION CORRIGÉE"""
        async with get_async_session_context() as session:  # CHANGEMENT ICI
            try:
                # État général des syncs
                query = """
                SELECT 
                    table_name,
                    last_sync_id,
                    last_sync_timestamp,
                    total_records,
                    last_sync_status,
                    records_processed_last_sync,
                    error_message
                FROM synergo_sync.sync_state
                ORDER BY table_name
                """

                result = await session.execute(text(query))
                sync_states = []

                for row in result.fetchall():
                    sync_state = {
                        'table_name': row[0],
                        'last_sync_id': row[1] or 0,
                        'last_sync_timestamp': row[2].isoformat() if row[2] else None,
                        'total_records': row[3] or 0,
                        'last_sync_status': row[4] or 'NEVER_SYNCED',
                        'records_processed_last_sync': row[5] or 0,
                        'error_message': row[6]
                    }
                    sync_states.append(sync_state)

                # Statistiques des dernières 24h
                stats_query = """
                SELECT 
                    COUNT(*) as total_syncs,
                    COALESCE(SUM(records_processed), 0) as total_records_processed,
                    COALESCE(AVG(processing_time_ms), 0) as avg_processing_time
                FROM synergo_sync.sync_log 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                AND operation = 'SYNC_COMPLETE'
                """

                stats_result = await session.execute(text(stats_query))
                stats_row = stats_result.fetchone()

                stats = {
                    'total_syncs': stats_row[0] or 0,
                    'total_records_processed': stats_row[1] or 0,
                    'avg_processing_time': float(stats_row[2] or 0)
                }

                return {
                    'sync_states': sync_states,
                    'stats_24h': stats,
                    'last_update': datetime.now().isoformat()
                }

            except Exception as e:
                logger.error(f"Erreur récupération dashboard: {e}")
                # Retourner des données par défaut en cas d'erreur
                return {
                    'sync_states': [],
                    'stats_24h': {
                        'total_syncs': 0,
                        'total_records_processed': 0,
                        'avg_processing_time': 0.0
                    },
                    'last_update': datetime.now().isoformat(),
                    'error': str(e)
                }

    async def sync_single_table(self, config: Dict[str, Any]) -> SyncResult:
        """Synchronise une table spécifique - VERSION CORRIGÉE"""
        start_time = datetime.now()

        try:
            # 1. Récupérer l'état de sync actuel
            async with get_async_session_context() as session:
                sync_state = await self._get_sync_state(session, config['table_name'])
                last_sync_id = sync_state.get('last_sync_id', 0) if sync_state else 0

            # 2. Créer la stratégie de sync appropriée
            if config['strategy'] == 'ID_BASED':
                strategy = IdBasedSyncStrategy(config, self.hfsql_connector)
            else:
                raise ValueError(f"Stratégie non supportée: {config['strategy']}")

            # 3. Récupérer les nouveaux enregistrements HFSQL
            new_records = await strategy.get_new_records(last_sync_id)

            if not new_records:
                return SyncResult(
                    table_name=config['table_name'],
                    status='NO_CHANGES',
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            # 4. Transformer les données
            transformer = config['transformer']()
            transformed_records = await transformer.transform_batch(new_records)

            # 5. Insérer en PostgreSQL + Mettre à jour l'état de sync
            async with get_async_session_context() as session:
                inserted_count = await strategy.insert_records(session, transformed_records)

                # 6. Mettre à jour last_sync_id - CORRECTION ICI
                new_last_id = max(record[config['id_field']] for record in new_records)

                # S'assurer que new_last_id est un entier
                if isinstance(new_last_id, str):
                    new_last_id = int(new_last_id)

                await self._update_sync_state(session, config['table_name'], {
                    'last_sync_id': new_last_id,  # Maintenant garanti d'être un int
                    'last_sync_timestamp': datetime.now(),
                    'total_records': sync_state.get('total_records',
                                                    0) + inserted_count if sync_state else inserted_count,
                    'last_sync_status': 'SUCCESS',
                    'records_processed_last_sync': inserted_count
                })

                await session.commit()

            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            return SyncResult(
                table_name=config['table_name'],
                status='SUCCESS',
                records_processed=inserted_count,
                duration_ms=duration_ms
            )

        except Exception as e:
            # En cas d'erreur, log et retourner l'erreur
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            try:
                async with get_async_session_context() as session:
                    await self._update_sync_state(session, config['table_name'], {
                        'last_sync_status': 'ERROR',
                        'error_message': str(e),
                        'last_sync_timestamp': datetime.now()
                    })
                    await session.commit()
            except Exception as update_error:
                logger.error(f"❌ Erreur mise à jour état après échec: {update_error}")

            return SyncResult(
                table_name=config['table_name'],
                status='ERROR',
                error_message=str(e),
                duration_ms=duration_ms
            )

    async def _update_sync_state(self, session: AsyncSession, table_name: str, updates: Dict):
        """Met à jour l'état de synchronisation - VERSION CORRIGÉE"""
        # Nettoyer et convertir les types avant requête
        clean_updates = {}

        for field, value in updates.items():
            if field == 'last_sync_id':
                # S'assurer que c'est un entier
                if isinstance(value, str):
                    try:
                        clean_updates[field] = int(value)
                    except ValueError:
                        logger.warning(f"⚠️ last_sync_id invalide: {value}, utilisation 0")
                        clean_updates[field] = 0
                elif isinstance(value, (int, float)):
                    clean_updates[field] = int(value)
                else:
                    clean_updates[field] = 0
            elif field in ['total_records', 'records_processed_last_sync', 'last_sync_duration']:
                # Autres entiers
                try:
                    clean_updates[field] = int(value) if value is not None else 0
                except (ValueError, TypeError):
                    clean_updates[field] = 0
            else:
                # Autres types - garder tel quel
                clean_updates[field] = value

        # Construire la requête UPDATE dynamiquement
        set_clauses = []
        params = {'table_name': table_name}

        for field, value in clean_updates.items():
            set_clauses.append(f"{field} = :{field}")
            params[field] = value

        query = f"""
        UPDATE synergo_sync.sync_state 
        SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
        WHERE table_name = :table_name
        """

        await session.execute(text(query), params)

    async def _log_sync_summary(self, results: List[SyncResult], duration_seconds: float):
        """Enregistre un résumé de la synchronisation - VERSION CORRIGÉE"""
        async with get_async_session_context() as session:
            for result in results:
                # Préparer error_details comme JSON ou NULL
                error_details = None
                if result.error_message:
                    # Convertir en JSON string pour PostgreSQL
                    import json
                    error_details = json.dumps({'message': result.error_message})

                log_entry = {
                    'table_name': result.table_name,
                    'operation': 'SYNC_COMPLETE',
                    'records_processed': int(result.records_processed),
                    'processing_time_ms': int(result.duration_ms),
                    'error_details': error_details  # JSON string ou NULL
                }

                query = """
                INSERT INTO synergo_sync.sync_log 
                (table_name, operation, records_processed, processing_time_ms, error_details)
                VALUES (:table_name, :operation, :records_processed, :processing_time_ms, :error_details)
                """

                await session.execute(text(query), log_entry)

            await session.commit()

# Fonction utilitaire pour tests
async def test_sync_manager():
    """Test du sync manager"""
    manager = SynergoSyncManager()

    # Test dashboard
    dashboard = await manager.get_sync_dashboard_data()

    print(f"Test dashboard: {len(dashboard['sync_states'])} tables")
    return dashboard


if __name__ == "__main__":
    # Test rapide
    asyncio.run(test_sync_manager())