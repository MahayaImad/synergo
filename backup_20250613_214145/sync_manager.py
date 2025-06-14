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

    async def _create_sync_state_if_missing(self, session, table_name: str, updates: Dict):
        """Crée un enregistrement sync_state s'il n'existe pas"""
        from sqlalchemy import text

        try:
            # Valeurs par défaut
            default_values = {
                'table_name': table_name,
                'last_sync_id': 0,
                'last_sync_timestamp': None,
                'total_records': 0,
                'last_sync_status': 'NEVER_SYNCED',
                'records_processed_last_sync': 0,
                'error_message': None
            }

            # Fusionner avec les updates
            default_values.update(updates)

            # Construire requête INSERT
            fields = list(default_values.keys())
            placeholders = [f":{field}" for field in fields]

            query = f"""
            INSERT INTO synergo_sync.sync_state ({', '.join(fields)})
            VALUES ({', '.join(placeholders)})
            """

            await session.execute(text(query), default_values)
            logger.info(f"✅ Sync state créé pour {table_name}")

        except Exception as e:
            logger.error(f"❌ Erreur création sync_state: {e}")
            raise

    async def sync_all_active_tables(self) -> List[SyncResult]:
        """Synchronise toutes les tables avec gestion d'erreurs globale robuste"""
        results = []

        logger.info("🔄 Début synchronisation globale Synergo")
        start_time = datetime.now()

        # Statistiques de sync
        total_records_processed = 0
        successful_tables = 0

        for table_key, config in self.sync_tables_config.items():
            table_start_time = datetime.now()

            try:
                logger.info(f"📊 Début sync table: {config['table_name']}")

                # Synchroniser la table
                result = await self.sync_single_table(config)
                results.append(result)

                # Mettre à jour les statistiques
                total_records_processed += result.records_processed

                # Log résultat avec détails
                duration_seconds = (datetime.now() - table_start_time).total_seconds()

                if result.status == 'SUCCESS':
                    successful_tables += 1
                    logger.info(
                        f"✅ {config['table_name']}: {result.records_processed} enregistrements en {duration_seconds:.2f}s")
                elif result.status == 'NO_CHANGES':
                    logger.info(f"📌 {config['table_name']}: Aucun nouveau enregistrement")
                else:
                    logger.error(f"❌ {config['table_name']}: {result.error_message}")

            except Exception as e:
                logger.error(f"❌ Exception critique pour {config['table_name']}: {e}")

                # Créer un résultat d'erreur
                error_result = SyncResult(
                    table_name=config['table_name'],
                    status='ERROR',
                    error_message=f"Exception critique: {str(e)}",
                    duration_ms=int((datetime.now() - table_start_time).total_seconds() * 1000)
                )
                results.append(error_result)

        # Résumé global avec statistiques détaillées
        total_duration = (datetime.now() - start_time).total_seconds()
        total_tables = len(results)
        error_count = sum(1 for r in results if r.status == 'ERROR')

        logger.info(f"🎯 Sync globale terminée:")
        logger.info(f"   📊 {total_records_processed} enregistrements traités")
        logger.info(f"   ✅ {successful_tables}/{total_tables} tables réussies")
        logger.info(f"   ❌ {error_count} erreurs")
        logger.info(f"   ⏱️ Durée totale: {total_duration:.2f}s")

        # Enregistrer le résumé en base
        try:
            await self._log_sync_summary_robust(results, total_duration)
        except Exception as log_error:
            logger.error(f"❌ Erreur logging résumé: {log_error}")

        return results

    async def _log_sync_summary_robust(self, results: List[SyncResult], duration_seconds: float):
        """Enregistre un résumé de synchronisation avec gestion d'erreurs robuste"""
        from sqlalchemy import text
        import json

        async with get_async_session_context() as session:
            try:
                for result in results:
                    # Préparer les détails d'erreur comme JSON valide
                    error_details = None
                    if result.error_message:
                        try:
                            error_details = json.dumps({
                                'message': str(result.error_message)[:500],  # Limiter la taille
                                'status': result.status,
                                'timestamp': result.timestamp.isoformat()
                            })
                        except Exception as json_error:
                            logger.debug(f"Erreur création JSON: {json_error}")
                            error_details = json.dumps({'message': 'Erreur de formatage'})

                    log_entry = {
                        'table_name': result.table_name,
                        'operation': 'SYNC_COMPLETE',
                        'records_processed': max(0, int(result.records_processed)),  # Assurer >= 0
                        'processing_time_ms': max(0, int(result.duration_ms)),  # Assurer >= 0
                        'error_details': error_details
                    }

                    query = """
                    INSERT INTO synergo_sync.sync_log 
                    (table_name, operation, records_processed, processing_time_ms, error_details)
                    VALUES (:table_name, :operation, :records_processed, :processing_time_ms, :error_details::jsonb)
                    """

                    try:
                        await session.execute(text(query), log_entry)
                    except Exception as insert_error:
                        logger.warning(f"⚠️ Erreur insertion log pour {result.table_name}: {insert_error}")

                await session.commit()
                logger.debug("✅ Résumé sync enregistré en base")

            except Exception as e:
                logger.error(f"❌ Erreur globale logging: {e}")
                await session.rollback()

    async def sync_single_table(self, config: Dict[str, Any]) -> SyncResult:
        """Synchronisation d'une table avec gestion d'erreurs robuste - VERSION CORRIGÉE"""
        start_time = datetime.now()
        hfsql_connected = False

        try:
            # 1. Connexion HFSQL avec vérification
            logger.info(f"🔌 Connexion HFSQL pour {config['table_name']}...")
            if not await self.hfsql_connector.connect():
                raise Exception("Échec connexion HFSQL")
            hfsql_connected = True

            # 2. Récupération état sync dans une transaction séparée
            async with get_async_session_context() as session:
                sync_state = await self._get_sync_state(session, config['table_name'])
                last_sync_id = sync_state.get('last_sync_id', 0) if sync_state else 0

            logger.info(f"📊 Sync {config['table_name']} depuis ID {last_sync_id}")

            # 3. Stratégie de sync
            if config['strategy'] == 'ID_BASED':
                from .strategies.id_based_sync import IdBasedSyncStrategy
                strategy = IdBasedSyncStrategy(config, self.hfsql_connector)
            else:
                raise ValueError(f"Stratégie non supportée: {config['strategy']}")

            # 4. Récupération nouveaux enregistrements avec validation
            logger.debug("📥 Récupération nouveaux enregistrements...")
            new_records = await strategy.get_new_records(last_sync_id)

            if not new_records:
                logger.info(f"📌 Aucun nouvel enregistrement pour {config['table_name']}")
                return SyncResult(
                    table_name=config['table_name'],
                    status='NO_CHANGES',
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            logger.info(f"📥 {len(new_records)} nouveaux enregistrements à traiter")

            # 5. Transformation avec gestion d'erreurs
            transformer = config['transformer']()
            logger.debug("🔄 Transformation des données...")

            try:
                transformed_records = await transformer.transform_batch(new_records)
            except Exception as transform_error:
                logger.error(f"❌ Erreur transformation: {transform_error}")
                raise Exception(f"Échec transformation: {transform_error}")

            if not transformed_records:
                logger.warning("⚠️ Aucun enregistrement valide après transformation")
                return SyncResult(
                    table_name=config['table_name'],
                    status='NO_CHANGES',
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            logger.info(f"✅ {len(transformed_records)} enregistrements transformés")

            # 6. Insertion en base avec transaction atomique
            async with get_async_session_context() as session:
                try:
                    # Démarrer transaction explicite
                    await session.begin()

                    # Insérer les enregistrements
                    logger.debug("💾 Insertion en base de données...")
                    inserted_count = await strategy.insert_records(session, transformed_records)

                    # Calculer le nouvel ID max de façon sécurisée
                    valid_ids = [
                        int(record[config['id_field']])
                        for record in new_records
                        if record.get(config['id_field']) is not None
                    ]

                    if not valid_ids:
                        raise Exception("Aucun ID valide trouvé dans les nouveaux enregistrements")

                    new_last_id = max(valid_ids)
                    logger.debug(f"📈 Nouvel ID max: {new_last_id}")

                    # Mettre à jour l'état de sync
                    await self._update_sync_state_robust(session, config['table_name'], {
                        'last_sync_id': new_last_id,
                        'last_sync_timestamp': datetime.now(),
                        'total_records': (sync_state.get('total_records', 0) if sync_state else 0) + inserted_count,
                        'last_sync_status': 'SUCCESS',
                        'records_processed_last_sync': inserted_count,
                        'error_message': None  # Nettoyer les erreurs précédentes
                    })

                    # Commit de toute la transaction
                    await session.commit()

                    logger.info(f"✅ {inserted_count} enregistrements synchronisés avec succès")

                    return SyncResult(
                        table_name=config['table_name'],
                        status='SUCCESS',
                        records_processed=inserted_count,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )

                except Exception as db_error:
                    # Rollback en cas d'erreur
                    await session.rollback()
                    logger.error(f"❌ Erreur base de données, rollback effectué: {db_error}")
                    raise db_error

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Erreur sync {config['table_name']}: {error_msg}")

            # Enregistrer l'erreur en base (dans une transaction séparée)
            try:
                async with get_async_session_context() as error_session:
                    await self._update_sync_state_robust(error_session, config['table_name'], {
                        'last_sync_status': 'ERROR',
                        'error_message': error_msg[:500],  # Limiter la taille pour éviter les erreurs DB
                        'last_sync_timestamp': datetime.now()
                    })
                    await error_session.commit()
            except Exception as log_error:
                logger.error(f"❌ Impossible de logger l'erreur en base: {log_error}")

            return SyncResult(
                table_name=config['table_name'],
                status='ERROR',
                error_message=error_msg,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )

        finally:
            # Nettoyage des connexions HFSQL
            if hfsql_connected:
                try:
                    self.hfsql_connector.close()
                    logger.debug("🔌 Connexion HFSQL fermée")
                except Exception as cleanup_error:
                    logger.debug(f"⚠️ Erreur nettoyage HFSQL (non critique): {cleanup_error}")

    async def _update_sync_state_robust(self, session, table_name: str, updates: Dict):
        """Met à jour l'état de synchronisation avec validation des types - VERSION ROBUSTE"""
        from sqlalchemy import text

        try:
            # Nettoyer et valider les updates
            clean_updates = {}

            for field, value in updates.items():
                if field == 'last_sync_id':
                    # Validation stricte de l'ID
                    if isinstance(value, str):
                        try:
                            clean_updates[field] = int(value)
                        except ValueError:
                            logger.error(f"❌ last_sync_id invalide (string): {value}")
                            clean_updates[field] = 0
                    elif isinstance(value, (int, float)):
                        clean_updates[field] = int(value)
                    elif value is None:
                        clean_updates[field] = 0
                    else:
                        logger.error(f"❌ Type last_sync_id non supporté: {type(value)}")
                        clean_updates[field] = 0

                elif field in ['total_records', 'records_processed_last_sync', 'last_sync_duration']:
                    # Autres entiers
                    try:
                        clean_updates[field] = int(value) if value is not None else 0
                    except (ValueError, TypeError):
                        logger.warning(f"⚠️ Conversion entier échouée pour {field}: {value}")
                        clean_updates[field] = 0

                elif field == 'error_message':
                    # Chaîne avec limitation de taille
                    if value is not None:
                        clean_updates[field] = str(value)[:500]  # Limiter à 500 caractères
                    else:
                        clean_updates[field] = None

                elif field in ['last_sync_status']:
                    # Statuts avec validation
                    valid_statuses = ['SUCCESS', 'ERROR', 'PENDING', 'NO_CHANGES', 'NEVER_SYNCED']
                    if value in valid_statuses:
                        clean_updates[field] = value
                    else:
                        logger.warning(f"⚠️ Statut invalide: {value}, utilisation 'ERROR'")
                        clean_updates[field] = 'ERROR'

                else:
                    # Autres champs - garder tel quel
                    clean_updates[field] = value

            # Vérifier qu'on a des updates valides
            if not clean_updates:
                logger.warning("⚠️ Aucune mise à jour valide à appliquer")
                return

            # Construire la requête UPDATE dynamiquement
            set_clauses = []
            params = {'table_name': table_name}

            for field, value in clean_updates.items():
                set_clauses.append(f"{field} = :{field}")
                params[field] = value

            # Ajouter timestamp automatique
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            query = f"""
            UPDATE synergo_sync.sync_state 
            SET {', '.join(set_clauses)}
            WHERE table_name = :table_name
            """

            # Exécuter avec logging
            logger.debug(f"🔄 Mise à jour sync_state pour {table_name}")
            result = await session.execute(text(query), params)

            # Vérifier que la mise à jour a affecté une ligne
            if result.rowcount == 0:
                logger.warning(f"⚠️ Aucune ligne mise à jour pour {table_name}")
                # Optionnel: créer l'enregistrement s'il n'existe pas
                await self._create_sync_state_if_missing(session, table_name, clean_updates)
            else:
                logger.debug(f"✅ Sync state mis à jour pour {table_name}")

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour sync_state: {e}")
            logger.debug(f"Paramètres: table_name={table_name}, updates={updates}")
            raise


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