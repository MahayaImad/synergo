# backend/app/sync/sync_manager.py - CONFIGURATION COMPLÈTE ERP
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ..core.database import get_async_session_context
from ..utils.hfsql_connector import HFSQLConnector
from .strategies.id_based_sync import IdBasedSyncStrategy

# Import de tous les transformers
from .transformers.product_transformer import ProductTransformer
from .transformers.purchase_order_transformer import PurchaseOrderTransformer
from .transformers.purchase_detail_transformer import PurchaseDetailTransformer
from .transformers.sales_order_transformer import SalesOrderTransformer
from .transformers.sales_detail_transformer import SalesDetailTransformer


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
    Gère la sync incrémentale ERP complet entre HFSQL et PostgreSQL
    """

    def __init__(self):
        self.hfsql_connector = HFSQLConnector()
        self.sync_tables_config = self._load_complete_sync_config()

    def _load_complete_sync_config(self) -> Dict[str, Dict]:
        """
        Configuration complète des tables à synchroniser
        Ordre critique pour respecter les FK: produits → achats → ventes
        """
        return {
            # 1. PRODUITS - Base du référentiel (priorité max)
            'products_catalog': {
                'table_name': 'products_catalog',
                'hfsql_table': 'nomenclature',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': ProductTransformer,
                'sync_interval_minutes': 60,  # Moins fréquent car stable
                'batch_size': 500,
                'schema': 'synergo_core',
                'sync_order': 1
            },

            # 2. ACHATS EN-TÊTES - Commandes fournisseurs
            'purchase_orders': {
                'table_name': 'purchase_orders',
                'hfsql_table': 'entrees',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': PurchaseOrderTransformer,
                'sync_interval_minutes': 45,
                'batch_size': 750,
                'schema': 'synergo_core',
                'sync_order': 2
            },

            # 3. ACHATS DÉTAILS - Crucial pour calcul marge
            'purchase_details': {
                'table_name': 'purchase_details',
                'hfsql_table': 'entrees_produits',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': PurchaseDetailTransformer,
                'sync_interval_minutes': 30,  # Fréquent car impact stock/prix
                'batch_size': 1000,
                'schema': 'synergo_core',
                'sync_order': 3
            },

            # 4. VENTES EN-TÊTES - Transactions clients
            'sales_orders': {
                'table_name': 'sales_orders',
                'hfsql_table': 'sorties',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': SalesOrderTransformer,
                'sync_interval_minutes': 15,  # Très fréquent car temps réel
                'batch_size': 1000,
                'schema': 'synergo_core',
                'sync_order': 4
            },

            # 5. VENTES DÉTAILS - Marges par ligne
            'sales_details': {
                'table_name': 'sales_details',
                'hfsql_table': 'ventes_produits',
                'id_field': 'id',
                'strategy': 'ID_BASED',
                'transformer': SalesDetailTransformer,
                'sync_interval_minutes': 15,  # Très fréquent car temps réel
                'batch_size': 1000,
                'schema': 'synergo_core',
                'sync_order': 5
            }
        }

    async def sync_all_active_tables(self) -> List[SyncResult]:
        """
        Synchronise toutes les tables actives dans l'ordre optimal
        """
        results = []

        logger.info("🔄 Début synchronisation ERP complète Synergo")
        start_time = datetime.now()

        # Tri par ordre de synchronisation (respect des FK)
        sorted_configs = sorted(
            self.sync_tables_config.items(),
            key=lambda x: x[1].get('sync_order', 999)
        )

        for table_key, config in sorted_configs:
            try:
                logger.info(f"📊 Sync {config['sync_order']}/5: {config['table_name']} ← {config['hfsql_table']}")
                result = await self.sync_single_table(config)
                results.append(result)

                # Log résultat avec émojis pour lisibilité
                if result.status == 'SUCCESS':
                    logger.info(
                        f"✅ {config['table_name']}: {result.records_processed} enregistrements en {result.duration_ms}ms")
                elif result.status == 'NO_CHANGES':
                    logger.info(f"📌 {config['table_name']}: Aucun nouveau enregistrement")
                else:
                    logger.error(f"❌ {config['table_name']}: {result.error_message}")

                # Pause courte entre tables pour éviter la surcharge
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"❌ Erreur sync {config['table_name']}: {e}")
                results.append(SyncResult(
                    table_name=config['table_name'],
                    status='ERROR',
                    error_message=str(e)
                ))

        # Résumé global avec statistiques détaillées
        total_duration = (datetime.now() - start_time).total_seconds()
        total_records = sum(r.records_processed for r in results)
        success_count = sum(1 for r in results if r.status == 'SUCCESS')
        no_changes_count = sum(1 for r in results if r.status == 'NO_CHANGES')
        error_count = sum(1 for r in results if r.status == 'ERROR')

        logger.info(f"🎯 Sync ERP terminée: {total_records} enregistrements, "
                    f"{success_count} OK, {no_changes_count} inchangées, {error_count} erreurs en {total_duration:.2f}s")

        # Log détaillé par catégorie
        products_results = [r for r in results if 'product' in r.table_name]
        purchase_results = [r for r in results if 'purchase' in r.table_name]
        sales_results = [r for r in results if 'sales' in r.table_name]

        if products_results:
            products_total = sum(r.records_processed for r in products_results)
            logger.info(f"📦 Produits: {products_total} enregistrements")

        if purchase_results:
            purchases_total = sum(r.records_processed for r in purchase_results)
            logger.info(f"🛒 Achats: {purchases_total} enregistrements")

        if sales_results:
            sales_total = sum(r.records_processed for r in sales_results)
            logger.info(f"💰 Ventes: {sales_total} enregistrements")

        # Enregistrer le résumé en base pour analytics
        await self._log_sync_summary(results, total_duration)

        # Déclencher recalcul des vues analytics si beaucoup de données
        if total_records > 100:
            await self._trigger_analytics_refresh()

        return results

    async def sync_single_table(self, config: Dict[str, Any]) -> SyncResult:
        """
        Synchronise une table spécifique avec gestion d'erreurs renforcée
        """
        start_time = datetime.now()
        table_name = config['table_name']

        try:
            # 1. Récupérer l'état de sync actuel
            async with get_async_session_context() as session:
                sync_state = await self._get_sync_state(session, table_name)
                last_sync_id = sync_state.get('last_sync_id', 0) if sync_state else 0

            logger.debug(f"🔍 {table_name}: Dernier ID synchronisé = {last_sync_id}")

            # 2. Créer la stratégie de sync appropriée
            if config['strategy'] == 'ID_BASED':
                strategy = IdBasedSyncStrategy(config, self.hfsql_connector)
            else:
                raise ValueError(f"Stratégie non supportée: {config['strategy']}")

            # 3. Récupérer les nouveaux enregistrements HFSQL
            new_records = await strategy.get_new_records(last_sync_id)

            if not new_records:
                logger.debug(f"📌 {table_name}: Aucun nouveau enregistrement depuis ID {last_sync_id}")
                return SyncResult(
                    table_name=table_name,
                    status='NO_CHANGES',
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            logger.debug(f"📥 {table_name}: {len(new_records)} nouveaux enregistrements trouvés")

            # 4. Transformer les données avec le bon transformer
            transformer = config['transformer']()
            transformed_records = await transformer.transform_batch(new_records)

            if not transformed_records:
                logger.warning(f"⚠️ {table_name}: Aucun enregistrement valide après transformation")
                return SyncResult(
                    table_name=table_name,
                    status='ERROR',
                    error_message="Aucun enregistrement valide après transformation",
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )

            logger.debug(f"🔄 {table_name}: {len(transformed_records)} enregistrements transformés")

            # 5. Insérer en PostgreSQL + Mettre à jour l'état de sync
            async with get_async_session_context() as session:
                inserted_count = await strategy.insert_records(session, transformed_records)

                # 6. Mettre à jour last_sync_id
                new_last_id = max(record[config['id_field']] for record in new_records)

                # S'assurer que new_last_id est un entier
                if isinstance(new_last_id, str):
                    new_last_id = int(new_last_id)

                await self._update_sync_state(session, table_name, {
                    'last_sync_id': new_last_id,
                    'last_sync_timestamp': datetime.now(),
                    'total_records': sync_state.get('total_records',
                                                    0) + inserted_count if sync_state else inserted_count,
                    'last_sync_status': 'SUCCESS',
                    'records_processed_last_sync': inserted_count,
                    'last_sync_duration': int((datetime.now() - start_time).total_seconds())
                })

                await session.commit()

            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            logger.debug(f"✅ {table_name}: {inserted_count} enregistrements synchronisés avec succès")

            return SyncResult(
                table_name=table_name,
                status='SUCCESS',
                records_processed=inserted_count,
                duration_ms=duration_ms
            )

        except Exception as e:
            # En cas d'erreur, log détaillé et mettre à jour l'état
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            error_msg = str(e)

            logger.error(f"❌ {table_name}: Erreur de synchronisation - {error_msg}")

            try:
                async with get_async_session_context() as session:
                    await self._update_sync_state(session, table_name, {
                        'last_sync_status': 'ERROR',
                        'error_message': error_msg[:500],  # Limitation taille
                        'last_sync_timestamp': datetime.now(),
                        'last_sync_duration': int((datetime.now() - start_time).total_seconds())
                    })
                    await session.commit()
            except Exception as update_error:
                logger.error(f"❌ Erreur mise à jour état après échec: {update_error}")

            return SyncResult(
                table_name=table_name,
                status='ERROR',
                error_message=error_msg,
                duration_ms=duration_ms
            )

    async def sync_specific_tables(self, table_names: List[str]) -> List[SyncResult]:
        """
        Synchronise uniquement les tables spécifiées
        Utile pour sync manuelle ou réparation
        """
        results = []

        logger.info(f"🎯 Synchronisation ciblée: {', '.join(table_names)}")

        for table_name in table_names:
            if table_name in self.sync_tables_config:
                config = self.sync_tables_config[table_name]
                try:
                    result = await self.sync_single_table(config)
                    results.append(result)
                except Exception as e:
                    logger.error(f"❌ Erreur sync {table_name}: {e}")
                    results.append(SyncResult(
                        table_name=table_name,
                        status='ERROR',
                        error_message=str(e)
                    ))
            else:
                logger.warning(f"⚠️ Table inconnue ignorée: {table_name}")
                results.append(SyncResult(
                    table_name=table_name,
                    status='ERROR',
                    error_message=f"Table {table_name} non configurée"
                ))

        return results

    async def get_sync_statistics(self) -> Dict[str, Any]:
        """
        Statistiques détaillées de synchronisation
        """
        async with get_async_session_context() as session:
            try:
                # Statistiques par table
                stats_query = """
                SELECT 
                    table_name,
                    last_sync_id,
                    total_records,
                    last_sync_status,
                    records_processed_last_sync,
                    last_sync_duration,
                    last_sync_timestamp
                FROM synergo_sync.sync_state
                ORDER BY table_name
                """

                result = await session.execute(text(stats_query))
                table_stats = []

                for row in result.fetchall():
                    table_stat = {
                        'table_name': row[0],
                        'last_sync_id': row[1] or 0,
                        'total_records': row[2] or 0,
                        'last_sync_status': row[3] or 'NEVER_SYNCED',
                        'records_processed_last_sync': row[4] or 0,
                        'last_sync_duration_seconds': row[5] or 0,
                        'last_sync_timestamp': row[6].isoformat() if row[6] else None,
                        'config': self.sync_tables_config.get(row[0], {})
                    }
                    table_stats.append(table_stat)

                # Statistiques globales des dernières 24h
                global_stats_query = """
                SELECT 
                    COUNT(*) as total_syncs,
                    COALESCE(SUM(records_processed), 0) as total_records_processed,
                    COALESCE(AVG(processing_time_ms), 0) as avg_processing_time_ms,
                    COALESCE(SUM(CASE WHEN error_details IS NOT NULL THEN 1 ELSE 0 END), 0) as error_count
                FROM synergo_sync.sync_log 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                AND operation = 'SYNC_COMPLETE'
                """

                stats_result = await session.execute(text(global_stats_query))
                stats_row = stats_result.fetchone()

                global_stats = {
                    'total_syncs_24h': stats_row[0] or 0,
                    'total_records_processed_24h': stats_row[1] or 0,
                    'avg_processing_time_ms': float(stats_row[2] or 0),
                    'error_count_24h': stats_row[3] or 0
                }

                # Calculs dérivés
                total_records_all_tables = sum(t['total_records'] for t in table_stats)
                successful_tables = sum(1 for t in table_stats if t['last_sync_status'] == 'SUCCESS')

                return {
                    'table_statistics': table_stats,
                    'global_statistics_24h': global_stats,
                    'summary': {
                        'total_tables_configured': len(self.sync_tables_config),
                        'total_records_all_tables': total_records_all_tables,
                        'successful_tables': successful_tables,
                        'error_tables': len(table_stats) - successful_tables,
                        'sync_health_percentage': (successful_tables / len(table_stats) * 100) if table_stats else 0
                    },
                    'generated_at': datetime.now().isoformat()
                }

            except Exception as e:
                logger.error(f"Erreur récupération statistiques: {e}")
                return {
                    'error': str(e),
                    'generated_at': datetime.now().isoformat()
                }

    async def _trigger_analytics_refresh(self):
        """
        Déclenche le recalcul des vues analytics après sync importante
        """
        try:
            logger.info("📊 Déclenchement recalcul analytics...")

            async with get_async_session_context() as session:
                # Rafraîchir les vues matérialisées ou tables analytics
                refresh_queries = [
                    # Exemple: Refresh de la vue stock actuel
                    """
                    INSERT INTO synergo_analytics.current_stock_view 
                    (product_hfsql_id, product_name, current_stock, last_entry_date, avg_purchase_price)
                    SELECT 
                        pd.product_hfsql_id,
                        pc.name,
                        MAX(pd.stock_after_entry) as current_stock,
                        MAX(pd.entry_date) as last_entry_date,
                        AVG(pd.purchase_price) as avg_purchase_price
                    FROM synergo_core.purchase_details pd
                    JOIN synergo_core.products_catalog pc ON pd.product_hfsql_id = pc.hfsql_id
                    GROUP BY pd.product_hfsql_id, pc.name
                    ON CONFLICT (product_hfsql_id) DO UPDATE SET
                        current_stock = EXCLUDED.current_stock,
                        last_entry_date = EXCLUDED.last_entry_date,
                        avg_purchase_price = EXCLUDED.avg_purchase_price,
                        calculated_at = CURRENT_TIMESTAMP
                    """,

                    # Mise à jour des stats journalières
                    """
                    INSERT INTO synergo_analytics.daily_sales_stats 
                    (stat_date, total_sales, total_profit, transaction_count, avg_transaction)
                    SELECT 
                        DATE(so.sale_date) as stat_date,
                        SUM(so.total_amount) as total_sales,
                        SUM(so.total_profit) as total_profit,
                        COUNT(*) as transaction_count,
                        AVG(so.total_amount) as avg_transaction
                    FROM synergo_core.sales_orders so
                    WHERE so.sale_date >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY DATE(so.sale_date)
                    ON CONFLICT (stat_date) DO UPDATE SET
                        total_sales = EXCLUDED.total_sales,
                        total_profit = EXCLUDED.total_profit,
                        transaction_count = EXCLUDED.transaction_count,
                        avg_transaction = EXCLUDED.avg_transaction,
                        calculated_at = CURRENT_TIMESTAMP
                    """
                ]

                for query in refresh_queries:
                    try:
                        await session.execute(text(query))
                    except Exception as e:
                        logger.debug(f"⚠️ Erreur refresh analytics (non critique): {e}")

                await session.commit()
                logger.debug("✅ Analytics refreshées")

        except Exception as e:
            logger.warning(f"⚠️ Erreur refresh analytics: {e}")

    # ... (Les autres méthodes restent identiques: _get_sync_state, _update_sync_state, _log_sync_summary)

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
        """Données pour le dashboard de monitoring"""
        return await self.get_sync_statistics()

    async def _update_sync_state(self, session: AsyncSession, table_name: str, updates: Dict):
        """Met à jour l'état de synchronisation"""
        clean_updates = {}

        for field, value in updates.items():
            if field == 'last_sync_id':
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
                try:
                    clean_updates[field] = int(value) if value is not None else 0
                except (ValueError, TypeError):
                    clean_updates[field] = 0
            else:
                clean_updates[field] = value

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
        """Enregistre un résumé de la synchronisation"""
        async with get_async_session_context() as session:
            try:
                for result in results:
                    error_details_param = None
                    if result.error_message:
                        error_details_param = f'{{"message": "{result.error_message.replace('"', '\\"')[:500]}"}}'

                    log_entry = {
                        'table_name': result.table_name,
                        'operation': 'SYNC_COMPLETE',
                        'records_processed': int(result.records_processed),
                        'processing_time_ms': int(result.duration_ms),
                        'error_details': error_details_param
                    }

                    query = """
                    INSERT INTO synergo_sync.sync_log 
                    (table_name, operation, records_processed, processing_time_ms, error_details)
                    VALUES (:table_name, :operation, :records_processed, :processing_time_ms, :error_details)
                    """

                    await session.execute(text(query), log_entry)

                await session.commit()
                logger.debug("✅ Logs de synchronisation enregistrés")

            except Exception as e:
                logger.error(f"⚠️ Erreur insertion log: {e}")


# Fonction utilitaire pour tests
async def test_complete_sync_manager():
    """Test du sync manager complet"""
    manager = SynergoSyncManager()

    print(f"📊 Configuration ERP Complète:")
    print(f"   Tables configurées: {len(manager.sync_tables_config)}")

    for table_name, config in manager.sync_tables_config.items():
        print(f"   {config['sync_order']}. {config['table_name']} ← {config['hfsql_table']} "
              f"({config['transformer'].__name__}, {config['sync_interval_minutes']}min)")

    # Test statistiques
    stats = await manager.get_sync_statistics()
    print(f"\n📈 Statistiques actuelles:")
    print(f"   Tables configurées: {stats['summary']['total_tables_configured']}")
    print(f"   Santé sync: {stats['summary']['sync_health_percentage']:.1f}%")

    return stats


if __name__ == "__main__":
    asyncio.run(test_complete_sync_manager())