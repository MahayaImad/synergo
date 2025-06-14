# backend/app/sync/strategies/id_based_sync.py - VERSION CORRIGÉE
from typing import List, Dict, Any, Optional
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ...utils.hfsql_connector import HFSQLConnector


class IdBasedSyncStrategy:
    """
    Stratégie de synchronisation basée sur l'ID croissant

    Principe:
    1. Garde en mémoire le dernier ID synchronisé
    2. Récupère uniquement les enregistrements avec ID > last_sync_id
    3. Optimisé pour les tables avec auto-increment
    """

    def __init__(self, config: Dict[str, Any], hfsql_connector: HFSQLConnector):
        self.table_name = config['table_name']
        self.hfsql_table = config['hfsql_table']
        self.id_field = config.get('id_field', 'id')
        self.batch_size = config.get('batch_size', 1000)
        self.schema = config.get('schema', 'synergo_core')
        self.hfsql_connector = hfsql_connector

    async def get_new_records(self, last_sync_id: int = 0) -> List[Dict[str, Any]]:
        """
        Récupère les nouveaux enregistrements depuis le dernier ID synchronisé
        """
        try:
            logger.debug(f"🔍 Recherche nouveaux enregistrements {self.hfsql_table} depuis ID {last_sync_id}")

            # Requête optimisée pour récupérer uniquement les nouveaux
            query = f"""
            SELECT * FROM {self.hfsql_table}
            WHERE {self.id_field} > {last_sync_id}
            ORDER BY {self.id_field} ASC
            LIMIT {self.batch_size}
            """

            records = await self.hfsql_connector.execute_query(query)

            if records:
                first_id = records[0][self.id_field]
                last_id = records[-1][self.id_field]
                logger.debug(f"✅ {len(records)} nouveaux enregistrements trouvés (ID {first_id} à {last_id})")
            else:
                logger.debug("📌 Aucun nouvel enregistrement")

            return records

        except Exception as e:
            logger.error(f"❌ Erreur récupération nouveaux enregistrements: {e}")
            raise

    async def get_hfsql_max_id(self) -> int:
        """Récupère l'ID maximum de la table HFSQL"""
        try:
            query = f"SELECT MAX({self.id_field}) as max_id FROM {self.hfsql_table}"
            result = await self.hfsql_connector.execute_query(query)
            return result[0]['max_id'] if result and result[0]['max_id'] else 0
        except Exception as e:
            logger.error(f"❌ Erreur récupération max ID: {e}")
            return 0

    async def get_postgres_max_hfsql_id(self, session: AsyncSession) -> int:
        """Récupère le dernier hfsql_id synchronisé dans PostgreSQL"""
        try:
            query = f"SELECT MAX(hfsql_id) as max_hfsql_id FROM {self.schema}.{self.table_name}"
            result = await session.execute(text(query))
            row = result.fetchone()
            return row[0] if row and row[0] else 0
        except Exception as e:
            logger.error(f"❌ Erreur récupération max hfsql_id PostgreSQL: {e}")
            return 0

    async def validate_sync_integrity(self, session: AsyncSession) -> Dict[str, Any]:
        """
        Valide l'intégrité de la synchronisation
        Compare les comptes et derniers IDs entre HFSQL et PostgreSQL
        """
        try:
            # Compter les enregistrements PostgreSQL
            pg_query = f"SELECT COUNT(*) as count, MAX(hfsql_id) as max_id FROM {self.schema}.{self.table_name}"
            pg_result = await session.execute(text(pg_query))
            pg_row = pg_result.fetchone()
            pg_count = pg_row[0] if pg_row else 0
            pg_max_id = pg_row[1] if pg_row else 0

            # Compter les enregistrements HFSQL
            hfsql_count_query = f"SELECT COUNT(*) as count FROM {self.hfsql_table}"
            hfsql_count_result = await self.hfsql_connector.execute_query(hfsql_count_query)
            hfsql_count = hfsql_count_result[0]['count'] if hfsql_count_result else 0

            # ID maximum HFSQL
            hfsql_max_id = await self.get_hfsql_max_id()

            # Calcul de l'écart
            count_difference = hfsql_count - pg_count
            id_difference = hfsql_max_id - pg_max_id

            sync_integrity = {
                'hfsql_count': hfsql_count,
                'postgres_count': pg_count,
                'count_difference': count_difference,
                'hfsql_max_id': hfsql_max_id,
                'postgres_max_hfsql_id': pg_max_id,
                'id_difference': id_difference,
                'is_synchronized': count_difference == 0 and id_difference == 0,
                'needs_sync': id_difference > 0
            }

            if sync_integrity['is_synchronized']:
                logger.info(f"✅ {self.table_name}: Synchronisation intègre ({hfsql_count} enregistrements)")
            else:
                logger.warning(
                    f"⚠️ {self.table_name}: {count_difference} enregistrements de différence, {id_difference} IDs de retard")

            return sync_integrity

        except Exception as e:
            logger.error(f"❌ Erreur validation intégrité: {e}")
            return {
                'error': str(e),
                'is_synchronized': False,
                'needs_sync': True
            }

    async def repair_sync_gaps(self, session: AsyncSession, gap_size_limit: int = 10000) -> Dict[str, Any]:
        """
        Répare les écarts de synchronisation en récupérant les enregistrements manquants
        """
        try:
            integrity = await self.validate_sync_integrity(session)

            if integrity.get('is_synchronized', False):
                return {'status': 'no_repair_needed', 'message': 'Synchronisation déjà intègre'}

            id_difference = integrity.get('id_difference', 0)

            if id_difference > gap_size_limit:
                return {
                    'status': 'gap_too_large',
                    'message': f'Écart trop important ({id_difference} enregistrements). Utiliser la synchronisation initiale.',
                    'gap_size': id_difference
                }

            # Récupérer et synchroniser les enregistrements manquants
            pg_max_id = integrity.get('postgres_max_hfsql_id', 0)
            missing_records = await self.get_new_records(pg_max_id)

            if missing_records:
                # On assume que les enregistrements sont déjà transformés
                # Dans un cas réel, il faudrait appliquer le transformer
                inserted_count = await self.insert_records(session, missing_records)

                return {
                    'status': 'repaired',
                    'message': f'{inserted_count} enregistrements manquants récupérés',
                    'records_inserted': inserted_count
                }
            else:
                return {'status': 'no_missing_records', 'message': 'Aucun enregistrement manquant trouvé'}

        except Exception as e:
            logger.error(f"❌ Erreur réparation sync: {e}")
            return {'status': 'error', 'message': str(e)}

    async def insert_records(self, session: AsyncSession, records: List[Dict[str, Any]]) -> int:
        """
        Insère les enregistrements transformés dans PostgreSQL - VERSION CORRIGÉE
        """
        if not records:
            return 0

        try:
            logger.debug(f"💾 Insertion de {len(records)} enregistrements dans {self.schema}.{self.table_name}")

            # Nettoyer et valider les données avant insertion
            clean_records = []
            for record in records:
                clean_record = self._clean_record_for_insert(record)
                if clean_record:
                    clean_records.append(clean_record)

            if not clean_records:
                logger.warning("⚠️ Aucun enregistrement valide après nettoyage")
                return 0

            # Construire la requête d'insertion dynamiquement
            sample_record = clean_records[0]
            columns = list(sample_record.keys())
            placeholders = [f":{col}" for col in columns]

            # CORRECTION: Exclure last_synced_at et created_at des champs mis à jour
            # pour éviter les doublons dans la clause UPDATE
            excluded_from_update = ['hfsql_id', 'last_synced_at', 'created_at']
            update_columns = [col for col in columns if col not in excluded_from_update]

            # Requête avec ON CONFLICT pour éviter les doublons - VERSION CORRIGÉE
            insert_query = f"""
            INSERT INTO {self.schema}.{self.table_name} 
            ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (hfsql_id) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])},
            last_synced_at = CURRENT_TIMESTAMP
            """

            # Exécution en batch pour performance
            await session.execute(text(insert_query), clean_records)

            logger.debug(f"✅ {len(clean_records)} enregistrements insérés/mis à jour")
            return len(clean_records)

        except Exception as e:
            logger.error(f"❌ Erreur insertion enregistrements: {e}")
            # Log des détails pour debug
            if records:
                logger.debug(f"Premier enregistrement problématique: {records[0]}")
            raise

    def _clean_record_for_insert(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Nettoie un enregistrement pour l'insertion PostgreSQL
        """
        try:
            clean_record = {}

            for key, value in record.items():
                # Conversion des types pour PostgreSQL
                if key == 'hfsql_id':
                    # S'assurer que l'ID est un entier
                    if isinstance(value, str):
                        try:
                            clean_record[key] = int(value)
                        except ValueError:
                            logger.warning(f"⚠️ ID invalide ignoré: {value}")
                            return None
                    elif isinstance(value, (int, float)):
                        clean_record[key] = int(value)
                    else:
                        logger.warning(f"⚠️ Type ID non supporté: {type(value)}")
                        return None

                elif key in ['total_amount', 'payment_amount', 'profit', 'price_buy', 'price_sell', 'margin']:
                    # Montants en decimal
                    if value is not None:
                        try:
                            clean_record[key] = float(value) if value != '' else 0.0
                        except (ValueError, TypeError):
                            clean_record[key] = 0.0
                    else:
                        clean_record[key] = 0.0

                elif key in ['item_count', 'sync_version', 'alert_quantity', 'current_stock']:
                    # Entiers
                    if value is not None:
                        try:
                            clean_record[key] = int(value) if value != '' else 0
                        except (ValueError, TypeError):
                            clean_record[key] = 0
                    else:
                        clean_record[key] = 0

                elif key in ['customer', 'cashier', 'register_name', 'name', 'barcode', 'family', 'supplier']:
                    # Chaînes de caractères
                    clean_record[key] = str(value).strip() if value is not None else ''

                elif key in ['sale_date', 'sale_time', 'last_synced_at', 'created_at']:
                    # Dates et heures - laisser tel quel si valides
                    clean_record[key] = value

                else:
                    # Autres champs - copie directe
                    clean_record[key] = value

            # Vérification finale de l'ID
            if 'hfsql_id' not in clean_record or clean_record['hfsql_id'] <= 0:
                logger.warning(f"⚠️ Enregistrement sans ID valide ignoré")
                return None

            return clean_record

        except Exception as e:
            logger.error(f"❌ Erreur nettoyage enregistrement: {e}")
            return None


# Classe de base pour autres stratégies futures
class BaseSyncStrategy:
    """Interface de base pour les stratégies de synchronisation"""

    def __init__(self, config: Dict[str, Any], hfsql_connector: HFSQLConnector):
        self.config = config
        self.hfsql_connector = hfsql_connector

    async def get_new_records(self, last_sync_point: Any) -> List[Dict[str, Any]]:
        """Récupère les nouveaux enregistrements à synchroniser"""
        raise NotImplementedError

    async def insert_records(self, session: AsyncSession, records: List[Dict[str, Any]]) -> int:
        """Insère les enregistrements dans PostgreSQL"""
        raise NotImplementedError

    async def validate_sync_integrity(self, session: AsyncSession) -> Dict[str, Any]:
        """Valide l'intégrité de la synchronisation"""
        raise NotImplementedError