# backend/app/sync/transformers/purchase_order_transformer.py
from typing import List, Dict, Any
from datetime import datetime, date, time
from loguru import logger
import re


class PurchaseOrderTransformer:
    """
    Transformateur pour les en-têtes d'achats HFSQL → PostgreSQL
    Table source: entrees → purchase_orders
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """Mappage corrigé avec nouveaux champs"""
        return {
            'id': 'hfsql_id',
            'date_commande': 'order_date',
            'heure_commande': 'order_time',
            'fournisseur': 'supplier',
            'reference': 'reference',

            # TYPE CRUCIAL A/AV
            'type': 'order_type',  # A ou AV

            # Champs avoirs (quand type = AV)
            'num_av': 'related_invoice_number',  # Numéro facture liée
            'motif': 'return_reason',  # Motif du retour

            # TOUS LES MONTANTS
            'sous_total_ht': 'subtotal_ht',
            'tva': 'tax_amount',
            'remise': 'discount_amount',
            'total_ttc': 'total_ttc',
            'montant_total': 'total_amount',

            # Autres champs
            'date_livraison': 'delivery_date',
            'numero_facture': 'invoice_number',
            'statut': 'status',
            'utilisateur': 'created_by',
            'notes': 'notes',
        }
    async def transform_batch(self, hfsql_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforme un lot d'enregistrements HFSQL vers le format PostgreSQL
        """
        transformed_records = []

        for record in hfsql_records:
            try:
                transformed = await self.transform_single_record(record)
                if transformed:
                    transformed_records.append(transformed)
            except Exception as e:
                logger.error(f"❌ Erreur transformation commande ID {record.get('id', 'inconnu')}: {e}")
                continue

        logger.debug(f"✅ {len(transformed_records)}/{len(hfsql_records)} commandes transformées")
        return transformed_records



    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transformation avec gestion types A/AV"""
        try:
            transformed = {}

            # 1. Mappage standard
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Conversion de l'ID
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int(transformed['hfsql_id'])

            # 3. Validation et nettoyage du type
            if 'order_type' in transformed:
                order_type = str(transformed['order_type']).strip().upper()
                if order_type not in ['A', 'AV']:
                    logger.warning(f"⚠️ Type commande invalide: {order_type}, utilisation 'A'")
                    transformed['order_type'] = 'A'
                else:
                    transformed['order_type'] = order_type
            else:
                transformed['order_type'] = 'A'  # Par défaut

            # 4. Dates
            if 'order_date' in transformed:
                transformed['order_date'] = self._convert_date_flexible(transformed['order_date'])

            if 'delivery_date' in transformed:
                transformed['delivery_date'] = self._convert_date_flexible(transformed['delivery_date'])

            # 5. Heures
            if 'order_time' in transformed:
                transformed['order_time'] = self._convert_time_flexible(transformed['order_time'])

            # 6. Nettoyage chaînes
            string_fields = ['supplier', 'reference', 'related_invoice_number', 'invoice_number', 'status', 'created_by', 'notes', 'return_reason']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # 7. Conversion montants avec précision
            money_fields = ['subtotal_ht', 'tax_amount', 'discount_amount', 'total_ttc', 'total_amount']
            for field in money_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field])

            # 8. Validation spécifique aux avoirs
            if transformed.get('order_type') == 'AV':
                # Pour les avoirs, s'assurer qu'on a les infos nécessaires
                if not transformed.get('related_invoice_number'):
                    logger.debug(f"⚠️ Avoir sans numéro facture liée: {transformed.get('hfsql_id')}")

            # 9. Métadonnées sync
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 10. Validation finale
            if not self._validate_transformed_record(transformed):
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation commande: {e}")
            raise

    def _convert_date_flexible(self, date_value: Any) -> date:
        """Convertit une date de différents formats vers date Python"""
        try:
            if not date_value:
                return None

            if isinstance(date_value, date):
                return date_value

            if isinstance(date_value, datetime):
                return date_value.date()

            date_str = str(date_value).strip()

            # Format YYYYMMDD (HFSQL)
            if len(date_str) == 8 and date_str.isdigit():
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return date(year, month, day)

            # Format ISO (YYYY-MM-DD)
            if '-' in date_str and len(date_str) >= 10:
                date_part = date_str[:10]
                return datetime.strptime(date_part, '%Y-%m-%d').date()

            # Format français (DD/MM/YYYY)
            if '/' in date_str:
                return datetime.strptime(date_str, '%d/%m/%Y').date()

            logger.warning(f"⚠️ Format de date non reconnu: {date_value}")
            return None

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion date {date_value}: {e}")
            return None

    def _convert_time_flexible(self, time_value: Any) -> time:
        """Convertit une heure de différents formats vers time Python"""
        try:
            if not time_value:
                return None

            if isinstance(time_value, time):
                return time_value

            if isinstance(time_value, datetime):
                return time_value.time()

            time_str = str(time_value).strip()

            # Format HHMMSS (HFSQL)
            if len(time_str) == 6 and time_str.isdigit():
                hour = int(time_str[:2])
                minute = int(time_str[2:4])
                second = int(time_str[4:6])
                return time(hour, minute, second)

            # Format HH:MM:SS
            if ':' in time_str:
                time_parts = time_str.split(':')
                if len(time_parts) >= 2:
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
                    second = int(time_parts[2]) if len(time_parts) > 2 else 0
                    return time(hour, minute, second)

            logger.warning(f"⚠️ Format d'heure non reconnu: {time_value}")
            return None

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion heure {time_value}: {e}")
            return None

    def _clean_string(self, value: Any) -> str:
        """Nettoie une chaîne de caractères"""
        if not value:
            return ""

        cleaned = str(value).strip()
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

        # Limitation selon le type de champ
        max_length = 255
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned

    def _convert_to_decimal(self, value: Any) -> float:
        """Convertit une valeur vers un décimal"""
        try:
            if value is None or value == '':
                return 0.0

            if isinstance(value, (int, float)):
                return float(value)

            if isinstance(value, str):
                cleaned = re.sub(r'[^\d.,-]', '', value.strip())
                cleaned = cleaned.replace(',', '.')
                if cleaned:
                    return float(cleaned)

            return 0.0

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion décimal {value}: {e}")
            return 0.0

    def _convert_to_int(self, value: Any) -> int:
        """Convertit une valeur vers un entier"""
        try:
            if value is None or value == '':
                return 0

            if isinstance(value, int):
                return value

            if isinstance(value, float):
                return int(round(value))

            if isinstance(value, str):
                cleaned = re.sub(r'[^\d-]', '', value.strip())
                if cleaned and cleaned != '-':
                    return int(cleaned)

            return 0

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion entier {value}: {e}")
            return 0

    def _validate_transformed_record(self, record: Dict[str, Any]) -> bool:
        """Validation avec vérification type A/AV"""
        if not record.get('hfsql_id') or record['hfsql_id'] <= 0:
            return False

        if record.get('order_type') not in ['A', 'AV']:
            return False

        return True
    def get_sample_transformation(self) -> Dict[str, Any]:
        """Exemple de transformation pour tests"""
        sample_hfsql = {
            'id': 789,
            'date_commande': '20241210',
            'heure_commande': '143000',
            'fournisseur': 'CERP ROUEN',
            'reference': 'CMD20241210-001',
            'montant_total': '1250.75',
            'statut': 'LIVREE',
            'date_livraison': '20241212',
            'numero_facture': 'FACT-001234',
            'utilisateur': 'admin',
            'notes': 'Livraison urgente'
        }

        expected_output = {
            'hfsql_id': 789,
            'order_date': date(2024, 12, 10),
            'order_time': time(14, 30, 0),
            'supplier': 'CERP ROUEN',
            'reference': 'CMD20241210-001',
            'total_amount': 1250.75,
            'status': 'LIVREE',
            'delivery_date': date(2024, 12, 12),
            'invoice_number': 'FACT-001234',
            'created_by': 'admin',
            'notes': 'Livraison urgente',
            'sync_version': 1,
            'last_synced_at': datetime.now(),
            'created_at': datetime.now()
        }

        return {
            'input_sample': sample_hfsql,
            'expected_output': expected_output,
            'field_mapping': self.field_mapping
        }


# Test du transformer
async def test_purchase_order_transformer():
    """Test du transformer de commandes"""
    transformer = PurchaseOrderTransformer()

    sample_data = [
        {
            'id': 789,
            'date_commande': '20241210',
            'fournisseur': 'CERP ROUEN',
            'montant_total': '1250.75',
            'statut': 'EN_COURS'
        },
        {
            'id': 790,
            'date_commande': '2024-12-11',
            'fournisseur': 'PHOENIX',
            'montant_total': '875.20',
            'statut': 'LIVREE'
        }
    ]

    transformed = await transformer.transform_batch(sample_data)

    print(f"📊 Test Purchase Order Transformer:")
    print(f"   Entrée: {len(sample_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    if transformed:
        print(f"   Première commande transformée:")
        for key, value in transformed[0].items():
            if key not in ['last_synced_at', 'created_at']:
                print(f"     - {key}: {value}")

    return transformed


if __name__ == "__main__":
    import asyncio
    from datetime import date, time

    asyncio.run(test_purchase_order_transformer())