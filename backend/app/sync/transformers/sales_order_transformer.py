# backend/app/sync/transformers/sales_order_transformer.py
from typing import List, Dict, Any
from datetime import datetime, date, time
from loguru import logger
import re


class SalesOrderTransformer:
    """
    Transformateur pour les en-têtes de ventes HFSQL → PostgreSQL
    Table source: sorties → sales_orders
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """Mappage corrigé avec nouveaux champs ventes"""
        return {
            'id': 'hfsql_id',
            'date': 'sale_date',
            'heure': 'sale_time',
            'caissier': 'cashier',
            'nom_caisse': 'register_name',
            'client': 'customer',
            'type_vente': 'sale_type',
            'type_client': 'customer_type',

            # NOUVEAUX CHAMPS IMPORTANTS
            'remise': 'discount_amount',  # Remise globale
            'no_facture_chifa': 'chifa_invoice_number',  # Numéro facture CHIFA
            'majoration': 'markup_amount',  # Majoration
            'reglement_ult': 'subsequent_payment',  # Règlement ultérieur (MAJ continue)

            # Montants
            'sous_total': 'subtotal',
            'tva': 'tax_amount',
            'total_a_payer': 'total_amount',
            'encaisse': 'payment_amount',
            'monnaie': 'change_amount',

            # CHIFA
            'numero_assurance': 'insurance_number',
            'taux_couverture': 'coverage_percent',
            'reste_a_charge': 'patient_copay',

            # Stats
            'nombre_article': 'item_count',
            'benefice': 'total_profit',
            'statut': 'status',
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
                logger.error(f"❌ Erreur transformation vente ID {record.get('id', 'inconnu')}: {e}")
                continue

        logger.debug(f"✅ {len(transformed_records)}/{len(hfsql_records)} ventes transformées")
        return transformed_records

    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transformation avec gestion des nouveaux champs"""
        try:
            transformed = {}

            # Mappage standard
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # ID
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int(transformed['hfsql_id'])

            # Dates/heures
            if 'sale_date' in transformed:
                transformed['sale_date'] = self._convert_date_flexible(transformed['sale_date'])
            if 'sale_time' in transformed:
                transformed['sale_time'] = self._convert_time_flexible(transformed['sale_time'])

            # Chaînes
            string_fields = ['cashier', 'register_name', 'customer', 'sale_type', 'customer_type',
                           'chifa_invoice_number', 'insurance_number', 'status', 'notes']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # Type de vente normalisé
            if 'sale_type' in transformed:
                transformed['sale_type'] = self._normalize_sale_type(transformed['sale_type'])

            # Montants avec attention au règlement ultérieur
            money_fields = ['discount_amount', 'markup_amount', 'subsequent_payment', 'subtotal',
                          'tax_amount', 'total_amount', 'payment_amount', 'change_amount',
                          'patient_copay', 'total_profit']
            for field in money_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field])

            # Règlement ultérieur - Champ qui change souvent
            if 'subsequent_payment' in transformed:
                # Ce champ peut être mis à jour fréquemment
                subsequent = transformed['subsequent_payment']
                if subsequent and subsequent > 0:
                    logger.debug(f"💳 Règlement ultérieur détecté: {subsequent}")

            # Pourcentages
            percent_fields = ['coverage_percent']
            for field in percent_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field], max_value=100.0)

            # Quantités
            if 'item_count' in transformed:
                transformed['item_count'] = self._convert_to_int(transformed['item_count'])

            # Métadonnées
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation vente: {e}")
            raise

    def _convert_date_flexible(self, date_value: Any) -> date:
        """Convertit une date de différents formats vers date Python"""
        try:
            if not date_value:
                return date.today()

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

            logger.warning(f"⚠️ Format de date non reconnu: {date_value}, utilisation date actuelle")
            return date.today()

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion date {date_value}: {e}, utilisation date actuelle")
            return date.today()

    def _convert_time_flexible(self, time_value: Any) -> time:
        """Convertit une heure de différents formats vers time Python"""
        try:
            if not time_value:
                return time(0, 0, 0)

            if isinstance(time_value, time):
                return time_value

            if isinstance(time_value, datetime):
                return time_value.time()

            time_str = str(time_value).strip()

            # Si c'est un timestamp complet, extraire seulement l'heure
            if '+' in time_str or 'T' in time_str:
                try:
                    dt = self._parse_datetime_value(time_value)
                    if dt:
                        return dt.time()
                except:
                    pass

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

            logger.warning(f"⚠️ Format d'heure non reconnu: {time_value}, utilisation 00:00:00")
            return time(0, 0, 0)

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion heure {time_value}: {e}, utilisation 00:00:00")
            return time(0, 0, 0)

    def _parse_datetime_value(self, dt_value: Any) -> datetime:
        """Parse une valeur datetime de différents formats"""
        try:
            if isinstance(dt_value, datetime):
                return dt_value

            if isinstance(dt_value, str):
                dt_str = dt_value.strip()

                # Format ISO avec timezone
                if '+' in dt_str:
                    dt_str = dt_str.split('+')[0]

                if 'T' in dt_str:
                    return datetime.fromisoformat(dt_str.replace('T', ' '))
                else:
                    return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

            return None

        except Exception:
            return None

    def _normalize_sale_type(self, sale_type: str) -> str:
        """Normalise le type de vente"""
        if not sale_type:
            return "LIBRE"

        sale_type_upper = sale_type.upper().strip()

        # Mapping des variantes
        chifa_variants = ['CHIFA', 'CNAC', 'ASSURANCE', 'SECURITE_SOCIALE']
        libre_variants = ['LIBRE', 'PRIVE', 'CASH', 'NORMAL']

        if any(variant in sale_type_upper for variant in chifa_variants):
            return "CHIFA"
        elif any(variant in sale_type_upper for variant in libre_variants):
            return "LIBRE"
        else:
            return sale_type_upper

    def _clean_string(self, value: Any) -> str:
        """Nettoie une chaîne de caractères"""
        if not value:
            return ""

        cleaned = str(value).strip()
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

        # Limitation de longueur selon le champ
        max_length = 255
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned

    def _convert_to_decimal(self, value: Any, max_value: float = None) -> float:
        """Convertit une valeur vers un décimal"""
        try:
            if value is None or value == '':
                return 0.0

            if isinstance(value, (int, float)):
                result = float(value)
            elif isinstance(value, str):
                cleaned = re.sub(r'[^\d.,-]', '', value.strip())
                cleaned = cleaned.replace(',', '.')
                result = float(cleaned) if cleaned else 0.0
            else:
                result = 0.0

            # Validation de la valeur max (pour les pourcentages)
            if max_value is not None and result > max_value:
                logger.debug(f"⚠️ Valeur supérieure au max ({max_value}): {result}")
                result = max_value

            return result

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
        """Valide qu'un enregistrement de vente transformé est correct"""
        try:
            # Vérifications obligatoires
            required_fields = ['hfsql_id', 'sale_date']

            for field in required_fields:
                if field not in record or record[field] is None:
                    logger.warning(f"⚠️ Champ obligatoire manquant: {field}")
                    return False

            # Vérification que hfsql_id est un entier positif
            if not isinstance(record['hfsql_id'], int) or record['hfsql_id'] <= 0:
                logger.warning(f"⚠️ hfsql_id invalide: {record['hfsql_id']}")
                return False

            # Vérification que sale_date est valide
            if not isinstance(record['sale_date'], date):
                logger.warning(f"⚠️ sale_date invalide: {record['sale_date']}")
                return False

            # Vérification des montants (warnings seulement)
            money_fields = ['total_amount', 'payment_amount']
            for field in money_fields:
                if field in record and isinstance(record[field], (int, float)):
                    if record[field] < 0:
                        logger.debug(f"💰 Montant négatif détecté: {field} = {record[field]}")

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation vente: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """Exemple de transformation pour tests"""
        sample_hfsql = {
            'id': 12345,
            'date': '20241210',
            'heure': '143022',
            'caissier': 'Marie DUPONT',
            'nom_caisse': 'CAISSE_1',
            'client': 'MARTIN Jean',
            'type_vente': 'CHIFA',
            'total_a_payer': '45.50',
            'encaisse': '50.00',
            'monnaie': '4.50',
            'numero_assurance': '123456789',
            'taux_couverture': '80.0',
            'reste_a_charge': '9.10',
            'nombre_article': '3',
            'benefice': '12.30',
            'statut': 'TERMINEE'
        }

        expected_output = {
            'hfsql_id': 12345,
            'sale_date': date(2024, 12, 10),
            'sale_time': time(14, 30, 22),
            'cashier': 'Marie DUPONT',
            'register_name': 'CAISSE_1',
            'customer': 'MARTIN Jean',
            'sale_type': 'CHIFA',
            'total_amount': 45.50,
            'payment_amount': 50.00,
            'change_amount': 4.50,
            'insurance_number': '123456789',
            'coverage_percent': 80.0,
            'patient_copay': 9.10,
            'item_count': 3,
            'total_profit': 12.30,
            'status': 'COMPLETED',
            'sync_version': 1,
            'last_synced_at': datetime.now(),
            'created_at': datetime.now()
        }

        return {
            'input_sample': sample_hfsql,
            'expected_output': expected_output,
            'field_mapping': self.field_mapping,
            'sale_type_normalization': {
                'CHIFA': ['CHIFA', 'CNAC', 'ASSURANCE'],
                'LIBRE': ['LIBRE', 'PRIVE', 'CASH']
            }
        }


# Test du transformer
async def test_sales_order_transformer():
    """Test du transformer de ventes"""
    transformer = SalesOrderTransformer()

    sample_data = [
        {
            'id': 12345,
            'date': '20241210',
            'heure': '143022',
            'caissier': 'Marie',
            'client': 'MARTIN Jean',
            'type_vente': 'CHIFA',
            'total_a_payer': '45.50',
            'benefice': '12.30'
        },
        {
            'id': 12346,
            'date': '2024-12-10',
            'heure': '15:45:30',
            'caissier': 'Ahmed',
            'client': 'BERNARD Paul',
            'type_vente': 'LIBRE',
            'total_a_payer': '22.80',
            'benefice': '8.15'
        }
    ]

    transformed = await transformer.transform_batch(sample_data)

    print(f"📊 Test Sales Order Transformer:")
    print(f"   Entrée: {len(sample_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    if transformed:
        print(f"   Première vente transformée:")
        for key, value in transformed[0].items():
            if key not in ['last_synced_at', 'created_at']:
                print(f"     - {key}: {value}")

        # Vérification normalisation type de vente
        for record in transformed:
            if 'sale_type' in record:
                print(f"   Type vente normalisé: {record['sale_type']}")

    return transformed


if __name__ == "__main__":
    import asyncio
    from datetime import date, time

    asyncio.run(test_sales_order_transformer())