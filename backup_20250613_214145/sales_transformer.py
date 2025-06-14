# backend/app/sync/transformers/sales_transformer.py - VERSION CORRIGÉE
from typing import List, Dict, Any
from datetime import datetime, date, time
from loguru import logger
import re


class SalesTransformer:
    """
    Transformateur pour les données de ventes HFSQL → PostgreSQL
    VERSION CORRIGÉE avec gestion robuste des types
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """Mappage des champs HFSQL → PostgreSQL"""
        return {
            'id': 'hfsql_id',
            'date': 'sale_date',
            'heure': 'sale_time',
            'client': 'customer',
            'caissier': 'cashier',
            'nom_caisse': 'register_name',
            'total_a_payer': 'total_amount',
            'encaisse': 'payment_amount',
            'benefice': 'profit',
            'nombre_article': 'item_count'
        }

    async def transform_batch(self, hfsql_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforme un lot d'enregistrements HFSQL vers PostgreSQL"""
        transformed_records = []
        error_count = 0

        for i, record in enumerate(hfsql_records):
            try:
                transformed = await self.transform_single_record(record)
                if transformed:
                    transformed_records.append(transformed)
                else:
                    error_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Erreur transformation vente #{i + 1} ID {record.get('id', 'inconnu')}: {e}")
                continue

        success_rate = (len(transformed_records) / len(hfsql_records)) * 100 if hfsql_records else 0
        logger.info(f"✅ Transformation: {len(transformed_records)}/{len(hfsql_records)} réussies ({success_rate:.1f}%)")

        if error_count > 0:
            logger.warning(f"⚠️ {error_count} erreurs de transformation détectées")

        return transformed_records

    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transforme un enregistrement de vente - VERSION ROBUSTE"""
        try:
            transformed = {}

            # 1. Mappage des champs avec vérification
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. CORRECTION CRITIQUE - Conversion de l'ID en premier
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int_robust(transformed['hfsql_id'])
                if transformed['hfsql_id'] <= 0:
                    logger.warning(f"⚠️ ID invalide détecté: {hfsql_record.get('id')}")
                    return None

            # 3. Gestion des dates/heures avec fallback
            if 'date' in hfsql_record:
                transformed['sale_date'] = self._convert_date_robust(hfsql_record['date'])

            if 'heure' in hfsql_record:
                transformed['sale_time'] = self._convert_time_robust(hfsql_record['heure'])

            # Gestion datetime combiné
            if 'datetime' in hfsql_record and 'date' not in hfsql_record:
                dt = self._parse_datetime_robust(hfsql_record['datetime'])
                if dt:
                    transformed['sale_date'] = dt.date()
                    transformed['sale_time'] = dt.time()

            # 4. Nettoyage des chaînes
            string_fields = ['customer', 'cashier', 'register_name']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string_robust(transformed[field])

            # 5. Conversion des montants avec gestion erreurs
            money_fields = ['total_amount', 'payment_amount', 'profit']
            for field in money_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal_robust(transformed[field])

            # 6. Conversion des entiers
            int_fields = ['item_count']
            for field in int_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int_robust(transformed[field])

            # 7. Métadonnées
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 8. Validation finale stricte
            if not self._validate_record_strict(transformed, hfsql_record):
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation: {e}")
            logger.debug(f"Record problématique: {hfsql_record}")
            return None

    def _convert_to_int_robust(self, value: Any) -> int:
        """Conversion entier ultra-robuste"""
        try:
            if value is None or value == '':
                return 0

            # Déjà un entier
            if isinstance(value, int):
                return max(0, value)  # Pas d'entiers négatifs pour les IDs

            # Float vers int
            if isinstance(value, float):
                if value != value:  # NaN check
                    return 0
                return max(0, int(round(value)))

            # String processing
            if isinstance(value, str):
                cleaned = value.strip()

                # Vide après nettoyage
                if not cleaned:
                    return 0

                # Supprimer tout sauf chiffres et signe moins
                numeric_only = re.sub(r'[^0-9\-]', '', cleaned)

                if numeric_only and numeric_only != '-':
                    result = int(numeric_only)
                    return max(0, result) if 'id' in str(value).lower() else result

                return 0

            # Tentative conversion directe
            result = int(float(str(value)))
            return max(0, result)

        except (ValueError, TypeError, OverflowError) as e:
            logger.debug(f"Conversion int échouée pour {value}: {e}")
            return 0

    def _convert_date_robust(self, date_value: Any) -> date:
        """Conversion date ultra-robuste"""
        try:
            if not date_value:
                return date.today()

            # Déjà une date
            if isinstance(date_value, date):
                return date_value

            # Datetime vers date
            if isinstance(date_value, datetime):
                return date_value.date()

            # String processing
            date_str = str(date_value).strip()

            # Format HFSQL YYYYMMDD (priorité)
            if len(date_str) == 8 and date_str.isdigit():
                try:
                    year = int(date_str[:4])
                    month = int(date_str[4:6])
                    day = int(date_str[6:8])

                    # Validation des ranges
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return date(year, month, day)
                except ValueError:
                    pass

            # Format ISO YYYY-MM-DD
            iso_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', date_str)
            if iso_match:
                try:
                    year, month, day = map(int, iso_match.groups())
                    return date(year, month, day)
                except ValueError:
                    pass

            # Format français DD/MM/YYYY
            fr_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
            if fr_match:
                try:
                    day, month, year = map(int, fr_match.groups())
                    return date(year, month, day)
                except ValueError:
                    pass

            # Timestamp avec T ou +
            if 'T' in date_str or '+' in date_str:
                try:
                    clean_date = date_str.split('T')[0] if 'T' in date_str else date_str.split('+')[0]
                    return datetime.fromisoformat(clean_date).date()
                except:
                    pass

            # Fallback: date actuelle avec warning
            logger.warning(f"⚠️ Format date non reconnu: {date_value}, utilisation date actuelle")
            return date.today()

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion date {date_value}: {e}")
            return date.today()

    def _convert_time_robust(self, time_value: Any) -> time:
        """Conversion heure ultra-robuste"""
        try:
            if not time_value:
                return time(0, 0, 0)

            # Déjà un time
            if isinstance(time_value, time):
                return time_value

            # Datetime vers time
            if isinstance(time_value, datetime):
                return time_value.time()

            # String processing
            time_str = str(time_value).strip()

            # Extraction depuis timestamp complet
            if 'T' in time_str or '+' in time_str:
                try:
                    dt = self._parse_datetime_robust(time_value)
                    if dt:
                        return dt.time()
                except:
                    pass

            # Format HFSQL HHMMSS
            if len(time_str) == 6 and time_str.isdigit():
                try:
                    hour = int(time_str[:2])
                    minute = int(time_str[2:4])
                    second = int(time_str[4:6])

                    # Validation
                    if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                        return time(hour, minute, second)
                except ValueError:
                    pass

            # Format HHMM
            if len(time_str) == 4 and time_str.isdigit():
                try:
                    hour = int(time_str[:2])
                    minute = int(time_str[2:4])
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        return time(hour, minute, 0)
                except ValueError:
                    pass

            # Format HH:MM:SS ou HH:MM
            time_match = re.match(r'^(\d{1,2}):(\d{2})(?::(\d{2}))?', time_str)
            if time_match:
                try:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    second = int(time_match.group(3) or 0)

                    if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                        return time(hour, minute, second)
                except ValueError:
                    pass

            # Fallback
            logger.debug(f"Format heure non reconnu: {time_value}")
            return time(0, 0, 0)

        except Exception as e:
            logger.debug(f"Erreur conversion heure {time_value}: {e}")
            return time(0, 0, 0)

    def _parse_datetime_robust(self, dt_value: Any) -> datetime:
        """Parse datetime robuste"""
        try:
            if isinstance(dt_value, datetime):
                return dt_value

            if isinstance(dt_value, str):
                dt_str = dt_value.strip()

                # Supprimer timezone
                if '+' in dt_str:
                    dt_str = dt_str.split('+')[0]

                # Format ISO
                if 'T' in dt_str:
                    return datetime.fromisoformat(dt_str.replace('T', ' '))

                # Format standard
                return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

            return None

        except Exception:
            return None

    def _clean_string_robust(self, value: Any) -> str:
        """Nettoyage chaîne ultra-robuste"""
        if not value:
            return ""

        try:
            cleaned = str(value).strip()

            # Supprimer caractères de contrôle
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

            # Supprimer espaces multiples
            cleaned = re.sub(r'\s+', ' ', cleaned)

            # Limiter longueur
            if len(cleaned) > 255:
                cleaned = cleaned[:255]
                logger.debug(f"Chaîne tronquée à 255 caractères")

            return cleaned

        except Exception as e:
            logger.warning(f"Erreur nettoyage chaîne {value}: {e}")
            return ""

    def _convert_to_decimal_robust(self, value: Any) -> float:
        """Conversion décimal ultra-robuste"""
        try:
            if value is None or value == '':
                return 0.0

            # Déjà un nombre
            if isinstance(value, (int, float)):
                if isinstance(value, float) and value != value:  # NaN check
                    return 0.0
                return float(value)

            # String processing
            if isinstance(value, str):
                cleaned = value.strip()

                if not cleaned:
                    return 0.0

                # Supprimer tout sauf chiffres, points, virgules, signes
                cleaned = re.sub(r'[^\d.,-]', '', cleaned)

                # Gérer virgule française
                if ',' in cleaned and '.' not in cleaned:
                    cleaned = cleaned.replace(',', '.')
                elif ',' in cleaned and '.' in cleaned:
                    # Format 1.234,56 -> 1234.56
                    if cleaned.rfind(',') > cleaned.rfind('.'):
                        cleaned = cleaned.replace('.', '').replace(',', '.')

                if cleaned and cleaned not in ['.', '-', '-.']:
                    try:
                        return float(cleaned)
                    except ValueError:
                        pass

            return 0.0

        except Exception as e:
            logger.debug(f"Erreur conversion décimal {value}: {e}")
            return 0.0

    def _validate_record_strict(self, record: Dict[str, Any], original: Dict[str, Any]) -> bool:
        """Validation stricte avec correction automatique"""
        try:
            # 1. hfsql_id obligatoire et valide
            if 'hfsql_id' not in record:
                logger.warning(f"❌ hfsql_id manquant")
                return False

            hfsql_id = record['hfsql_id']
            if not isinstance(hfsql_id, int) or hfsql_id <= 0:
                logger.warning(f"❌ hfsql_id invalide: {hfsql_id} (type: {type(hfsql_id)})")
                return False

            # 2. Correction automatique des dates
            if 'sale_date' in record and not isinstance(record['sale_date'], date):
                logger.debug("Correction automatique sale_date")
                record['sale_date'] = date.today()

            if 'sale_time' in record and not isinstance(record['sale_time'], time):
                logger.debug("Correction automatique sale_time")
                record['sale_time'] = time(0, 0, 0)

            # 3. Validation montants (avec tolérance)
            money_fields = ['total_amount', 'payment_amount', 'profit']
            for field in money_fields:
                if field in record:
                    value = record[field]
                    if isinstance(value, (int, float)):
                        if value < -1000000 or value > 1000000:  # Sanity check
                            logger.warning(f"⚠️ Montant suspicieux: {field}={value}")
                            record[field] = 0.0

            # 4. Validation entiers
            int_fields = ['item_count']
            for field in int_fields:
                if field in record:
                    value = record[field]
                    if not isinstance(value, int) or value < 0 or value > 10000:
                        logger.debug(f"Correction {field}: {value} -> 0")
                        record[field] = 0

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation: {e}")
            return False


# Fonction de test améliorée
async def test_transformer_robustness():
    """Test de robustesse du transformer"""
    transformer = SalesTransformer()

    # Données de test avec tous les cas problématiques
    problematic_data = [
        {
            'id': '12345',  # String
            'date': '20241210',
            'heure': '143022',
            'client': ' Client Test ',
            'total_a_payer': '45,50'  # Virgule française
        },
        {
            'id': None,  # Null
            'date': 'invalid_date',
            'heure': '999999',  # Heure invalide
            'total_a_payer': 'not_a_number'
        },
        {
            'id': 12347.5,  # Float
            'date': '2024-12-10T15:30:00+01:00',  # ISO complet
            'total_a_payer': '1.234,56'  # Format européen
        }
    ]

    transformed = await transformer.transform_batch(problematic_data)

    print(f"Test robustesse:")
    print(f"  Entrée: {len(problematic_data)} records")
    print(f"  Sortie: {len(transformed)} records valides")

    for i, record in enumerate(transformed):
        print(f"  Record {i + 1}: ID={record.get('hfsql_id')}, Date={record.get('sale_date')}")

    return len(transformed) > 0


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_transformer_robustness())