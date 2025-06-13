# backend/app/sync/transformers/sales_transformer.py - VERSION CORRIGÉE
from typing import List, Dict, Any
from datetime import datetime, date, time
from loguru import logger
import re


class SalesTransformer:
    """
    Transformateur pour les données de ventes HFSQL → PostgreSQL

    Adapte les formats de données entre les deux systèmes:
    - Dates HFSQL (YYYYMMDD) ou datetime → PostgreSQL DATE
    - Heures HFSQL (HHMMSS) ou datetime → PostgreSQL TIME
    - Nettoyage des chaînes de caractères
    - Conversion des types numériques
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Mappage des champs HFSQL → PostgreSQL
        """
        return {
            # Champs système
            'id': 'hfsql_id',
            'date': 'sale_date',
            'heure': 'sale_time',

            # Informations client/caisse
            'client': 'customer',
            'caissier': 'cashier',
            'nom_caisse': 'register_name',

            # Montants
            'total_a_payer': 'total_amount',
            'encaisse': 'payment_amount',
            'benefice': 'profit',

            # Statistiques
            'nombre_article': 'item_count'
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
        """
        Transforme un enregistrement de vente individuel - VERSION CORRIGÉE
        """
        try:
            transformed = {}

            # 1. Mappage des champs simples
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Conversion explicite de l'ID (CRITIQUE)
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int(transformed['hfsql_id'])

            # 3. Transformations spécifiques

            # Date: Gestion des différents formats
            if 'date' in hfsql_record:
                transformed['sale_date'] = self._convert_date_flexible(hfsql_record['date'])

            # Heure: Gestion des différents formats
            if 'heure' in hfsql_record:
                transformed['sale_time'] = self._convert_time_flexible(hfsql_record['heure'])

            # Si on a une seule colonne datetime, la diviser
            if 'datetime' in hfsql_record and 'date' not in hfsql_record:
                dt = self._parse_datetime_value(hfsql_record['datetime'])
                if dt:
                    transformed['sale_date'] = dt.date()
                    transformed['sale_time'] = dt.time()

            # Nettoyage des chaînes de caractères
            string_fields = ['customer', 'cashier', 'register_name']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # Conversion des montants
            money_fields = ['total_amount', 'payment_amount', 'profit']
            for field in money_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field])

            # Conversion des entiers
            int_fields = ['item_count']
            for field in int_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # 4. Ajout des métadonnées de synchronisation
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 5. Validation finale - VERSION CORRIGÉE
            if not self._validate_transformed_record(transformed):
                logger.warning(f"⚠️ Enregistrement invalide, ignoré: {hfsql_record.get('id', 'inconnu')}")
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation enregistrement: {e}")
            raise

    def _convert_date_flexible(self, date_value: Any) -> date:
        """
        Convertit une date de différents formats vers date Python
        """
        try:
            if not date_value:
                return date.today()

            # Si c'est déjà une date Python
            if isinstance(date_value, date):
                return date_value

            # Si c'est un datetime Python
            if isinstance(date_value, datetime):
                return date_value.date()

            # Conversion en chaîne
            date_str = str(date_value).strip()

            # Format YYYYMMDD (HFSQL)
            if len(date_str) == 8 and date_str.isdigit():
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                return date(year, month, day)

            # Format ISO (YYYY-MM-DD)
            if '-' in date_str and len(date_str) >= 10:
                # Prendre seulement la partie date
                date_part = date_str[:10]
                return datetime.strptime(date_part, '%Y-%m-%d').date()

            # Format français (DD/MM/YYYY)
            if '/' in date_str:
                return datetime.strptime(date_str, '%d/%m/%Y').date()

            # Si rien ne fonctionne, date du jour
            logger.warning(f"⚠️ Format de date non reconnu: {date_value}, utilisation date actuelle")
            return date.today()

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion date {date_value}: {e}, utilisation date actuelle")
            return date.today()

    def _convert_time_flexible(self, time_value: Any) -> time:
        """
        Convertit une heure de différents formats vers time Python
        """
        try:
            if not time_value:
                return time(0, 0, 0)

            # Si c'est déjà un time Python
            if isinstance(time_value, time):
                return time_value

            # Si c'est un datetime Python
            if isinstance(time_value, datetime):
                return time_value.time()

            # Conversion en chaîne
            time_str = str(time_value).strip()

            # Si c'est un timestamp complet, extraire seulement l'heure
            if '+' in time_str or 'T' in time_str:
                try:
                    # Parser comme datetime ISO
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

            # Format HHMM
            if len(time_str) == 4 and time_str.isdigit():
                hour = int(time_str[:2])
                minute = int(time_str[2:4])
                return time(hour, minute, 0)

            # Format HH:MM:SS
            if ':' in time_str:
                time_parts = time_str.split(':')
                if len(time_parts) >= 2:
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
                    second = int(time_parts[2]) if len(time_parts) > 2 else 0
                    return time(hour, minute, second)

            # Si rien ne fonctionne, minuit
            logger.warning(f"⚠️ Format d'heure non reconnu: {time_value}, utilisation 00:00:00")
            return time(0, 0, 0)

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion heure {time_value}: {e}, utilisation 00:00:00")
            return time(0, 0, 0)

    def _parse_datetime_value(self, dt_value: Any) -> datetime:
        """
        Parse une valeur datetime de différents formats
        """
        try:
            if isinstance(dt_value, datetime):
                return dt_value

            if isinstance(dt_value, str):
                dt_str = dt_value.strip()

                # Format ISO avec timezone
                if '+' in dt_str:
                    # Supprimer la timezone pour simplifier
                    dt_str = dt_str.split('+')[0]

                if 'T' in dt_str:
                    # Format ISO
                    return datetime.fromisoformat(dt_str.replace('T', ' '))
                else:
                    # Format standard
                    return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

            return None

        except Exception:
            return None

    def _clean_string(self, value: Any) -> str:
        """
        Nettoie une chaîne de caractères
        """
        if not value:
            return ""

        cleaned = str(value).strip()

        # Suppression des caractères de contrôle
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

        # Limitation de longueur
        if len(cleaned) > 255:
            cleaned = cleaned[:255]

        return cleaned

    def _convert_to_decimal(self, value: Any) -> float:
        """
        Convertit une valeur vers un décimal (float)
        """
        try:
            if value is None or value == '':
                return 0.0

            if isinstance(value, (int, float)):
                return float(value)

            if isinstance(value, str):
                # Suppression des espaces et caractères non numériques
                cleaned = re.sub(r'[^\d.,-]', '', value.strip())

                # Gestion des virgules comme séparateurs décimaux
                cleaned = cleaned.replace(',', '.')

                if cleaned:
                    return float(cleaned)

            return 0.0

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion décimal {value}: {e}")
            return 0.0

    def _convert_to_int(self, value: Any) -> int:
        """
        Convertit une valeur vers un entier - VERSION RENFORCÉE
        """
        try:
            if value is None or value == '':
                return 0

            # Si c'est déjà un entier
            if isinstance(value, int):
                return value

            # Si c'est un float, arrondir
            if isinstance(value, float):
                return int(round(value))

            # Si c'est une chaîne, nettoyer et convertir
            if isinstance(value, str):
                # Supprimer les espaces
                cleaned = value.strip()

                # Supprimer tout sauf les chiffres et le signe moins
                cleaned = re.sub(r'[^\d-]', '', cleaned)

                if cleaned and cleaned != '-':
                    return int(cleaned)

            # Essayer une conversion directe
            return int(value)

        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Erreur conversion entier {value}: {e}, utilisation 0")
            return 0

    def _validate_transformed_record(self, record: Dict[str, Any]) -> bool:
        """
        Valide qu'un enregistrement transformé est correct - VERSION CORRIGÉE
        """
        try:
            # Vérifications obligatoires
            required_fields = ['hfsql_id']

            for field in required_fields:
                if field not in record:
                    logger.warning(f"⚠️ Champ obligatoire manquant: {field}")
                    return False

            # Vérification que hfsql_id est un entier positif - CORRECTION ICI
            hfsql_id = record.get('hfsql_id')

            # Convertir si nécessaire
            if isinstance(hfsql_id, str):
                try:
                    hfsql_id = int(hfsql_id)
                    record['hfsql_id'] = hfsql_id  # Mettre à jour dans le record
                except ValueError:
                    logger.warning(f"⚠️ hfsql_id non numérique: {record['hfsql_id']}")
                    return False

            # Vérifier que c'est un entier positif
            if not isinstance(hfsql_id, int) or hfsql_id <= 0:
                logger.warning(f"⚠️ hfsql_id invalide: {hfsql_id} (type: {type(hfsql_id)})")
                return False

            # Vérification que sale_date est valide (mais pas obligatoire)
            if 'sale_date' in record and record['sale_date'] is not None:
                if not isinstance(record['sale_date'], date):
                    logger.warning(f"⚠️ sale_date invalide: {record['sale_date']}")
                    # Corriger au lieu de rejeter
                    record['sale_date'] = date.today()

            # Vérification des montants (warnings seulement)
            money_fields = ['total_amount', 'payment_amount', 'profit']
            for field in money_fields:
                if field in record and isinstance(record[field], (int, float)):
                    if record[field] < 0:
                        logger.debug(f"💰 Montant négatif détecté: {field} = {record[field]} (gardé)")

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """
        Exemple de transformation pour tests
        """
        sample_hfsql = {
            'id': 12345,
            'date': '20241210',
            'heure': '143022',
            'client': ' DUPONT Jean ',
            'caissier': 'Marie',
            'nom_caisse': 'CAISSE_1',
            'total_a_payer': '45.50',
            'encaisse': '50.00',
            'benefice': '12.30',
            'nombre_article': '3'
        }

        return {
            'input_sample': sample_hfsql,
            'field_mapping': self.field_mapping
        }


# Test du transformer
async def test_sales_transformer_flexible():
    """Test avec différents formats de date/heure"""
    transformer = SalesTransformer()

    # Données test avec différents formats
    test_data = [
        {
            'id': 1,
            'date': '20241210',  # Format HFSQL
            'heure': '143022',  # Format HFSQL
            'client': 'Test 1',
            'total_a_payer': '100.00'
        },
        {
            'id': 2,
            'date': '1999-11-30',  # Format ISO
            'heure': '1999-11-30 17:12:00+00:00',  # Datetime complet
            'client': 'Test 2',
            'total_a_payer': '200.00'
        }
    ]

    transformed = await transformer.transform_batch(test_data)

    print(f"Test transformer flexible:")
    print(f"   Entrée: {len(test_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    for i, record in enumerate(transformed):
        print(f"   Record {i + 1}:")
        print(f"     Date: {record.get('sale_date')}")
        print(f"     Heure: {record.get('sale_time')}")

    return transformed


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_sales_transformer_flexible())