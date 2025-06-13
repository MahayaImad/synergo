# backend/app/sync/transformers/product_transformer.py
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
import re


class ProductTransformer:
    """
    Transformateur pour les données de produits HFSQL → PostgreSQL

    Adapte les formats de données de la table nomenclature vers products_catalog
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Mappage des champs HFSQL nomenclature → PostgreSQL products_catalog
        """
        return {
            # Champs de base
            'id': 'hfsql_id',
            'nom': 'name',
            'code_barre': 'barcode',
            'famille': 'family',
            'fournisseur': 'supplier',

            # Prix
            'prix_achat': 'price_buy',
            'prix_vente': 'price_sell',
            'marge': 'margin',

            # Stock
            'quantite_alerte': 'alert_quantity',
            'stock_actuel': 'current_stock'
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
                logger.error(f"❌ Erreur transformation produit ID {record.get('id', 'inconnu')}: {e}")
                continue

        logger.debug(f"✅ {len(transformed_records)}/{len(hfsql_records)} produits transformés")
        return transformed_records

    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme un enregistrement de produit individuel
        """
        try:
            transformed = {}

            # 1. Mappage des champs simples
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Transformations spécifiques

            # Nettoyage du nom de produit
            if 'name' in transformed:
                transformed['name'] = self._clean_product_name(transformed['name'])

            # Nettoyage du code-barres
            if 'barcode' in transformed:
                transformed['barcode'] = self._clean_barcode(transformed['barcode'])

            # Nettoyage des chaînes
            string_fields = ['family', 'supplier']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # Conversion des prix
            price_fields = ['price_buy', 'price_sell', 'margin']
            for field in price_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field])

            # Conversion des quantités
            qty_fields = ['alert_quantity', 'current_stock']
            for field in qty_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # 3. Ajout des métadonnées de synchronisation
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 4. Validation finale
            if not self._validate_transformed_record(transformed):
                logger.warning(f"⚠️ Produit invalide, ignoré: {hfsql_record.get('id', 'inconnu')}")
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation produit: {e}")
            raise

    def _clean_product_name(self, name: Any) -> str:
        """
        Nettoie le nom d'un produit
        """
        if not name:
            return ""

        cleaned = str(name).strip()

        # Supprimer les caractères de contrôle
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

        # Supprimer les espaces multiples
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # Capitaliser proprement
        cleaned = cleaned.title()

        # Limitation de longueur
        if len(cleaned) > 255:
            cleaned = cleaned[:255]

        return cleaned

    def _clean_barcode(self, barcode: Any) -> str:
        """
        Nettoie un code-barres
        """
        if not barcode:
            return ""

        # Conversion en chaîne et suppression des espaces
        cleaned = str(barcode).strip()

        # Garder seulement les chiffres et lettres
        cleaned = re.sub(r'[^a-zA-Z0-9]', '', cleaned)

        # Limitation de longueur
        if len(cleaned) > 50:
            cleaned = cleaned[:50]

        return cleaned

    def _clean_string(self, value: Any) -> str:
        """
        Nettoie une chaîne de caractères générique
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
        Convertit une valeur vers un entier
        """
        try:
            if value is None or value == '':
                return 0

            if isinstance(value, int):
                return value

            if isinstance(value, float):
                return int(round(value))

            if isinstance(value, str):
                cleaned = re.sub(r'[^\d-]', '', value.strip())
                if cleaned:
                    return int(cleaned)

            return 0

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion entier {value}: {e}")
            return 0

    def _validate_transformed_record(self, record: Dict[str, Any]) -> bool:
        """
        Valide qu'un enregistrement transformé est correct
        """
        try:
            # Vérifications obligatoires
            required_fields = ['hfsql_id', 'name']

            for field in required_fields:
                if field not in record or record[field] is None:
                    logger.warning(f"⚠️ Champ obligatoire manquant: {field}")
                    return False

            # Vérification que hfsql_id est un entier positif
            if not isinstance(record['hfsql_id'], int) or record['hfsql_id'] <= 0:
                logger.warning(f"⚠️ hfsql_id invalide: {record['hfsql_id']}")
                return False

            # Vérification que le nom n'est pas vide
            if not record['name'] or len(record['name'].strip()) == 0:
                logger.warning(f"⚠️ Nom de produit vide")
                return False

            # Vérification des prix (doivent être >= 0)
            price_fields = ['price_buy', 'price_sell']
            for field in price_fields:
                if field in record and isinstance(record[field], (int, float)):
                    if record[field] < 0:
                        logger.warning(f"⚠️ Prix négatif détecté: {field} = {record[field]}")
                        # On ne rejette pas, on log juste

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation produit: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """
        Retourne un exemple de transformation pour documentation/tests
        """
        sample_hfsql = {
            'id': 123,
            'nom': ' DOLIPRANE 1000MG CPR 8 ',
            'code_barre': '3400930084267',
            'famille': 'ANTALGIQUES',
            'fournisseur': 'CERP ROUEN',
            'prix_achat': '2.50',
            'prix_vente': '3.85',
            'marge': '35.0',
            'quantite_alerte': '5',
            'stock_actuel': '12'
        }

        expected_output = {
            'hfsql_id': 123,
            'name': 'Doliprane 1000mg Cpr 8',
            'barcode': '3400930084267',
            'family': 'ANTALGIQUES',
            'supplier': 'CERP ROUEN',
            'price_buy': 2.50,
            'price_sell': 3.85,
            'margin': 35.0,
            'alert_quantity': 5,
            'current_stock': 12,
            'sync_version': 1,
            'last_synced_at': datetime.now(),
            'created_at': datetime.now()
        }

        return {
            'input_sample': sample_hfsql,
            'expected_output': expected_output,
            'field_mapping': self.field_mapping
        }


# Utilitaires de test
async def test_product_transformer():
    """Test du transformer de produits"""
    transformer = ProductTransformer()

    # Test avec des données d'exemple
    sample_data = [
        {
            'id': 123,
            'nom': ' DOLIPRANE 1000MG ',
            'code_barre': '3400930084267',
            'famille': 'ANTALGIQUES',
            'prix_achat': '2.50',
            'prix_vente': '3.85'
        },
        {
            'id': 124,
            'nom': 'EFFERALGAN 500MG',
            'code_barre': '3400930087654',
            'famille': 'ANTALGIQUES',
            'prix_achat': '1.80',
            'prix_vente': '2.95'
        }
    ]

    # Transformation
    transformed = await transformer.transform_batch(sample_data)

    print(f"📊 Test Product Transformer:")
    print(f"   Entrée: {len(sample_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    if transformed:
        print(f"   Premier transformé: {transformed[0]}")

    return transformed


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_product_transformer())