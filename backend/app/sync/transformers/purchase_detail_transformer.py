# backend/app/sync/transformers/purchase_details_transformer.py
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
import re


class PurchaseDetailTransformer:
    """
    Transformateur pour les données de détails d'achat HFSQL → PostgreSQL

    Adapte les formats de données de la table entrees_produits vers purchase_details
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Mappage des champs HFSQL entrees_produits → PostgreSQL purchase_details
        """
        return {
            # Identifiants
            'id': 'hfsql_id',
            'id_produit': 'product_hfsql_id',
            'id_entree': 'purchase_order_hfsql_id',  # Peut être NULL
            'id_fournisseur': 'supplier_hfsql_id',  # Peut être NULL

            # Informations produit
            'nom_produit': 'product_name',
            'code_produit': 'product_code',

            # Prix et marges
            'prix_achat': 'purchase_price',
            'prix_vente': 'sale_price',
            'marge': 'margin_percent',

            # Stock et type
            'stock': 'stock_snapshot',
            'type_entree': 'entry_type'  # 'A', 'M', 'S', etc.
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
                logger.error(f"❌ Erreur transformation détail achat ID {record.get('id', 'inconnu')}: {e}")
                continue

        logger.debug(f"✅ {len(transformed_records)}/{len(hfsql_records)} détails d'achat transformés")
        return transformed_records

    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme un enregistrement de détail d'achat individuel
        """
        try:
            transformed = {}

            # 1. Mappage des champs simples
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Conversion de l'ID principal (obligatoire)
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int(transformed['hfsql_id'])

            # 3. Gestion des clés étrangères (peuvent être NULL)
            nullable_foreign_keys = ['product_hfsql_id', 'purchase_order_hfsql_id', 'supplier_hfsql_id']

            for field in nullable_foreign_keys:
                if field in transformed:
                    value = transformed[field]
                    if value is None or value == '' or value == 0:
                        transformed[field] = None  # Explicitement NULL
                    else:
                        transformed[field] = self._convert_to_int(value)
                else:
                    transformed[field] = None  # Défaut à NULL si absent

            # 4. Nettoyage des chaînes de caractères
            string_fields = ['product_name', 'product_code', 'entry_type']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # 5. Conversion des prix et marges
            price_fields = ['purchase_price', 'sale_price', 'margin_percent']
            for field in price_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field])

            # 6. Conversion des quantités entières
            int_fields = ['stock_snapshot']
            for field in int_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # 7. Nettoyage du type d'entrée
            if 'entry_type' in transformed:
                entry_type = str(transformed['entry_type']).strip().upper()
                # Normaliser les types connus
                if entry_type in ['A', 'ACHAT', 'AJOUT']:
                    transformed['entry_type'] = 'A'
                elif entry_type in ['M', 'MODIFICATION', 'MODIF']:
                    transformed['entry_type'] = 'M'
                elif entry_type in ['S', 'SORTIE', 'SUPPR']:
                    transformed['entry_type'] = 'S'
                elif entry_type in ['R', 'RETOUR']:
                    transformed['entry_type'] = 'R'
                else:
                    transformed['entry_type'] = entry_type[:10] if entry_type else 'A'

            # 8. Ajout des métadonnées de synchronisation
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 9. Validation finale
            if not self._validate_transformed_record(transformed):
                logger.warning(f"⚠️ Détail d'achat invalide, ignoré: {hfsql_record.get('id', 'inconnu')}")
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation détail d'achat: {e}")
            raise

    def _clean_string(self, value: Any) -> str:
        """
        Nettoie une chaîne de caractères
        """
        if not value:
            return ""

        cleaned = str(value).strip()

        # Suppression des caractères de contrôle
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

        # Limitation de longueur selon le champ
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
                if cleaned and cleaned != '-':
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
            # Vérifications obligatoires - SEUL hfsql_id est obligatoire
            required_fields = ['hfsql_id']

            for field in required_fields:
                if field not in record or record[field] is None:
                    logger.warning(f"⚠️ Champ obligatoire manquant: {field}")
                    return False

            # Vérification que hfsql_id est un entier positif
            hfsql_id = record.get('hfsql_id')
            if not isinstance(hfsql_id, int) or hfsql_id <= 0:
                logger.warning(f"⚠️ hfsql_id invalide: {hfsql_id}")
                return False

            # Vérification des clés étrangères (peuvent être NULL mais si présentes, doivent être valides)
            foreign_key_fields = ['product_hfsql_id', 'purchase_order_hfsql_id', 'supplier_hfsql_id']
            for field in foreign_key_fields:
                if field in record and record[field] is not None:
                    if not isinstance(record[field], int) or record[field] <= 0:
                        logger.debug(f"💡 Clé étrangère invalide mise à NULL: {field} = {record[field]}")
                        record[field] = None  # Corriger au lieu de rejeter

            # Vérification des prix (doivent être >= 0 si présents)
            price_fields = ['purchase_price', 'sale_price']
            for field in price_fields:
                if field in record and isinstance(record[field], (int, float)):
                    if record[field] < 0:
                        logger.debug(f"💰 Prix négatif détecté: {field} = {record[field]}")

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation détail d'achat: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """
        Retourne un exemple de transformation pour documentation/tests
        """
        sample_hfsql = {
            'id': 456,
            'id_produit': 123,
            'id_entree': 789,  # Peut être NULL
            'id_fournisseur': None,  # Peut être NULL
            'nom_produit': 'DOLIPRANE 1000MG',
            'code_produit': 'DOL1000',
            'prix_achat': '2.50',
            'prix_vente': '3.85',
            'marge': '35.0',
            'stock': '15',
            'type_entree': 'A'
        }

        expected_output = {
            'hfsql_id': 456,
            'product_hfsql_id': 123,
            'purchase_order_hfsql_id': 789,  # Peut être NULL
            'supplier_hfsql_id': None,  # NULL explicite
            'product_name': 'DOLIPRANE 1000MG',
            'product_code': 'DOL1000',
            'purchase_price': 2.50,
            'sale_price': 3.85,
            'margin_percent': 35.0,
            'stock_snapshot': 15,
            'entry_type': 'A',
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
async def test_purchase_details_transformer():
    """Test du transformer avec données manquantes"""
    transformer = PurchaseDetailTransformer()

    # Test avec différents cas
    test_data = [
        {
            'id': 1,
            'id_produit': 123,
            'id_entree': 789,
            'nom_produit': 'DOLIPRANE',
            'prix_achat': '2.50',
            'type_entree': 'A'
        },
        {
            'id': 2,
            'id_produit': 456,
            'id_entree': None,  # NULL
            'id_fournisseur': '',  # Vide -> NULL
            'nom_produit': 'EFFERALGAN',
            'prix_achat': '1.80',
            'type_entree': 'M'
        }
    ]

    transformed = await transformer.transform_batch(test_data)

    print(f"📊 Test Purchase Details Transformer:")
    print(f"   Entrée: {len(test_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    for i, record in enumerate(transformed):
        print(f"   Record {i + 1}:")
        print(f"     ID: {record.get('hfsql_id')}")
        print(f"     Product ID: {record.get('product_hfsql_id')}")
        print(f"     Order ID: {record.get('purchase_order_hfsql_id')} (NULL OK)")
        print(f"     Supplier ID: {record.get('supplier_hfsql_id')} (NULL OK)")

    return transformed


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_purchase_details_transformer())