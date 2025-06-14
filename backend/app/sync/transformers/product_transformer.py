# backend/app/sync/transformers/product_transformer.py - VERSION FINALE CORRIGÉE
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
import re


class ProductTransformer:
    """
    Transformateur pour les données de produits HFSQL → PostgreSQL
    VERSION CORRIGÉE : Sans prix + avec nouveaux champs essentiels (labo, CNAS, etc.)
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Mappage des champs HFSQL nomenclature → PostgreSQL products_catalog
        VERSION CORRIGÉE SANS PRIX
        """
        return {
            # Champs de base
            'id': 'hfsql_id',
            'nom': 'name',
            'famille': 'family',
            'quantite_alerte': 'alert_quantity',

            # NOUVEAUX CHAMPS ESSENTIELS
            'labo': 'labo',  # Laboratoire
            'id_cnas': 'id_cnas',  # ID CNAS
            'de_equiv': 'de_equiv',  # Code produit équivalent
            'psychotrope': 'psychotrope',  # Médicament psychotrope (Boolean)
            'code_barre_origine': 'code_barre_origine',  # Code-barres d'origine

            # CHAMPS SUPPRIMÉS (maintenant dans table purchase_details) :
            # 'prix_achat' -> SUPPRIMÉ
            # 'prix_vente' -> SUPPRIMÉ
            # 'marge' -> SUPPRIMÉ
            # 'stock_actuel' -> SUPPRIMÉ
            # 'fournisseur' -> SUPPRIMÉ
            # 'code_barre' -> Remplacé par code_barre_origine
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
        Transforme un enregistrement de produit individuel - VERSION CORRIGÉE
        """
        try:
            transformed = {}

            # 1. Mappage des champs simples
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Conversion critique de l'ID
            if 'hfsql_id' in transformed:
                transformed['hfsql_id'] = self._convert_to_int(transformed['hfsql_id'])

            # 3. Transformations spécifiques

            # Nettoyage du nom de produit
            if 'name' in transformed:
                transformed['name'] = self._clean_product_name(transformed['name'])

            # Nettoyage du code-barres d'origine
            if 'code_barre_origine' in transformed:
                transformed['code_barre_origine'] = self._clean_barcode(transformed['code_barre_origine'])

            # Nettoyage des chaînes
            string_fields = ['family', 'labo', 'id_cnas', 'de_equiv']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # Conversion du champ psychotrope en booléen
            if 'psychotrope' in transformed:
                transformed['psychotrope'] = self._convert_to_boolean(transformed['psychotrope'])

            # Conversion des quantités
            qty_fields = ['alert_quantity']
            for field in qty_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # 4. Ajout des métadonnées de synchronisation
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 5. Validation finale
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

        # Limitation de longueur appropriée selon le champ
        max_length = 255  # Par défaut
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned

    def _convert_to_boolean(self, value: Any) -> bool:
        """
        Convertit une valeur vers un booléen
        """
        try:
            if value is None:
                return False

            if isinstance(value, bool):
                return value

            if isinstance(value, (int, float)):
                return bool(value)

            if isinstance(value, str):
                value_lower = value.lower().strip()
                # Valeurs considérées comme True
                true_values = ['true', '1', 'oui', 'yes', 'o', 'y', 'vrai']
                return value_lower in true_values

            return bool(value)

        except Exception as e:
            logger.warning(f"⚠️ Erreur conversion booléen {value}: {e}")
            return False

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
                if value != value:  # NaN check
                    return 0
                return int(round(value))

            # Si c'est une chaîne, nettoyer et convertir
            if isinstance(value, str):
                # Supprimer les espaces
                cleaned = value.strip()

                # Vide après nettoyage
                if not cleaned:
                    return 0

                # Supprimer tout sauf les chiffres et le signe moins
                cleaned = re.sub(r'[^\d-]', '', cleaned)

                if cleaned and cleaned != '-':
                    return int(cleaned)

            # Essayer une conversion directe
            return int(float(str(value)))

        except (ValueError, TypeError, OverflowError) as e:
            logger.warning(f"⚠️ Erreur conversion entier {value}: {e}, utilisation 0")
            return 0

    def _validate_transformed_record(self, record: Dict[str, Any]) -> bool:
        """
        Valide qu'un enregistrement transformé est correct - VERSION CORRIGÉE FINALE
        """
        try:
            # Vérifications obligatoires
            required_fields = ['hfsql_id', 'name']

            for field in required_fields:
                if field not in record or record[field] is None:
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

            # Vérification que le nom n'est pas vide
            name = record.get('name', '').strip()
            if not name:
                logger.warning(f"⚠️ Nom de produit vide pour ID {hfsql_id}")
                return False

            # Mettre à jour le nom nettoyé
            record['name'] = name

            # Vérification du champ psychotrope (doit être booléen)
            if 'psychotrope' in record and not isinstance(record['psychotrope'], bool):
                logger.warning(f"⚠️ Champ psychotrope doit être booléen: {record['psychotrope']}")
                # Corriger automatiquement
                record['psychotrope'] = self._convert_to_boolean(record['psychotrope'])

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation produit: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """
        Retourne un exemple de transformation pour documentation/tests - VERSION CORRIGÉE
        """
        sample_hfsql = {
            'id': 123,
            'nom': ' DOLIPRANE 1000MG CPR 8 ',
            'famille': 'ANTALGIQUES',
            'labo': 'SANOFI',
            'id_cnas': 'CN123456',
            'de_equiv': 'DCI001',
            'psychotrope': '0',  # ou False, 'non', etc.
            'code_barre_origine': '3400930084267',
            'quantite_alerte': '5'
            # PLUS DE PRIX ICI - ils sont maintenant dans purchase_details
        }

        expected_output = {
            'hfsql_id': 123,
            'name': 'Doliprane 1000mg Cpr 8',
            'family': 'ANTALGIQUES',
            'labo': 'SANOFI',
            'id_cnas': 'CN123456',
            'de_equiv': 'DCI001',
            'psychotrope': False,
            'code_barre_origine': '3400930084267',
            'alert_quantity': 5,
            'sync_version': 1,
            'last_synced_at': datetime.now(),
            'created_at': datetime.now()
        }

        return {
            'input_sample': sample_hfsql,
            'expected_output': expected_output,
            'field_mapping': self.field_mapping,
            'removed_fields': [
                'prix_achat', 'prix_vente', 'marge', 'stock_actuel', 'fournisseur', 'code_barre'
            ],
            'new_fields': [
                'labo', 'id_cnas', 'de_equiv', 'psychotrope', 'code_barre_origine'
            ]
        }


# Utilitaires de test
async def test_corrected_product_transformer():
    """Test du transformer de produits corrigé"""
    transformer = ProductTransformer()

    # Test avec des données d'exemple incluant les nouveaux champs
    sample_data = [
        {
            'id': 123,
            'nom': ' DOLIPRANE 1000MG ',
            'famille': 'ANTALGIQUES',
            'labo': 'SANOFI',
            'id_cnas': 'CN123456',
            'de_equiv': 'DCI001',
            'psychotrope': '0',
            'code_barre_origine': '3400930084267',
            'quantite_alerte': '5'
            # PLUS DE PRIX - maintenant dans purchase_details
        },
        {
            'id': 124,
            'nom': 'EFFERALGAN 500MG',
            'famille': 'ANTALGIQUES',
            'labo': 'UPSA',
            'id_cnas': 'CN789012',
            'de_equiv': 'DCI002',
            'psychotrope': 'false',
            'code_barre_origine': '3400930087654',
            'quantite_alerte': '3'
        }
    ]

    # Transformation
    transformed = await transformer.transform_batch(sample_data)

    print(f"📊 Test Product Transformer Corrigé:")
    print(f"   Entrée: {len(sample_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    if transformed:
        print(f"   Premier transformé:")
        for key, value in transformed[0].items():
            if key not in ['last_synced_at', 'created_at']:
                print(f"     - {key}: {value}")

        print(f"   Nouveaux champs:")
        for field in ['labo', 'id_cnas', 'de_equiv', 'psychotrope', 'code_barre_origine']:
            if field in transformed[0]:
                print(f"     ✅ {field}: {transformed[0][field]}")

        print(f"   Champs supprimés (maintenant dans purchase_details):")
        removed_fields = ['prix_achat', 'prix_vente', 'marge', 'stock_actuel', 'fournisseur']
        for field in removed_fields:
            print(f"     🚫 {field}")

    return transformed


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_corrected_product_transformer())