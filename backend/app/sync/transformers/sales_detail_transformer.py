# backend/app/sync/transformers/sales_detail_transformer.py - VERSION CORRIGÉE
from typing import List, Dict, Any
from datetime import datetime, date, time  # CORRECTION: Ajout de 'date' et 'time'
from loguru import logger
import re


class SalesDetailTransformer:
    """
    Transformateur pour les détails de ventes HFSQL → PostgreSQL
    Table source: ventes_produits → sales_details
    CRUCIAL pour calcul précis des marges par ligne de vente
    """

    def __init__(self):
        self.field_mapping = self._get_field_mapping()

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Mappage des champs HFSQL ventes_produits → PostgreSQL sales_details
        """
        return {
            # Champs de base
            'id': 'hfsql_id',
            'id_sortie': 'sales_order_hfsql_id',  # Référence vers sorties.id

            # CORRECTION: id_produit = ID lot, id_nom = ID nomenclature
            'id_produit': 'lot_hfsql_id',  # ID du lot
            'id_nom': 'product_hfsql_id',  # ID de la nomenclature

            # Informations produit vendu
            'nom_produit': 'product_name',  # Nom produit (dénormalisé)
            'numero_lot': 'lot_number',  # Numéro de lot vendu

            # Prix et quantités
            'prix_vente': 'sale_price',  # Prix de vente unitaire
            'quantite': 'quantity_sold',  # Quantité vendue
            'total_ligne': 'line_total',  # Total ligne (prix × quantité)

            # Calcul de marge - CRUCIAL
            'prix_achat': 'purchase_price',  # Prix d'achat correspondant
            'benefice_unitaire': 'unit_profit',  # Bénéfice unitaire
            'benefice_ligne': 'line_profit',  # Bénéfice ligne
            'marge_pourcent': 'margin_percent',  # Marge en %

            # Remises
            'remise_pourcent': 'discount_percent',  # % remise ligne
            'remise_montant': 'discount_amount',  # Montant remise

            # Type de vente et assurance
            'type_vente': 'sale_type',  # CHIFA, LIBRE
            'taux_couverture': 'insurance_coverage',  # % couverture assurance
            'part_patient': 'patient_portion',  # Part patient
            'part_assurance': 'insurance_portion',  # Part assurance

            # Stock après vente
            'stock_apres': 'stock_after_sale',  # Stock restant après vente
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
                logger.error(f"❌ Erreur transformation détail vente ID {record.get('id', 'inconnu')}: {e}")
                continue

        logger.debug(f"✅ {len(transformed_records)}/{len(hfsql_records)} détails ventes transformés")
        return transformed_records

    async def transform_single_record(self, hfsql_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme un enregistrement de détail de vente individuel
        """
        try:
            transformed = {}

            # 1. Mappage des champs simples
            for hfsql_field, pg_field in self.field_mapping.items():
                if hfsql_field in hfsql_record:
                    transformed[pg_field] = hfsql_record[hfsql_field]

            # 2. Transformations spécifiques

            # Conversion des IDs
            id_fields = ['hfsql_id', 'sales_order_hfsql_id', 'lot_hfsql_id', 'product_hfsql_id']
            for field in id_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # Nettoyage des chaînes
            string_fields = ['product_name', 'lot_number', 'sale_type']
            for field in string_fields:
                if field in transformed:
                    transformed[field] = self._clean_string(transformed[field])

            # Normalisation du type de vente
            if 'sale_type' in transformed:
                transformed['sale_type'] = self._normalize_sale_type(transformed['sale_type'])

            # Conversion des prix - PRECISION IMPORTANTE pour calcul marge
            price_fields = ['sale_price', 'purchase_price', 'unit_profit']
            for field in price_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field], precision=4)

            # Conversion des montants totaux
            money_fields = ['line_total', 'line_profit', 'discount_amount', 'patient_portion', 'insurance_portion']
            for field in money_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field], precision=2)

            # Conversion des pourcentages
            percent_fields = ['margin_percent', 'discount_percent', 'insurance_coverage']
            for field in percent_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_decimal(transformed[field], precision=2, max_value=100.0)

            # Conversion des quantités et stock
            qty_fields = ['quantity_sold', 'stock_after_sale']
            for field in qty_fields:
                if field in transformed:
                    transformed[field] = self._convert_to_int(transformed[field])

            # 3. Calculs et validations de cohérence
            self._validate_and_fix_calculations(transformed)

            # 4. Ajout des métadonnées de synchronisation
            transformed.update({
                'sync_version': 1,
                'last_synced_at': datetime.now(),
                'created_at': datetime.now()
            })

            # 5. Validation finale
            if not self._validate_transformed_record(transformed):
                logger.warning(f"⚠️ Détail vente invalide, ignoré: {hfsql_record.get('id', 'inconnu')}")
                return None

            return transformed

        except Exception as e:
            logger.error(f"❌ Erreur transformation détail vente: {e}")
            raise

    def _validate_and_fix_calculations(self, record: Dict[str, Any]):
        """
        Valide et corrige les calculs de marge si nécessaire
        """
        try:
            # Vérifier cohérence prix × quantité = total
            if all(field in record for field in ['sale_price', 'quantity_sold']):
                calculated_total = record['sale_price'] * record['quantity_sold']

                if 'line_total' in record:
                    if abs(calculated_total - record['line_total']) > 0.01:
                        logger.debug(f"⚠️ Incohérence total ligne: {calculated_total} vs {record['line_total']}")
                        record['line_total'] = calculated_total
                else:
                    record['line_total'] = calculated_total

            # Calculer le bénéfice unitaire si manquant
            if all(field in record for field in ['sale_price', 'purchase_price']):
                calculated_unit_profit = record['sale_price'] - record['purchase_price']

                if 'unit_profit' in record:
                    if abs(calculated_unit_profit - record['unit_profit']) > 0.0001:
                        logger.debug(
                            f"⚠️ Incohérence bénéfice unitaire: {calculated_unit_profit} vs {record['unit_profit']}")
                        record['unit_profit'] = calculated_unit_profit
                else:
                    record['unit_profit'] = calculated_unit_profit

            # Calculer le bénéfice ligne si manquant
            if all(field in record for field in ['unit_profit', 'quantity_sold']):
                calculated_line_profit = record['unit_profit'] * record['quantity_sold']

                if 'line_profit' in record:
                    if abs(calculated_line_profit - record['line_profit']) > 0.01:
                        logger.debug(
                            f"⚠️ Incohérence bénéfice ligne: {calculated_line_profit} vs {record['line_profit']}")
                        record['line_profit'] = calculated_line_profit
                else:
                    record['line_profit'] = calculated_line_profit

            # Calculer la marge % si manquante
            if all(field in record for field in ['sale_price', 'purchase_price']) and record['sale_price'] > 0:
                calculated_margin = ((record['sale_price'] - record['purchase_price']) / record['sale_price']) * 100

                if 'margin_percent' in record:
                    if abs(calculated_margin - record['margin_percent']) > 0.1:
                        logger.debug(f"⚠️ Incohérence marge %: {calculated_margin:.2f} vs {record['margin_percent']}")
                        record['margin_percent'] = calculated_margin
                else:
                    record['margin_percent'] = calculated_margin

        except Exception as e:
            logger.debug(f"⚠️ Erreur validation calculs: {e}")

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

        # Limitation selon le type de champ
        max_length = 255
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned

    def _convert_to_decimal(self, value: Any, precision: int = 2, max_value: float = None) -> float:
        """
        Convertit une valeur vers un décimal avec précision spécifiée
        """
        try:
            if value is None or value == '':
                return 0.0

            if isinstance(value, (int, float)):
                result = round(float(value), precision)
            elif isinstance(value, str):
                cleaned = re.sub(r'[^\d.,-]', '', value.strip())
                cleaned = cleaned.replace(',', '.')
                result = round(float(cleaned), precision) if cleaned else 0.0
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
        """
        Valide qu'un enregistrement de détail de vente transformé est correct
        """
        try:
            # Vérifications obligatoires
            required_fields = ['hfsql_id', 'sales_order_hfsql_id', 'lot_hfsql_id', 'product_hfsql_id']

            for field in required_fields:
                if field not in record or record[field] is None:
                    logger.warning(f"⚠️ Champ obligatoire manquant: {field}")
                    return False

            # Vérification que les IDs sont des entiers positifs
            for field in required_fields:
                if not isinstance(record[field], int) or record[field] <= 0:
                    logger.warning(f"⚠️ {field} invalide: {record[field]}")
                    return False

            # Vérification que la quantité vendue est positive
            if 'quantity_sold' in record:
                if not isinstance(record['quantity_sold'], int) or record['quantity_sold'] <= 0:
                    logger.warning(f"⚠️ Quantité vendue invalide: {record['quantity_sold']}")
                    return False

            # Vérification que le prix de vente est positif
            if 'sale_price' in record:
                if not isinstance(record['sale_price'], (int, float)) or record['sale_price'] <= 0:
                    logger.warning(f"⚠️ Prix de vente invalide: {record['sale_price']}")
                    return False

            # Vérifications de cohérence (warnings seulement)
            if all(field in record for field in ['sale_price', 'purchase_price']):
                if record['purchase_price'] > record['sale_price']:
                    logger.debug(
                        f"⚠️ Prix d'achat > prix de vente: {record['purchase_price']} > {record['sale_price']}")

            return True

        except Exception as e:
            logger.error(f"❌ Erreur validation détail vente: {e}")
            return False

    def get_sample_transformation(self) -> Dict[str, Any]:
        """Exemple de transformation pour tests"""
        sample_hfsql = {
            'id': 2001,
            'id_sortie': 12345,  # Référence vente
            'id_produit': 456,  # ID du LOT
            'id_nom': 123,  # ID de la NOMENCLATURE
            'nom_produit': 'DOLIPRANE 1000MG',
            'numero_lot': 'LOT20241210A',
            'prix_vente': '3.8500',  # Prix unitaire avec précision
            'quantite': '2',  # Boîtes vendues
            'total_ligne': '7.70',  # 2 × 3.85
            'prix_achat': '2.5000',  # Prix d'achat correspondant
            'benefice_unitaire': '1.3500',  # 3.85 - 2.50
            'benefice_ligne': '2.70',  # 1.35 × 2
            'marge_pourcent': '35.06',  # (1.35 / 3.85) × 100
            'type_vente': 'CHIFA',
            'taux_couverture': '80.0',
            'part_patient': '1.54',  # 20% de 7.70
            'part_assurance': '6.16',  # 80% de 7.70
            'stock_apres': '46'  # Stock après cette vente
        }

        expected_output = {
            'hfsql_id': 2001,
            'sales_order_hfsql_id': 12345,
            'lot_hfsql_id': 456,  # ID du lot
            'product_hfsql_id': 123,  # ID nomenclature
            'product_name': 'DOLIPRANE 1000MG',
            'lot_number': 'LOT20241210A',
            'sale_price': 3.8500,  # Précision 4 décimales
            'quantity_sold': 2,
            'line_total': 7.70,
            'purchase_price': 2.5000,  # Précision 4 décimales
            'unit_profit': 1.3500,
            'line_profit': 2.70,
            'margin_percent': 35.06,
            'sale_type': 'CHIFA',
            'insurance_coverage': 80.0,
            'patient_portion': 1.54,
            'insurance_portion': 6.16,
            'stock_after_sale': 46,
            'sync_version': 1,
            'last_synced_at': datetime.now(),
            'created_at': datetime.now()
        }

        return {
            'input_sample': sample_hfsql,
            'expected_output': expected_output,
            'field_mapping': self.field_mapping,
            'calculation_rules': {
                'line_total': 'sale_price × quantity_sold',
                'unit_profit': 'sale_price - purchase_price',
                'line_profit': 'unit_profit × quantity_sold',
                'margin_percent': '(unit_profit / sale_price) × 100'
            }
        }


# Test du transformer
async def test_sales_detail_transformer():
    """Test du transformer de détails de ventes"""
    transformer = SalesDetailTransformer()

    sample_data = [
        {
            'id': 2001,
            'id_sortie': 12345,
            'id_produit': 456,  # ID lot
            'id_nom': 123,  # ID nomenclature
            'prix_vente': '3.8500',
            'quantite': '2',
            'prix_achat': '2.5000',
            'type_vente': 'CHIFA'
        },
        {
            'id': 2002,
            'id_sortie': 12345,
            'id_produit': 789,  # ID lot
            'id_nom': 124,  # ID nomenclature
            'prix_vente': '2.9500',
            'quantite': '1',
            'prix_achat': '1.8500',
            'type_vente': 'LIBRE'
        }
    ]

    transformed = await transformer.transform_batch(sample_data)

    print(f"📊 Test Sales Detail Transformer:")
    print(f"   Entrée: {len(sample_data)} enregistrements")
    print(f"   Sortie: {len(transformed)} enregistrements")

    if transformed:
        print(f"   Premier détail transformé:")
        for key, value in transformed[0].items():
            if key not in ['last_synced_at', 'created_at']:
                print(f"     - {key}: {value}")

        # Vérification des calculs
        record = transformed[0]
        if all(field in record for field in ['sale_price', 'purchase_price', 'quantity_sold']):
            unit_profit = record['sale_price'] - record['purchase_price']
            line_total = record['sale_price'] * record['quantity_sold']
            line_profit = unit_profit * record['quantity_sold']
            margin_percent = (unit_profit / record['sale_price']) * 100

            print(f"   Vérification calculs:")
            print(f"     - Bénéfice unitaire: {record['sale_price']} - {record['purchase_price']} = {unit_profit}")
            print(f"     - Total ligne: {record['sale_price']} × {record['quantity_sold']} = {line_total}")
            print(f"     - Bénéfice ligne: {unit_profit} × {record['quantity_sold']} = {line_profit}")
            print(f"     - Marge %: ({unit_profit} / {record['sale_price']}) × 100 = {margin_percent:.2f}%")

        # Vérification des IDs corrigés
        print(f"   IDs corrigés:")
        print(f"     - Lot ID: {record.get('lot_hfsql_id')} (id_produit)")
        print(f"     - Produit ID: {record.get('product_hfsql_id')} (id_nom)")

    return transformed


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_sales_detail_transformer())