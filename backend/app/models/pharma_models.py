# backend/app/models/pharma_models.py - MODÈLES ACHATS CORRIGÉS
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Boolean, Text, DECIMAL, ForeignKey,Time,Date
from sqlalchemy.sql import func
from ..core.database import Base


class ProductsCatalog(Base):
    """Catalogue produits (miroir nomenclature)"""
    __tablename__ = "products_catalog"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(Integer, unique=True, nullable=False)
    name = Column(String(255), nullable=False)

    # NOUVEAUX CHAMPS ESSENTIELS
    labo = Column(String(255))  # Laboratoire
    id_cnas = Column(String(50))  # ID CNAS
    de_equiv = Column(String(100))  # Code produit équivalent
    psychotrope = Column(Boolean, default=False)  # Médicament psychotrope
    code_barre_origine = Column(String(50))  # Code-barres d'origine

    # CHAMPS CONSERVÉS
    family = Column(String(100))  # Famille de produit
    alert_quantity = Column(Integer, default=0)  # Seuil d'alerte

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class PurchaseOrders(Base):
    """En-têtes des commandes d'achat (miroir entrees) - VERSION CORRIGÉE"""
    __tablename__ = "purchase_orders"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(BigInteger, unique=True, nullable=False)

    # Informations commande
    order_date = Column(DateTime(timezone=True))
    order_time = Column(DateTime(timezone=True))
    supplier = Column(String(255))  # Fournisseur
    reference = Column(String(100))  # Référence commande

    # TYPE CRUCIAL: A = Achat normal, AV = Avoir/Retour
    order_type = Column(String(2), nullable=False)  # A ou AV

    # Champs liés aux avoirs (quand type = AV)
    related_invoice_number = Column(String(100))  # num_av - Numéro facture liée
    return_reason = Column(Text)  # motif - Motif du retour

    # TOUS LES MONTANTS IMPORTANTS
    subtotal_ht = Column(DECIMAL(12, 2))  # Sous-total HT
    tax_amount = Column(DECIMAL(12, 2))  # TVA
    discount_amount = Column(DECIMAL(12, 2))  # Remise
    total_ttc = Column(DECIMAL(12, 2))  # Total TTC
    total_amount = Column(DECIMAL(12, 2))  # Montant total final

    # Informations livraison
    delivery_date = Column(DateTime(timezone=True))
    invoice_number = Column(String(100))  # Numéro facture

    # Statut et utilisateur
    status = Column(String(50))  # Statut (en_cours, livré, etc.)
    created_by = Column(String(100))  # Utilisateur créateur
    notes = Column(Text)  # Notes diverses

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class PurchaseDetails(Base):
    """Détails des achats avec gestion stock dynamique - VERSION CORRIGÉE"""
    __tablename__ = "purchase_details"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(BigInteger, unique=True, nullable=False)

    # Relations
    purchase_order_hfsql_id = Column(BigInteger, nullable=False)  # Référence entrees.id
    product_hfsql_id = Column(Integer, nullable=False)  # Référence nomenclature.id

    # TYPE CRUCIAL: A = Entrée normale, AV = Retour (sortie)
    entry_type = Column(String(2), nullable=False)  # A ou AV

    # Informations produit/lot
    lot_number = Column(String(100))  # Numéro de lot
    expiry_date = Column(DateTime(timezone=True))  # Date d'expiration

    # Prix et quantités - CRUCIAL pour calcul marge
    purchase_price = Column(DECIMAL(10, 4))  # Prix d'achat unitaire
    quantity_received = Column(Integer)  # Quantité reçue (+ pour A, - pour AV)
    total_cost = Column(DECIMAL(12, 2))  # Coût total ligne

    # STOCK DYNAMIQUE - Ne pas calculer ici mais garder la valeur HFSQL
    # Le stock réel sera calculé par une vue ou procédure
    stock_snapshot = Column(Integer)  # Snapshot du stock à ce moment (pour historique)

    # Prix de vente et marge
    suggested_sale_price = Column(DECIMAL(10, 2))  # Prix vente suggéré
    margin_percent = Column(DECIMAL(5, 2))  # Marge en %

    # Dates
    entry_date = Column(DateTime(timezone=True))  # Date d'entrée en stock

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SalesOrders(Base):
    """En-têtes des ventes (miroir sorties) - VERSION CORRIGÉE"""
    __tablename__ = "sales_orders"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(BigInteger, unique=True, nullable=False)

    # Informations vente
    sale_date = Column(Date(nullable=False))
    sale_time = Column(Time(timezone=True))

    # Point de vente et personnel
    cashier = Column(String(100))  # Vendeur/Caissier
    register_name = Column(String(50))  # Nom de la caisse

    # Client et type de vente
    customer = Column(String(255))  # Client
    sale_type = Column(String(20))  # CHIFA, LIBRE, etc.
    customer_type = Column(String(50))  # Type client

    # NOUVEAUX CHAMPS IMPORTANTS
    discount_amount = Column(DECIMAL(10, 2))  # remise - Remise globale
    chifa_invoice_number = Column(String(100))  # no_facture_chifa - Numéro facture CHIFA
    markup_amount = Column(DECIMAL(10, 2))  # majoration - Majoration appliquée

    # RÈGLEMENT ULTÉRIEUR - Champ qui change continuellement
    subsequent_payment = Column(DECIMAL(12, 2))  # reglement_ult - Versements ultérieurs

    # Montants globaux
    subtotal = Column(DECIMAL(12, 2))  # Sous-total
    tax_amount = Column(DECIMAL(10, 2))  # TVA
    total_amount = Column(DECIMAL(12, 2))  # Total à payer
    payment_amount = Column(DECIMAL(12, 2))  # Montant encaissé
    change_amount = Column(DECIMAL(10, 2))  # Monnaie rendue

    # Informations CHIFA/Assurance
    insurance_number = Column(String(100))  # Numéro assurance
    coverage_percent = Column(DECIMAL(5, 2))  # % couverture
    patient_copay = Column(DECIMAL(10, 2))  # Reste à charge patient

    # Statistiques
    item_count = Column(Integer, default=0)  # Nombre d'articles
    total_profit = Column(DECIMAL(12, 2))  # Bénéfice total

    # Statut et notes
    status = Column(String(20), default='COMPLETED')  # COMPLETED, CANCELLED, etc.
    notes = Column(Text)  # Notes diverses

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SalesDetails(Base):
    """Détails des ventes par produit - VERSION CORRIGÉE avec bons IDs"""
    __tablename__ = "sales_details"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(BigInteger, unique=True, nullable=False)

    # Relations - CORRECTION IMPORTANTE
    sales_order_hfsql_id = Column(BigInteger, nullable=False)  # Référence sorties.id

    # CORRECTION: id_produit = ID du LOT, id_nom = ID de la nomenclature
    lot_hfsql_id = Column(Integer, nullable=False)  # id_produit = ID du lot
    product_hfsql_id = Column(Integer, nullable=False)  # id_nom = ID nomenclature

    # Informations produit vendu
    product_name = Column(String(255))  # Nom produit (dénormalisé)
    lot_number = Column(String(100))  # Numéro de lot vendu

    # Prix et quantités
    sale_price = Column(DECIMAL(10, 4))  # Prix de vente unitaire
    quantity_sold = Column(Integer)  # Quantité vendue
    line_total = Column(DECIMAL(12, 2))  # Total ligne (prix × quantité)

    # Calcul de marge (CRUCIAL)
    purchase_price = Column(DECIMAL(10, 4))  # Prix d'achat correspondant
    unit_profit = Column(DECIMAL(10, 4))  # Bénéfice unitaire
    line_profit = Column(DECIMAL(12, 2))  # Bénéfice ligne
    margin_percent = Column(DECIMAL(5, 2))  # Marge en %

    # Remises
    discount_percent = Column(DECIMAL(5, 2))  # % remise ligne
    discount_amount = Column(DECIMAL(10, 2))  # Montant remise

    # Type de vente et assurance
    sale_type = Column(String(20))  # CHIFA, LIBRE
    insurance_coverage = Column(DECIMAL(5, 2))  # % couverture assurance
    patient_portion = Column(DECIMAL(10, 2))  # Part patient
    insurance_portion = Column(DECIMAL(10, 2))  # Part assurance

    # Stock après vente (si disponible)
    stock_after_sale = Column(Integer)  # Stock restant après vente

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# VUE STOCK CALCULÉ - Pour gérer le stock dynamique
# ============================================================================

class CurrentStockCalculated(Base):
    """Vue calculée du stock actuel - Mise à jour par trigger ou job"""
    __tablename__ = "current_stock_calculated"
    __table_args__ = {'schema': 'synergo_analytics', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    product_hfsql_id = Column(Integer, unique=True, nullable=False)
    product_name = Column(String(255))

    # Stock calculé en temps réel
    total_entries = Column(Integer, default=0)  # Total entrées (A)
    total_returns = Column(Integer, default=0)  # Total retours (AV)
    total_sales = Column(Integer, default=0)  # Total ventes
    current_stock = Column(Integer, default=0)  # Stock calculé = entrées - retours - ventes

    # Dernières activités
    last_entry_date = Column(DateTime(timezone=True))
    last_sale_date = Column(DateTime(timezone=True))
    last_return_date = Column(DateTime(timezone=True))

    # Prix moyens
    avg_purchase_price = Column(DECIMAL(10, 4))
    last_purchase_price = Column(DECIMAL(10, 4))
    current_sale_price = Column(DECIMAL(10, 2))

    # Indicateurs
    days_of_stock = Column(Integer)  # Jours de stock restant
    needs_reorder = Column(Boolean, default=False)

    # Mise à jour
    calculated_at = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# SQL POUR CALCUL DU STOCK EN TEMPS RÉEL
# ============================================================================

STOCK_CALCULATION_SQL = """
-- Vue pour calculer le stock actuel en temps réel
CREATE OR REPLACE VIEW synergo_analytics.real_time_stock AS
SELECT 
    pc.hfsql_id as product_hfsql_id,
    pc.name as product_name,

    -- Entrées (type A)
    COALESCE(SUM(CASE WHEN pd.entry_type = 'A' THEN pd.quantity_received ELSE 0 END), 0) as total_entries,

    -- Retours fournisseurs (type AV) 
    COALESCE(SUM(CASE WHEN pd.entry_type = 'AV' THEN pd.quantity_received ELSE 0 END), 0) as total_returns,

    -- Ventes
    COALESCE(SUM(sd.quantity_sold), 0) as total_sales,

    -- Stock calculé = Entrées - Retours - Ventes
    COALESCE(SUM(CASE WHEN pd.entry_type = 'A' THEN pd.quantity_received ELSE 0 END), 0) - 
    COALESCE(SUM(CASE WHEN pd.entry_type = 'AV' THEN pd.quantity_received ELSE 0 END), 0) - 
    COALESCE(SUM(sd.quantity_sold), 0) as current_stock,

    -- Dernières activités
    MAX(pd.entry_date) as last_entry_date,
    MAX(so.sale_date) as last_sale_date

FROM synergo_core.products_catalog pc
LEFT JOIN synergo_core.purchase_details pd ON pc.hfsql_id = pd.product_hfsql_id
LEFT JOIN synergo_core.sales_details sd ON pc.hfsql_id = sd.product_hfsql_id
LEFT JOIN synergo_core.sales_orders so ON sd.sales_order_hfsql_id = so.hfsql_id
GROUP BY pc.hfsql_id, pc.name
ORDER BY pc.name;
"""