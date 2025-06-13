# backend/app/models/pharma_models.py
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Boolean, Text, DECIMAL
from sqlalchemy.sql import func
from ..core.database import Base


class SalesSummary(Base):
    """Résumé des ventes (miroir optimisé de HFSQL sorties)"""
    __tablename__ = "sales_summary"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(BigInteger, unique=True, nullable=False)  # Référence sorties.id
    sale_date = Column(DateTime(timezone=True), nullable=False)
    sale_time = Column(DateTime(timezone=True))
    customer = Column(String(255))
    cashier = Column(String(100))
    register_name = Column(String(50))
    total_amount = Column(DECIMAL(12, 2))
    payment_amount = Column(DECIMAL(12, 2))
    profit = Column(DECIMAL(12, 2))
    item_count = Column(Integer, default=0)

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ProductsCatalog(Base):
    """Catalogue produits (miroir nomenclature)"""
    __tablename__ = "products_catalog"
    __table_args__ = {'schema': 'synergo_core', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    hfsql_id = Column(Integer, unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    barcode = Column(String(50))
    family = Column(String(100))
    supplier = Column(String(255))
    price_buy = Column(DECIMAL(10, 2))
    price_sell = Column(DECIMAL(10, 2))
    margin = Column(DECIMAL(5, 2))
    alert_quantity = Column(Integer, default=0)
    current_stock = Column(Integer, default=0)

    # Métadonnées sync
    sync_version = Column(Integer, default=1)
    last_synced_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())