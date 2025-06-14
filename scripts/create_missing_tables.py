# scripts/create_missing_tables_corrected.py - VERSION ERP COMPLÈTE CORRIGÉE
"""
Création des tables ERP complètes pour Synergo - VERSION CORRIGÉE
Architecture: produits → achats → ventes avec types A/AV
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "backend"))

from app.core.database import get_async_session_context
from sqlalchemy import text
from loguru import logger


async def create_schemas():
    """Crée les schémas nécessaires"""
    async with get_async_session_context() as session:
        schemas = ['synergo_core', 'synergo_sync', 'synergo_analytics']

        for schema in schemas:
            try:
                await session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                print(f"✅ Schéma {schema} créé/vérifié")
            except Exception as e:
                print(f"❌ Erreur création schéma {schema}: {e}")

        await session.commit()


async def create_core_tables():
    """Crée les tables ERP complètes du schéma synergo_core"""
    async with get_async_session_context() as session:

        # 1. Table products_catalog - VERSION CORRIGÉE SANS PRIX
        products_catalog_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.products_catalog (
            id SERIAL PRIMARY KEY,
            hfsql_id INTEGER UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,

            -- Nouveaux champs essentiels pharmacie
            labo VARCHAR(255),
            id_cnas VARCHAR(50),
            de_equiv VARCHAR(100),
            psychotrope BOOLEAN DEFAULT FALSE,
            code_barre_origine VARCHAR(50),
            family VARCHAR(100),
            alert_quantity INTEGER DEFAULT 0,

            -- Métadonnées sync
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # 2. Table purchase_orders - EN-TÊTES ACHATS avec types A/AV
        purchase_orders_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.purchase_orders (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,

            -- Informations commande
            order_date TIMESTAMP,
            order_time TIMESTAMP,
            supplier VARCHAR(255),
            reference VARCHAR(100),

            -- TYPE CRUCIAL: A = Achat, AV = Avoir/Retour
            order_type VARCHAR(2) NOT NULL DEFAULT 'A',

            -- Champs avoirs (quand type = AV)
            related_invoice_number VARCHAR(100),
            return_reason TEXT,

            -- TOUS LES MONTANTS
            subtotal_ht DECIMAL(12,2),
            tax_amount DECIMAL(12,2),
            discount_amount DECIMAL(12,2),
            total_ttc DECIMAL(12,2),
            total_amount DECIMAL(12,2),

            -- Autres
            delivery_date TIMESTAMP,
            invoice_number VARCHAR(100),
            status VARCHAR(50),
            created_by VARCHAR(100),
            notes TEXT,

            -- Métadonnées sync
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # 3. Table purchase_details - DÉTAILS ACHATS avec stock snapshot
        purchase_details_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.purchase_details (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,

            -- Relations (NULLABLE pour compatibilité)
            purchase_order_hfsql_id BIGINT,
            product_hfsql_id INTEGER,
            supplier_hfsql_id INTEGER,

            -- Informations produit
            product_name VARCHAR(255),
            product_code VARCHAR(100),

            -- TYPE A/AV crucial
            entry_type VARCHAR(10) DEFAULT 'A',

            -- Informations lot
            lot_number VARCHAR(100),
            expiry_date TIMESTAMP,

            -- Prix et quantités - CRUCIAL pour marge
            purchase_price DECIMAL(10,4),
            sale_price DECIMAL(10,2),
            quantity_received INTEGER,
            total_cost DECIMAL(12,2),
            margin_percent DECIMAL(5,2),

            -- Stock snapshot (pas de calcul)
            stock_snapshot INTEGER DEFAULT 0,

            -- Prix suggéré
            suggested_sale_price DECIMAL(10,2),
            entry_date TIMESTAMP,

            -- Métadonnées sync
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # 4. Table sales_orders - EN-TÊTES VENTES avec nouveaux champs
        sales_orders_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.sales_orders (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,

            -- Informations vente (TYPES CORRECTS)
            sale_date DATE,
            sale_time TIME,  -- TIME pas DATE !

            -- Personnel et caisse
            cashier VARCHAR(100),
            register_name VARCHAR(50),

            -- Client
            customer VARCHAR(255),
            sale_type VARCHAR(20),
            customer_type VARCHAR(50),

            -- NOUVEAUX CHAMPS IMPORTANTS
            discount_amount DECIMAL(10,2) DEFAULT 0.0,
            chifa_invoice_number VARCHAR(100),
            markup_amount DECIMAL(10,2) DEFAULT 0.0,
            subsequent_payment DECIMAL(12,2) DEFAULT 0.0,

            -- Montants
            subtotal DECIMAL(12,2),
            tax_amount DECIMAL(10,2),
            total_amount DECIMAL(12,2),
            payment_amount DECIMAL(12,2),
            change_amount DECIMAL(10,2),

            -- CHIFA
            insurance_number VARCHAR(100),
            coverage_percent DECIMAL(5,2),
            patient_copay DECIMAL(10,2),

            -- Stats
            item_count INTEGER DEFAULT 0,
            total_profit DECIMAL(12,2),
            status VARCHAR(20) DEFAULT 'COMPLETED',
            notes TEXT,

            -- Métadonnées sync
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # 5. Table sales_details - DÉTAILS VENTES avec IDs corrigés
        sales_details_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.sales_details (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,

            -- Relations CORRIGÉES
            sales_order_hfsql_id BIGINT NOT NULL,
            lot_hfsql_id INTEGER NOT NULL,         -- id_produit = ID du lot
            product_hfsql_id INTEGER NOT NULL,     -- id_nom = ID nomenclature

            -- Produit vendu
            product_name VARCHAR(255),
            lot_number VARCHAR(100),

            -- Prix et quantités
            sale_price DECIMAL(10,4),
            quantity_sold INTEGER,
            line_total DECIMAL(12,2),

            -- Calcul marge CRUCIAL
            purchase_price DECIMAL(10,4),
            unit_profit DECIMAL(10,4),
            line_profit DECIMAL(12,2),
            margin_percent DECIMAL(5,2),

            -- Remises
            discount_percent DECIMAL(5,2),
            discount_amount DECIMAL(10,2),

            -- CHIFA
            sale_type VARCHAR(20),
            insurance_coverage DECIMAL(5,2),
            patient_portion DECIMAL(10,2),
            insurance_portion DECIMAL(10,2),

            -- Stock
            stock_after_sale INTEGER,

            -- Métadonnées sync
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        tables = [
            ("products_catalog", products_catalog_sql),
            ("purchase_orders", purchase_orders_sql),
            ("purchase_details", purchase_details_sql),
            ("sales_orders", sales_orders_sql),
            ("sales_details", sales_details_sql)
        ]

        for table_name, sql in tables:
            try:
                await session.execute(text(sql))
                print(f"✅ Table synergo_core.{table_name} créée/vérifiée")
            except Exception as e:
                print(f"❌ Erreur table {table_name}: {e}")

        await session.commit()


async def create_sync_tables():
    """Crée les tables de synchronisation"""
    async with get_async_session_context() as session:

        sync_tables_sql = """
        CREATE TABLE IF NOT EXISTS synergo_sync.sync_tables (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) UNIQUE NOT NULL,
            hfsql_table VARCHAR(100) NOT NULL,
            sync_strategy VARCHAR(20) DEFAULT 'ID_BASED',
            is_active BOOLEAN DEFAULT true,
            sync_interval_minutes INTEGER DEFAULT 30,
            batch_size INTEGER DEFAULT 1000,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        sync_state_sql = """
        CREATE TABLE IF NOT EXISTS synergo_sync.sync_state (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) UNIQUE NOT NULL,
            last_sync_id BIGINT DEFAULT 0,
            last_sync_timestamp TIMESTAMP,
            last_sync_date VARCHAR(8),
            last_sync_time VARCHAR(6),
            total_records BIGINT DEFAULT 0,
            last_sync_duration INTEGER,
            last_sync_status VARCHAR(20) DEFAULT 'PENDING',
            error_message TEXT,
            records_processed_last_sync INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        sync_log_sql = """
        CREATE TABLE IF NOT EXISTS synergo_sync.sync_log (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100),
            operation VARCHAR(20),
            hfsql_id BIGINT,
            postgres_id BIGINT,
            records_processed INTEGER DEFAULT 0,
            processing_time_ms INTEGER,
            error_details JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        sync_tables = [
            ("sync_tables", sync_tables_sql),
            ("sync_state", sync_state_sql),
            ("sync_log", sync_log_sql)
        ]

        for table_name, sql in sync_tables:
            try:
                await session.execute(text(sql))
                print(f"✅ Table synergo_sync.{table_name} créée/vérifiée")
            except Exception as e:
                print(f"❌ Erreur table sync {table_name}: {e}")

        await session.commit()


async def create_indexes():
    """Crée les index pour performance"""
    async with get_async_session_context() as session:

        indexes = [
            # Index produits
            "CREATE INDEX IF NOT EXISTS idx_products_catalog_hfsql_id ON synergo_core.products_catalog(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_products_catalog_labo ON synergo_core.products_catalog(labo)",
            "CREATE INDEX IF NOT EXISTS idx_products_catalog_code_barre ON synergo_core.products_catalog(code_barre_origine)",

            # Index achats
            "CREATE INDEX IF NOT EXISTS idx_purchase_orders_hfsql_id ON synergo_core.purchase_orders(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_orders_type ON synergo_core.purchase_orders(order_type)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_details_hfsql_id ON synergo_core.purchase_details(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_details_product ON synergo_core.purchase_details(product_hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_details_order ON synergo_core.purchase_details(purchase_order_hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_details_supplier ON synergo_core.purchase_details(supplier_hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchase_details_type ON synergo_core.purchase_details(entry_type)",

            # Index ventes
            "CREATE INDEX IF NOT EXISTS idx_sales_orders_hfsql_id ON synergo_core.sales_orders(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_sales_orders_date ON synergo_core.sales_orders(sale_date)",
            "CREATE INDEX IF NOT EXISTS idx_sales_orders_type ON synergo_core.sales_orders(sale_type)",
            "CREATE INDEX IF NOT EXISTS idx_sales_details_hfsql_id ON synergo_core.sales_details(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_sales_details_order ON synergo_core.sales_details(sales_order_hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_sales_details_product ON synergo_core.sales_details(product_hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_sales_details_lot ON synergo_core.sales_details(lot_hfsql_id)",

            # Index sync
            "CREATE INDEX IF NOT EXISTS idx_sync_log_table_created ON synergo_sync.sync_log(table_name, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_sync_state_table ON synergo_sync.sync_state(table_name)"
        ]

        for index_sql in indexes:
            try:
                await session.execute(text(index_sql))
                index_name = index_sql.split()[-1].split('(')[0]
                print(f"✅ Index créé: {index_name}")
            except Exception as e:
                print(f"⚠️ Index existant ou erreur: {e}")

        await session.commit()


async def setup_initial_data():
    """Configure les données initiales ERP complètes"""
    async with get_async_session_context() as session:

        # Configuration ERP complète des tables à synchroniser
        config_sql = """
        INSERT INTO synergo_sync.sync_tables (table_name, hfsql_table, sync_strategy, sync_interval_minutes, batch_size) 
        VALUES 
        ('products_catalog', 'nomenclature', 'ID_BASED', 60, 500),
        ('purchase_orders', 'entrees', 'ID_BASED', 45, 750),
        ('purchase_details', 'entrees_produits', 'ID_BASED', 30, 1000),
        ('sales_orders', 'sorties', 'ID_BASED', 15, 1000),
        ('sales_details', 'ventes_produits', 'ID_BASED', 15, 1000)
        ON CONFLICT (table_name) DO NOTHING
        """

        # États initiaux
        state_sql = """
        INSERT INTO synergo_sync.sync_state (table_name, last_sync_id, last_sync_status) 
        VALUES 
        ('products_catalog', 0, 'NEVER_SYNCED'),
        ('purchase_orders', 0, 'NEVER_SYNCED'),
        ('purchase_details', 0, 'NEVER_SYNCED'),
        ('sales_orders', 0, 'NEVER_SYNCED'),
        ('sales_details', 0, 'NEVER_SYNCED')
        ON CONFLICT (table_name) DO NOTHING
        """

        try:
            await session.execute(text(config_sql))
            await session.execute(text(state_sql))
            await session.commit()
            print("✅ Configuration ERP complète créée")
        except Exception as e:
            print(f"⚠️ Configuration déjà présente ou erreur: {e}")


async def verify_tables():
    """Vérifie que toutes les tables ERP existent"""
    async with get_async_session_context() as session:

        # Vérifier les schémas
        schema_check = """
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name IN ('synergo_core', 'synergo_sync', 'synergo_analytics')
        ORDER BY schema_name
        """

        result = await session.execute(text(schema_check))
        schemas = [row[0] for row in result.fetchall()]
        print(f"📊 Schémas présents: {schemas}")

        # Vérifier les tables core
        core_tables_check = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'synergo_core'
        ORDER BY table_name
        """

        result = await session.execute(text(core_tables_check))
        core_tables = [row[0] for row in result.fetchall()]
        print(f"📊 Tables core: {core_tables}")

        # Vérifier la configuration
        config_check = "SELECT COUNT(*) FROM synergo_sync.sync_tables WHERE is_active = true"
        result = await session.execute(text(config_check))
        active_tables = result.fetchone()[0]
        print(f"📊 Tables configurées pour sync: {active_tables}")

        # Tables attendues
        expected_core_tables = ['products_catalog', 'purchase_orders', 'purchase_details', 'sales_orders', 'sales_details']
        missing_core = set(expected_core_tables) - set(core_tables)

        if missing_core:
            print(f"❌ Tables core manquantes: {missing_core}")
            return False
        else:
            print("✅ Toutes les tables ERP sont présentes")
            return True


async def test_table_insertions():
    """Test les insertions dans les tables créées"""
    print(f"\n🧪 TEST DES TABLES CRÉÉES")
    print("=" * 30)

    async with get_async_session_context() as session:
        try:
            # Test purchase_details avec les colonnes requises par le transformer
            test_purchase = {
                'hfsql_id': 999999,
                'product_hfsql_id': None,  # NULL autorisé
                'supplier_hfsql_id': 1,
                'purchase_order_hfsql_id': None,  # NULL autorisé
                'product_name': 'Test Product',
                'product_code': 'TEST001',
                'purchase_price': 65.34,
                'sale_price': 120.0,
                'margin_percent': 83.65,
                'stock_snapshot': 0,
                'entry_type': 'A'
            }

            insert_purchase = """
            INSERT INTO synergo_core.purchase_details 
            (hfsql_id, product_hfsql_id, supplier_hfsql_id, purchase_order_hfsql_id,
             product_name, product_code, purchase_price, sale_price, margin_percent,
             stock_snapshot, entry_type)
            VALUES (:hfsql_id, :product_hfsql_id, :supplier_hfsql_id, :purchase_order_hfsql_id,
                    :product_name, :product_code, :purchase_price, :sale_price, :margin_percent,
                    :stock_snapshot, :entry_type)
            """

            await session.execute(text(insert_purchase), test_purchase)
            print("✅ Test purchase_details: insertion réussie")

            # Test sales_orders avec types TIME corrects
            test_sale = {
                'hfsql_id': 999998,
                'sale_date': '2025-06-14',
                'sale_time': '14:30:45',  # TIME
                'customer': 'Test Customer',
                'discount_amount': 10.0,
                'chifa_invoice_number': 'CHIFA123',
                'markup_amount': 5.0,
                'subsequent_payment': 130.0,
                'total_amount': 250.0
            }

            insert_sale = """
            INSERT INTO synergo_core.sales_orders 
            (hfsql_id, sale_date, sale_time, customer, discount_amount,
             chifa_invoice_number, markup_amount, subsequent_payment, total_amount)
            VALUES (:hfsql_id, :sale_date, :sale_time, :customer, :discount_amount,
                    :chifa_invoice_number, :markup_amount, :subsequent_payment, :total_amount)
            """

            await session.execute(text(insert_sale), test_sale)
            print("✅ Test sales_orders: insertion réussie (TIME correct)")

            # Nettoyer
            await session.execute(text("DELETE FROM synergo_core.purchase_details WHERE hfsql_id = 999999"))
            await session.execute(text("DELETE FROM synergo_core.sales_orders WHERE hfsql_id = 999998"))

            await session.commit()
            print("✅ Tous les tests d'insertion réussis")
            return True

        except Exception as e:
            print(f"❌ Test d'insertion échoué: {e}")
            await session.rollback()
            return False


async def main():
    """Point d'entrée principal - VERSION CORRIGÉE"""

    print("🏗️ CRÉATION TABLES ERP SYNERGO - VERSION CORRIGÉE")
    print("=" * 65)

    try:
        # 1. Créer les schémas
        print("\n📁 Création des schémas...")
        await create_schemas()

        # 2. Créer les tables de synchronisation
        print("\n🔄 Création des tables de sync...")
        await create_sync_tables()

        # 3. Créer les tables métier
        print("\n📊 Création des tables métier...")
        await create_core_tables()

        # 4. Créer les index
        print("\n⚡ Création des index...")
        await create_indexes()

        # 5. Configuration initiale
        print("\n⚙️ Configuration initiale...")
        await setup_initial_data()

        # 6. Tests d'insertion
        print("\n🧪 Tests d'insertion...")
        test_ok = await test_table_insertions()

        # 7. Vérification finale
        print("\n🔍 Vérification finale...")
        tables_ok = await verify_tables()

        if tables_ok and test_ok:
            print(f"\n🎉 CRÉATION ERP TERMINÉE AVEC SUCCÈS!")
            print(f"\n📋 Tables ERP créées:")
            print(f"   ✅ products_catalog (avec champs pharmacie)")
            print(f"   ✅ purchase_orders (en-têtes achats A/AV)")
            print(f"   ✅ purchase_details (détails avec colonnes requises)")
            print(f"   ✅ sales_orders (en-têtes ventes, TIME correct)")
            print(f"   ✅ sales_details (détails ventes)")
            print(f"\n📊 Configuration:")
            print(f"   ✅ Tables sync configurées")
            print(f"   ✅ Index de performance créés")
            print(f"   ✅ Tests d'insertion OK")
            print(f"\n🚀 PRÊT POUR LA SYNCHRONISATION ERP!")
            print(f"   Votre système peut maintenant synchroniser:")
            print(f"   - Produits (nomenclature)")
            print(f"   - Achats (entrees + entrees_produits)")
            print(f"   - Ventes (sorties + ventes_produits)")
            print(f"   - Calculs de stock et marges en temps réel")
        else:
            print(f"\n⚠️ Problèmes détectés lors de la création")

    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())