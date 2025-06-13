# scripts/create_missing_tables.py
"""
Création des tables manquantes pour Synergo
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "backend"))

from app.core.database import get_async_session_context, async_engine, Base
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
    """Crée les tables du schéma synergo_core"""
    async with get_async_session_context() as session:

        # Table sales_summary
        sales_summary_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.sales_summary (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,
            sale_date DATE,
            sale_time TIME,
            customer VARCHAR(255),
            cashier VARCHAR(100),
            register_name VARCHAR(50),
            total_amount DECIMAL(12,2),
            payment_amount DECIMAL(12,2),
            profit DECIMAL(12,2),
            item_count INTEGER DEFAULT 0,
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # Table products_catalog
        products_catalog_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.products_catalog (
            id SERIAL PRIMARY KEY,
            hfsql_id INTEGER UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            barcode VARCHAR(50),
            family VARCHAR(100),
            supplier VARCHAR(255),
            price_buy DECIMAL(10,2),
            price_sell DECIMAL(10,2),
            margin DECIMAL(5,2),
            alert_quantity INTEGER DEFAULT 0,
            current_stock INTEGER DEFAULT 0,
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # Table stock_movements
        stock_movements_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.stock_movements (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,
            product_id INTEGER,
            movement_type VARCHAR(20),
            quantity INTEGER,
            movement_date DATE,
            reference VARCHAR(100),
            supplier VARCHAR(255),
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # Table purchase_orders
        purchase_orders_sql = """
        CREATE TABLE IF NOT EXISTS synergo_core.purchase_orders (
            id SERIAL PRIMARY KEY,
            hfsql_id BIGINT UNIQUE NOT NULL,
            order_date DATE,
            supplier VARCHAR(255),
            total_amount DECIMAL(12,2),
            status VARCHAR(50),
            reference VARCHAR(100),
            sync_version INTEGER DEFAULT 1,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        tables = [
            ("sales_summary", sales_summary_sql),
            ("products_catalog", products_catalog_sql),
            ("stock_movements", stock_movements_sql),
            ("purchase_orders", purchase_orders_sql)
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

        # Tables déjà créées normalement, mais on vérifie
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
            "CREATE INDEX IF NOT EXISTS idx_sales_summary_hfsql_id ON synergo_core.sales_summary(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_sales_summary_date ON synergo_core.sales_summary(sale_date)",
            "CREATE INDEX IF NOT EXISTS idx_products_catalog_hfsql_id ON synergo_core.products_catalog(hfsql_id)",
            "CREATE INDEX IF NOT EXISTS idx_products_catalog_barcode ON synergo_core.products_catalog(barcode)",
            "CREATE INDEX IF NOT EXISTS idx_sync_log_table_created ON synergo_sync.sync_log(table_name, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_sync_state_table ON synergo_sync.sync_state(table_name)"
        ]

        for index_sql in indexes:
            try:
                await session.execute(text(index_sql))
                print(f"✅ Index créé: {index_sql.split()[-1]}")
            except Exception as e:
                print(f"⚠️ Index existant ou erreur: {e}")

        await session.commit()


async def setup_initial_data():
    """Configure les données initiales"""
    async with get_async_session_context() as session:

        # Configuration des tables à synchroniser
        config_sql = """
        INSERT INTO synergo_sync.sync_tables (table_name, hfsql_table, sync_strategy, sync_interval_minutes, batch_size) 
        VALUES 
        ('sales_summary', 'sorties', 'ID_BASED', 30, 1000),
        ('products_catalog', 'nomenclature', 'ID_BASED', 60, 500),
        ('stock_movements', 'entrees_produits', 'ID_BASED', 30, 1000),
        ('purchase_orders', 'entrees', 'ID_BASED', 60, 500)
        ON CONFLICT (table_name) DO NOTHING
        """

        # États initiaux
        state_sql = """
        INSERT INTO synergo_sync.sync_state (table_name, last_sync_id, last_sync_status) 
        VALUES 
        ('sales_summary', 0, 'NEVER_SYNCED'),
        ('products_catalog', 0, 'NEVER_SYNCED'),
        ('stock_movements', 0, 'NEVER_SYNCED'),
        ('purchase_orders', 0, 'NEVER_SYNCED')
        ON CONFLICT (table_name) DO NOTHING
        """

        try:
            await session.execute(text(config_sql))
            await session.execute(text(state_sql))
            await session.commit()
            print("✅ Configuration initiale créée")
        except Exception as e:
            print(f"⚠️ Configuration déjà présente ou erreur: {e}")


async def verify_tables():
    """Vérifie que toutes les tables existent"""
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

        # Vérifier les tables sync
        sync_tables_check = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'synergo_sync'
        ORDER BY table_name
        """

        result = await session.execute(text(sync_tables_check))
        sync_tables = [row[0] for row in result.fetchall()]
        print(f"📊 Tables sync: {sync_tables}")

        # Vérifier la configuration
        config_check = "SELECT COUNT(*) FROM synergo_sync.sync_tables WHERE is_active = true"
        result = await session.execute(text(config_check))
        active_tables = result.fetchone()[0]
        print(f"📊 Tables configurées pour sync: {active_tables}")

        expected_core_tables = ['sales_summary', 'products_catalog', 'stock_movements', 'purchase_orders']
        missing_core = set(expected_core_tables) - set(core_tables)

        if missing_core:
            print(f"❌ Tables core manquantes: {missing_core}")
            return False
        else:
            print("✅ Toutes les tables nécessaires sont présentes")
            return True


async def main():
    """Création complète de la base Synergo"""

    print("🏗️ CRÉATION DES TABLES SYNERGO")
    print("=" * 50)

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

        # 6. Vérification finale
        print("\n🔍 Vérification finale...")
        tables_ok = await verify_tables()

        if tables_ok:
            print(f"\n🎉 TOUTES LES TABLES CRÉÉES!")
            print(f"   Prêt pour la synchronisation")
            print(f"   Lancez: python scripts/test_manual_sync.py")
        else:
            print(f"\n⚠️ Quelques tables manquent encore")

        return tables_ok

    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        return False


if __name__ == "__main__":
    asyncio.run(main())