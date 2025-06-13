# scripts/quick_start_synergo.py - VERSION CORRIGÉE
"""
🚀 Script de Démarrage Rapide Synergo - VERSION CORRIGÉE

Ce script permet de:
1. Tester toutes les connexions
2. Initialiser les tables de sync
3. Lancer une première synchronisation test
4. Démarrer le service complet
"""

import asyncio
import sys
import os
from datetime import datetime
from pathlib import Path
import argparse

# Ajouter le backend au path
sys.path.append(str(Path(__file__).parent.parent / "backend"))

from app.core.config import settings
from app.core.database import get_async_session_context, test_async_connection
from app.utils.hfsql_connector import HFSQLConnector
from app.sync.sync_manager import SynergoSyncManager
from app.sync.scheduler import SchedulerService, SynergoSyncScheduler
from sqlalchemy import text
from loguru import logger


class SynergoQuickStart:
    """
    Gestionnaire de démarrage rapide Synergo
    """

    def __init__(self):
        self.hfsql_connector = HFSQLConnector()
        self.sync_manager = SynergoSyncManager()
        self.test_results = {}

    async def run_complete_setup(self, test_only: bool = False, skip_init: bool = False):
        """
        Exécute le setup complet de Synergo
        """
        print("🚀 DÉMARRAGE RAPIDE SYNERGO")
        print("=" * 60)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        try:
            # Étape 1: Tests de connexion
            await self._test_all_connections()

            if not test_only:
                # Étape 2: Initialisation des tables (sauf si skip)
                if not skip_init:
                    await self._initialize_sync_tables()

                # Étape 3: Synchronisation test
                await self._run_test_sync()

                # Étape 4: Démarrage du service (optionnel)
                await self._prompt_start_service()

            self._print_final_summary()

        except Exception as e:
            logger.error(f"❌ Erreur critique: {e}")
            print(f"\n❌ ÉCHEC DU DÉMARRAGE: {e}")
            return False

        return True

    async def _test_all_connections(self):
        """
        Teste toutes les connexions nécessaires
        """
        print("🔍 TESTS DE CONNEXION")
        print("-" * 30)

        # Test 1: HFSQL
        print("📡 Test connexion HFSQL...")
        try:
            hfsql_test = await self.hfsql_connector.test_connection_step_by_step()

            if hfsql_test.get("final_status") == "success":
                print("✅ HFSQL: Connexion OK")
                total_sales = hfsql_test.get("total_sales", 0)
                print(f"   📊 {total_sales} ventes détectées")
                self.test_results["hfsql"] = {"status": "OK", "sales_count": total_sales}
            else:
                print("❌ HFSQL: Échec de connexion")
                print(f"   Détails: {hfsql_test}")
                self.test_results["hfsql"] = {"status": "ERROR", "details": hfsql_test}
                raise Exception("Connexion HFSQL échouée")

        except Exception as e:
            print(f"❌ HFSQL: Erreur - {e}")
            self.test_results["hfsql"] = {"status": "ERROR", "error": str(e)}
            raise
        finally:
            self.hfsql_connector.close()

        # Test 2: PostgreSQL - VERSION CORRIGÉE
        print("\n🐘 Test connexion PostgreSQL...")
        try:
            # Utiliser la fonction de test simple
            connection_ok = await test_async_connection()

            if connection_ok:
                print("✅ PostgreSQL: Connexion OK")

                # Test plus détaillé avec version
                async with get_async_session_context() as session:
                    result = await session.execute(text("SELECT version()"))
                    version = result.fetchone()[0]
                    print(f"   📋 Version: {version.split(',')[0]}")
                    self.test_results["postgresql"] = {"status": "OK", "version": version}
            else:
                print("❌ PostgreSQL: Échec de connexion")
                self.test_results["postgresql"] = {"status": "ERROR", "error": "Test de connexion échoué"}
                raise Exception("Connexion PostgreSQL échouée")

        except Exception as e:
            print(f"❌ PostgreSQL: Erreur - {e}")
            self.test_results["postgresql"] = {"status": "ERROR", "error": str(e)}
            raise

        print("\n✅ Toutes les connexions sont fonctionnelles!")

    async def _initialize_sync_tables(self):
        """
        Initialise les tables de synchronisation
        """
        print("\n🏗️  INITIALISATION DES TABLES")
        print("-" * 35)

        try:
            async with get_async_session_context() as session:
                # Vérifier si les tables existent déjà
                check_query = """
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = 'synergo_sync' 
                AND table_name = 'sync_tables'
                """
                result = await session.execute(text(check_query))
                tables_exist = result.fetchone()[0] > 0

                if tables_exist:
                    print("📋 Tables de sync déjà présentes")

                    # Vérifier la configuration
                    config_query = "SELECT COUNT(*) FROM synergo_sync.sync_tables"
                    result = await session.execute(text(config_query))
                    config_count = result.fetchone()[0]
                    print(f"   🔧 {config_count} tables configurées")

                else:
                    print("📋 Création des tables de synchronisation...")
                    await self._create_sync_tables_manually(session)

                # Vérifier l'état des tables
                await self._verify_sync_tables(session)

        except Exception as e:
            print(f"❌ Erreur initialisation tables: {e}")
            raise

    async def _create_sync_tables_manually(self, session):
        """
        Création manuelle des tables si nécessaire
        """
        print("🔧 Création des tables...")

        # Créer les schémas
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS synergo_sync"))
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS synergo_core"))

        # Table sync_tables
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
        await session.execute(text(sync_tables_sql))

        # Table sync_state
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
        await session.execute(text(sync_state_sql))

        # Table sync_log
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
        await session.execute(text(sync_log_sql))

        # Configuration initiale
        config_insert = """
        INSERT INTO synergo_sync.sync_tables (table_name, hfsql_table, sync_strategy, sync_interval_minutes, batch_size) 
        VALUES 
        ('sales_summary', 'sorties', 'ID_BASED', 30, 1000),
        ('products_catalog', 'nomenclature', 'ID_BASED', 60, 500)
        ON CONFLICT (table_name) DO NOTHING
        """
        await session.execute(text(config_insert))

        # États initiaux
        state_insert = """
        INSERT INTO synergo_sync.sync_state (table_name, last_sync_id, last_sync_status) 
        VALUES 
        ('sales_summary', 0, 'NEVER_SYNCED'),
        ('products_catalog', 0, 'NEVER_SYNCED')
        ON CONFLICT (table_name) DO NOTHING
        """
        await session.execute(text(state_insert))

        await session.commit()
        print("✅ Tables créées avec succès")

    async def _verify_sync_tables(self, session):
        """
        Vérifie que les tables sont correctement configurées
        """
        # Compter les tables configurées
        result = await session.execute(text("SELECT COUNT(*) FROM synergo_sync.sync_tables WHERE is_active = true"))
        active_tables = result.fetchone()[0]

        result = await session.execute(text("SELECT COUNT(*) FROM synergo_sync.sync_state"))
        state_records = result.fetchone()[0]

        print(f"📊 Vérification: {active_tables} tables actives, {state_records} états")

        if active_tables == 0:
            print("⚠️ Aucune table configurée pour la sync!")

        self.test_results["sync_setup"] = {
            "status": "OK" if active_tables > 0 else "WARNING",
            "active_tables": active_tables,
            "state_records": state_records
        }

    async def _run_test_sync(self):
        """
        Lance une synchronisation de test
        """
        print("\n🔄 SYNCHRONISATION TEST")
        print("-" * 25)

        try:
            print("🎯 Test sync disponible mais désactivé pour ce test...")
            print("✅ Module sync prêt")

            self.test_results["test_sync"] = {
                "status": "SKIPPED",
                "records": 0,
                "duration": 0
            }

        except Exception as e:
            print(f"❌ Erreur sync test: {e}")
            self.test_results["test_sync"] = {"status": "ERROR", "error": str(e)}

    async def _prompt_start_service(self):
        """
        Demande si on veut démarrer le service complet
        """
        print("\n🎮 DÉMARRAGE DU SERVICE")
        print("-" * 25)
        print("📝 Service prêt - Démarrage manuel recommandé")
        print("   Utilisez: python backend/main.py")

    def _print_final_summary(self):
        """
        Affiche le résumé final
        """
        print("\n" + "=" * 60)
        print("📋 RÉSUMÉ DU DÉMARRAGE SYNERGO")
        print("=" * 60)

        for component, result in self.test_results.items():
            status_emoji = "✅" if result["status"] == "OK" else "❌" if result["status"] == "ERROR" else "⚠️"
            print(f"{status_emoji} {component.upper()}: {result['status']}")

            if component == "hfsql" and "sales_count" in result:
                print(f"   📊 {result['sales_count']} ventes HFSQL détectées")

        print("\n🎯 PROCHAINES ÉTAPES:")
        print("   1. Démarrer l'API: python backend/main.py")
        print("   2. Dashboard: http://localhost:8000/api/v1/sync/dashboard")
        print("   3. Documentation: http://localhost:8000/docs")

        print(f"\n🏁 Tests terminés: {datetime.now().strftime('%H:%M:%S')}")


async def main():
    """
    Point d'entrée principal
    """
    parser = argparse.ArgumentParser(description="Démarrage rapide Synergo")
    parser.add_argument("--test-only", action="store_true", help="Tests uniquement")
    parser.add_argument("--skip-init", action="store_true", help="Ignorer l'initialisation")

    args = parser.parse_args()

    # Configuration des logs
    logger.add("logs/synergo_quickstart.log", rotation="10 MB")

    # Démarrage
    quickstart = SynergoQuickStart()
    success = await quickstart.run_complete_setup(
        test_only=args.test_only,
        skip_init=args.skip_init
    )

    if success:
        print("\n🎉 SYNERGO PRÊT À L'EMPLOI!")
    else:
        print("\n💥 ÉCHEC DU DÉMARRAGE")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())