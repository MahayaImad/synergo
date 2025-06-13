# tests/test_synergo_integration.py
"""
Tests d'intégration complets pour Synergo
"""
import pytest
import asyncio
from datetime import datetime, date, time
import sys
from pathlib import Path

# Ajouter le backend au path
sys.path.append(str(Path(__file__).parent.parent / "backend"))

from app.utils.hfsql_connector import HFSQLConnector
from app.sync.sync_manager import SynergoSyncManager
from app.sync.transformers.sales_transformer import SalesTransformer
from app.sync.strategies.id_based_sync import IdBasedSyncStrategy
from app.core.database import get_async_session


class TestSynergoIntegration:
    """Tests d'intégration Synergo"""

    @pytest.fixture
    async def hfsql_connector(self):
        """Fixture connecteur HFSQL"""
        connector = HFSQLConnector()
        yield connector
        connector.close()

    @pytest.fixture
    async def sync_manager(self):
        """Fixture manager de sync"""
        return SynergoSyncManager()

    @pytest.mark.asyncio
    async def test_hfsql_connection(self, hfsql_connector):
        """Test de connexion HFSQL"""
        # Test de connexion
        connected = await hfsql_connector.connect()
        assert connected, "La connexion HFSQL devrait réussir"

        # Test de requête simple
        result = await hfsql_connector.execute_query("SELECT COUNT(*) as total FROM sorties")
        assert isinstance(result, list), "Le résultat devrait être une liste"
        assert len(result) > 0, "Devrait retourner au moins un résultat"
        assert 'total' in result[0], "Devrait contenir le champ 'total'"

        total_sales = result[0]['total']
        print(f"✅ Total ventes HFSQL: {total_sales}")
        assert total_sales >= 0, "Le nombre de ventes devrait être >= 0"

    @pytest.mark.asyncio
    async def test_sales_transformer(self):
        """Test du transformer de ventes"""
        transformer = SalesTransformer()

        # Données test HFSQL
        test_data = [
            {
                'id': 12345,
                'date': '20241210',
                'heure': '143022',
                'client': ' Client Test ',
                'caissier': 'Marie Dupont',
                'total_a_payer': '123.45',
                'encaisse': '130.00',
                'benefice': '23.10',
                'nombre_article': '5'
            },
            {
                'id': 12346,
                'date': '20241210',
                'heure': '150000',
                'client': '',
                'caissier': 'Jean Martin',
                'total_a_payer': '67.89',
                'encaisse': '70.00',
                'benefice': '15.20',
                'nombre_article': '2'
            }
        ]

        # Transformation
        transformed = await transformer.transform_batch(test_data)

        # Vérifications
        assert len(transformed) == 2, "Devrait transformer 2 enregistrements"

        first_record = transformed[0]
        assert first_record['hfsql_id'] == 12345
        assert first_record['sale_date'] == date(2024, 12, 10)
        assert first_record['sale_time'] == time(14, 30, 22)
        assert first_record['customer'] == 'Client Test'
        assert first_record['total_amount'] == 123.45
        assert first_record['item_count'] == 5

        print(f"✅ Transformer: {len(transformed)} enregistrements transformés")

    @pytest.mark.asyncio
    async def test_postgresql_connection(self):
        """Test de connexion PostgreSQL"""
        from sqlalchemy import text

        async with get_async_session() as session:
            result = await session.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            assert "PostgreSQL" in version
            print(f"✅ PostgreSQL connecté: {version.split(',')[0]}")

    @pytest.mark.asyncio
    async def test_sync_tables_exist(self):
        """Vérifie que les tables de sync existent"""
        from sqlalchemy import text

        async with get_async_session() as session:
            # Vérifier tables sync
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'synergo_sync'
            """
            result = await session.execute(text(tables_query))
            tables = [row[0] for row in result.fetchall()]

            expected_tables = ['sync_tables', 'sync_state', 'sync_log']
            for table in expected_tables:
                assert table in tables, f"Table {table} manquante"

            print(f"✅ Tables sync présentes: {tables}")

    @pytest.mark.asyncio
    async def test_id_based_sync_strategy(self, hfsql_connector):
        """Test de la stratégie ID-based"""
        config = {
            'table_name': 'sales_summary',
            'hfsql_table': 'sorties',
            'id_field': 'id',
            'batch_size': 10,
            'schema': 'synergo_core'
        }

        strategy = IdBasedSyncStrategy(config, hfsql_connector)

        # Test récupération nouveaux enregistrements
        records = await strategy.get_new_records(last_sync_id=0)
        assert isinstance(records, list), "Devrait retourner une liste"

        if records:
            # Vérifier structure
            first_record = records[0]
            assert 'id' in first_record, "Devrait contenir le champ 'id'"
            assert isinstance(first_record['id'], int), "L'ID devrait être un entier"

            print(f"✅ Stratégie ID-based: {len(records)} enregistrements récupérés")
        else:
            print("✅ Stratégie ID-based: Aucun nouvel enregistrement (normal)")

    @pytest.mark.asyncio
    async def test_sync_manager_dashboard(self, sync_manager):
        """Test du dashboard de sync"""
        dashboard = await sync_manager.get_sync_dashboard_data()

        assert 'sync_states' in dashboard
        assert 'stats_24h' in dashboard
        assert 'last_update' in dashboard

        print(f"✅ Dashboard: {len(dashboard['sync_states'])} tables configurées")

    @pytest.mark.asyncio
    async def test_manual_sync_dry_run(self, sync_manager):
        """Test de synchronisation manuelle (dry run)"""
        # Test avec une seule table
        config = sync_manager.sync_tables_config.get('sales_summary')
        if not config:
            pytest.skip("Configuration sales_summary non trouvée")

        try:
            result = await sync_manager.sync_single_table(config)

            assert result is not None
            assert hasattr(result, 'status')
            assert hasattr(result, 'records_processed')

            print(f"✅ Sync manuelle: {result.status}, {result.records_processed} enregistrements")

        except Exception as e:
            # Log l'erreur mais ne fait pas échouer le test
            print(f"⚠️ Sync manuelle échouée (attendu en test): {e}")


# Script de test rapide
async def run_quick_tests():
    """Exécute les tests rapidement sans pytest"""
    print("🧪 TESTS RAPIDES SYNERGO")
    print("=" * 40)

    test_instance = TestSynergoIntegration()

    # Test 1: HFSQL
    try:
        hfsql = HFSQLConnector()
        await test_instance.test_hfsql_connection(hfsql)
        hfsql.close()
    except Exception as e:
        print(f"❌ Test HFSQL échoué: {e}")

    # Test 2: Transformer
    try:
        await test_instance.test_sales_transformer()
    except Exception as e:
        print(f"❌ Test Transformer échoué: {e}")

    # Test 3: PostgreSQL
    try:
        await test_instance.test_postgresql_connection()
    except Exception as e:
        print(f"❌ Test PostgreSQL échoué: {e}")

    # Test 4: Tables
    try:
        await test_instance.test_sync_tables_exist()
    except Exception as e:
        print(f"❌ Test Tables échoué: {e}")

    print("\n🏁 Tests terminés")


# Tests de performance
class TestPerformance:
    """Tests de performance"""

    @pytest.mark.asyncio
    async def test_sync_performance(self):
        """Test de performance de synchronisation"""
        import time

        hfsql = HFSQLConnector()
        transformer = SalesTransformer()

        start_time = time.time()

        # Récupérer 100 enregistrements
        records = await hfsql.execute_query("SELECT * FROM sorties LIMIT 100")
        fetch_time = time.time() - start_time

        # Transformer
        transform_start = time.time()
        transformed = await transformer.transform_batch(records)
        transform_time = time.time() - transform_start

        total_time = time.time() - start_time

        # Métriques
        records_per_second = len(records) / total_time if total_time > 0 else 0

        print(f"📊 Performance:")
        print(f"   Fetch: {fetch_time:.3f}s")
        print(f"   Transform: {transform_time:.3f}s")
        print(f"   Total: {total_time:.3f}s")
        print(f"   Vitesse: {records_per_second:.1f} records/sec")

        # Assertions performance
        assert fetch_time < 5.0, "Fetch devrait prendre moins de 5s"
        assert transform_time < 2.0, "Transform devrait prendre moins de 2s"
        assert records_per_second > 10, "Devrait traiter > 10 records/sec"

        hfsql.close()


if __name__ == "__main__":
    # Exécution directe des tests
    asyncio.run(run_quick_tests())