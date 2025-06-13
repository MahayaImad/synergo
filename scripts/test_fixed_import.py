# scripts/test_fixed_imports.py
"""
Test des imports après les corrections
"""

import sys
import os
from pathlib import Path

# Ajouter backend au path
backend_dir = Path(__file__).parent.parent / "backend"
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))


def test_imports():
    """Test tous les imports critiques"""

    print("🔍 TEST DES IMPORTS CORRIGÉS")
    print("=" * 40)

    tests = []

    # Test 1: Configuration
    try:
        from app.core.config import settings
        print("✅ Config: OK")
        tests.append(("config", True, None))
    except Exception as e:
        print(f"❌ Config: {e}")
        tests.append(("config", False, str(e)))

    # Test 2: Database
    try:
        from app.core.database import Base, get_async_session
        print("✅ Database: OK")
        tests.append(("database", True, None))
    except Exception as e:
        print(f"❌ Database: {e}")
        tests.append(("database", False, str(e)))

    # Test 3: Modèles sync
    try:
        from app.models.sync_models import SyncTable, SyncState, SyncLog
        print("✅ Sync Models: OK")
        tests.append(("sync_models", True, None))
    except Exception as e:
        print(f"❌ Sync Models: {e}")
        tests.append(("sync_models", False, str(e)))

    # Test 4: Modèles pharma
    try:
        from app.models.pharma_models import SalesSummary, ProductsCatalog
        print("✅ Pharma Models: OK")
        tests.append(("pharma_models", True, None))
    except Exception as e:
        print(f"❌ Pharma Models: {e}")
        tests.append(("pharma_models", False, str(e)))

    # Test 5: HFSQL Connector
    try:
        from app.utils.hfsql_connector import HFSQLConnector
        print("✅ HFSQL Connector: OK")
        tests.append(("hfsql_connector", True, None))
    except Exception as e:
        print(f"❌ HFSQL Connector: {e}")
        tests.append(("hfsql_connector", False, str(e)))

    # Test 6: Transformers
    try:
        from app.sync.transformers.sales_transformer import SalesTransformer
        print("✅ Sales Transformer: OK")
        tests.append(("sales_transformer", True, None))
    except Exception as e:
        print(f"❌ Sales Transformer: {e}")
        tests.append(("sales_transformer", False, str(e)))

    # Test 7: Strategies
    try:
        from app.sync.strategies.id_based_sync import IdBasedSyncStrategy
        print("✅ ID-Based Strategy: OK")
        tests.append(("id_based_strategy", True, None))
    except Exception as e:
        print(f"❌ ID-Based Strategy: {e}")
        tests.append(("id_based_strategy", False, str(e)))

    # Test 8: Sync Manager
    try:
        from app.sync.sync_manager import SynergoSyncManager
        print("✅ Sync Manager: OK")
        tests.append(("sync_manager", True, None))
    except Exception as e:
        print(f"❌ Sync Manager: {e}")
        tests.append(("sync_manager", False, str(e)))

    # Test 9: Scheduler
    try:
        from app.sync.scheduler import SynergoSyncScheduler
        print("✅ Scheduler: OK")
        tests.append(("scheduler", True, None))
    except Exception as e:
        print(f"❌ Scheduler: {e}")
        tests.append(("scheduler", False, str(e)))

    # Test 10: API
    try:
        from app.api.v1.sync_status import router
        print("✅ Sync API: OK")
        tests.append(("sync_api", True, None))
    except Exception as e:
        print(f"❌ Sync API: {e}")
        tests.append(("sync_api", False, str(e)))

    # Résumé
    print(f"\n📊 RÉSUMÉ:")
    success_count = sum(1 for _, success, _ in tests if success)
    total_count = len(tests)

    print(f"   ✅ {success_count}/{total_count} imports réussis")
    print(f"   ❌ {total_count - success_count} échecs")

    if success_count == total_count:
        print(f"\n🎉 TOUS LES IMPORTS FONCTIONNENT!")
        print(f"   Prêt pour les tests de connexion")
        return True
    else:
        print(f"\n🔧 Erreurs restantes:")
        for name, success, error in tests:
            if not success:
                print(f"   - {name}: {error}")
        return False


def test_basic_functionality():
    """Test de fonctionnalité de base"""

    print(f"\n🧪 TEST DE FONCTIONNALITÉ")
    print("=" * 30)

    try:
        # Test création d'instances
        from app.sync.transformers.sales_transformer import SalesTransformer
        transformer = SalesTransformer()
        print("✅ SalesTransformer: Instance créée")

        from app.utils.hfsql_connector import HFSQLConnector
        connector = HFSQLConnector()
        print("✅ HFSQLConnector: Instance créée")

        from app.sync.sync_manager import SynergoSyncManager
        manager = SynergoSyncManager()
        print("✅ SynergoSyncManager: Instance créée")

        print(f"\n🎯 Prêt pour les tests de connexion!")
        return True

    except Exception as e:
        print(f"❌ Erreur de fonctionnalité: {e}")
        return False


if __name__ == "__main__":
    print("Démarrage des tests...\n")

    imports_ok = test_imports()

    if imports_ok:
        functionality_ok = test_basic_functionality()

        if functionality_ok:
            print(f"\n🚀 PRÊT POUR LE DÉMARRAGE SYNERGO!")
            print(f"   Prochaine étape: python scripts/quick_start_synergo.py --test-only")
        else:
            print(f"\n🔧 Problèmes de fonctionnalité à corriger")
    else:
        print(f"\n🔧 Problèmes d'imports à corriger d'abord")

    print(f"\n🏁 Test terminé")