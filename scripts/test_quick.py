# scripts/test_quick.py
"""
Test rapide pour valider les corrections Synergo
Version finale optimisée
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Ajouter backend au path
sys.path.append(str(Path(__file__).parent.parent / "backend"))


async def test_all_corrections():
    """Test rapide de toutes les corrections"""

    print("⚡ TEST RAPIDE POST-CORRECTIONS")
    print("=" * 40)
    print(f"📅 {datetime.now().strftime('%H:%M:%S')}")
    print()

    tests_results = {"passed": 0, "total": 0}

    # Test 1: Import des modules critiques
    print("📦 Test des imports...")
    tests_results["total"] += 1

    try:
        from app.utils.hfsql_connector import HFSQLConnector
        from app.sync.transformers.sales_transformer import SalesTransformer
        from app.core.database import test_async_connection
        from app.sync.sync_manager import SynergoSyncManager

        print("✅ Imports: OK")
        tests_results["passed"] += 1
    except Exception as e:
        print(f"❌ Imports: {e}")
        print("   💡 Solution: Vérifiez que tous les artifacts ont été appliqués")

    # Test 2: Création d'instances (test de syntaxe)
    print("\n🏗️ Test création d'instances...")
    tests_results["total"] += 1

    try:
        connector = HFSQLConnector()
        transformer = SalesTransformer()
        manager = SynergoSyncManager()

        print("✅ Instances: OK")
        tests_results["passed"] += 1
    except Exception as e:
        print(f"❌ Instances: {e}")
        print("   💡 Solution: Vérifiez la syntaxe des fichiers corrigés")

    # Test 3: Transformer robustesse (test critique)
    print("\n🔄 Test transformer robustesse...")
    tests_results["total"] += 1

    try:
        transformer = SalesTransformer()

        # Données problématiques qui cassaient avant
        test_data = [
            {'id': '123', 'date': '20241210', 'total_a_payer': '45,50'},  # Virgule française
            {'id': None, 'date': 'invalid', 'total_a_payer': 'abc'},  # Données invalides
            {'id': 125.7, 'date': '2024-12-10T15:30:00+01:00', 'total_a_payer': '1.234,56'},  # Types mixtes
            {'id': '', 'date': '', 'total_a_payer': ''},  # Valeurs vides
            {'id': '999999', 'date': '20241250', 'total_a_payer': '-999.99'}  # Valeurs limites
        ]

        transformed = await transformer.transform_batch(test_data)

        # On s'attend à au moins 3 enregistrements valides sur 5
        if len(transformed) >= 3:
            print(f"✅ Transformer: {len(transformed)}/5 records OK")

            # Vérifier la qualité des transformations
            valid_ids = [r for r in transformed if r.get('hfsql_id', 0) > 0]
            if len(valid_ids) == len(transformed):
                print("   📊 Tous les IDs transformés sont valides")
                tests_results["passed"] += 1
            else:
                print(f"   ⚠️ {len(valid_ids)}/{len(transformed)} IDs valides")
        else:
            print(f"❌ Transformer: Seulement {len(transformed)}/5 records")
            print("   💡 Solution: Vérifiez les méthodes de conversion dans SalesTransformer")
    except Exception as e:
        print(f"❌ Transformer: {e}")
        print("   💡 Solution: Vérifiez la syntaxe de sales_transformer.py")

    # Test 4: Connexion PostgreSQL
    print("\n🐘 Test PostgreSQL...")
    tests_results["total"] += 1

    try:
        pg_ok = await test_async_connection()
        if pg_ok:
            print("✅ PostgreSQL: OK")
            tests_results["passed"] += 1
        else:
            print("❌ PostgreSQL: Connexion échouée")
            print("   💡 Solution: Vérifiez DATABASE_URL dans .env")
    except Exception as e:
        print(f"❌ PostgreSQL: {e}")
        print("   💡 Solution: Démarrez PostgreSQL et vérifiez la configuration")

    # Test 5: Connexion HFSQL (test critique)
    print("\n🔌 Test HFSQL...")
    tests_results["total"] += 1

    try:
        connector = HFSQLConnector()

        # Test de connexion simple
        connection_ok = await connector.connect()

        if connection_ok:
            print("✅ HFSQL: Connexion OK")

            # Test requête basique pour vérifier la stabilité
            try:
                result = await connector.execute_query("SELECT COUNT(*) as total FROM sorties")
                if result:
                    count = result[0].get('total', 0)
                    print(f"   📊 {count} ventes détectées")
                    tests_results["passed"] += 1
                else:
                    print("   ⚠️ Pas de résultat mais connexion OK")
                    tests_results["passed"] += 0.5  # Demi-point
            except Exception as query_error:
                print(f"   ⚠️ Erreur requête: {query_error}")
                print("   💡 Solution: Vérifiez les permissions sur la table sorties")
        else:
            print("❌ HFSQL: Connexion échouée")
            print("   💡 Solution: Vérifiez HFSQL_SERVER et HFSQL_PASSWORD dans .env")

        connector.close()

    except Exception as e:
        print(f"❌ HFSQL: {e}")
        print("   💡 Solution: Vérifiez hfsql_connector.py et les paramètres de connexion")

    # Test 6: Configuration Sync Manager
    print("\n⚙️ Test configuration sync...")
    tests_results["total"] += 1

    try:
        manager = SynergoSyncManager()

        # Vérifier la configuration des tables
        config = manager.sync_tables_config
        if config and len(config) > 0:
            print(f"✅ Configuration: {len(config)} tables configurées")

            # Vérifier qu'on a au moins sales_summary
            if 'sales_summary' in config:
                sales_config = config['sales_summary']
                print(f"   📋 Table ventes: {sales_config['hfsql_table']} → {sales_config['table_name']}")
                tests_results["passed"] += 1
            else:
                print("   ⚠️ Configuration sales_summary manquante")
        else:
            print("❌ Configuration: Aucune table configurée")
            print("   💡 Solution: Vérifiez _load_sync_config() dans SynergoSyncManager")
    except Exception as e:
        print(f"❌ Configuration: {e}")
        print("   💡 Solution: Vérifiez sync_manager.py")

    # Résumé avec recommandations
    print("\n📊 RÉSUMÉ DES TESTS")
    print("=" * 20)

    success_rate = (tests_results["passed"] / tests_results["total"]) * 100
    print(f"✅ {tests_results['passed']:.1f}/{tests_results['total']} tests réussis ({success_rate:.1f}%)")

    if success_rate >= 85:
        print("\n🎉 SYSTÈME OPÉRATIONNEL!")
        print("   ✅ Prêt pour la synchronisation en production")
        print("   🚀 Prochaine étape: python scripts/test_manual_sync.py")
        return True
    elif success_rate >= 70:
        print("\n⚠️ SYSTÈME PARTIELLEMENT FONCTIONNEL")
        print("   🔧 Quelques ajustements mineurs nécessaires")
        print("   📋 Consultez les solutions suggérées ci-dessus")
        return True
    else:
        print("\n❌ PROBLÈMES MAJEURS DÉTECTÉS")
        print("   🚨 Corrections importantes nécessaires")
        print("   📖 Consultez GUIDE_CORRECTIONS.md pour l'aide détaillée")
        return False


async def test_integration_simple():
    """Test d'intégration simple du pipeline complet"""
    print("\n🔄 TEST D'INTÉGRATION SIMPLE")
    print("=" * 35)

    try:
        from app.sync.sync_manager import SynergoSyncManager

        manager = SynergoSyncManager()

        # Test dashboard data (doit fonctionner même sans données)
        try:
            dashboard = await manager.get_sync_dashboard_data()
            if dashboard and 'sync_states' in dashboard:
                print(f"✅ Dashboard: {len(dashboard['sync_states'])} tables")
                print(f"   📊 Stats 24h: {dashboard.get('stats_24h', {}).get('total_syncs', 0)} syncs")
                return True
            else:
                print("⚠️ Dashboard: Structure incomplète")
                return False
        except Exception as dashboard_error:
            print(f"❌ Dashboard: {dashboard_error}")
            print("   💡 Vérifiez les tables synergo_sync en base")
            return False

    except Exception as e:
        print(f"❌ Intégration: {e}")
        return False


async def main():
    """Point d'entrée principal avec workflow complet"""

    print("🔍 VALIDATION COMPLÈTE SYNERGO")
    print("=" * 50)

    # Test principal
    general_ok = await test_all_corrections()

    # Test d'intégration si le général passe
    if general_ok:
        integration_ok = await test_integration_simple()

        if integration_ok:
            print("\n🚀 VALIDATION COMPLÈTE RÉUSSIE!")
            print("\n📋 WORKFLOW RECOMMANDÉ:")
            print("1. 🗄️  python scripts/create_missing_tables.py")
            print("2. 🧪 python scripts/test_manual_sync.py")
            print("3. 🚀 python backend/main.py")
            print("4. 🌐 http://localhost:8000/api/v1/sync/dashboard")

            print("\n🎯 SURVEILLANCE:")
            print("• Logs: tail -f logs/synergo.log")
            print("• Health: http://localhost:8000/api/v1/sync/health")
            print("• Manual sync: POST http://localhost:8000/api/v1/sync/manual")
        else:
            print("\n⚠️ Tests généraux OK, problème d'intégration")
            print("   🔧 Vérifiez la structure de base de données")
    else:
        print("\n🔧 CORRECTIONS NÉCESSAIRES")
        print("   📖 Consultez GUIDE_CORRECTIONS.md")
        print("   🔄 Réappliquez les artifacts manquants")

    print(f"\n🏁 Validation terminée: {datetime.now().strftime('%H:%M:%S')}")
    return general_ok


if __name__ == "__main__":
    success = asyncio.run(main())

    if success:
        print("\n✅ Système validé et prêt!")
    else:
        print("\n❌ Corrections nécessaires")
        sys.exit(1)