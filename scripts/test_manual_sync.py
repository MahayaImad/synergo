# scripts/test_manual_sync.py
"""
Test de synchronisation manuelle pour vérifier que tout fonctionne
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Ajouter backend au path
sys.path.append(str(Path(__file__).parent.parent / "backend"))

from app.sync.sync_manager import SynergoSyncManager
from app.sync.scheduler import get_scheduler_instance, SchedulerService


async def test_manual_sync():
    """Test complet de synchronisation manuelle"""

    print("🔄 TEST DE SYNCHRONISATION MANUELLE")
    print("=" * 50)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # 1. Créer le manager de sync
        sync_manager = SynergoSyncManager()

        print("📊 Configuration des tables:")
        for table_name, config in sync_manager.sync_tables_config.items():
            print(f"   - {config['table_name']} ← {config['hfsql_table']}")
        print()

        # 2. Vérifier l'état avant sync
        print("🔍 État avant synchronisation:")
        dashboard_before = await sync_manager.get_sync_dashboard_data()

        for table in dashboard_before['sync_states']:
            print(f"   📋 {table['table_name']}: {table['last_sync_status']} (ID: {table['last_sync_id']})")
        print()

        # 3. Test sync d'une seule table d'abord
        print("🎯 Test sync table 'sales_summary':")
        config = sync_manager.sync_tables_config.get('sales_summary')

        if config:
            start_time = datetime.now()
            result = await sync_manager.sync_single_table(config)
            duration = (datetime.now() - start_time).total_seconds()

            print(f"   📊 Résultat: {result.status}")
            print(f"   📈 Enregistrements: {result.records_processed}")
            print(f"   ⏱️ Durée: {duration:.2f}s")

            if result.error_message:
                print(f"   ❌ Erreur: {result.error_message}")

            if result.status == 'SUCCESS':
                print("   ✅ Sync réussie!")
            elif result.status == 'NO_CHANGES':
                print("   📌 Aucun nouveau enregistrement")
            else:
                print("   ⚠️ Problème détecté")

        print()

        # 4. Synchronisation complète
        print("🔄 Synchronisation complète de toutes les tables:")

        start_time = datetime.now()
        all_results = await sync_manager.sync_all_active_tables()
        total_duration = (datetime.now() - start_time).total_seconds()

        print(f"   📊 {len(all_results)} tables traitées en {total_duration:.2f}s")

        # Résumé des résultats
        success_count = sum(1 for r in all_results if r.status == 'SUCCESS')
        no_changes_count = sum(1 for r in all_results if r.status == 'NO_CHANGES')
        error_count = sum(1 for r in all_results if r.status == 'ERROR')
        total_records = sum(r.records_processed for r in all_results)

        print(f"   ✅ {success_count} réussies")
        print(f"   📌 {no_changes_count} sans changement")
        print(f"   ❌ {error_count} en erreur")
        print(f"   📈 {total_records} enregistrements au total")

        # Détails par table
        print("\n📋 Détails par table:")
        for result in all_results:
            status_emoji = "✅" if result.status == 'SUCCESS' else "📌" if result.status == 'NO_CHANGES' else "❌"
            print(f"   {status_emoji} {result.table_name}: {result.records_processed} records ({result.duration_ms}ms)")
            if result.error_message:
                print(f"       💬 {result.error_message}")

        # 5. Vérifier l'état après sync
        print("\n🔍 État après synchronisation:")
        dashboard_after = await sync_manager.get_sync_dashboard_data()

        for table in dashboard_after['sync_states']:
            status_emoji = "✅" if table['last_sync_status'] == 'SUCCESS' else "📌" if table[
                                                                                         'last_sync_status'] == 'NO_CHANGES' else "❌"
            timestamp = table['last_sync_timestamp'][:19] if table['last_sync_timestamp'] else 'Jamais'
            print(
                f"   {status_emoji} {table['table_name']}: {table['last_sync_status']} - {table['total_records']} records")
            print(f"       📅 Dernière sync: {timestamp}")

        print(f"\n🎉 TEST TERMINÉ AVEC SUCCÈS!")

        return True

    except Exception as e:
        print(f"❌ ERREUR PENDANT LE TEST: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_scheduler_service():
    """Test du service scheduler"""

    print("\n🎮 TEST DU SERVICE SCHEDULER")
    print("=" * 35)

    try:
        # Test statut
        status = SchedulerService.get_sync_status()
        print(f"📊 Scheduler running: {status['is_running']}")
        print(f"📊 Sync count: {status['sync_count']}")
        print(f"📊 Error count: {status['error_count']}")

        # Test sync manuelle via le service
        print("\n🔄 Test sync manuelle via SchedulerService...")
        start_time = datetime.now()
        results = await SchedulerService.trigger_manual_sync()
        duration = (datetime.now() - start_time).total_seconds()

        print(f"   📊 {len(results)} tables en {duration:.2f}s")
        for result in results:
            print(f"   📋 {result.table_name}: {result.status} ({result.records_processed} records)")

        # Test rapport détaillé
        print("\n📈 Test rapport détaillé...")
        report = await SchedulerService.get_sync_report()

        print(f"   📊 Tables configurées: {len(report['sync_states'])}")
        print(f"   📊 Stats 24h: {report['stats_24h']['total_syncs']} syncs")

        print("✅ Service scheduler OK!")
        return True

    except Exception as e:
        print(f"❌ Erreur service scheduler: {e}")
        return False


async def main():
    """Point d'entrée principal"""

    print("🧪 SUITE DE TESTS SYNERGO")
    print("=" * 60)

    # Test 1: Sync manuelle
    sync_ok = await test_manual_sync()

    if sync_ok:
        # Test 2: Service scheduler
        scheduler_ok = await test_scheduler_service()

        if scheduler_ok:
            print(f"\n🎉 TOUS LES TESTS RÉUSSIS!")
            print(f"🚀 Synergo est opérationnel et prêt!")
            print(f"\nℹ️  Prochaines étapes:")
            print(f"   1. Démarrer le scheduler: POST /api/v1/sync/start")
            print(f"   2. Surveiller le dashboard: GET /api/v1/sync/dashboard")
            print(f"   3. Vérifier les logs: GET /api/v1/sync/logs")
        else:
            print(f"\n⚠️ Problème avec le service scheduler")
    else:
        print(f"\n❌ Problème avec la synchronisation")

    print(f"\n🏁 Tests terminés: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())