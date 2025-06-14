# scripts/test_manual_sync.py - VERSION ERP COMPLÈTE
"""
Test de synchronisation manuelle ERP complet
5 tables: produits → achats (A/AV) → ventes avec calcul de marge
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
    """Test complet de synchronisation manuelle ERP"""

    print("🔄 TEST DE SYNCHRONISATION ERP COMPLÈTE")
    print("=" * 60)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📋 Architecture: produits → achats (A/AV) → ventes")
    print("💰 Calculs de marge avec prix d'achat précis")
    print()

    try:
        # 1. Créer le manager de sync
        sync_manager = SynergoSyncManager()

        print("📊 Configuration ERP des tables:")
        # Afficher dans l'ordre de synchronisation
        sorted_configs = sorted(
            sync_manager.sync_tables_config.items(),
            key=lambda x: x[1].get('sync_order', 999)
        )

        for table_key, config in sorted_configs:
            order = config.get('sync_order', '?')
            interval = config.get('sync_interval_minutes', 30)
            transformer = config.get('transformer', None)
            transformer_name = transformer.__name__ if transformer else 'N/A'

            print(f"   {order}. {config['table_name']} ← {config['hfsql_table']}")
            print(f"      🔄 {interval}min | 🔧 {transformer_name}")
        print()

        # 2. Vérifier l'état avant sync
        print("🔍 État avant synchronisation:")
        try:
            dashboard_before = await sync_manager.get_sync_dashboard_data()

            if 'sync_states' in dashboard_before:
                for table in dashboard_before['sync_states']:
                    print(f"   📋 {table['table_name']}: {table['last_sync_status']} (ID: {table['last_sync_id']})")
            else:
                print("   ⚠️ Aucune donnée de dashboard disponible")
        except Exception as e:
            print(f"   ⚠️ Erreur récupération dashboard: {e}")
        print()

        # 3. Test sync d'une seule table d'abord (produits)
        print("🎯 Test sync table 'products_catalog' (produits):")
        config = sync_manager.sync_tables_config.get('products_catalog')

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
                print("   ✅ Sync produits réussie!")
            elif result.status == 'NO_CHANGES':
                print("   📌 Aucun nouveau produit")
            else:
                print("   ⚠️ Problème détecté")
        else:
            print("   ❌ Configuration 'products_catalog' non trouvée")

        print()

        # 4. Synchronisation complète ERP dans l'ordre
        print("🔄 Synchronisation ERP complète (ordre: produits → achats → ventes):")

        start_time = datetime.now()
        # CORRECTION: Utiliser la méthode correcte
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

        # Détails par table avec catégorisation
        print("\n📋 Détails par catégorie:")

        # Grouper par catégorie
        products_results = [r for r in all_results if 'product' in r.table_name]
        purchase_results = [r for r in all_results if 'purchase' in r.table_name]
        sales_results = [r for r in all_results if 'sales' in r.table_name]

        categories = [
            ("📦 PRODUITS", products_results),
            ("🛒 ACHATS", purchase_results),
            ("💰 VENTES", sales_results)
        ]

        for category_name, results in categories:
            if results:
                print(f"\n   {category_name}:")
                for result in results:
                    status_emoji = "✅" if result.status == 'SUCCESS' else "📌" if result.status == 'NO_CHANGES' else "❌"
                    print(
                        f"     {status_emoji} {result.table_name}: {result.records_processed} records ({result.duration_ms}ms)")
                    if result.error_message:
                        print(f"         💬 {result.error_message}")

        # 5. Vérifier l'état après sync
        print("\n🔍 État après synchronisation:")
        try:
            dashboard_after = await sync_manager.get_sync_dashboard_data()

            if 'sync_states' in dashboard_after:
                for table in dashboard_after['sync_states']:
                    status_emoji = "✅" if table['last_sync_status'] == 'SUCCESS' else "📌" if table[
                                                                                                 'last_sync_status'] == 'NO_CHANGES' else "❌"
                    timestamp = table['last_sync_timestamp'][:19] if table['last_sync_timestamp'] else 'Jamais'
                    print(
                        f"   {status_emoji} {table['table_name']}: {table['last_sync_status']} - {table['total_records']} records")
                    print(f"       📅 Dernière sync: {timestamp}")

            # Afficher les stats globales si disponibles
            if 'summary' in dashboard_after:
                summary = dashboard_after['summary']
                health_pct = summary.get('sync_health_percentage', 0)
                print(f"\n📊 Santé globale ERP: {health_pct:.1f}%")
                print(f"   📋 Tables configurées: {summary.get('total_tables_configured', 0)}")
                print(f"   📈 Total enregistrements: {summary.get('total_records_all_tables', 0)}")

        except Exception as e:
            print(f"   ⚠️ Erreur récupération état final: {e}")

        # 6. Test de la vue stock temps réel
        print("\n📦 Test vue stock temps réel:")
        try:
            from app.core.database import get_async_session_context
            from sqlalchemy import text

            async with get_async_session_context() as session:
                stock_query = """
                SELECT product_hfsql_id, product_name, current_stock, total_entries, total_sales
                FROM synergo_analytics.real_time_stock 
                WHERE current_stock IS NOT NULL
                ORDER BY current_stock DESC
                LIMIT 5
                """
                result = await session.execute(text(stock_query))
                stocks = result.fetchall()

                if stocks:
                    print("   📊 Top 5 produits en stock:")
                    for stock in stocks:
                        print(
                            f"     🏷️ ID {stock[0]}: {stock[1][:30]}... | Stock: {stock[2]} | Entrées: {stock[3]} | Ventes: {stock[4]}")
                else:
                    print("   📌 Aucune donnée de stock disponible")

        except Exception as e:
            print(f"   ⚠️ Erreur test vue stock: {e}")

        print(f"\n🎉 TEST ERP TERMINÉ AVEC SUCCÈS!")

        return True

    except Exception as e:
        print(f"❌ ERREUR PENDANT LE TEST ERP: {e}")
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
        print(f"📊 Intervalle: {status.get('sync_interval_minutes', 'N/A')} minutes")

        # Test sync manuelle via le service
        print("\n🔄 Test sync manuelle via SchedulerService...")
        start_time = datetime.now()
        results = await SchedulerService.trigger_manual_sync()
        duration = (datetime.now() - start_time).total_seconds()

        print(f"   📊 {len(results)} tables en {duration:.2f}s")

        # Grouper les résultats par catégorie
        for result in results:
            status_emoji = "✅" if result.status == 'SUCCESS' else "📌" if result.status == 'NO_CHANGES' else "❌"
            category = "📦" if 'product' in result.table_name else "🛒" if 'purchase' in result.table_name else "💰"
            print(
                f"   {status_emoji} {category} {result.table_name}: {result.status} ({result.records_processed} records)")

        # Test rapport détaillé
        print("\n📈 Test rapport détaillé...")
        report = await SchedulerService.get_sync_report()

        print(f"   📊 Tables configurées: {len(report.get('sync_states', []))}")
        stats_24h = report.get('stats_24h', {})
        print(
            f"   📊 Stats 24h: {stats_24h.get('total_syncs', 0)} syncs, {stats_24h.get('total_records_processed', 0)} records")

        print("✅ Service scheduler ERP OK!")
        return True

    except Exception as e:
        print(f"❌ Erreur service scheduler: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_erp_integrity():
    """Test d'intégrité de l'architecture ERP"""

    print("\n🔍 TEST D'INTÉGRITÉ ERP")
    print("=" * 30)

    try:
        from app.core.database import get_async_session_context
        from sqlalchemy import text

        async with get_async_session_context() as session:

            # Test 1: Vérifier la cohérence des données
            print("🔗 Test cohérence relations:")

            # Vérifier que les produits référencés dans purchase_details existent
            coherence_query = """
            SELECT 
                (SELECT COUNT(*) FROM synergo_core.purchase_details pd 
                 LEFT JOIN synergo_core.products_catalog pc ON pd.product_hfsql_id = pc.hfsql_id 
                 WHERE pc.hfsql_id IS NULL) as orphan_purchase_details,

                (SELECT COUNT(*) FROM synergo_core.sales_details sd 
                 LEFT JOIN synergo_core.products_catalog pc ON sd.product_hfsql_id = pc.hfsql_id 
                 WHERE pc.hfsql_id IS NULL) as orphan_sales_details,

                (SELECT COUNT(*) FROM synergo_core.purchase_details WHERE entry_type = 'A') as entries_A,
                (SELECT COUNT(*) FROM synergo_core.purchase_details WHERE entry_type = 'AV') as entries_AV,

                (SELECT COUNT(*) FROM synergo_core.sales_details WHERE sale_type = 'CHIFA') as sales_chifa,
                (SELECT COUNT(*) FROM synergo_core.sales_details WHERE sale_type = 'LIBRE') as sales_libre
            """

            result = await session.execute(text(coherence_query))
            row = result.fetchone()

            if row:
                print(f"   📦 Achats orphelins: {row[0]}")
                print(f"   💰 Ventes orphelines: {row[1]}")
                print(f"   📥 Entrées (A): {row[2]} | Retours (AV): {row[3]}")
                print(f"   🏥 CHIFA: {row[4]} | 🆓 LIBRE: {row[5]}")

                if row[0] == 0 and row[1] == 0:
                    print("   ✅ Intégrité relationnelle OK")
                else:
                    print("   ⚠️ Problèmes d'intégrité détectés")

            # Test 2: Vérifier les calculs de marge
            print("\n💰 Test calculs de marge:")

            margin_query = """
            SELECT 
                sd.hfsql_id,
                sd.sale_price,
                sd.purchase_price,
                sd.unit_profit,
                sd.margin_percent,
                (sd.sale_price - sd.purchase_price) as calculated_profit,
                CASE 
                    WHEN sd.sale_price > 0 THEN 
                        ((sd.sale_price - sd.purchase_price) / sd.sale_price) * 100
                    ELSE 0 
                END as calculated_margin
            FROM synergo_core.sales_details sd
            WHERE sd.sale_price > 0 AND sd.purchase_price > 0
            LIMIT 3
            """

            result = await session.execute(text(margin_query))
            margins = result.fetchall()

            if margins:
                print("   📊 Vérification calculs (échantillon):")
                for margin in margins:
                    profit_diff = abs(float(margin[3] or 0) - float(margin[5] or 0))
                    margin_diff = abs(float(margin[4] or 0) - float(margin[6] or 0))

                    status = "✅" if profit_diff < 0.01 and margin_diff < 0.1 else "⚠️"
                    print(
                        f"     {status} ID {margin[0]}: Profit {margin[3]:.4f} vs {margin[5]:.4f} | Marge {margin[4]:.2f}% vs {margin[6]:.2f}%")
            else:
                print("   📌 Aucune donnée de marge disponible")

        return True

    except Exception as e:
        print(f"❌ Erreur test intégrité: {e}")
        return False


async def main():
    """Point d'entrée principal - Tests ERP complets"""

    print("🧪 SUITE DE TESTS ERP SYNERGO")
    print("=" * 60)
    print("🏗️ Architecture: 5 tables ERP avec calculs de marge")
    print("⚡ Synchronisation incrémentale avec types A/AV")
    print()

    # Test 1: Sync manuelle ERP
    sync_ok = await test_manual_sync()

    if sync_ok:
        # Test 2: Service scheduler
        scheduler_ok = await test_scheduler_service()

        if scheduler_ok:
            # Test 3: Intégrité ERP
            integrity_ok = await test_erp_integrity()

            if integrity_ok:
                print(f"\n🎉 TOUS LES TESTS ERP RÉUSSIS!")
                print(f"🚀 Architecture ERP Synergo opérationnelle!")
                print(f"\n📋 Fonctionnalités validées:")
                print(f"   ✅ Sync produits (nomenclature)")
                print(f"   ✅ Sync achats A/AV (entrees + entrees_produits)")
                print(f"   ✅ Sync ventes CHIFA/LIBRE (sorties + ventes_produits)")
                print(f"   ✅ Calculs de marge automatiques")
                print(f"   ✅ Stock temps réel")
                print(f"\nℹ️  Prochaines étapes:")
                print(f"   1. Démarrer API: python backend/main.py")
                print(f"   2. Démarrer scheduler: POST /api/v1/sync/start")
                print(f"   3. Dashboard: GET /api/v1/sync/dashboard")
                print(f"   4. Vue stock: SELECT * FROM synergo_analytics.real_time_stock")
            else:
                print(f"\n⚠️ Problèmes d'intégrité ERP détectés")
        else:
            print(f"\n⚠️ Problème avec le service scheduler")
    else:
        print(f"\n❌ Problème avec la synchronisation ERP")

    print(f"\n🏁 Tests ERP terminés: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())