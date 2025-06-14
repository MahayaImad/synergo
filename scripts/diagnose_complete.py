# scripts/diagnose_complete.py
"""
Script de diagnostic complet après corrections
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent / "backend"))

async def test_all_systems():
    """Test de tous les systèmes"""
    print("🔍 DIAGNOSTIC POST-CORRECTIONS")
    print("=" * 40)
    print(f"📅 {datetime.now()}")
    print()

    # Test 1: Imports
    print("📦 Test des imports...")
    try:
        from app.utils.hfsql_connector import HFSQLConnector
        from app.sync.transformers.sales_transformer import SalesTransformer
        from app.core.database import test_async_connection
        print("✅ Imports: OK")
    except Exception as e:
        print(f"❌ Imports: {e}")
        return False

    # Test 2: HFSQL
    print("\n🔌 Test HFSQL...")
    try:
        connector = HFSQLConnector()
        test_result = await connector.test_connection_step_by_step()
        if test_result["final_status"] == "success":
            print("✅ HFSQL: OK")
            print(f"   📊 {test_result.get('total_sales', 0)} ventes")
        else:
            print("❌ HFSQL: Échec")
        connector.close()
    except Exception as e:
        print(f"❌ HFSQL: Exception {e}")

    # Test 3: Transformer
    print("\n🔄 Test Transformer...")
    try:
        transformer = SalesTransformer()
        test_data = [{'id': '123', 'date': '20241210', 'total_a_payer': '45,50'}]
        result = await transformer.transform_batch(test_data)
        if result:
            print("✅ Transformer: OK")
        else:
            print("❌ Transformer: Aucun résultat")
    except Exception as e:
        print(f"❌ Transformer: Exception {e}")

    # Test 4: PostgreSQL
    print("\n🐘 Test PostgreSQL...")
    try:
        if await test_async_connection():
            print("✅ PostgreSQL: OK")
        else:
            print("❌ PostgreSQL: Échec")
    except Exception as e:
        print(f"❌ PostgreSQL: Exception {e}")

    print("\n🏁 Diagnostic terminé")
    return True

if __name__ == "__main__":
    asyncio.run(test_all_systems())
