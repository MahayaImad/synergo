"""
Test minimal HFSQL - Just une connexion et une requête
"""

import asyncio
import sys
import os
import win32com.client

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

from app.core.config import settings


async def test_minimal():
    print("🔍 Test Minimal HFSQL OLE DB")
    print("=" * 50)

    # Paramètres
    provider = (
        'Provider=PCSOFT.HFSQL;'
        f' Data Source={settings.HFSQL_SERVER}:{settings.HFSQL_PORT};'
        f' Initial Catalog={settings.HFSQL_DATABASE};'
        f' User ID={settings.HFSQL_USER};'
        ' Password=;'
        f' Extended Properties="Password=*:{settings.HFSQL_PASSWORD};"'
    )

    print(f"Provider: {provider}")
    print()

    connection = None
    recordset = None

    try:
        # Étape 1: Créer la connexion
        print("📝 Création ADODB.Connection...")
        connection = win32com.client.Dispatch("ADODB.Connection")
        connection.ConnectionString = provider
        print("✅ Objet créé")

        # Étape 2: Ouvrir
        print("📝 Ouverture connexion...")
        connection.Open()
        print(f"✅ Connexion ouverte (État: {connection.State})")

        # Étape 3: Requête simple
        print("📝 Test requête...")
        recordset = win32com.client.Dispatch("ADODB.Recordset")
        recordset.Open("SELECT COUNT(*) as total FROM sorties", connection)

        # Lire le résultat
        if not recordset.EOF:
            total = recordset.Fields[0].Value
            print(f"✅ Requête OK - Total ventes: {total}")

        recordset.Close()
        connection.Close()

        print(f"\n🎉 TEST MINIMAL RÉUSSI!")
        print(f"✅ OLE DB fonctionne parfaitement")

        return True

    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

    finally:
        # Nettoyage
        try:
            if recordset and recordset.State == 1:
                recordset.Close()
        except:
            pass

        try:
            if connection and connection.State == 1:
                connection.Close()
        except:
            pass


if __name__ == "__main__":
    success = asyncio.run(test_minimal())

    if success:
        print(f"\n🚀 PRÊT POUR SYNERGO!")
        print(f"   Le connecteur OLE DB est opérationnel")
        print(f"   Prochaine étape: Setup PostgreSQL")
    else:
        print(f"\n🔧 À corriger avant de continuer")
        print(f"   Vérifier serveur HFSQL et paramètres")