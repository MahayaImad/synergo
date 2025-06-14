# scripts/apply_fixes.py
"""
Script pour appliquer automatiquement les corrections Synergo
Version complète avec tous les fichiers séparés
"""

import shutil
import sys
import os
from pathlib import Path
from datetime import datetime


def backup_files():
    """Sauvegarde les fichiers actuels"""
    base_dir = Path(__file__).parent.parent
    backup_dir = base_dir / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(exist_ok=True)

    print(f"📁 Sauvegarde dans: {backup_dir}")

    files_to_backup = [
        "backend/app/sync/transformers/sales_transformer.py",
        "backend/app/utils/hfsql_connector.py",
        "backend/app/sync/sync_manager.py"
    ]

    for file_path in files_to_backup:
        source = base_dir / file_path
        if source.exists():
            dest = backup_dir / source.name
            shutil.copy2(source, dest)
            print(f"✅ {file_path} sauvegardé")

    return backup_dir


def ensure_directories():
    """S'assure que tous les dossiers nécessaires existent"""
    base_dir = Path(__file__).parent.parent

    directories = [
        "backend/app/sync/transformers",
        "backend/app/utils",
        "backend/app/sync",
        "scripts"
    ]

    for directory in directories:
        dir_path = base_dir / directory
        dir_path.mkdir(parents=True, exist_ok=True)


def apply_sales_transformer_fixes():
    """Applique les corrections au Sales Transformer"""
    print("\n🔄 Application des corrections Sales Transformer...")

    base_dir = Path(__file__).parent.parent
    transformer_file = base_dir / "backend/app/sync/transformers/sales_transformer.py"

    # Le contenu est maintenant dans l'artifact sales_transformer_fixed
    # Vous devez copier le contenu de cet artifact dans le fichier

    print("⚠️  ATTENTION: Copiez le contenu de l'artifact 'Sales Transformer Corrigé'")
    print(f"   dans le fichier: {transformer_file}")
    print("   (Le script ne peut pas accéder directement aux artifacts)")

    return transformer_file.exists()


def apply_hfsql_connector_fixes():
    """Applique les corrections au connecteur HFSQL"""
    print("\n🔌 Application des corrections HFSQL Connector...")

    base_dir = Path(__file__).parent.parent
    connector_file = base_dir / "backend/app/utils/hfsql_connector.py"

    print("⚠️  ATTENTION: Copiez le contenu de l'artifact 'HFSQL Connector Corrigé'")
    print(f"   dans le fichier: {connector_file}")

    return connector_file.exists()


def apply_sync_manager_fixes():
    """Applique les corrections au Sync Manager"""
    print("\n⚙️ Application des corrections Sync Manager...")

    base_dir = Path(__file__).parent.parent
    manager_file = base_dir / "backend/app/sync/sync_manager.py"

    print("⚠️  ATTENTION: Appliquez les corrections de l'artifact 'Sync Manager - Corrections Critiques'")
    print(f"   au fichier: {manager_file}")
    print("   (Remplacez les méthodes correspondantes)")

    return manager_file.exists()


def create_diagnostic_script():
    """Crée le script de diagnostic complet"""
    print("\n🔍 Création du script de diagnostic...")

    base_dir = Path(__file__).parent.parent
    diagnostic_file = base_dir / "scripts/diagnose_complete.py"

    diagnostic_content = '''# scripts/diagnose_complete.py
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
    print("\\n🔌 Test HFSQL...")
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
    print("\\n🔄 Test Transformer...")
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
    print("\\n🐘 Test PostgreSQL...")
    try:
        if await test_async_connection():
            print("✅ PostgreSQL: OK")
        else:
            print("❌ PostgreSQL: Échec")
    except Exception as e:
        print(f"❌ PostgreSQL: Exception {e}")

    print("\\n🏁 Diagnostic terminé")
    return True

if __name__ == "__main__":
    asyncio.run(test_all_systems())
'''

    with open(diagnostic_file, 'w', encoding='utf-8') as f:
        f.write(diagnostic_content)

    print("✅ Script de diagnostic créé")


def create_quick_test_script():
    """Crée le script de test rapide"""
    print("\n⚡ Création du script de test rapide...")

    base_dir = Path(__file__).parent.parent
    test_file = base_dir / "scripts/test_quick.py"

    # Référence au contenu de l'artifact test_quick
    print("📝 Script de test rapide disponible dans l'artifact 'Script de Test Rapide'")
    print(f"   Créez le fichier: {test_file}")

    return True


def create_manual_application_guide():
    """Crée un guide pour l'application manuelle des corrections"""
    base_dir = Path(__file__).parent.parent
    guide_file = base_dir / "GUIDE_CORRECTIONS.md"

    guide_content = '''# Guide d'Application des Corrections Synergo

## 🎯 Corrections à Appliquer

### 1. Sales Transformer (PRIORITÉ HAUTE)

**Fichier**: `backend/app/sync/transformers/sales_transformer.py`

**Action**: Remplacez TOUT le contenu du fichier par celui de l'artifact "Sales Transformer Corrigé - Version Ultra-Robuste"

**Corrections incluses**:
- ✅ Conversion d'ID robuste avec gestion de tous les types
- ✅ Gestion des dates HFSQL (YYYYMMDD) et formats mixtes  
- ✅ Conversion des montants avec virgules françaises
- ✅ Validation stricte avec correction automatique

### 2. HFSQL Connector (PRIORITÉ HAUTE)

**Fichier**: `backend/app/utils/hfsql_connector.py`

**Action**: Remplacez TOUT le contenu du fichier par celui de l'artifact "HFSQL Connector Corrigé - Version Ultra-Robuste"

**Corrections incluses**:
- ✅ Gestion COM avec context manager
- ✅ Reconnexion automatique avec backoff exponentiel
- ✅ Timeout et protection contre requêtes infinies
- ✅ Nettoyage forcé des connexions

### 3. Sync Manager (PRIORITÉ MOYENNE)

**Fichier**: `backend/app/sync/sync_manager.py`

**Action**: Appliquez les corrections de l'artifact "Sync Manager - Corrections Critiques"

**Méthodes à remplacer**:
- `sync_single_table()` → Version avec gestion d'erreurs robuste
- `_update_sync_state()` → `_update_sync_state_robust()`
- Ajouter: `_create_sync_state_if_missing()`
- Ajouter: `sync_all_active_tables()`

## 🚀 Ordre d'Application

1. **Sauvegarde** (OBLIGATOIRE)
   ```bash
   python scripts/apply_fixes.py
   ```

2. **Appliquer Sales Transformer**
   - Copier le contenu de l'artifact → `sales_transformer.py`

3. **Appliquer HFSQL Connector**
   - Copier le contenu de l'artifact → `hfsql_connector.py`

4. **Appliquer Sync Manager**
   - Remplacer les méthodes selon les instructions de l'artifact

5. **Test rapide**
   ```bash
   python scripts/test_quick.py
   ```

6. **Test complet**
   ```bash
   python scripts/diagnose_complete.py
   ```

## 🔧 Scripts de Test

### Test Rapide
```bash
python scripts/test_quick.py
```
- Valide les imports
- Test transformation de données  
- Test connexions de base

### Diagnostic Complet
```bash
python scripts/diagnose_complete.py
```
- Test approfondi de tous les composants
- Vérification de la configuration
- Test des requêtes HFSQL

### Test de Synchronisation
```bash
python scripts/test_manual_sync.py
```
- Test de synchronisation réelle
- Validation du pipeline complet

## 📊 Critères de Succès

Après application des corrections, vous devriez avoir :

- ✅ **Imports sans erreur** : Tous les modules se chargent
- ✅ **Connexion HFSQL stable** : >95% de réussite
- ✅ **Transformation 100%** : Aucune erreur de type  
- ✅ **Insertion PostgreSQL** : Transactions atomiques
- ✅ **Logs détaillés** : Erreurs tracées et recovery auto

## 🚨 En Cas de Problème

1. **Restaurer la sauvegarde** :
   ```bash
   cp backup_YYYYMMDD_HHMMSS/* backend/app/sync/transformers/
   ```

2. **Vérifier les logs** :
   ```bash
   tail -f logs/synergo.log
   ```

3. **Test minimal** :
   ```bash
   python scripts/test_quick.py
   ```

## 📞 Points de Contrôle

- [ ] Sauvegarde effectuée
- [ ] Sales Transformer remplacé
- [ ] HFSQL Connector remplacé  
- [ ] Sync Manager corrigé
- [ ] Test rapide réussi
- [ ] Diagnostic complet réussi
- [ ] Synchronisation testée

## 🎉 Finalisation

Une fois tous les tests réussis :

1. **Démarrer l'API** :
   ```bash
   python backend/main.py
   ```

2. **Vérifier le dashboard** :
   ```
   http://localhost:8000/api/v1/sync/dashboard
   ```

3. **Surveillance** :
   ```
   http://localhost:8000/api/v1/sync/health
   ```
'''

    with open(guide_file, 'w', encoding='utf-8') as f:
        f.write(guide_content)

    print(f"✅ Guide créé: {guide_file}")


def main():
    """Fonction principale de migration"""
    print("🚀 MIGRATION AUTOMATIQUE SYNERGO")
    print("=" * 50)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # 1. S'assurer que les dossiers existent
        ensure_directories()

        # 2. Sauvegarde
        backup_dir = backup_files()
        print(f"✅ Sauvegarde créée: {backup_dir.name}")

        # 3. Créer les scripts de diagnostic
        create_diagnostic_script()
        create_quick_test_script()

        # 4. Créer le guide d'application manuelle
        create_manual_application_guide()

        print("\n🎯 ÉTAPES SUIVANTES (MANUELLES):")
        print("=" * 35)
        print("1. Consultez le fichier GUIDE_CORRECTIONS.md")
        print("2. Copiez les artifacts dans les fichiers correspondants:")
        print("   • Sales Transformer → backend/app/sync/transformers/sales_transformer.py")
        print("   • HFSQL Connector → backend/app/utils/hfsql_connector.py")
        print("   • Sync Manager → Appliquez les corrections selon l'artifact")
        print("3. python scripts/test_quick.py")
        print("4. python scripts/diagnose_complete.py")

        print(f"\n💾 Sauvegarde disponible dans: {backup_dir}")
        print("   (Restauration possible en cas de problème)")

        print("\n📋 ARTIFACTS À UTILISER:")
        print("   • 'Sales Transformer Corrigé - Version Ultra-Robuste'")
        print("   • 'HFSQL Connector Corrigé - Version Ultra-Robuste'")
        print("   • 'Sync Manager - Corrections Critiques'")
        print("   • 'Script de Test Rapide Post-Corrections'")

        return True

    except Exception as e:
        print(f"\n❌ ERREUR MIGRATION: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Préparation terminée!")
        print("👉 Suivez maintenant les étapes du GUIDE_CORRECTIONS.md")
    else:
        print("\n❌ Préparation échouée!")
        sys.exit(1)