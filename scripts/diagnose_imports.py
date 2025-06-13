# scripts/diagnose_imports.py
"""
Script pour diagnostiquer les problèmes d'imports
"""

import sys
import os
from pathlib import Path


def diagnose_structure():
    """Diagnostique la structure des fichiers"""

    print("🔍 DIAGNOSTIC DE LA STRUCTURE SYNERGO")
    print("=" * 50)

    # Déterminer le répertoire de base
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent
    backend_dir = base_dir / "backend"

    print(f"📂 Répertoire de base: {base_dir}")
    print(f"📂 Backend: {backend_dir}")
    print()

    # Vérifier que backend existe
    if not backend_dir.exists():
        print("❌ Le dossier 'backend' n'existe pas!")
        print("   Créez-le avec: mkdir backend")
        return False

    # Vérifier la structure attendue
    expected_structure = {
        "backend/app/__init__.py": "Module app principal",
        "backend/app/core/__init__.py": "Module core",
        "backend/app/core/config.py": "Configuration",
        "backend/app/core/database.py": "Base de données",
        "backend/app/models/__init__.py": "Module models",
        "backend/app/models/sync_models.py": "Modèles de sync",
        "backend/app/models/pharma_models.py": "Modèles pharmacie",
        "backend/app/sync/__init__.py": "Module sync",
        "backend/app/sync/sync_manager.py": "Gestionnaire sync",
        "backend/app/sync/scheduler.py": "Planificateur",
        "backend/app/sync/strategies/__init__.py": "Module stratégies",
        "backend/app/sync/strategies/id_based_sync.py": "Stratégie ID-based",
        "backend/app/sync/transformers/__init__.py": "Module transformers",
        "backend/app/sync/transformers/sales_transformer.py": "Transformer ventes",
        "backend/app/utils/__init__.py": "Module utils",
        "backend/app/utils/hfsql_connector.py": "Connecteur HFSQL",
        "backend/app/api/__init__.py": "Module API",
        "backend/app/api/v1/__init__.py": "API v1",
        "backend/app/api/v1/sync_status.py": "API sync"
    }

    missing_files = []
    existing_files = []

    for file_path, description in expected_structure.items():
        full_path = base_dir / file_path
        if full_path.exists():
            existing_files.append((file_path, description))
            print(f"✅ {file_path}")
        else:
            missing_files.append((file_path, description))
            print(f"❌ {file_path} - {description}")

    print(f"\n📊 Résumé:")
    print(f"   ✅ {len(existing_files)} fichiers présents")
    print(f"   ❌ {len(missing_files)} fichiers manquants")

    if missing_files:
        print(f"\n🔧 Fichiers à créer:")
        for file_path, description in missing_files[:5]:  # Top 5
            print(f"   - {file_path}")

        if len(missing_files) > 5:
            print(f"   ... et {len(missing_files) - 5} autres")

    # Test d'import
    print(f"\n🐍 Test d'imports Python:")

    # Ajouter backend au path temporairement
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    # Tests d'imports critiques
    import_tests = [
        ("app.core.config", "Configuration"),
        ("app.core.database", "Base de données"),
        ("app.models.sync_models", "Modèles sync"),
        ("app.utils.hfsql_connector", "Connecteur HFSQL"),
        ("app.sync.sync_manager", "Manager sync")
    ]

    for module_name, description in import_tests:
        try:
            __import__(module_name)
            print(f"✅ {module_name}")
        except ImportError as e:
            print(f"❌ {module_name} - {e}")
        except Exception as e:
            print(f"⚠️ {module_name} - Erreur: {e}")

    print(f"\n🎯 Recommandations:")

    if missing_files:
        print("1. Exécuter: python scripts/setup_structure.py")
        print("2. Créer les fichiers manquants")
        print("3. Copier le code des artifacts dans les fichiers")
    else:
        print("1. Structure OK - Tester les imports")
        print("2. Vérifier la configuration database")
        print("3. Tester les connexions")

    return len(missing_files) == 0


def create_minimal_files():
    """Crée les fichiers minimaux pour faire fonctionner les imports"""

    base_dir = Path(__file__).parent.parent

    print("🔧 Création des fichiers minimaux...")

    # Fichiers critiques à créer
    critical_files = {
        "backend/app/__init__.py": "",
        "backend/app/core/__init__.py": "",
        "backend/app/models/__init__.py": "",
        "backend/app/sync/__init__.py": "",
        "backend/app/sync/strategies/__init__.py": "",
        "backend/app/sync/transformers/__init__.py": "",
        "backend/app/utils/__init__.py": "",
        "backend/app/api/__init__.py": "",
        "backend/app/api/v1/__init__.py": ""
    }

    for file_path, content in critical_files.items():
        full_path = base_dir / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if not full_path.exists():
            full_path.write_text(content)
            print(f"✅ Créé: {file_path}")
        else:
            print(f"📄 Existe: {file_path}")

    print("✅ Fichiers minimaux créés")


if __name__ == "__main__":
    print("Diagnostic en cours...\n")

    success = diagnose_structure()

    if not success:
        print("\n🔧 Voulez-vous créer les fichiers minimaux? (o/n): ", end="")
        response = input().lower()

        if response in ['o', 'oui', 'y', 'yes']:
            create_minimal_files()
            print("\n🔄 Relancer le diagnostic...")
            diagnose_structure()

    print(f"\n🏁 Diagnostic terminé")