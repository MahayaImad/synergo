# scripts/setup_structure.py
"""
Script pour créer toute la structure Synergo rapidement
"""

import os
from pathlib import Path


def create_structure():
    """Crée toute la structure des dossiers et fichiers __init__.py"""

    base_dir = Path(__file__).parent.parent
    backend_dir = base_dir / "backend"

    print("🏗️ Création de la structure Synergo...")

    # Dossiers à créer
    directories = [
        "backend/app/models",
        "backend/app/sync/strategies",
        "backend/app/sync/transformers",
        "backend/app/sync/connectors",
        "backend/app/api/v1",
        "backend/app/core",
        "backend/app/utils",
        "backend/app/services",
        "scripts",
        "tests",
        "logs"
    ]

    # Créer les dossiers
    for directory in directories:
        dir_path = base_dir / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 {directory}")

        # Créer __init__.py dans les modules Python
        if directory.startswith("backend/app"):
            init_file = dir_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text("# Module Synergo\n")
                print(f"   ✅ __init__.py créé")

    # Fichiers __init__.py spéciaux
    init_contents = {
        "backend/app/__init__.py": """# Application Synergo
__version__ = "1.0.0"
""",

        "backend/app/models/__init__.py": """# Modèles Synergo
from .sync_models import SyncTable, SyncState, SyncLog
from .pharma_models import SalesSummary, ProductsCatalog

__all__ = [
    'SyncTable', 'SyncState', 'SyncLog',
    'SalesSummary', 'ProductsCatalog'
]
""",

        "backend/app/sync/__init__.py": """# Module de synchronisation
from .sync_manager import SynergoSyncManager, SyncResult
from .scheduler import SynergoSyncScheduler, SchedulerService

__all__ = [
    'SynergoSyncManager', 'SyncResult',
    'SynergoSyncScheduler', 'SchedulerService'
]
""",

        "backend/app/sync/strategies/__init__.py": """# Stratégies de sync
from .id_based_sync import IdBasedSyncStrategy

__all__ = ['IdBasedSyncStrategy']
""",

        "backend/app/sync/transformers/__init__.py": """# Transformateurs
from .sales_transformer import SalesTransformer

__all__ = ['SalesTransformer']
""",

        "backend/app/api/v1/__init__.py": """# API v1
""",

        "backend/app/core/__init__.py": """# Configuration et utilitaires core
""",

        "backend/app/utils/__init__.py": """# Utilitaires
""",

        "backend/app/services/__init__.py": """# Services métier
"""
    }

    # Écrire les fichiers __init__.py
    for file_path, content in init_contents.items():
        full_path = base_dir / file_path
        full_path.write_text(content)
        print(f"📝 {file_path}")

    print("\n✅ Structure créée avec succès!")
    print("\n📋 Prochaines étapes:")
    print("1. Copier les fichiers de code dans les dossiers appropriés")
    print("2. Installer les dépendances: pip install -r requirements.txt")
    print("3. Configurer la base de données")
    print("4. Tester: python scripts/quick_start_synergo.py --test-only")


if __name__ == "__main__":
    create_structure()