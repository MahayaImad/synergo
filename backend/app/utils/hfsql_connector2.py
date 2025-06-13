# backend/app/utils/hfsql_connector.py
import pyodbc
from typing import List, Dict, Any, Optional
from loguru import logger
from ..core.config import settings

class HFSQLConnector:
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.connection: Optional[pyodbc.Connection] = None

    def _build_connection_string(self) -> str:
        """Format HFSQL standard - Version 1"""
        return (
            f"DRIVER={{HFSQL}};"
            f"SERVER={settings.HFSQL_SERVER};"
            f"PORT={settings.HFSQL_PORT};"
            f"DATABASE={settings.HFSQL_DATABASE};"
            f"UID={settings.HFSQL_USER};"
            f"PWD=;"  # Serveur
            f"DATAPASSWORD={settings.HFSQL_PASSWORD}"
        )
    
    async def connect(self) -> bool:
        """Établir la connexion HFSQL"""
        try:
            # Adaptation de votre code template.php
            connection_string = self._build_connection_string()
            print(connection_string)
            self.connection = pyodbc.connect(connection_string,autocommit=True)
            logger.info("✅ Connexion HFSQL établie")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur connexion HFSQL: {e}")
            return False
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Exécuter une requête et retourner les résultats"""
        if not self.connection:
            await self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Récupérer les noms de colonnes
            columns = [desc[0] for desc in cursor.description]
            
            # Récupérer les données et les transformer en dictionnaires
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                results.append(row_dict)
            
            cursor.close()
            logger.debug(f"✅ Requête exécutée: {len(results)} résultats")
            return results
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution requête: {e}")
            raise
    
    async def get_last_id(self, table: str, id_field: str = 'id') -> int:
        """Récupérer le dernier ID d'une table"""
        query = f"SELECT MAX({id_field}) as max_id FROM {table}"
        result = await self.execute_query(query)
        return result[0]['max_id'] if result and result[0]['max_id'] else 0
    
    async def get_new_records(self, table: str, last_id: int, limit: int = 1000, id_field: str = 'id') -> List[Dict]:
        """Récupérer les nouveaux enregistrements depuis le dernier ID synchronisé"""
        query = f"""
        SELECT * FROM {table} 
        WHERE {id_field} > {last_id}
        ORDER BY {id_field} ASC
        LIMIT {limit}
        """
        return await self.execute_query(query)
    
    async def test_connection(self) -> Dict[str, Any]:
        """Tester la connexion et retourner des infos sur la base"""
        try:
            if not await self.connect():
                return {"status": "error", "message": "Impossible de se connecter"}
            
            # Test avec une requête simple (adapté de vos scripts)
            test_query = "SELECT COUNT(*) as total FROM sorties"
            result = await self.execute_query(test_query)
            
            return {
                "status": "success",
                "message": "Connexion OK",
                "total_sales": result[0]['total'] if result else 0,
                "database": settings.HFSQL_DATABASE
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def close(self):
        """Fermer la connexion"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("🔌 Connexion HFSQL fermée")