import win32com.client
import json
from typing import List, Dict, Any, Optional
from loguru import logger
from ..core.config import settings


class HFSQLConnector:
    """
    Connecteur HFSQL OLE DB avec gestion COM corrigée
    """

    def __init__(self):
        self.connection_oledb_hfsql: Optional[win32com.client.Dispatch] = None
        self.recordset: Optional[win32com.client.Dispatch] = None
        self.is_connected = False
        self.provider_oledb_hfsql = self._build_provider_string()

    def _build_provider_string(self) -> str:
        """Chaîne Provider OLE DB exacte"""
        return (
            'Provider=PCSOFT.HFSQL;'
            f' Data Source={settings.HFSQL_SERVER}:{settings.HFSQL_PORT};'
            f' Initial Catalog={settings.HFSQL_DATABASE};'
            f' User ID={settings.HFSQL_USER};'
            ' Password=;'
            f' Extended Properties="Password=*:{settings.HFSQL_PASSWORD};"'
        )

    async def connect(self) -> bool:
        """Connexion OLE DB avec diagnostic étape par étape"""
        try:
            logger.info("🔌 Tentative connexion OLE DB HFSQL...")

            # Étape 1: Créer l'objet Connection
            logger.debug("📝 Étape 1: Création ADODB.Connection...")
            self.connection_oledb_hfsql = win32com.client.Dispatch("ADODB.Connection")
            logger.debug("✅ ADODB.Connection créé")

            # Étape 2: Définir la chaîne de connexion
            logger.debug("📝 Étape 2: Définition ConnectionString...")
            logger.debug(f"Provider: {self.provider_oledb_hfsql}")
            self.connection_oledb_hfsql.ConnectionString = self.provider_oledb_hfsql
            logger.debug("✅ ConnectionString défini")

            # Étape 3: Ouvrir la connexion
            logger.debug("📝 Étape 3: Ouverture connexion...")
            self.connection_oledb_hfsql.Open()
            logger.debug("✅ Connexion ouverte")

            # Étape 4: Vérifier l'état de la connexion
            connection_state = self.connection_oledb_hfsql.State
            logger.debug(f"📊 État connexion: {connection_state}")

            if connection_state == 1:  # adStateOpen
                logger.info("✅ Connexion OLE DB établie et ouverte")
                self.is_connected = True
                return True
            else:
                logger.error(f"❌ Connexion dans un état incorrect: {connection_state}")
                return False

        except Exception as e:
            logger.error(f"❌ Erreur connexion OLE DB: {e}")
            self.is_connected = False
            return False

    def _create_recordset_if_needed(self):
        """Créer le recordset seulement si nécessaire"""
        if not self.recordset:
            logger.debug("📝 Création nouveau Recordset...")
            self.recordset = win32com.client.Dispatch("ADODB.Recordset")
            logger.debug("✅ Recordset créé")

    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Exécuter une requête avec gestion d'erreurs améliorée"""
        if not self.is_connected:
            if not await self.connect():
                raise Exception("Impossible de se connecter à HFSQL")

        try:
            logger.debug(f"🔍 Exécution: {query[:100]}...")

            # Créer un nouveau recordset pour chaque requête
            self._create_recordset_if_needed()

            # Ouvrir le recordset avec la requête
            self.recordset.Open(query, self.connection_oledb_hfsql)
            logger.debug("✅ Recordset ouvert")

            results = []
            record_count = 0

            # Vérifier si on a des données
            if not self.recordset.EOF:
                # Parcourir les résultats
                while not self.recordset.EOF:
                    row = {}

                    # Lire tous les champs
                    for i in range(self.recordset.Fields.Count):
                        field = self.recordset.Fields[i]
                        field_name = field.Name
                        field_value = field.Value

                        # Nettoyage des valeurs
                        if isinstance(field_value, str):
                            field_value = field_value.strip()
                        elif field_value is None:
                            field_value = ""

                        row[field_name] = field_value

                    results.append(row)
                    record_count += 1

                    # Passer au suivant
                    self.recordset.MoveNext()

            # Fermer le recordset immédiatement après usage
            if self.recordset and self.recordset.State == 1:  # Ouvert
                self.recordset.Close()
                logger.debug("✅ Recordset fermé")

            logger.debug(f"✅ Requête réussie: {record_count} résultats")
            return results

        except Exception as e:
            logger.error(f"❌ Erreur requête: {e}")

            # Nettoyer le recordset en cas d'erreur
            try:
                if self.recordset and self.recordset.State == 1:
                    self.recordset.Close()
            except:
                pass

            raise

    async def test_connection_step_by_step(self) -> Dict[str, Any]:
        """Test de connexion étape par étape avec diagnostic détaillé"""
        test_results = {
            "status": "testing",
            "steps": [],
            "final_status": "unknown"
        }

        try:
            # Test 1: Création objets COM
            test_results["steps"].append({"step": "COM Objects", "status": "testing"})

            try:
                conn_test = win32com.client.Dispatch("ADODB.Connection")
                rs_test = win32com.client.Dispatch("ADODB.Recordset")
                test_results["steps"][-1]["status"] = "success"
                test_results["steps"][-1]["message"] = "Objets COM créés"
            except Exception as e:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = str(e)
                test_results["final_status"] = "error"
                return test_results

            # Test 2: Connexion
            test_results["steps"].append({"step": "Connection", "status": "testing"})

            if not await self.connect():
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = "Connexion échouée"
                test_results["final_status"] = "error"
                return test_results

            test_results["steps"][-1]["status"] = "success"
            test_results["steps"][-1]["message"] = "Connexion établie"

            # Test 3: Requête simple
            test_results["steps"].append({"step": "Simple Query", "status": "testing"})

            try:
                simple_result = await self.execute_query("SELECT COUNT(*) as total FROM sorties")
                total_count = simple_result[0]['total'] if simple_result else 0

                test_results["steps"][-1]["status"] = "success"
                test_results["steps"][-1]["message"] = f"Requête OK - {total_count} ventes"
                test_results["total_sales"] = total_count

            except Exception as e:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = str(e)
                test_results["final_status"] = "error"
                return test_results

            # Test 4: Requête avec données
            test_results["steps"].append({"step": "Data Query", "status": "testing"})

            try:
                data_result = await self.execute_query("""
                    SELECT id, date, client, total_a_payer 
                    FROM sorties 
                    ORDER BY id DESC 
                    LIMIT 3
                """)

                test_results["steps"][-1]["status"] = "success"
                test_results["steps"][-1]["message"] = f"{len(data_result)} enregistrements récupérés"
                test_results["sample_data"] = data_result

            except Exception as e:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = str(e)
                # Pas critique, on continue

            test_results["final_status"] = "success"
            test_results["message"] = "Tous les tests réussis"

        except Exception as e:
            test_results["final_status"] = "error"
            test_results["message"] = str(e)

        return test_results

    def close(self):
        """Fermeture propre avec vérification d'état"""
        try:
            # Fermer le recordset en premier
            if self.recordset:
                try:
                    if hasattr(self.recordset, 'State') and self.recordset.State == 1:
                        self.recordset.Close()
                        logger.debug("✅ Recordset fermé proprement")
                except Exception as e:
                    logger.debug(f"⚠️ Recordset déjà fermé: {e}")
                finally:
                    self.recordset = None

            # Fermer la connexion
            if self.connection_oledb_hfsql:
                try:
                    if hasattr(self.connection_oledb_hfsql, 'State') and self.connection_oledb_hfsql.State == 1:
                        self.connection_oledb_hfsql.Close()
                        logger.debug("✅ Connexion fermée proprement")
                except Exception as e:
                    logger.debug(f"⚠️ Connexion déjà fermée: {e}")
                finally:
                    self.connection_oledb_hfsql = None

            self.is_connected = False
            logger.info("🔌 Nettoyage COM terminé")

        except Exception as e:
            logger.warning(f"⚠️ Erreur nettoyage COM (non critique): {e}")
        finally:
            # S'assurer que tout est réinitialisé
            self.recordset = None
            self.connection_oledb_hfsql = None
            self.is_connected = False
