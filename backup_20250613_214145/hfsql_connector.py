# backend/app/utils/hfsql_connector.py - VERSION ULTRA-ROBUSTE
import win32com.client
import pythoncom
import time
import json
from typing import List, Dict, Any, Optional
from loguru import logger
from contextlib import contextmanager
from ..core.config import settings


class HFSQLConnector:
    """
    Connecteur HFSQL OLE DB ultra-robuste
    Gestion complète des erreurs et reconnexion automatique
    """

    def __init__(self):
        self.connection_oledb_hfsql: Optional[win32com.client.Dispatch] = None
        self.recordset: Optional[win32com.client.Dispatch] = None
        self.is_connected = False
        self.provider_oledb_hfsql = self._build_provider_string()
        self.connection_attempts = 0
        self.max_retries = 3

    def _build_provider_string(self) -> str:
        """Chaîne Provider OLE DB optimisée"""
        return (
            'Provider=PCSOFT.HFSQL;'
            f' Data Source={settings.HFSQL_SERVER}:{settings.HFSQL_PORT};'
            f' Initial Catalog={settings.HFSQL_DATABASE};'
            f' User ID={settings.HFSQL_USER};'
            ' Password=;'
            f' Extended Properties="Password=*:{settings.HFSQL_PASSWORD};"'
        )

    @contextmanager
    def com_context(self):
        """Context manager pour COM avec gestion d'erreurs"""
        com_initialized = False
        try:
            pythoncom.CoInitialize()
            com_initialized = True
            yield
        except Exception as e:
            logger.debug(f"COM context error: {e}")
        finally:
            if com_initialized:
                try:
                    pythoncom.CoUninitialize()
                except:
                    pass

    async def connect(self) -> bool:
        """Connexion robuste avec retry automatique"""
        self.connection_attempts += 1

        if self.connection_attempts > self.max_retries:
            logger.error(f"❌ Limite de tentatives atteinte ({self.max_retries})")
            return False

        retry_delay = min(2 ** (self.connection_attempts - 1), 10)  # Backoff exponentiel

        try:
            with self.com_context():
                logger.info(f"🔌 Tentative connexion HFSQL #{self.connection_attempts}")

                # Nettoyage préventif
                self._force_cleanup()

                # Créer nouvelle connexion avec timeouts
                self.connection_oledb_hfsql = win32com.client.Dispatch("ADODB.Connection")
                self.connection_oledb_hfsql.ConnectionTimeout = 30
                self.connection_oledb_hfsql.CommandTimeout = 120

                # Définir la chaîne et ouvrir
                self.connection_oledb_hfsql.ConnectionString = self.provider_oledb_hfsql
                self.connection_oledb_hfsql.Open()

                # Vérification état
                connection_state = self.connection_oledb_hfsql.State
                logger.debug(f"État connexion: {connection_state}")

                if connection_state == 1:  # adStateOpen
                    logger.info("✅ Connexion HFSQL établie avec succès")
                    self.is_connected = True
                    self.connection_attempts = 0  # Reset compteur
                    return True
                else:
                    logger.error(f"❌ État connexion incorrect: {connection_state}")
                    return False

        except Exception as e:
            logger.warning(f"⚠️ Échec tentative #{self.connection_attempts}: {e}")
            self._force_cleanup()

            # Retry automatique avec délai
            if self.connection_attempts < self.max_retries:
                logger.info(f"⏳ Retry dans {retry_delay}s...")
                time.sleep(retry_delay)
                return await self.connect()
            else:
                logger.error("❌ Toutes les tentatives de connexion ont échoué")
                return False

    def _force_cleanup(self):
        """Nettoyage forcé et agressif"""
        try:
            # Fermer recordset
            if self.recordset:
                try:
                    if hasattr(self.recordset, 'State') and self.recordset.State == 1:
                        self.recordset.Close()
                except:
                    pass
                finally:
                    self.recordset = None

            # Fermer connexion
            if self.connection_oledb_hfsql:
                try:
                    if hasattr(self.connection_oledb_hfsql, 'State') and self.connection_oledb_hfsql.State == 1:
                        self.connection_oledb_hfsql.Close()
                except:
                    pass
                finally:
                    self.connection_oledb_hfsql = None

        except Exception as e:
            logger.debug(f"Erreur nettoyage (ignorée): {e}")
        finally:
            self.is_connected = False

    async def execute_query(self, query: str, max_records: int = 10000) -> List[Dict[str, Any]]:
        """Exécution de requête ultra-robuste"""
        if not self.is_connected:
            if not await self.connect():
                raise Exception("Impossible de se connecter à HFSQL")

        query_recordset = None
        try:
            with self.com_context():
                logger.debug(f"🔍 Exécution: {query[:100]}...")

                # Créer recordset dédié pour cette requête
                query_recordset = win32com.client.Dispatch("ADODB.Recordset")
                query_recordset.CursorType = 1  # adOpenKeyset
                query_recordset.LockType = 1  # adLockReadOnly

                # Ouvrir avec timeout
                start_time = time.time()
                query_recordset.Open(query, self.connection_oledb_hfsql)

                results = []
                record_count = 0

                # Protection contre les requêtes infinies
                while not query_recordset.EOF and record_count < max_records:
                    # Timeout protection
                    if time.time() - start_time > 300:  # 5 minutes max
                        logger.warning("⚠️ Timeout requête (5min), arrêt forcé")
                        break

                    row = {}

                    # Lecture sécurisée des champs
                    try:
                        for i in range(query_recordset.Fields.Count):
                            field = query_recordset.Fields[i]
                            field_name = str(field.Name)
                            field_value = field.Value

                            # Nettoyage et conversion
                            row[field_name] = self._clean_field_value(field_value)

                    except Exception as field_error:
                        logger.warning(f"⚠️ Erreur lecture champ: {field_error}")
                        continue

                    if row:  # Seulement si on a des données
                        results.append(row)
                        record_count += 1

                    # Avancement sécurisé
                    try:
                        query_recordset.MoveNext()
                    except Exception as move_error:
                        logger.warning(f"⚠️ Erreur MoveNext: {move_error}")
                        break

                # Fermeture immédiate
                query_recordset.Close()

                execution_time = time.time() - start_time
                logger.debug(f"✅ {record_count} enregistrements en {execution_time:.2f}s")

                return results

        except Exception as e:
            logger.error(f"❌ Erreur exécution requête: {e}")

            # Nettoyage en cas d'erreur
            try:
                if query_recordset and query_recordset.State == 1:
                    query_recordset.Close()
            except:
                pass

            # Déconnecter si erreur de connexion
            if any(keyword in str(e).lower() for keyword in ['connection', 'provider', 'timeout']):
                logger.warning("🔌 Erreur de connexion détectée, reset nécessaire")
                self.is_connected = False

            raise

    def _clean_field_value(self, value: Any) -> Any:
        """Nettoyage des valeurs de champs HFSQL"""
        try:
            if value is None:
                return None

            # String: nettoyage et trim
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned if cleaned else None

            # Nombres: vérification validité
            if isinstance(value, (int, float)):
                if isinstance(value, float):
                    # Check NaN/Infinity
                    if value != value or abs(value) == float('inf'):
                        return None
                return value

            # Autres types: conversion string safe
            return str(value) if value is not None else None

        except Exception as e:
            logger.debug(f"Erreur nettoyage valeur {value}: {e}")
            return None

    async def test_connection_step_by_step(self) -> Dict[str, Any]:
        """Test de connexion détaillé avec diagnostic"""
        test_results = {
            "status": "testing",
            "steps": [],
            "final_status": "unknown",
            "performance": {}
        }

        start_time = time.time()

        try:
            # Étape 1: Test objets COM
            test_results["steps"].append({"step": "COM Objects", "status": "testing"})

            try:
                with self.com_context():
                    test_conn = win32com.client.Dispatch("ADODB.Connection")
                    test_rs = win32com.client.Dispatch("ADODB.Recordset")
                    test_results["steps"][-1]["status"] = "success"
                    test_results["steps"][-1]["message"] = "Objets COM disponibles"
            except Exception as e:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = str(e)
                test_results["final_status"] = "error"
                return test_results

            # Étape 2: Test connexion
            test_results["steps"].append({"step": "Connection", "status": "testing"})

            connection_start = time.time()
            if await self.connect():
                connection_time = time.time() - connection_start
                test_results["steps"][-1]["status"] = "success"
                test_results["steps"][-1]["message"] = f"Connexion établie en {connection_time:.2f}s"
                test_results["performance"]["connection_time"] = connection_time
            else:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = "Échec connexion"
                test_results["final_status"] = "error"
                return test_results

            # Étape 3: Test requête simple
            test_results["steps"].append({"step": "Simple Query", "status": "testing"})

            try:
                query_start = time.time()
                simple_result = await self.execute_query("SELECT COUNT(*) as total FROM sorties")
                query_time = time.time() - query_start

                if simple_result and len(simple_result) > 0:
                    total_count = simple_result[0]['total'] if simple_result[0].get('total') is not None else 0
                    test_results["steps"][-1]["status"] = "success"
                    test_results["steps"][-1]["message"] = f"Requête OK - {total_count} ventes en {query_time:.2f}s"
                    test_results["total_sales"] = total_count
                    test_results["performance"]["query_time"] = query_time
                else:
                    test_results["steps"][-1]["status"] = "warning"
                    test_results["steps"][-1]["message"] = "Requête OK mais aucun résultat"

            except Exception as e:
                test_results["steps"][-1]["status"] = "error"
                test_results["steps"][-1]["message"] = str(e)
                test_results["final_status"] = "error"
                return test_results

            # Étape 4: Test requête avec données
            test_results["steps"].append({"step": "Data Query", "status": "testing"})

            try:
                data_query = """
                    SELECT TOP 3 id, date, client, total_a_payer 
                    FROM sorties 
                    ORDER BY id DESC
                """
                data_result = await self.execute_query(data_query)

                test_results["steps"][-1]["status"] = "success"
                test_results["steps"][-1]["message"] = f"{len(data_result)} échantillons récupérés"
                test_results["sample_data"] = data_result[:2]  # Limiter pour la réponse

            except Exception as e:
                test_results["steps"][-1]["status"] = "warning"
                test_results["steps"][-1]["message"] = f"Erreur échantillon: {e}"

            # Résultat final
            test_results["final_status"] = "success"
            test_results["message"] = "Tous les tests essentiels réussis"
            test_results["performance"]["total_time"] = time.time() - start_time

        except Exception as e:
            test_results["final_status"] = "error"
            test_results["message"] = str(e)
            test_results["performance"]["total_time"] = time.time() - start_time

        finally:
            # Nettoyage
            self.close()

        return test_results

    def close(self):
        """Fermeture propre avec nettoyage complet"""
        try:
            logger.debug("🔌 Fermeture connexion HFSQL...")
            self._force_cleanup()
            logger.debug("✅ Connexion fermée proprement")
        except Exception as e:
            logger.warning(f"⚠️ Erreur fermeture: {e}")

    # Méthodes utilitaires pour sync
    async def get_max_id(self, table: str, id_field: str = 'id') -> int:
        """Récupère l'ID maximum d'une table"""
        try:
            query = f"SELECT MAX({id_field}) as max_id FROM {table}"
            result = await self.execute_query(query)

            if result and result[0].get('max_id') is not None:
                return int(result[0]['max_id'])
            return 0

        except Exception as e:
            logger.error(f"❌ Erreur récupération max ID: {e}")
            return 0

    async def get_records_since_id(self, table: str, last_id: int, limit: int = 1000, id_field: str = 'id') -> List[
        Dict]:
        """Récupère les enregistrements depuis un ID"""
        try:
            query = f"""
            SELECT TOP {limit} * FROM {table} 
            WHERE {id_field} > {last_id}
            ORDER BY {id_field} ASC
            """
            return await self.execute_query(query, max_records=limit)

        except Exception as e:
            logger.error(f"❌ Erreur récupération enregistrements: {e}")
            return []

    async def test_table_access(self, table: str) -> Dict[str, Any]:
        """Test d'accès à une table spécifique"""
        try:
            # Test comptage
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count_result = await self.execute_query(count_query)

            if not count_result:
                return {"status": "error", "message": f"Table {table} inaccessible"}

            record_count = count_result[0].get('count', 0)

            # Test échantillon si il y a des données
            sample_data = []
            if record_count > 0:
                sample_query = f"SELECT TOP 1 * FROM {table}"
                sample_result = await self.execute_query(sample_query)
                sample_data = sample_result

            return {
                "status": "success",
                "table": table,
                "record_count": record_count,
                "has_data": record_count > 0,
                "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                "accessible": True
            }

        except Exception as e:
            return {
                "status": "error",
                "table": table,
                "message": str(e),
                "accessible": False
            }


# Fonction utilitaire pour tests rapides
async def quick_hfsql_test():
    """Test rapide du connecteur"""
    connector = HFSQLConnector()

    try:
        print("🔍 Test connexion HFSQL...")
        result = await connector.test_connection_step_by_step()

        if result["final_status"] == "success":
            print(f"✅ Connexion OK")
            print(f"   📊 {result.get('total_sales', 0)} ventes")
            print(f"   ⏱️ Temps total: {result['performance'].get('total_time', 0):.2f}s")
            return True
        else:
            print(f"❌ Échec: {result.get('message', 'Erreur inconnue')}")
            return False

    except Exception as e:
        print(f"❌ Exception: {e}")
        return False
    finally:
        connector.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(quick_hfsql_test())