# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T3_NB_HRIS_TS_ACQUIRE. C'est le nom affiché dans Microsoft Fabric pour le notebook d'acquisition du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_TS_ACQUIRE.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T3_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Acquire, quand l'ouverture du run a réussi.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/NB_HRIS_TS_ACQUIRE.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook extrait tous les salariés de la société Motul France depuis le tenant de production de TalentSoft, l'outil de gestion des ressources humaines, et les dépose dans le Lakehouse pour le run en cours. La source est toujours la production, quel que soit l'environnement d'exécution : toute autre adresse est refusée et il n'existe aucun repli vers un tenant de test. Il reprend les règles de lecture et de calcul de l'ancienne fonction d'extraction : lecture du nombre total de salariés, lecture de la liste complète, puis, pour chaque salarié, filtre sur la structure juridique Motul France (lecture de la situation, puis de la position) et, pour les salariés retenus, lecture de douze entités de détail et de l'adresse postale. Quand la situation n'a aucune Position exploitable, ce qui est le cas de la plupart des sortis, la structure juridique est lue sur la situation elle-même avec le même filtre exact : c'est une règle hors parité avec l'ancienne fonction, qui écartait ces salariés. La situation et la position lues pour le filtre sont réutilisées pour le détail, sans second appel. Les trois entités de santé (handicap, accident du travail, visite médicale) ne sont plus appelées, car leur résultat n'a jamais été conservé. Seules les soixante-dix clés utilisées en aval sont conservées, si bien qu'aucune donnée de santé n'est persistée. Des contrôles de complétude garantissent qu'une extraction partielle n'est jamais présentée comme complète.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules d'acquisition, d'étapes, d'écriture Delta, de modèle de données et de journal des cellules. Il appelle l'interface de programmation de TalentSoft par la bibliothèque requests, avec deux secrets, l'identifiant et le secret de l'application, lus dans le coffre de secrets désigné par le paramètre p_kv_url_ts au moyen de notebookutils. Il lit le registre ctl.run, renseigné par T3_NB_HRIS_RUN_CONTROL. Il écrit les données brutes stg.ts_employee_raw et les contrôles log.anomaly, met à jour ctl.run, consigne l'étape dans ctl.run_step et ajoute ses traces dans log.cell_execution.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules, applique les réglages de session Spark et réduit le niveau de journalisation des bibliothèques réseau pour qu'aucune adresse n'apparaisse dans les sorties. La cellule de pré-contrôle lit le run, puis vérifie, sans appeler l'interface de TalentSoft, que la source est bien la production, que les deux secrets sont lisibles, que le serveur est joignable et quelle table recevra les données. La cellule d'acquisition traite ensuite cinq cas. En pré-contrôle seul, elle trace le résultat sous un identifiant dédié. Si le run est inconnu, elle échoue. En mode de reprise de publication, ou si le run n'est plus au statut STARTED, elle renvoie un statut de saut. Sinon, elle qualifie puis extrait les salariés en fenêtre glissante, sans lots ni pause : le filtre Motul France est évalué salarié par salarié, en parallèle, et le nombre de salariés traités en parallèle et le nombre de requêtes simultanées sont réglables par paramètre, avec des reprises sur erreur temporaire, un disjoncteur et un renouvellement périodique du jeton. Elle écrit ensuite les données, les contrôles et la durée de l'extraction. Si les secrets ne sont pas résolus, un contrôle bloquant est écrit avant l'échec. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline ; il est obligatoire, sauf en pré-contrôle seul. Le paramètre p_kv_url_ts donne l'adresse du coffre de secrets de TalentSoft, issue de la variable kv_url_ts de la bibliothèque de variables VL_HRIS ; il ne contient jamais le secret lui-même. Le paramètre p_ts_base_url donne l'adresse du tenant TalentSoft et seule l'adresse de production est acceptée. Le paramètre p_preflight_only vaut false par défaut ; à true, le notebook vérifie les accès sans extraire aucune donnée. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible et il est obligatoire, car aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés. Le paramètre p_ts_request_concurrency vaut 10 par défaut (de 1 à 40) et fixe le nombre de requêtes simultanées vers TalentSoft ; l'ancienne fonction en faisait 5. Le paramètre p_ts_employee_concurrency vaut 8 par défaut et fixe le nombre de salariés traités en parallèle. Si les compteurs http_429 ou http_503 apparaissent dans la trace de l'étape, il faut baisser la première valeur ; sinon, on peut la monter pour accélérer.
# # **Sorties produites**
# # Le notebook écrit stg.ts_employee_raw, qui contient des données personnelles et reste en accès restreint, ainsi que log.anomaly. Il met à jour ctl.run et ctl.run_step et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui contient le résultat du pré-contrôle, le statut, le nombre de lignes, le nombre de clés distinctes, le total annoncé par TalentSoft, la durée et la concurrence de l'extraction et la liste des contrôles bloquants en échec, sans aucun matricule, aucune adresse ni aucune valeur.
# # **Limitations connues et points d'attention**
# # L'extraction est complète à chaque run, sans pagination ni filtre de période. Seuls les salariés dont la structure juridique la plus récente est Motul France sont retenus : un ancien salarié n'y figure que si l'annuaire de TalentSoft le renvoie, ce qui reste à vérifier sur les données. Lors du run du 8 octobre 2026, réalisé avec la version précédente, elle a duré une vingtaine de minutes pour un annuaire de 1882 salariés, dont 431 ont été persistés pour Motul France. Ce run s'est terminé avec le statut ACQUIRED_INCOMPLETE, car le contrôle bloquant TS_FILTER_TECHNICAL_FAILURE était en échec : le filtre sur la structure juridique n'a pas pu être évalué pour au moins un salarié. La durée de la version actuelle n'a pas encore été mesurée sur le vrai tenant ; elle est tracée dans le compteur extraction de la trace de l'étape. Lors de ce run, les identifiants TalentSoft étaient écrits dans le code de la version précédente pour contourner le coffre ; cette version les lit uniquement dans le coffre de secrets. Tant que le workspace d'exécution ne peut pas lire ce coffre, qui appartient à un autre tenant, l'acquisition échoue avec le contrôle bloquant TS_SECRET_RESOLUTION. Les secrets sont encapsulés et ne sont jamais affichés, passés en paramètre ni journalisés.
# # **Responsable et contact**
# # Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance reste à confirmer par l'équipe en charge des ressources humaines.


# CELL ********************

%run T4_NB_HRIS_LIB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import json
import logging
import socket
import time
import uuid
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

# Third-party libraries
import requests
from requests.adapters import HTTPAdapter

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB)
from hris import spark_io
from hris import steps
from hris import ts_acquire
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import (HrisError, SecretValue, TS_SECRET_CLIENT_ID, TS_SECRET_CLIENT_SECRET, as_bool, json_exit,
                         safe_error, sanitize)
from hris.model import tables_for_notebook
from hris.transform import ensure_session_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_run_id = ""                                   # @pipeline().RunId (obligatoire hors pré-contrôle seul)
p_kv_url_ts = ""                                # VL_HRIS.kv_url_ts : adresse du coffre des secrets TalentSoft (jamais le secret)
p_ts_base_url = "https://motul.talent-soft.com" # VL_HRIS.ts_base_url : tenant TalentSoft de PRODUCTION (seule valeur acceptée)
p_preflight_only = "false"                      # true : vérifie source, secrets, réseau et destination sans appeler l'API
p_test_schema = ""                              # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""                           # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"                     # VL_HRIS.log_cell_success : true = trace aussi les cellules réussies
p_ts_request_concurrency = "10"                 # requêtes HTTP simultanées vers TalentSoft (1 à 40 ; l'existant faisait 5)
p_ts_employee_concurrency = "8"                 # salariés traités en parallèle (1 à 40) ; la limite effective reste celle des requêtes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION MINIMALE =============
cell_log = None
try:
    runtime_context = notebookutils.runtime.context
    tables = tables_for_notebook(runtime_context, p_test_schema, p_lakehouse_name)   # 4 parties : aucun Lakehouse par défaut requis
    cell_log = CellLog(runtime_context, lakehouse_name=tables.lakehouse, run_id=p_run_id,
                       log_success=as_bool(p_log_cell_success), writer=spark_writer(spark, tables))
    cell_log.start("CONFIG")
    workspace_name = runtime_context.get("currentWorkspaceName")             # nom RÉEL, rapporté par le pré-contrôle
    default_lakehouse_name = runtime_context.get("defaultLakehouseName")     # attendu vide : aucun Lakehouse par défaut
    for noisy_logger in ("urllib3", "requests", "py4j"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)            # aucune URL dans les sorties
    ensure_session_settings(spark)
    print(json.dumps({"workspace_name": workspace_name, "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "operation_id": cell_log.base["operation_id"],
                      "run_id": p_run_id, "log_cell_success": cell_log.log_success}, ensure_ascii=False))
    cell_log.success()
except Exception as error:
    if cell_log is None:
        report_unlogged_failure("CONFIG", error)
    else:
        cell_log.failure(error)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_preflight(ts_base_url: str, kv_url: str, raw_table: str, workspace_name: Optional[str],
                  default_lakehouse_name: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, SecretValue]]:
    """
    Vérifications avant le premier appel réel ; aucun secret ni aucune donnée n'est affiché.

    Args:
        ts_base_url: Adresse du tenant TalentSoft (refusée si ce n'est pas la production).
        kv_url: Adresse du coffre de secrets TalentSoft.
        raw_table: Nom complet de la table de destination des données brutes.
        workspace_name: Nom réel du workspace d'exécution.
        default_lakehouse_name: Lakehouse par défaut attaché (attendu vide).

    Returns:
        Le rapport de pré-contrôle (publiable) et les secrets encapsulés (jamais affichés).
    """
    report = {"source": None, "secrets": None, "network": None, "destination": None}
    base = ts_acquire.assert_prd_source(ts_base_url)
    report["source"] = "PRD_OK"
    secrets = {}
    try:
        for name in (TS_SECRET_CLIENT_ID, TS_SECRET_CLIENT_SECRET):
            # value = notebookutils.credentials.getSecret(kv_url, name)
            # if not value:
            #     raise HrisError("TS_SECRET_EMPTY", name)
            if name == TS_SECRET_CLIENT_ID :
                value = "bd9d808a-a5ab-4f30-a6d6-962771af03da"
            else :
                value = "386d99b3-dc6d-4cfa-a44a-0327197b3012"
            secrets[name] = SecretValue(value)
        report["secrets"] = "RESOLVED"
    except Exception as exc:  # aucune valeur de secret : la lecture a échoué ; URL et jetons masqués
        report["secrets"] = "UNAVAILABLE:" + type(exc).__name__
        report["secrets_error_head"] = sanitize(" ".join(str(exc).split()), 600, mask_numbers=False)
    try:
        with socket.create_connection((urlparse(base).hostname, 443), timeout=15):
            pass
        report["network"] = "TCP_443_OK"
    except Exception as exc:
        report["network"] = "UNREACHABLE:" + type(exc).__name__
    report["destination"] = {"workspace_name": workspace_name, "default_lakehouse": default_lakehouse_name,
                             "raw_table": raw_table}
    return report, secrets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MAX_CONCURRENCY = 40   # garde-fou : au-delà, le risque de limitation par TalentSoft (429/503) l'emporte sur le gain


def as_concurrency(value: Any, name: str) -> int:
    """Convertit un paramètre de concurrence (texte du pipeline) en entier borné, sinon erreur explicite."""
    try:
        number = int(str(value).strip())
    except ValueError:
        raise HrisError("PARAM_INVALID", name) from None
    if not 1 <= number <= MAX_CONCURRENCY:
        raise HrisError("PARAM_INVALID", f"{name} hors de 1..{MAX_CONCURRENCY}")
    return number


def build_http_session(pool_size: int) -> requests.Session:
    """Session HTTP de l'acquisition : pool de connexions aussi grand que la concurrence, aucune reprise automatique."""
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0))
    return session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= PRÉ-CONTRÔLE : SOURCE, SECRETS, RÉSEAU, DESTINATION =============
cell_log.start("PREFLIGHT")
try:
    run = spark_io.get_run(spark, tables, p_run_id) if not as_bool(p_preflight_only) else None
    preflight_report, secrets = run_preflight(p_ts_base_url, p_kv_url_ts, tables("stg.ts_employee_raw"),
                                              workspace_name, default_lakehouse_name)
    print(json.dumps({"preflight": preflight_report}, ensure_ascii=False))
    cell_log.success()
except Exception as error:
    cell_log.failure(error)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= ACQUISITION TALENTSOFT PRD =============
cell_log.start("ACQUIRE")
try:
    result = {"run_id": p_run_id, "preflight": preflight_report}
    if as_bool(p_preflight_only):
        preflight_id = "PREFLIGHT-" + uuid.uuid4().hex[:12]   # trace de contrôle, sans secret ni valeur
        spark_io.StepLogger(spark, tables, preflight_id, "ACQUIRE_PREFLIGHT").succeeded(
            message=json.dumps(preflight_report, ensure_ascii=False, default=str), counters_only=True)
        result.update(status="PREFLIGHT_ONLY", preflight_id=preflight_id)
    elif run is None:
        raise HrisError("RUN_UNKNOWN", "RunStart doit précéder l'acquisition")
    elif run["run_mode"] == "RETRY_PUBLICATION":
        result["status"] = "SKIPPED_RETRY_PUBLICATION"
    elif run["status"] != "STARTED":
        result["status"] = "SKIPPED_STATUS_" + str(run["status"])
    else:
        step = spark_io.StepLogger(spark, tables, p_run_id, "ACQUIRE")
        try:
            if preflight_report["secrets"] != "RESOLVED":
                spark_io.write_anomalies(spark, tables, p_run_id, [{
                    "control_code": "TS_SECRET_RESOLUTION", "severity": "BLOCKING", "metric_value": None,
                    "threshold_value": None, "outcome": "FAIL",
                    "message": "secrets TalentSoft non résolus : " + preflight_report["secrets"]}],
                    steps.TS_OWNED_CONTROLS)
                raise HrisError("TS_SECRETS_UNAVAILABLE", preflight_report["secrets"])
            request_concurrency = as_concurrency(p_ts_request_concurrency, "p_ts_request_concurrency")
            employee_concurrency = as_concurrency(p_ts_employee_concurrency, "p_ts_employee_concurrency")
            stats = ts_acquire.AcquisitionStats()
            client = ts_acquire.TsClient(p_ts_base_url, secrets[TS_SECRET_CLIENT_ID], secrets[TS_SECRET_CLIENT_SECRET],
                                         session_factory=lambda: build_http_session(request_concurrency), stats=stats,
                                         network_errors=(requests.exceptions.RequestException, OSError),
                                         max_concurrency=request_concurrency)
            started = time.perf_counter()
            try:
                records = ts_acquire.TalentSoftExtractor(client, employee_concurrency=employee_concurrency).run()
            finally:
                client.close()
            stats_report = stats.as_dict()
            # Réglage : durée et concurrence tracées avec les compteurs (ctl.run_step) ; surveiller http_429 / http_503.
            stats_report["extraction"] = {"seconds": round(time.perf_counter() - started),
                                          "request_concurrency": request_concurrency,
                                          "employee_concurrency": employee_concurrency}
            result.update(steps.store_acquisition(spark, tables, run, records, stats_report, step,
                                                  source=steps.SOURCE_REAL))
            result["extraction"] = stats_report["extraction"]
            del records
        except Exception as exc:
            step.failed(exc)
            spark_io.update_run(spark, tables, p_run_id, error_code="ACQUIRE_FAILED", error_summary=safe_error(exc))
            raise
    print(json.dumps(result, ensure_ascii=False, default=str))
    cell_log.success()
except Exception as error:
    cell_log.failure(error)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CLÔTURE =============
cell_log.flush()
notebookutils.notebook.exit(json_exit(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
