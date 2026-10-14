# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# 
# T5_NB_HRIS_TS_ACQUIRE
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_TS_ACQUIRE.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook extrait tous les salariés de la société Motul France depuis le tenant de production de TalentSoft, l'outil de gestion des ressources humaines, et les dépose dans le Lakehouse pour le run en cours. Il reprend les règles de l'ancienne fonction Azure d'extraction : même population, même construction des soixante-neuf champs, mêmes valeurs par défaut. Contrairement à l'existant, il vérifie que l'extraction est complète et bloque le run si elle ne l'est pas, plutôt que de transmettre une liste partielle. Il met aussi à jour la partie TalentSoft des tables de correspondance à partir des référentiels de TalentSoft.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'acquisition, d'étapes, d'écriture dans les tables, de correspondances et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du tenant TalentSoft, l'adresse du coffre de secrets et les niveaux de parallélisme. Il lit les secrets client-id-ts et client-secret-ts dans le coffre avec l'identité d'exécution du notebook. Il appelle l'interface de programmation de TalentSoft en HTTPS. Il lit ctl.run, écrit stg.ts_employee_raw, log.anomaly, ctl.run_step, la partie TalentSoft des tables cfg.lu_hr_ et log.notebook_log. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Acquire.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le pré-contrôle vérifie que l'adresse est bien celle du tenant de production, lit les deux secrets dans le coffre et teste la connexion réseau, sans rien afficher de secret. L'acquisition lit ensuite le run ; si le run n'est pas à l'état ouvert ou s'il s'agit d'une republication, elle ne fait rien. Sinon, elle extrait les salariés avec un parallélisme borné, contrôle la complétude par rapport au nombre total annoncé par TalentSoft, écrit les données brutes et les contrôles, puis marque le run comme acquis. Enfin, la synchronisation du catalogue met à jour les codes et libellés TalentSoft des tables de correspondance ; elle n'écrit jamais de code ADP et son échec ne bloque pas le run.
# 
# **Paramètres**
# 
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire, sauf en pré-contrôle seul. Le paramètre p_preflight_only vaut false par défaut ; à true, le notebook vérifie seulement la source, les secrets, le réseau et la destination, sans appeler TalentSoft. Le paramètre p_sync_reference_catalogue vaut true par défaut et active la mise à jour de la partie TalentSoft des correspondances. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit les salariés bruts du run dans stg.ts_employee_raw, qui contient des données personnelles et reste en accès restreint, les contrôles de complétude dans log.anomaly, la trace de l'étape dans ctl.run_step, les codes TalentSoft dans les tables cfg.lu_hr_ et son journal dans log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.
# 
# **Limitations connues et points d'attention**
# 
# Le niveau de parallélisme est de dix requêtes et dix salariés simultanés, comme l'existant ; l'augmenter suppose l'accord de TalentSoft, faute de quoi des limitations de débit peuvent apparaître. Le coffre de secrets est dans le tenant Motul : il n'est lisible que depuis un workspace de ce tenant, avec une identité autorisée. Les messages d'erreur et les compteurs ne contiennent aucune valeur de salarié.
# 
# **Responsable et contact**
# 
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

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

# Azure libraries
# (aucune)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run T5_NB_HRIS_LIB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= LOGGER =============
logger = setup_fabric_logger()
logger.info("✓ Imports réussis")
logger.info("✓ Logger initialisé")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION ENVIRONNEMENT =============
try:
    item_type = "Notebook"
    context_values = initialize_notebook_context(
        item_type=item_type,
        logger_instance=logger,
        required_keys=("ts_base_url", "key_vault_url", "ts_request_concurrency", "ts_employee_concurrency"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    ts_base_url = context_values["ts_base_url"]
    key_vault_url = context_values["key_vault_url"]
    ts_request_concurrency = context_values["ts_request_concurrency"]
    ts_employee_concurrency = context_values["ts_employee_concurrency"]
except Exception as e:
    logger.error(f"❌ Erreur lors de la configuration de l'environnement: {e}", exc_info=True)
    flush_logger(logger)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_run_id = ""                         # @pipeline().RunId (obligatoire hors pré-contrôle seul)
p_preflight_only = "false"            # true : vérifie source, secrets, réseau et destination sans appeler l'API
p_sync_reference_catalogue = "true"   # true : met à jour la partie TalentSoft des tables cfg.lu_hr_* (aucun fichier)
p_test_schema = ""                    # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""                        # eventpath Fabric (ex. /p_run_id=...)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= EVENTPATH =============
try:
    if eventpath != "":
        parsed_path = parse_eventpath(eventpath, logger)
        p_run_id = parsed_path.get("p_run_id", p_run_id)
        p_preflight_only = parsed_path.get("p_preflight_only", p_preflight_only)
        p_sync_reference_catalogue = parsed_path.get("p_sync_reference_catalogue", p_sync_reference_catalogue)
        p_test_schema = parsed_path.get("p_test_schema", p_test_schema)
    logger.info("✓ Paramètres du notebook prêts")
except Exception as e:
    error_message = f"Erreur lors de la lecture de l'eventpath: {e}"
    logger.error(f"❌ {error_message}", exc_info=True)
    flush_logger(
        logger,
        workspace_name=workspace_name,
        item_name=item_name,
        item_type=item_type,
        error_message=error_message,
        notebook_id=notebook_id,
        operation_id=operation_id,
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= LAKEHOUSE : SCHÉMAS ET JOURNAL (R14) =============
try:
    tables = tables_for_run(p_test_schema, lakehouse_name)
    prepared_schemas = create_lakehouse_schemas(tables)
    attach_logger_to_lakehouse(logger, tables, context_values, run_id=p_run_id)
    hris.transform.ensure_session_settings(spark)
    logger.info(f"✓ Schémas prêts dans le Lakehouse {lakehouse_name} : {len(prepared_schemas)}")
except Exception as e:
    error_message = f"Erreur lors de la préparation des schémas du Lakehouse: {e}"
    logger.error(f"❌ {error_message}", exc_info=True)
    flush_logger(
        logger,
        workspace_name=workspace_name,
        item_name=item_name,
        item_type=item_type,
        error_message=error_message,
        notebook_id=notebook_id,
        operation_id=operation_id,
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_ts_secrets(vault_url: str) -> Dict[str, Any]:
    """Identifiant et secret de l'application TalentSoft, lus dans le coffre et encapsulés ; jamais affichés."""
    secrets = {}
    for secret_name in (hris.common.TS_SECRET_CLIENT_ID, hris.common.TS_SECRET_CLIENT_SECRET):
        secrets[secret_name] = hris.common.SecretValue(get_parameter(secret_name, vault_url))
    return secrets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_preflight(base_url: str, vault_url: str, raw_table: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Vérifications avant le premier appel réel ; aucun secret ni aucune donnée n'est affiché.

    Args:
        base_url: Adresse du tenant TalentSoft (refusée si ce n'est pas la production).
        vault_url: Adresse du coffre de secrets TalentSoft.
        raw_table: Nom complet de la table de destination des données brutes.

    Returns:
        Le rapport de pré-contrôle (publiable) et les secrets encapsulés (jamais affichés).
    """
    report = {"source": None, "secrets": None, "network": None, "destination": raw_table}
    checked_url = hris.ts_acquire.assert_prd_source(base_url)
    report["source"] = "PRD_OK"
    secrets = {}
    try:
        secrets = read_ts_secrets(vault_url)
        report["secrets"] = "RESOLVED"
    except Exception as e:
        report["secrets"] = "UNAVAILABLE:" + type(e).__name__
        report["secrets_error_head"] = hris.common.sanitize(" ".join(str(e).split()), 600, mask_numbers=False)
    try:
        with socket.create_connection((urlparse(checked_url).hostname, 443), timeout=15):
            pass
        report["network"] = "TCP_443_OK"
    except Exception as e:
        report["network"] = "UNREACHABLE:" + type(e).__name__
    return report, secrets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def as_concurrency(value: Any, name: str, upper_bound: int = 40) -> int:
    """Convertit un niveau de parallélisme (texte de la variable library) en entier borné, sinon erreur explicite."""
    try:
        number = int(str(value).strip())
    except ValueError:
        raise hris.common.HrisError("PARAM_INVALID", name) from None
    if not 1 <= number <= upper_bound:
        raise hris.common.HrisError("PARAM_INVALID", f"{name} hors de 1..{upper_bound}")
    return number

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
try:
    for noisy_logger in ("urllib3", "requests", "py4j"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)            # aucune URL dans les sorties
    preflight_only = hris.common.as_bool(p_preflight_only)
    run = None if preflight_only else hris.spark_io.get_run(spark, tables, p_run_id)
    preflight_report, secrets = run_preflight(ts_base_url, key_vault_url, tables("stg.ts_employee_raw"))
    logger.info(f"✓ Pré-contrôle : {json.dumps(preflight_report, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de le pré-contrôle de l'acquisition: {e}"
    logger.error(f"❌ {error_message}", exc_info=True)
    flush_logger(
        logger,
        workspace_name=workspace_name,
        item_name=item_name,
        item_type=item_type,
        error_message=error_message,
        notebook_id=notebook_id,
        operation_id=operation_id,
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= ACQUISITION TALENTSOFT PRD =============
try:
    result = {"run_id": p_run_id, "preflight": preflight_report}
    if preflight_only:
        preflight_id = "PREFLIGHT-" + uuid.uuid4().hex[:12]
        hris.spark_io.StepLogger(spark, tables, preflight_id, "ACQUIRE_PREFLIGHT").succeeded(
            message=json.dumps(preflight_report, ensure_ascii=False, default=str), counters_only=True)
        result.update(status="PREFLIGHT_ONLY", preflight_id=preflight_id)
    elif run is None:
        raise hris.common.HrisError("RUN_UNKNOWN", "RunStart doit précéder l'acquisition")
    elif run["run_mode"] == "RETRY_PUBLICATION":
        result["status"] = "SKIPPED_RETRY_PUBLICATION"
    elif run["status"] != "STARTED":
        result["status"] = "SKIPPED_STATUS_" + str(run["status"])
    else:
        step = hris.spark_io.StepLogger(spark, tables, p_run_id, "ACQUIRE")
        try:
            if preflight_report["secrets"] != "RESOLVED":
                hris.spark_io.write_anomalies(spark, tables, p_run_id, [{
                    "control_code": "TS_SECRET_RESOLUTION", "severity": "BLOCKING", "metric_value": None,
                    "threshold_value": None, "outcome": "FAIL",
                    "message": "secrets TalentSoft non résolus : " + preflight_report["secrets"]}],
                    hris.steps.TS_OWNED_CONTROLS)
                raise hris.common.HrisError("TS_SECRETS_UNAVAILABLE", preflight_report["secrets"])
            request_concurrency = as_concurrency(ts_request_concurrency, "ts_request_concurrency")
            employee_concurrency = as_concurrency(ts_employee_concurrency, "ts_employee_concurrency")
            stats = hris.ts_acquire.AcquisitionStats()
            client = hris.ts_acquire.TsClient(
                ts_base_url, secrets[hris.common.TS_SECRET_CLIENT_ID], secrets[hris.common.TS_SECRET_CLIENT_SECRET],
                session_factory=lambda: build_http_session(request_concurrency), stats=stats,
                network_errors=(requests.exceptions.RequestException, OSError), max_concurrency=request_concurrency)
            started = time.perf_counter()
            try:
                records = hris.ts_acquire.TalentSoftExtractor(client, employee_concurrency=employee_concurrency).run()
            finally:
                client.close()
            stats_report = stats.as_dict()
            stats_report["extraction"] = {"seconds": round(time.perf_counter() - started),
                                          "request_concurrency": request_concurrency,
                                          "employee_concurrency": employee_concurrency}
            result.update(hris.steps.store_acquisition(spark, tables, run, records, stats_report, step,
                                                       source=hris.steps.SOURCE_REAL))
            result["extraction"] = stats_report["extraction"]
            del records
        except Exception as acquire_error:
            step.failed(acquire_error)
            hris.spark_io.update_run(spark, tables, p_run_id, error_code="ACQUIRE_FAILED",
                                     error_summary=hris.common.safe_error(acquire_error))
            raise
    logger.info(f"✓ Acquisition : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de l'acquisition TalentSoft: {e}"
    logger.error(f"❌ {error_message}", exc_info=True)
    flush_logger(
        logger,
        workspace_name=workspace_name,
        item_name=item_name,
        item_type=item_type,
        error_message=error_message,
        notebook_id=notebook_id,
        operation_id=operation_id,
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= RÉFÉRENTIELS TALENTSOFT VERS LA PARTIE TALENTSOFT DES TABLES DE CORRESPONDANCE =============
try:
    catalogue_result = {"status": "SKIPPED"}
    if hris.common.as_bool(p_sync_reference_catalogue) and str(result.get("status", "")).startswith("ACQUIRED"):
        catalogue_step = hris.spark_io.StepLogger(spark, tables, p_run_id, "REFERENCE_CATALOGUE")
        catalogue_report, catalogue_error = None, ""
        try:
            catalogue_client = hris.ts_acquire.TsClient(
                ts_base_url, secrets[hris.common.TS_SECRET_CLIENT_ID], secrets[hris.common.TS_SECRET_CLIENT_SECRET],
                session_factory=lambda: build_http_session(2), stats=hris.ts_acquire.AcquisitionStats(),
                network_errors=(requests.exceptions.RequestException, OSError), max_concurrency=2)
            try:
                catalogue_client.get_token()
                reference_store = hris.reference_api.SparkReferenceStore(spark, tables)
                catalogue_report = hris.reference_api.synchronize_catalogue(
                    lambda library: hris.reference_api.fetch_library(catalogue_client, library), reference_store,
                    lambda keys: reference_store.observed(p_run_id, keys), loaded_by=item_name + ":" + p_run_id)
            finally:
                catalogue_client.close()
            catalogue_step.succeeded(rows_out=catalogue_report["inserted"] + catalogue_report["updated"],
                                     message=hris.reference_api.step_message(catalogue_report), counters_only=True)
        except Exception as catalogue_failure:      # jamais bloquant : la transformation juge l'état réel des tables
            catalogue_error = hris.common.safe_error(catalogue_failure)
            catalogue_step.failed(catalogue_failure)
        hris.spark_io.write_anomalies(spark, tables, p_run_id,
                                      hris.reference_api.catalogue_anomalies(catalogue_report, catalogue_error),
                                      hris.reference_api.CATALOGUE_CONTROLS)
        catalogue_result = ({key: catalogue_report[key] for key in (
            "status", "tables_written", "tables_skipped", "inserted", "updated", "used_codes_without_adp",
            "used_tables_without_any_adp")} if catalogue_report else {"status": "FAILED", "error": catalogue_error})
    result["reference_catalogue"] = catalogue_result
    logger.info(f"✓ Catalogue des codes TalentSoft : {json.dumps(catalogue_result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la synchronisation du catalogue: {e}"
    logger.error(f"❌ {error_message}", exc_info=True)
    flush_logger(
        logger,
        workspace_name=workspace_name,
        item_name=item_name,
        item_type=item_type,
        error_message=error_message,
        notebook_id=notebook_id,
        operation_id=operation_id,
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CLÔTURE =============
logger.info("📝 Logs sauvegardés dans le Lakehouse")
flush_logger(logger, is_error=False)
mssparkutils.notebook.exit(hris.common.json_exit(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
