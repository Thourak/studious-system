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
# T5_NB_HRIS_REMUNERATION
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_REMUNERATION.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook reprend le flux de rémunération de l'ancien traitement Azure, qui comparait chaque jour les salaires de base et les primes cibles des salariés à ceux de la veille. Il lit l'export de rémunération de TalentSoft par son interface de programmation, sans aucun fichier, le met au format des exports de l'existant, calcule les changements avec les règles exactes des deux vues de l'existant et les conserve en table. Conformément à la décision du projet, aucun changement n'est renvoyé vers TalentSoft : l'ancien dépôt de fichiers sur le serveur de transfert est abandonné. L'instantané du jour devient la référence du run suivant, ce qui reproduit la comparaison quotidienne entre le jour et la veille.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de rémunération, d'acquisition, d'écriture dans les tables et de journal. Il lit dans la variable library T5_VL_HRIS l'activation du flux, l'adresse du tenant TalentSoft et l'adresse du coffre. Il lit les secrets client-id-ts et client-secret-ts dans le coffre. Il lit ctl.run et cfg.ts_remuneration_mapping, lit et écrit ref.ts_remuneration_snapshot, et écrit stg.ts_remuneration_snapshot, pub.ts_remuneration_change, log.ts_remuneration_computation, log.anomaly, ctl.run_step et log.notebook_log. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Remuneration, en parallèle de l'acquisition des salariés.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le pré-contrôle vérifie le run, la source de production, les secrets et le réseau. En mode RUN, le notebook demande l'export de rémunération à TalentSoft, attend sa production, le lit en mémoire, construit l'instantané selon la table de correspondance, calcule les changements de salaire de base et de prime par rapport à la référence, enregistre l'instantané, les changements et leur suivi, puis fait avancer la référence. Au premier run, la référence est seulement initialisée. En mode VALIDATE, il ne modifie rien : il compare la structure du contenu qu'il produirait à celle du dernier import accepté par TalentSoft dans l'existant, pour valider les hypothèses de correspondance.
# 
# **Paramètres**
# 
# Le paramètre p_mode vaut RUN pour le flux quotidien ou VALIDATE pour la vérification de structure. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire en mode RUN. Le paramètre p_only_active_employees vaut false par défaut, comme l'existant, et limiterait l'export aux salariés actifs. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit l'instantané du run dans stg.ts_remuneration_snapshot, la référence dans ref.ts_remuneration_snapshot, les lignes de changement au format de l'existant dans pub.ts_remuneration_change, leur suivi sans valeur dans log.ts_remuneration_computation, un contrôle d'information dans log.anomaly et son journal dans log.notebook_log. Les trois premières tables contiennent des données personnelles et restent en accès restreint.
# 
# **Limitations connues et points d'attention**
# 
# La correspondance entre l'export de TalentSoft et le format de l'existant repose sur des hypothèses semées par l'installation : elles doivent être validées par le mode VALIDATE puis par la comparaison avec les archives de l'existant. L'export est demandé à six heures alors que l'existant lisait des exports produits avant deux heures : des changements saisis entre-temps apparaissent un jour plus tôt.
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
import uuid
from typing import Any, Dict
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
        required_keys=("remuneration_enabled", "ts_base_url", "key_vault_url"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    remuneration_enabled = context_values["remuneration_enabled"]
    ts_base_url = context_values["ts_base_url"]
    key_vault_url = context_values["key_vault_url"]
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

p_mode = "RUN"                        # RUN : flux complet ; VALIDATE : compare la structure au dernier import de l'existant
p_run_id = ""                         # @pipeline().RunId (obligatoire en RUN)
p_only_active_employees = "false"     # true : export limité aux salariés actifs (l'existant exportait tout le monde)
p_test_schema = ""                    # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""                        # eventpath Fabric (ex. /p_mode=VALIDATE)

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
        p_mode = parsed_path.get("p_mode", p_mode)
        p_run_id = parsed_path.get("p_run_id", p_run_id)
        p_only_active_employees = parsed_path.get("p_only_active_employees", p_only_active_employees)
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

def build_http_session(pool_size: int) -> requests.Session:
    """Session HTTP : aucune reprise automatique, pool à la taille de la concurrence."""
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0))
    return session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= PRÉ-CONTRÔLE : RUN, SECRETS, RÉSEAU =============
try:
    for noisy_logger in ("urllib3", "requests", "py4j"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
    mode = str(p_mode).strip().upper()
    if mode not in ("RUN", "VALIDATE"):
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir RUN ou VALIDATE")
    if mode == "RUN" and not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    flow_enabled = hris.common.as_bool(remuneration_enabled)
    trace_id = p_run_id or ("REMVAL-" + uuid.uuid4().hex[:12])
    run = hris.spark_io.get_run(spark, tables, p_run_id) if p_run_id else None
    if mode == "RUN" and run is None:
        raise hris.common.HrisError("RUN_UNKNOWN", "RunStart doit précéder le flux de rémunération")
    preflight = {"source": None, "secrets": None, "network": None}
    checked_url = hris.ts_acquire.assert_prd_source(ts_base_url)
    preflight["source"] = "PRD_OK"
    secrets = None
    if flow_enabled:
        try:
            secrets = read_ts_secrets(key_vault_url)
            preflight["secrets"] = "RESOLVED"
        except Exception as secret_error:
            preflight["secrets"] = "UNAVAILABLE:" + type(secret_error).__name__
        try:
            with socket.create_connection((urlparse(checked_url).hostname, 443), timeout=15):
                pass
            preflight["network"] = "TCP_443_OK"
        except Exception as network_error:
            preflight["network"] = "UNREACHABLE:" + type(network_error).__name__
    logger.info(f"✓ Pré-contrôle : {json.dumps(preflight, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de le pré-contrôle du flux rémunération: {e}"
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

# ============= FLUX RÉMUNÉRATION (EXPORT API VERS TABLES, AUCUN ENVOI) =============
try:
    result = {"run_id": p_run_id or None, "mode": mode, "preflight": preflight}
    if not flow_enabled:
        result["status"] = "SKIPPED_DISABLED"
    elif run is not None and run["run_mode"] == "RETRY_PUBLICATION":
        result["status"] = "SKIPPED_RETRY_PUBLICATION"
    else:
        step = hris.spark_io.StepLogger(spark, tables, trace_id, "REMUNERATION" if mode == "RUN" else "REMUNERATION_VALIDATE")
        try:
            if secrets is None:
                raise hris.common.HrisError("TS_SECRETS_UNAVAILABLE", preflight["secrets"])
            client = hris.ts_acquire.TsClient(
                ts_base_url, secrets[hris.common.TS_SECRET_CLIENT_ID], secrets[hris.common.TS_SECRET_CLIENT_SECRET],
                session_factory=lambda: build_http_session(4), stats=hris.ts_acquire.AcquisitionStats(),
                network_errors=(requests.exceptions.RequestException, OSError), max_concurrency=4)
            try:
                client.get_token()
                api = hris.remuneration.TalentSoftApi(client)
                store = hris.remuneration.SparkRemunerationStore(spark, tables)
                only_active = hris.common.as_bool(p_only_active_employees)
                if mode == "RUN":
                    outcome = hris.remuneration.run_remuneration(api, store, run_id=p_run_id,
                                                                 business_date=run["business_date"],
                                                                 only_active_employees=only_active)
                    hris.spark_io.write_anomalies(spark, tables, p_run_id, [{
                        "control_code": "REMUNERATION_FLOW", "severity": "INFO", "metric_value": None,
                        "threshold_value": None, "outcome": "PASS",
                        "message": json.dumps(outcome["computations"])}], ["REMUNERATION_FLOW"])
                    step.succeeded(rows_out=sum(outcome["summary"][k] for k in ("baseSalary_nb_inserts", "bonus_nb_inserts")),
                                   message=hris.remuneration.step_message(outcome), counters_only=True)
                    result.update(status=outcome["status"], summary=outcome["summary"],
                                  computations=outcome["computations"], first_run=outcome["first_run"])
                else:   # VALIDATE : aucune écriture métier ; structure produite comparée au dernier import de l'existant
                    snapshot, stats = hris.remuneration.build_snapshot(
                        hris.remuneration.extract_compensation(api, only_active_employees=only_active), store.mapping())
                    produced = {
                        hris.remuneration.IMPORT_SALARY: hris.remuneration.base_salary_changes(
                            snapshot[hris.remuneration.BASE_SALARY], []),
                        hris.remuneration.IMPORT_BONUS: hris.remuneration.bonus_changes(
                            snapshot[hris.remuneration.BONUS_PERCENT], [], snapshot[hris.remuneration.BONUS_TARGET_VALUE],
                            [], snapshot[hris.remuneration.BASE_SALARY])}
                    latest = api.latest_imports()
                    gaps = {}
                    for import_type, rows in produced.items():
                        mine = hris.remuneration.content_structure(hris.remuneration.import_content(import_type, rows))
                        legacy = (hris.remuneration.content_structure(api.import_file(latest[import_type]["id"]))
                                  if import_type in latest else None)
                        gaps[import_type] = (hris.remuneration.compare_structures(mine, legacy) if legacy
                                             else ["aucun import de l'existant trouvé"])
                        logger.info(f"✓ {import_type} : {len(gaps[import_type])} écart(s) de structure")
                        for gap in gaps[import_type]:
                            logger.info(f"   - {gap}")
                    step.succeeded(counters_only=True, message=json.dumps({t: len(g) for t, g in gaps.items()}))
                    result.update(status="VALIDATED_STRUCTURE", snapshot_stats=stats,
                                  gaps={t: len(g) for t, g in gaps.items()})
            finally:
                client.close()
        except Exception as flow_error:
            step.failed(flow_error)
            raise
    logger.info(f"✓ Flux rémunération : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de le flux rémunération: {e}"
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
