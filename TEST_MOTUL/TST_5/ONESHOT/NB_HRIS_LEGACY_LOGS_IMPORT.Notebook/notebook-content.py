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
# T5_NB_HRIS_LEGACY_LOGS_IMPORT
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_LEGACY_LOGS_IMPORT.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook importe une seule fois, dans le Lakehouse, l'historique des journaux de publication de l'ancienne fonction Azure qui alimentait l'outil de paie ADP. Il assure la continuité de la traçabilité des envois vers ADP entre l'ancien traitement et le nouveau. Les journaux sont des fichiers produits par l'existant, un par jour, rangés dans le stockage Azure de l'existant ; ils sont lus en mémoire et aucun fichier n'est créé, modifié ni supprimé. C'est la seule lecture de fichiers du projet, décidée comme exception ponctuelle et hors du traitement quotidien.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'import des journaux historiques, d'écriture dans les tables et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du compte de stockage de l'existant, le nom du conteneur, le dossier des journaux et l'adresse du coffre. Il lit dans le coffre les trois secrets de l'application Azure autorisée à lire ce stockage : app-tenant-id, app-client-id et app-client-secret. Il utilise les bibliothèques Azure d'identité et de stockage. Il lit et écrit log.log_file_import, et écrit log.adp_publication_log et log.log_file_import_reject. Il est lancé à la main ; aucun pipeline ne l'appelle.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le notebook contrôle ses paramètres et la présence des valeurs de stockage. Il lit ensuite les trois secrets et construit l'identité de l'application Azure. Il liste les fichiers du dossier des journaux dans le conteneur, les lit en mémoire et les analyse : en-têtes attendus, séparateur, encodage, date tirée du dossier daté. Un fichier déjà importé est ignoré, ce qui rend l'import rejouable. Les lignes sans horodatage valide ou au nombre de colonnes incorrect sont rejetées et tracées sans valeur. En mode CHECK, le notebook affiche seulement les compteurs ; en mode APPLY, il écrit les lignes, les rejets et le registre des fichiers.
# 
# **Paramètres**
# 
# Le paramètre p_mode vaut CHECK par défaut, pour compter sans écrire, ou APPLY pour écrire. Le paramètre p_extensions liste les extensions de fichiers acceptées, séparées par des virgules, et vaut .csv par défaut. Le paramètre p_sheet_name désigne la feuille à lire dans un classeur, la première par défaut. Le paramètre p_change_ref porte la référence de la validation et il est obligatoire en mode APPLY. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# En mode APPLY, le notebook écrit les lignes de journal dans log.adp_publication_log, qui contient des noms et des matricules et reste en accès restreint, les rejets sans valeur dans log.log_file_import_reject et le registre des fichiers dans log.log_file_import. Dans tous les modes, il écrit son journal de fonctionnement dans log.notebook_log et renvoie des compteurs.
# 
# **Limitations connues et points d'attention**
# 
# L'existant écrasait son journal à chaque run du jour : un seul journal par jour est disponible. Le compte de stockage de l'existant n'accepte que des accès privés : le workspace doit y avoir accès, par exemple par un point de terminaison privé géré, et l'application Azure doit avoir le droit de lecture des blobs. Les valeurs de stockage ne sont renseignées que dans le jeu de valeurs de production.
# 
# **Responsable et contact**
# 
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import json
import uuid

# Third-party libraries
# (aucune)

# Azure libraries
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

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
        required_keys=("key_vault_url", "legacy_storage_account_url", "legacy_container_name", "legacy_log_prefix"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    key_vault_url = context_values["key_vault_url"]
    legacy_storage_account_url = context_values["legacy_storage_account_url"]
    legacy_container_name = context_values["legacy_container_name"]
    legacy_log_prefix = context_values["legacy_log_prefix"]
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

p_mode = "CHECK"            # CHECK : compte sans écrire ; APPLY : écrit les lignes, les rejets et le registre
p_extensions = ".csv"       # extensions acceptées, séparées par des virgules (ex. ".csv,.xlsx")
p_sheet_name = ""           # feuille à lire dans un classeur ; vide = première feuille
p_change_ref = ""           # APPLY : référence de la validation, obligatoire
p_test_schema = ""          # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""              # eventpath Fabric (ex. /p_mode=APPLY/p_change_ref=...)

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
        p_extensions = parsed_path.get("p_extensions", p_extensions)
        p_sheet_name = parsed_path.get("p_sheet_name", p_sheet_name)
        p_change_ref = parsed_path.get("p_change_ref", p_change_ref)
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

# ============= VALIDATION DES PRÉ-CONDITIONS (R19) =============
try:
    mode = str(p_mode).strip().upper()
    if mode not in ("CHECK", "APPLY"):
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir CHECK ou APPLY")
    if mode == "APPLY" and not p_change_ref.strip():
        raise hris.common.HrisError("PARAM_MISSING", "p_change_ref (obligatoire pour écrire)")
    extensions = {e.strip().lower() for e in p_extensions.split(",") if e.strip()}
    if not extensions:
        raise hris.common.HrisError("PARAM_INVALID", "p_extensions vide")
    logger.info(f"✓ Import historique {mode}, extensions {sorted(extensions)}")
except Exception as e:
    error_message = f"Erreur lors de la validation des pré-conditions: {e}"
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
    attach_logger_to_lakehouse(logger, tables, context_values, run_id="")
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

# ============= AUTHENTIFICATION AZURE CREDENTIALS (R16) =============
try:
    logger.info("=" * 80)
    logger.info("🔐 AUTHENTIFICATION AZURE CREDENTIALS")
    logger.info("=" * 80)
    TENANT_ID = get_parameter("app-tenant-id")
    CLIENT_ID = get_parameter("app-client-id")
    CLIENT_SECRET = get_parameter("app-client-secret")
    credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    logger.info("✓ Credentials récupérés avec succès")
except Exception as e:
    error_message = f"Erreur lors de la récupération des credentials: {e}"
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

# ============= LECTURE DES JOURNAUX HISTORIQUES (SDK DE STOCKAGE AZURE, R13) =============
try:
    blob_service_client = BlobServiceClient(account_url=legacy_storage_account_url, credential=credential)
    container_client = blob_service_client.get_container_client(legacy_container_name)
    entries = hris.legacy_logs.blob_entries(container_client, legacy_log_prefix)
    already_imported = {row.source_uri for row in spark.table(tables("log.log_file_import"))
                        .where("status = 'IMPORTED'").select("source_uri").collect()}
    import_id = "LEGACY-" + uuid.uuid4().hex[:12]
    uri_prefix = f"{legacy_storage_account_url.rstrip('/')}/{legacy_container_name}/{legacy_log_prefix.strip('/')}"
    records, rejects, ledger = hris.legacy_logs.process_entries(
        entries, uri_prefix, extensions, already_imported, import_id, item_name, workspace_name, p_sheet_name)
    statuses = {}
    for entry in ledger:
        statuses[entry["status"]] = statuses.get(entry["status"], 0) + 1
    result = {"mode": mode, "import_id": import_id, "files": len(ledger), "files_by_status": statuses,
              "rows_loaded": len(records), "rows_rejected": len(rejects)}
    del entries
    logger.info(f"✓ Analyse des journaux : {json.dumps(result, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de la lecture des journaux historiques: {e}"
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

# ============= ÉCRITURE DES JOURNAUX HISTORIQUES (APPLY) =============
try:
    if mode == "APPLY":
        for logical, rows in (("log.adp_publication_log", records), ("log.log_file_import_reject", rejects),
                              ("log.log_file_import", ledger)):
            if rows:
                hris.spark_io.append(hris.spark_io.rows_df(spark, rows, logical), tables(logical), logical)
        hris.spark_io.StepLogger(spark, tables, import_id, "LEGACY_LOGS_IMPORT").succeeded(
            rows_out=len(records), counters_only=True,
            message=json.dumps(dict(result, change_ref=p_change_ref.strip()), ensure_ascii=False))
        result["written"] = True
    else:
        result["written"] = False
    logger.info(f"✓ Import historique : {json.dumps(result, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de l'écriture des journaux historiques: {e}"
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
