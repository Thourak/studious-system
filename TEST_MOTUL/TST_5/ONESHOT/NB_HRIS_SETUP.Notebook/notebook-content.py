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
# T5_NB_HRIS_SETUP
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_SETUP.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook prépare le Lakehouse T5_LH_HRIS dont dépendent tous les autres notebooks du hub. Il crée, s'ils n'existent pas, les six schémas cfg, stg, ref, pub, ctl et log, puis toutes les tables du modèle de données, dont la table du journal de fonctionnement et celles du flux de rémunération. Il insère ensuite, uniquement si elles sont absentes, la ligne de paramétrage du processus, la ligne de verrou à l'état libéré et les trois lignes de correspondance du flux de rémunération. Une table existante n'est jamais supprimée, vidée, remplacée ni modifiée : son schéma est comparé à la définition attendue et tout écart est signalé comme incompatible.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'écriture dans les tables, d'étapes, de modèle de données et de journal. Il lit dans la variable library T5_VL_HRIS le nom du Lakehouse. Il est appelé en premier par le pipeline de premier chargement T5_PL_HRIS_BOOTSTRAP, qui s'arrête si une table est incompatible ; on peut aussi le lancer à la main après une évolution du modèle.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le notebook parcourt la définition de chaque table : il crée la table si elle manque, sinon il compare ses colonnes et leurs types au modèle. Il insère ensuite les graines absentes. Il renvoie un statut OK ou INCOMPATIBLE avec le nombre de tables créées et existantes ; le pipeline de premier chargement lit ce statut.
# 
# **Paramètres**
# 
# Le paramètre p_test_schema isole les tables pendant les tests ; vide, ce sont les tables réelles. Le paramètre eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook crée les schémas et les tables du Lakehouse, insère les graines dans cfg.process, ctl.process_lock et cfg.ts_remuneration_mapping, et écrit son journal dans log.notebook_log. Les tables de correspondance sont créées vides : leur partie TalentSoft est remplie par l'acquisition et leurs codes ADP par T5_NB_HRIS_REFERENCE_ADMIN.
# 
# **Limitations connues et points d'attention**
# 
# Aucune migration automatique n'est faite : une évolution du modèle sur une table existante doit être appliquée par une opération dédiée et validée. Les seuils du processus sont laissés vides, comme dans l'existant, qui n'en avait pas.
# 
# **Responsable et contact**
# 
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import json

# Third-party libraries
# (aucune)

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
        required_keys=("lakehouse_name",),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
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

p_test_schema = ""     # tests uniquement : schéma d'isolement (ex. "hris_tst") ; vide = tables réelles
eventpath = ""         # eventpath Fabric (ex. /p_test_schema=hris_tst)

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

# ============= SOCLE : SCHÉMAS, TABLES, GRAINES =============
try:
    report = hris.spark_io.ensure_schemas_and_tables(spark, tables)
    seeded = hris.steps.seed(spark, tables)
    incompatible = [entry["table"] for entry in report if entry["detail"].startswith("INCOMPATIBLE")]
    result = {"status": "INCOMPATIBLE" if incompatible else "OK",
              "created": sum(entry["action"] == "CREATED" for entry in report),
              "existing": sum(entry["action"] == "EXISTS" for entry in report), "incompatible": incompatible,
              "seed_process_inserted": seeded["process"], "seed_lock_inserted": seeded["lock"],
              "seed_remuneration_mapping_inserted": seeded["remuneration_mapping"]}
    for entry in report:
        logger.info(f"   {entry['action']} {entry['table']} {entry['detail']}")
    logger.info(f"✓ Socle : {json.dumps(result, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de l'installation du socle: {e}"
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
