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
# T5_NB_HRIS_PROMOTE
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_PROMOTE.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook fait d'un run validé la nouvelle référence et prépare la liste des salariés à transmettre à l'outil de paie ADP. La référence contient l'état validé de chaque salarié tel qu'il a été transmis ; elle sert de point de comparaison au run suivant. La liste à transmettre, appelée outbox, contient les salariés nouveaux ou modifiés du run, avec un statut qui permet de suivre leur publication. Au premier chargement, la référence est construite sans aucune liste à transmettre.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal. Il lit ctl.run, la décision de la comparaison, stg.ts_employee_adp et log.comparison_detail du run. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Promote, après une comparaison réussie.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, la promotion vérifie le mode du run et ne fait rien pour une republication. Elle relit la décision du run : un run bloqué n'est jamais promu. Pour un run validé, elle met à jour la référence par fusion, en retirant les salariés absents et en conservant ceux qui sont rejetés, puis écrit la liste à transmettre avec le statut PENDING, sauf au premier chargement. Une promotion rejouée est sans effet.
# 
# **Paramètres**
# 
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL, INIT_REFERENCE ou RETRY_PUBLICATION et doit être identique au mode enregistré. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit la référence ref.ts_employee_adp et la liste pub.adp_outbox, qui contiennent des données personnelles et restent en accès restreint, ainsi que ctl.run, ctl.run_step et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.
# 
# **Limitations connues et points d'attention**
# 
# La référence est remplacée à chaque promotion ; son historique reste disponible par le voyage dans le temps des tables Delta pendant trente jours. Une restauration ne doit être faite qu'avec une validation distincte.
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

p_run_id = ""            # @pipeline().RunId (obligatoire)
p_run_mode = "NORMAL"    # NORMAL | INIT_REFERENCE | RETRY_PUBLICATION (identique au mode enregistré)
p_test_schema = ""       # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""           # eventpath Fabric (ex. /p_run_id=.../p_run_mode=NORMAL)

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
        p_run_mode = parsed_path.get("p_run_mode", p_run_mode)
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

# ============= PROMOTION DE LA RÉFÉRENCE ET LISTE À PUBLIER =============
try:
    if not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    result = hris.steps.run_promote(spark, tables, p_run_id, p_run_mode)
    logger.info(f"✓ Promotion : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la promotion: {e}"
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
