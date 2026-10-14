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
# T5_NB_HRIS_TRANSFORM
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_TRANSFORM.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook met en forme pour l'outil de paie ADP les salariés extraits de TalentSoft pendant le run. Il reprend exactement les règles des procédures SQL de l'ancien traitement Azure : lecture des soixante-neuf champs avec les mêmes conversions et troncatures, quinze correspondances entre les codes TalentSoft et les codes ADP, rejets pour valeur absente, format incorrect ou code sans correspondance, puis construction des soixante-douze colonnes attendues par la paie. Un salarié rejeté n'est pas transmis. Comme dans l'existant, une catégorie professionnelle inconnue seule ne provoque pas de rejet.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de transformation, de modèle de données et de journal. Il lit le registre ctl.run, les données brutes stg.ts_employee_raw du run, produites par T5_NB_HRIS_TS_ACQUIRE, et les dix-sept tables de correspondance cfg.lu_hr_. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Transform, après une acquisition réussie.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, la transformation vérifie que le run est bien acquis, puis écrit la source typée, contrôle les tables de correspondance et s'arrête proprement si l'une d'elles est absente, vide ou contient une clé en double. Elle écrit ensuite les données enrichies par les correspondances, calcule les rejets, écrit une copie des rejets dont les valeurs sensibles sont masquées, construit la sortie au format de la paie pour les salariés non rejetés, puis marque le run comme transformé. Une erreur de conversion reproduite de l'existant arrête le run, comme l'existant.
# 
# **Paramètres**
# 
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit, pour le run, les tables stg.ts_employee_source, stg.ts_employee_enriched, stg.ts_employee_reject et stg.ts_employee_adp, qui contiennent des données personnelles et restent en accès restreint, ainsi que log.reject, log.anomaly, ctl.run_step et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.
# 
# **Limitations connues et points d'attention**
# 
# Les correspondances doivent contenir les codes ADP saisis par T5_NB_HRIS_REFERENCE_ADMIN ; une table entièrement vide bloque le run. Les écarts éventuels entre les fonctions de conversion de SQL Server et leur émulation ne peuvent être mesurés que sur données réelles, pendant la non-régression.
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

p_run_id = ""          # @pipeline().RunId (obligatoire)
p_test_schema = ""     # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""         # eventpath Fabric (ex. /p_run_id=...)

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

# ============= TRANSFORMATION =============
try:
    if not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    result = hris.steps.run_transform(spark, tables, p_run_id)
    logger.info(f"✓ Transformation : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la transformation: {e}"
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
