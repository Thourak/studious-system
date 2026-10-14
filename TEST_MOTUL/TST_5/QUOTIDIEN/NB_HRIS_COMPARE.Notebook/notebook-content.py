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
# T5_NB_HRIS_COMPARE
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_COMPARE.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook décide quels salariés doivent être transmis à l'outil de paie ADP. Il compare la sortie au format de la paie du run à la référence, qui est l'état validé du dernier run promu, et classe chaque salarié comme nouveau, modifié, inchangé, absent ou rejeté. Ce choix, approuvé par le projet, remplace la comparaison du fichier brut du jour à celui de la veille de l'existant : la population transmise est la même dans le cas courant et plus juste quand une correspondance change ou qu'un rejet est corrigé. Il contrôle aussi la cohérence du run et décide si le run est validé ou bloqué.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal. Il lit ctl.run, ctl.run_step, cfg.process pour les seuils facultatifs, stg.ts_employee_adp du run et la référence ref.ts_employee_adp. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Compare, après une transformation réussie.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, la comparaison vérifie que le mode demandé est celui enregistré pour le run. En mode NORMAL, une référence vide bloque le run, car le premier chargement doit passer par le mode INIT_REFERENCE. Elle contrôle l'absence de clé vide ou en double, calcule les catégories de chaque salarié sur les colonnes comparées, écrit le détail des colonnes changées sans leurs valeurs et le résumé, puis décide VALIDATED ou BLOCKED. Les seuils de taux ne sont pas renseignés, comme dans l'existant : ils sont calculés et journalisés sans jamais bloquer.
# 
# **Paramètres**
# 
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL, INIT_REFERENCE ou RETRY_PUBLICATION et doit être identique au mode enregistré par l'ouverture du run ; en RETRY_PUBLICATION, la comparaison n'est pas recalculée. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit log.comparison_summary, log.comparison_detail, qui contient les clés des salariés et reste en accès restreint, log.anomaly, ctl.run_step, ctl.run et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs et la décision.
# 
# **Limitations connues et points d'attention**
# 
# Un run bloqué laisse la référence intacte ; il suffit de corriger la cause puis de relancer. Le pipeline échoue explicitement sur un run bloqué, à la clôture, pour que le blocage soit visible dans le suivi de Fabric.
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

# ============= COMPARAISON ET DÉCISION =============
try:
    if not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    result = hris.steps.run_compare(spark, tables, p_run_id, p_run_mode)
    logger.info(f"✓ Comparaison : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la comparaison: {e}"
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
