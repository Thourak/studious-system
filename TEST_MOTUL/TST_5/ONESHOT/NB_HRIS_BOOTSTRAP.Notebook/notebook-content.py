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
# T5_NB_HRIS_BOOTSTRAP
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_BOOTSTRAP.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook prend les deux décisions du premier chargement du hub. Avant le run, il choisit le mode du run quotidien comme le fait la comparaison : si la référence des salariés est vide, le run se fait en INIT_REFERENCE pour la construire ; sinon il se fait en NORMAL. Le premier chargement peut donc être relancé autant de fois que nécessaire sans jamais réinitialiser la référence. Après le run, il lit l'issue et en déduit une seule action suivante : activer la planification, saisir les codes ADP manquants, corriger le flux de rémunération, relancer plus tard ou analyser un incident.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de premier chargement, de correspondances, de modèle de données et de journal. Il lit ref.ts_employee_adp, ctl.process_lock, ctl.run, ctl.run_step, log.anomaly et les tables de correspondance. Il est appelé deux fois par le pipeline T5_PL_HRIS_BOOTSTRAP, dans les activités Plan et Status ; on peut aussi le lancer à la main en mode STATUS.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le mode PLAN compte les lignes de la référence et lit le verrou : si un run tient le verrou et que son bail n'a pas expiré, le statut renvoyé arrête le pipeline avant tout run. Le mode STATUS retrouve le run lancé depuis le déclenchement du premier chargement, lit le dernier statut de chaque étape et les contrôles bloquants, déduit l'action suivante et affiche, si des codes ADP manquent, la grille des codes TalentSoft sans code ADP, les plus employés en premier.
# 
# **Paramètres**
# 
# Le paramètre p_mode vaut PLAN ou STATUS. Le paramètre p_since reçoit l'heure de déclenchement du pipeline de premier chargement, en temps universel, et il est obligatoire en mode STATUS. Le paramètre p_max_lines limite le nombre de codes affichés par table dans la grille et vaut cent par défaut. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook n'écrit que son journal dans log.notebook_log. Il renvoie au pipeline le mode choisi ou l'action suivante et son message ; le pipeline échoue avec ce message pour toute action autre que l'activation de la planification.
# 
# **Limitations connues et points d'attention**
# 
# Le run retenu par le mode STATUS est le plus récent depuis le déclenchement du premier chargement : la planification quotidienne doit rester désactivée tant que le premier chargement n'est pas terminé. En production, les runs complets sans publication, parce que la garde ou les interrupteurs l'empêchent, sont considérés comme aboutis.
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

p_mode = "PLAN"          # PLAN : mode du run de l'orchestrateur ; STATUS : issue du run et action suivante
p_since = ""             # STATUS : @pipeline().TriggerTime du pipeline de premier chargement (UTC)
p_max_lines = "100"      # STATUS : nombre maximal de codes affichés par table dans la grille des codes ADP
p_test_schema = ""       # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""           # eventpath Fabric (ex. /p_mode=STATUS/p_since=...)

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
        p_since = parsed_path.get("p_since", p_since)
        p_max_lines = parsed_path.get("p_max_lines", p_max_lines)
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

# ============= PREMIER CHARGEMENT : PLAN OU STATUS =============
try:
    mode = str(p_mode).strip().upper()
    if mode not in ("PLAN", "STATUS"):
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir PLAN ou STATUS")
    if mode == "PLAN":
        inputs = hris.bootstrap.read_plan_inputs(spark, tables)
        result = hris.bootstrap.plan(inputs["reference_rows"], inputs["lock"])
    else:
        if not str(p_since).strip():
            raise hris.common.HrisError("PARAM_MISSING", "p_since (déclenchement du pipeline de premier chargement)")
        since = hris.common.parse_trigger_time(p_since)
        run = hris.bootstrap.find_run(spark, tables, since)
        inputs = hris.bootstrap.read_status_inputs(spark, tables, run["run_id"]) if run else {"steps": [], "anomalies": []}
        result = hris.bootstrap.outcome(run, inputs["steps"], inputs["anomalies"])
        grid = []
        if run is not None:
            store = hris.reference_api.SparkReferenceStore(spark, tables)
            summary, grid = hris.reference_api.adp_grid_report(store, hris.model.USED_LOOKUP_TABLES,
                                                               lambda table: store.usage(run["run_id"], table),
                                                               int(str(p_max_lines).strip()))
            result["adp_codes_missing"] = {table: item["without_adp"] for table, item in summary.items()
                                           if isinstance(item, dict) and item["without_adp"]}
        logger.info(f"✓ ACTION SUIVANTE : {result['next_action']} - {result['message']}")
        if result.get("adp_codes_missing"):
            print("\n".join(grid))          # grille à copier : codes TalentSoft et libellés, aucune donnée de salarié
    logger.info(f"✓ Premier chargement {mode} : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la décision du premier chargement: {e}"
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
