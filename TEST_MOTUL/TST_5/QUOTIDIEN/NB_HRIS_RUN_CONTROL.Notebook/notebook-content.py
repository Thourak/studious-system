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
# T5_NB_HRIS_RUN_CONTROL
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_RUN_CONTROL.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook ouvre, clôture ou déclare en échec un run du hub qui transmet les salariés de Motul France de TalentSoft vers l'outil de paie ADP. À l'ouverture, il prend un verrou pour qu'un seul run s'exécute à la fois et enregistre le run dans le registre des runs avec sa date métier. À la clôture, il calcule le bilan chiffré du run, libère le verrou et renvoie le statut final au pipeline, qui échoue explicitement lorsqu'un contrôle a bloqué le run. En cas d'échec d'une étape, il marque le run en échec et libère le verrou.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de contrôle du run, de modèle de données et de journal. Il lit la variable library T5_VL_HRIS pour le fuseau de la date métier et la durée du bail du verrou. Il lit et écrit les tables ctl.run, ctl.run_step et ctl.process_lock, écrit log.run_summary à la clôture et log.notebook_log pour son journal de fonctionnement. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans les activités RunStart, RunEnd, RunFail et RunEndFail.
# 
# **Fonctionnement et logique de traitement**
# 
# La configuration lit le contexte réel du notebook et la variable library, puis la cellule du Lakehouse crée les schémas s'ils manquent et dirige le journal vers la table log.notebook_log. En mode START, le notebook calcule la date métier à partir de l'heure de déclenchement et du fuseau, prend le verrou, puis enregistre le run ; si le verrou est déjà tenu par un autre run dont le bail n'a pas expiré, il renvoie le statut SKIPPED_CONCURRENT et le pipeline s'arrête sans rien faire. En mode END, il détermine le statut final, calcule le bilan dans le format de l'existant, enregistre la fin du run et libère le verrou. En mode FAIL, il enregistre l'échec avec un message expurgé et libère le verrou.
# 
# **Paramètres**
# 
# Le paramètre p_mode vaut START, END ou FAIL et choisit l'action. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_process_code identifie le processus et vaut TS_ADP_EMPLOYEE. Le paramètre p_run_mode vaut NORMAL pour l'exploitation courante, INIT_REFERENCE pour le premier chargement ou RETRY_PUBLICATION pour republier un run déjà promu, dont l'identifiant est alors donné par p_target_run_id. Le paramètre p_trigger_time reçoit l'heure de déclenchement du pipeline en temps universel. Le paramètre p_business_date_override force la date métier pour les tests et il est refusé en mode NORMAL dans un workspace de production. Le paramètre p_pipeline_name est enregistré pour information, p_error_message porte le message d'échec en mode FAIL, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# Le notebook écrit le registre ctl.run, le journal des étapes ctl.run_step, le verrou ctl.process_lock, le bilan log.run_summary et son journal de fonctionnement log.notebook_log. Il renvoie au pipeline un résultat court qui contient le statut du run et des compteurs, sans aucune donnée personnelle ; le pipeline lit ce statut pour décider de la suite.
# 
# **Limitations connues et points d'attention**
# 
# Le bail du verrou doit rester supérieur à la durée possible d'un run ; il est fixé à six cents minutes dans la variable library. Un verrou orphelin ne doit jamais être supprimé à la main : il faut attendre l'expiration de son bail. Le bilan reprend les colonnes de l'existant, mais il compte les salariés de la population comparée par Fabric et non ceux du delta brut de l'existant.
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
        required_keys=("business_timezone", "lock_lease_minutes"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    business_timezone = context_values["business_timezone"]
    lock_lease_minutes = context_values["lock_lease_minutes"]
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

p_mode = "START"                     # START | END | FAIL
p_run_id = ""                        # @pipeline().RunId (obligatoire)
p_process_code = "TS_ADP_EMPLOYEE"   # seul processus actif
p_run_mode = "NORMAL"                # NORMAL | INIT_REFERENCE | RETRY_PUBLICATION
p_target_run_id = ""                 # RETRY_PUBLICATION uniquement : run déjà promu à republier
p_trigger_time = ""                  # @pipeline().TriggerTime (UTC)
p_business_date_override = ""        # tests uniquement : date métier forcée (refusée en NORMAL dans un workspace PRD)
p_pipeline_name = ""                 # @pipeline().PipelineName : information enregistrée dans ctl.run
p_error_message = ""                 # FAIL : message d'erreur du pipeline (expurgé avant écriture)
p_test_schema = ""                   # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""                       # eventpath Fabric (ex. /p_mode=END/p_run_id=...)

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
        p_process_code = parsed_path.get("p_process_code", p_process_code)
        p_run_mode = parsed_path.get("p_run_mode", p_run_mode)
        p_target_run_id = parsed_path.get("p_target_run_id", p_target_run_id)
        p_trigger_time = parsed_path.get("p_trigger_time", p_trigger_time)
        p_business_date_override = parsed_path.get("p_business_date_override", p_business_date_override)
        p_pipeline_name = parsed_path.get("p_pipeline_name", p_pipeline_name)
        p_error_message = parsed_path.get("p_error_message", p_error_message)
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

# ============= CONTRÔLE DU RUN (START / END / FAIL) =============
try:
    mode = str(p_mode).strip().upper()
    if not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    if mode == "START":
        result = hris.run_control.start(
            spark, tables, run_id=p_run_id, process_code=p_process_code, run_mode=p_run_mode,
            target_run_id=p_target_run_id, trigger_time=p_trigger_time, business_timezone=business_timezone,
            lock_lease_minutes=int(lock_lease_minutes), business_date_override=p_business_date_override,
            pipeline_name=p_pipeline_name, workspace_id=workspace_id, workspace_name=workspace_name)
    elif mode == "END":
        result = hris.run_control.end(spark, tables, p_run_id)
    elif mode == "FAIL":
        result = hris.run_control.fail(spark, tables, p_run_id, p_error_message)
    else:
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir START, END ou FAIL")
    logger.info(f"✓ Contrôle du run {mode} : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de le contrôle du run: {e}"
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
