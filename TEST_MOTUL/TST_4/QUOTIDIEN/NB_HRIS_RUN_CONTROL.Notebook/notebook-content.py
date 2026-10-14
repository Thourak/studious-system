# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# T4_NB_HRIS_RUN_CONTROL. C'est le nom affiché dans Microsoft Fabric pour le notebook de contrôle des runs du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_RUN_CONTROL.Notebook.
# **Type d'objet**
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T4_PL_HRIS_ORCHESTRATOR l'appelle à l'ouverture du run (activité RunStart), à sa clôture (activité RunEnd) et en cas d'échec (activités RunFail et RunEndFail).
# **Chemin dans le dépôt**
# TEST_MOTUL/TST_4/QUOTIDIEN/NB_HRIS_RUN_CONTROL.Notebook/notebook-content.py
# **Description fonctionnelle**
# Ce notebook encadre chaque exécution du hub, qui extrait les salariés depuis TalentSoft, les transforme, les compare à l'état validé précédent et prépare leur publication vers l'outil de paie ADP. À l'ouverture, il contrôle les paramètres, calcule la date métier dans le fuseau de Paris, pose un verrou pour qu'un seul run soit actif à la fois et enregistre le run dans le registre. Si le verrou est déjà pris, le run se termine proprement avec le statut SKIPPED_CONCURRENT, sans erreur et sans aucune écriture métier. À la clôture, il calcule le statut final, écrit le résumé du run, libère le verrou et prépare le contenu d'une notification sans donnée personnelle, dont l'envoi reste bloqué. En cas d'échec, il marque le run en échec avec un message assaini et libère le verrou.
# **Dépendances**
# Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules de contrôle du run, de modèle de données, de réglage de session et de journal des cellules. Il lit et écrit la table de verrou ctl.process_lock et le registre ctl.run. À la clôture, il lit le détail de la comparaison log.comparison_detail, les rejets stg.ts_employee_reject et la liste à publier pub.adp_outbox, puis écrit le résumé log.run_summary. Il écrit enfin ses traces de cellules dans log.cell_execution. Le contexte d'exécution de Fabric fournit le nom et l'identifiant réels du workspace, enregistrés dans le registre. Aucun secret n'est lu.
# **Fonctionnement et logique de traitement**
# La configuration lit le contexte réel du notebook, résout les noms complets des tables à partir du workspace courant et du Lakehouse reçu en paramètre, puis ouvre le journal des cellules et applique les réglages de session Spark attendus par la bibliothèque. La cellule de contrôle exécute ensuite l'action demandée par le paramètre p_mode. Pour START, elle valide les paramètres et, en mode de reprise de publication, l'existence du run cible déjà promu ; elle détermine la date métier et le rang du run dans la journée, tente d'obtenir le verrou puis enregistre le run. Pour END, elle calcule le statut final, écrit le résumé et libère le verrou. Pour FAIL, elle enregistre l'échec et libère le verrou. Toute autre valeur de p_mode provoque une erreur. La clôture écrit les traces du journal puis renvoie le résultat au pipeline.
# **Paramètres**
# Le paramètre p_mode vaut START, END ou FAIL ; sa valeur par défaut est START. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_process_code vaut TS_ADP_EMPLOYEE, seul processus actif. Le paramètre p_run_mode vaut NORMAL par défaut, ou INIT_REFERENCE pour le premier chargement, ou RETRY_PUBLICATION pour republier un run déjà promu désigné par p_target_run_id, vide sinon. Le paramètre p_trigger_time reçoit l'heure de déclenchement du pipeline en temps universel coordonné. Les paramètres p_business_timezone, qui vaut Europe/Paris par défaut, et p_lock_lease_minutes, qui vaut 240 par défaut, viennent de la bibliothèque de variables VL_HRIS. Le paramètre p_business_date_override, réservé aux tests, force la date métier et reste vide en exploitation. Les paramètres p_pipeline_name et p_env_code, vides par défaut, sont seulement enregistrés et ne commandent aucune décision. Le paramètre p_error_message porte le message d'échec transmis par le pipeline en mode FAIL et il est assaini avant écriture. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible et il est obligatoire, car aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
# **Sorties produites**
# Le notebook met à jour ctl.process_lock et ctl.run, écrit log.run_summary à la clôture et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré court, sans donnée personnelle. Pour START, il contient notamment le champ status, qui vaut OK si le run est ouvert ; le pipeline s'en sert pour décider d'enchaîner les étapes suivantes.
# **Limitations connues et points d'attention**
# Le verrou repose sur une fusion conditionnelle suivie d'une relecture. Un verrou orphelin n'est repris qu'à l'expiration de son bail et la ligne de verrou ne doit jamais être supprimée. La notification est préparée mais jamais envoyée tant que son mécanisme et ses destinataires ne sont pas décidés. La sortie de START est lue par la condition du pipeline : son champ status ne doit pas changer de forme.
# **Responsable et contact**
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance reste à confirmer par l'équipe en charge des ressources humaines.


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

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB)
from hris import run_control
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import as_bool, json_exit
from hris.model import tables_for_notebook
from hris.transform import ensure_session_settings

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
p_business_timezone = "Europe/Paris" # VL_HRIS.business_timezone : fuseau de la date métier
p_lock_lease_minutes = "240"         # VL_HRIS.lock_lease_minutes : durée du bail du verrou
p_business_date_override = ""        # tests uniquement : date métier forcée
p_pipeline_name = ""                 # @pipeline().PipelineName : information enregistrée dans ctl.run
p_env_code = ""                      # VL_HRIS.env_code : information (journaux, notification)
p_error_message = ""                 # FAIL : message d'erreur du pipeline (assaini avant écriture)
p_test_schema = ""                   # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""                # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"          # VL_HRIS.log_cell_success : true = trace aussi les cellules réussies

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
    workspace_name = runtime_context.get("currentWorkspaceName")   # nom RÉEL, enregistré dans ctl.run
    workspace_id = runtime_context.get("currentWorkspaceId")
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

# ============= CONTRÔLE DU RUN (START / END / FAIL) =============
cell_log.start("RUN_CONTROL")
try:
    if p_mode == "START":
        result = run_control.start(spark, tables, run_id=p_run_id, process_code=p_process_code, run_mode=p_run_mode,
                                   target_run_id=p_target_run_id, trigger_time=p_trigger_time,
                                   business_timezone=p_business_timezone,
                                   lock_lease_minutes=int(p_lock_lease_minutes),
                                   business_date_override=p_business_date_override, pipeline_name=p_pipeline_name,
                                   workspace_id=workspace_id, workspace_name=workspace_name)
    elif p_mode == "END":
        result = run_control.end(spark, tables, p_run_id, p_env_code)
    elif p_mode == "FAIL":
        result = run_control.fail(spark, tables, p_run_id, p_error_message)
    else:
        raise ValueError("p_mode invalide")
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
