# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T4_NB_HRIS_BOOTSTRAP. C'est le nom affiché dans Microsoft Fabric pour le notebook qui encadre le premier chargement du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_BOOTSTRAP.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T4_PL_HRIS_BOOTSTRAP l'appelle deux fois : avant le run de l'orchestrateur quotidien (activité Plan) et après (activité Status). On peut aussi le lancer à la main en mode STATUS.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/ONESHOT/NB_HRIS_BOOTSTRAP.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Le premier chargement se lance en une seule action, par le pipeline T4_PL_HRIS_BOOTSTRAP. Ce notebook en prend les deux décisions. En mode PLAN, il choisit le mode du run de l'orchestrateur comme la comparaison juge la référence : si la table ref.ts_employee_adp est vide, le run se fait en INIT_REFERENCE ; sinon, le premier chargement est déjà fait et le run se fait en NORMAL. Le pipeline peut donc être relancé autant de fois que nécessaire sans jamais réinitialiser la référence. Si un run tient encore le verrou, PLAN arrête le pipeline avant tout run. En mode STATUS, il lit l'issue du run lancé et en déduit une seule action suivante : activer la planification du pipeline quotidien, saisir les codes ADP manquants, corriger le flux de rémunération, relancer plus tard ou analyser un incident. Quand des codes ADP manquent, il affiche directement la grille à coller dans T4_NB_HRIS_REFERENCE_ADMIN.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules hris.bootstrap et hris.reference_api, ainsi que le modèle de données et le journal des cellules. En mode PLAN, il lit ref.ts_employee_adp et ctl.process_lock. En mode STATUS, il lit ctl.run, ctl.run_step, log.anomaly, les tables de correspondance cfg.lu_hr_* et l'extraction stg.ts_employee_raw du run. Il n'écrit aucune table métier : il ajoute seulement ses traces dans log.cell_execution. Aucun secret n'est lu et aucun appel réseau n'est fait.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et valide les paramètres. La cellule d'amorçage exécute ensuite le mode demandé. En PLAN, elle compte les lignes de la référence, lit le verrou et renvoie le mode retenu et son motif. En STATUS, elle retrouve le run de l'orchestrateur le plus récent ouvert depuis le déclenchement du pipeline d'amorçage. Elle lit le dernier statut de chaque étape et les contrôles bloquants en échec, puis calcule l'action suivante et son message. Elle affiche ensuite, pour les treize tables utilisées, le nombre de codes TalentSoft sans code ADP et, s'il y en a, la grille à remplir. La clôture écrit les traces du journal puis renvoie le résultat au pipeline.
# # **Paramètres**
# # Le paramètre p_mode vaut PLAN ou STATUS ; sa valeur par défaut est PLAN. Le paramètre p_since, obligatoire en STATUS, reçoit l'heure de déclenchement du pipeline d'amorçage en temps universel coordonné ; il borne la recherche du run. Le paramètre p_max_lines vaut 100 et limite le nombre de codes affichés par table dans la grille. Les paramètres p_test_schema, p_lakehouse_name et p_log_cell_success ont le même rôle que dans les autres notebooks.
# # **Sorties produites**
# # Le notebook n'écrit que ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré, sans donnée personnelle, lu par le pipeline. En PLAN, elle contient status, qui vaut OK ou LOCK_BUSY, run_mode, reference_rows, reason et, si le verrou est pris, message. En STATUS, elle contient ok, next_action, message, le run trouvé, son mode et son statut, le dernier statut de chaque étape, les contrôles bloquants, le statut du flux de rémunération et le nombre de codes sans code ADP par table.
# # **Limitations connues et points d'attention**
# # STATUS retient le run le plus récent ouvert après le déclenchement du pipeline d'amorçage. Si la planification du pipeline quotidien est active pendant le premier chargement, un run planifié pourrait être retenu à la place : il faut la laisser désactivée jusqu'à la fin. Le notebook ne décide jamais à la place de l'exploitant : il n'active pas la planification et ne saisit aucun code.
# # **Responsable et contact**
# # Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance reste à confirmer par l'équipe en charge des ressources humaines.

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
from hris import bootstrap
from hris import reference_api
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import HrisError, as_bool, json_exit, parse_trigger_time
from hris.model import USED_LOOKUP_TABLES, tables_for_notebook
from hris.transform import ensure_session_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_mode = "PLAN"                  # PLAN : mode du run de l'orchestrateur ; STATUS : issue du run et action suivante
p_since = ""                     # STATUS : @pipeline().TriggerTime du pipeline d'amorçage (UTC), borne la recherche du run
p_max_lines = "100"              # STATUS : nombre maximal de codes affichés par table dans la grille des codes ADP
p_test_schema = ""               # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""            # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"      # true : trace aussi les cellules réussies ; false : échecs seulement

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
    cell_log = CellLog(runtime_context, lakehouse_name=tables.lakehouse, log_success=as_bool(p_log_cell_success),
                       writer=spark_writer(spark, tables))
    cell_log.start("CONFIG")
    ensure_session_settings(spark)
    mode = str(p_mode).strip().upper()
    if mode not in ("PLAN", "STATUS"):
        raise HrisError("PARAM_INVALID", "p_mode doit valoir PLAN ou STATUS")
    if mode == "STATUS" and not str(p_since).strip():
        raise HrisError("PARAM_MISSING", "p_since (déclenchement du pipeline d'amorçage)")
    since = parse_trigger_time(p_since) if mode == "STATUS" else None
    max_lines = int(str(p_max_lines).strip())
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "operation_id": cell_log.base["operation_id"],
                      "mode": mode, "since": str(since) if since else None}, ensure_ascii=False))
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

# ============= AMORÇAGE : PLAN (mode du run) OU STATUS (action suivante) =============
cell_log.start("BOOTSTRAP")
try:
    if mode == "PLAN":
        inputs = bootstrap.read_plan_inputs(spark, tables)
        result = bootstrap.plan(inputs["reference_rows"], inputs["lock"])
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        run = bootstrap.find_run(spark, tables, since)
        inputs = bootstrap.read_status_inputs(spark, tables, run["run_id"]) if run else {"steps": [], "anomalies": []}
        result = bootstrap.outcome(run, inputs["steps"], inputs["anomalies"])
        grid = []
        if run is not None:                                   # codes TalentSoft sans code ADP, comptés sur l'extraction du run
            store = reference_api.SparkReferenceStore(spark, tables)
            summary, grid = reference_api.adp_grid_report(store, USED_LOOKUP_TABLES,
                                                          lambda table: store.usage(run["run_id"], table), max_lines)
            result["adp_codes_missing"] = {table: item["without_adp"] for table, item in summary.items()
                                           if isinstance(item, dict) and item["without_adp"]}
        print(json.dumps(result, ensure_ascii=False, default=str))
        print("ACTION SUIVANTE :", result["next_action"], "-", result["message"])
        if result.get("adp_codes_missing"):
            print("\n".join(grid))
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
