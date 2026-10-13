# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T3_NB_HRIS_COMPARE. C'est le nom affiché dans Microsoft Fabric pour le notebook de comparaison du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_COMPARE.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T3_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Compare, après une transformation réussie.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/QUOTIDIEN/NB_HRIS_COMPARE.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook compare les salariés mis en forme pour la paie par la transformation à la référence, c'est-à-dire l'état validé lors du dernier run promu. Chaque salarié, identifié par la clé id_unique, est classé comme nouveau, modifié, inchangé, absent ou rejeté, en comparant l'empreinte de sa ligne. Le notebook rassemble ensuite tous les contrôles bloquants du run, ceux de l'acquisition et de la transformation compris, et décide si le run est validé ou bloqué. Un premier chargement en mode NORMAL est bloqué parce que la référence est vide. Le mode INIT_REFERENCE est refusé si la référence n'est pas vide. Le recalcul est interdit dès qu'une écriture de promotion a eu lieu pour ce run.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal des cellules. Il lit ctl.run, ctl.run_step, cfg.process pour les seuils, stg.ts_employee_adp et stg.ts_employee_reject produits par T3_NB_HRIS_TRANSFORM, la référence ref.ts_employee_adp et la liste à publier pub.adp_outbox pour détecter une promotion déjà commencée. Il écrit log.comparison_summary, log.comparison_detail et log.anomaly, met à jour ctl.run, consigne l'étape dans ctl.run_step et ajoute ses traces dans log.cell_execution. Aucun secret n'est lu.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et applique les réglages de session Spark. La cellule de comparaison vérifie que le mode reçu est celui enregistré pour le run ; en mode de reprise de publication, elle renvoie un statut de saut sans rien calculer. Elle contrôle ensuite les clés manquantes ou en double, classe les salariés, calcule les compteurs et les pourcentages, journalise les noms des colonnes modifiées sans aucune valeur et enregistre la décision VALIDATED ou BLOCKED. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL par défaut, INIT_REFERENCE pour le premier chargement ou RETRY_PUBLICATION pour une reprise de publication ; il doit être identique au mode enregistré à l'ouverture du run. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible, en pratique la variable lakehouse_name de la bibliothèque de variables VL_HRIS ; il est obligatoire puisqu'aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
# # **Sorties produites**
# # Le notebook écrit log.comparison_summary, qui contient les compteurs et les pourcentages, log.comparison_detail, qui contient la clé, la catégorie, les noms de colonnes et les empreintes, mais jamais de valeur, et log.anomaly. Il met à jour ctl.run et ctl.run_step et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui contient la décision, la liste des contrôles bloquants et les compteurs par catégorie.
# # **Limitations connues et points d'attention**
# # Les seuils de taux d'absents, de modifiés et de rejetés ne sont pas fournis : ils sont calculés et journalisés avec le résultat NOT_CONFIGURED, sans seuil inventé. La clé id_unique et la liste des soixante et onze colonnes comparées restent à confirmer par le métier.
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
from hris import steps
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

p_run_id = ""                    # @pipeline().RunId (obligatoire)
p_run_mode = "NORMAL"            # NORMAL | INIT_REFERENCE | RETRY_PUBLICATION (identique au mode enregistré)
p_test_schema = ""               # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""            # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"      # VL_HRIS.log_cell_success : true = trace aussi les cellules réussies

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
    ensure_session_settings(spark)
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
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

# ============= COMPARAISON ET DÉCISION =============
cell_log.start("COMPARE")
try:
    result = steps.run_compare(spark, tables, p_run_id, p_run_mode)
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
