# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T4_NB_HRIS_PROMOTE. C'est le nom affiché dans Microsoft Fabric pour le notebook de promotion du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_PROMOTE.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T4_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Promote, après une comparaison réussie.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/QUOTIDIEN/NB_HRIS_PROMOTE.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook fait d'un run validé la nouvelle référence, c'est-à-dire l'état de chaque salarié contre lequel le run suivant sera comparé, et fige la liste des salariés à publier vers l'outil de paie ADP. Il ne promeut qu'un run au statut VALIDATED, après avoir revalidé les contrôles bloquants et les seuils avec la configuration courante et vérifié qu'aucun autre run n'a déplacé la référence depuis la comparaison. Une seule fusion met à jour la référence : les nouveaux salariés sont insérés, les modifiés sont mis à jour, les absents sont supprimés et les rejetés sont conservés pour être retraités. La liste à publier est créée pour les nouveaux et les modifiés, sauf lors d'un premier chargement. Un run déjà promu n'est jamais fusionné une seconde fois.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal des cellules. Il lit ctl.run, ctl.run_step, cfg.process, log.anomaly, log.comparison_summary, log.comparison_detail et stg.ts_employee_adp produits par les étapes précédentes. Il écrit la référence ref.ts_employee_adp et la liste à publier pub.adp_outbox, met à jour ctl.run, consigne l'étape dans ctl.run_step et ajoute ses traces dans log.cell_execution. Aucun secret n'est lu.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et applique les réglages de session Spark. La cellule de promotion vérifie que le mode reçu est celui enregistré pour le run ; en mode de reprise de publication, elle renvoie un statut de saut. Elle contrôle ensuite le statut, revalide les blocages et la stabilité de la référence, écrit un marqueur d'étape avant toute écriture, crée la liste à publier puis exécute la fusion sur la référence. Elle enregistre enfin la date de promotion une seule fois. Si une condition n'est pas remplie, l'étape est marquée comme sautée avec son motif. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL par défaut, INIT_REFERENCE pour le premier chargement ou RETRY_PUBLICATION pour une reprise de publication ; il doit être identique au mode enregistré à l'ouverture du run. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible, en pratique la variable lakehouse_name de la bibliothèque de variables VL_HRIS ; il est obligatoire puisqu'aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
# # **Sorties produites**
# # Le notebook met à jour ref.ts_employee_adp et pub.adp_outbox, qui contiennent des données personnelles et restent en accès restreint. Il met à jour ctl.run et ctl.run_step et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui indique si le run a été promu, le motif éventuel et le nombre d'éléments à publier.
# # **Limitations connues et points d'attention**
# # Il n'existe pas de transaction commune à plusieurs tables : la cohérence repose sur les marqueurs d'étape, sur l'écriture unique de la date de promotion et sur l'idempotence des écritures. Un retour arrière de la référence ne se fait qu'avec une validation distincte, par la restauration d'une version Delta antérieure.
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

# ============= PROMOTION DE LA RÉFÉRENCE ET LISTE À PUBLIER =============
cell_log.start("PROMOTE")
try:
    result = steps.run_promote(spark, tables, p_run_id, p_run_mode)
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
