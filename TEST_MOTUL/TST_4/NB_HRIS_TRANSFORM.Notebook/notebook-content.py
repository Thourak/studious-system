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
# T3_NB_HRIS_TRANSFORM. C'est le nom affiché dans Microsoft Fabric pour le notebook de transformation du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_TRANSFORM.Notebook.
#
# **Type d'objet**
#
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T3_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Transform, après une acquisition réussie.
#
# **Chemin dans le dépôt**
#
# TEST_MOTUL/TST_4/NB_HRIS_TRANSFORM.Notebook/notebook-content.py
#
# **Description fonctionnelle**
#
# Ce notebook reproduit, à l'identique et défauts compris, les règles de transformation que l'existant appliquait en langage de requête structuré avant l'envoi vers l'outil de paie ADP. Il découpe les données brutes de chaque salarié en soixante-neuf colonnes tronquées à cinquante caractères, contrôle les tables de correspondance, enrichit chaque salarié par quinze jointures, sépare les salariés rejetés avec le motif de leur rejet et met en forme les soixante-douze colonnes attendues par la paie, avec une empreinte de ligne. Si une table de correspondance est absente, vide, incomplète ou contient une clé en double, seul l'enrichissement est interrompu : le découpage reste écrit et un contrôle bloquant est enregistré, ce qui évite un rejet massif trompeur. Une erreur de conversion de date ou de montant fait échouer le run, comme dans l'existant.
#
# **Dépendances**
#
# Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules d'étapes, de transformation, de modèle de données et de journal des cellules. Il lit le registre ctl.run et les données brutes stg.ts_employee_raw du run, produites par T3_NB_HRIS_TS_ACQUIRE, ainsi que les tables de correspondance du schéma cfg, dont le nom commence par lu_hr. Il écrit stg.ts_employee_source, stg.ts_employee_enriched, stg.ts_employee_reject, log.reject, stg.ts_employee_adp et log.anomaly, met à jour ctl.run, consigne l'étape dans ctl.run_step et ajoute ses traces dans log.cell_execution. Aucun secret n'est lu.
#
# **Fonctionnement et logique de traitement**
#
# La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et applique les réglages de session Spark : fuseau horaire universel et analyse stricte des dates, indispensables à la parité. La cellule de transformation vérifie que le run existe et que son statut autorise la transformation ; sinon elle renvoie un statut de saut sans rien écrire. Elle enchaîne ensuite le découpage, le contrôle des correspondances, l'enrichissement, les rejets et la mise en forme pour la paie, chaque table étant remplacée uniquement pour l'identifiant du run, ce qui rend l'étape rejouable. Elle calcule enfin les compteurs de contrôle et met à jour le statut du run. La clôture écrit les traces du journal puis renvoie le résultat.
#
# **Paramètres**
#
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible, en pratique la variable lakehouse_name de la bibliothèque de variables VL_HRIS ; il est obligatoire puisqu'aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
#
# **Sorties produites**
#
# Le notebook écrit, pour le run, les tables stg.ts_employee_source, stg.ts_employee_enriched, stg.ts_employee_reject et stg.ts_employee_adp, qui contiennent des données personnelles et restent en accès restreint, ainsi que log.reject, où les valeurs sensibles sont masquées, et log.anomaly. Il met à jour ctl.run et ctl.run_step et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui ne contient que des compteurs et des codes, lu ensuite par T3_NB_HRIS_COMPARE à travers les tables.
#
# **Limitations connues et points d'attention**
#
# La parité avec l'existant a été vérifiée sur des données fictives, pas sur des données réelles. Les dix-sept tables de correspondance sont vides tant que leur contenu validé n'est pas fourni, ce qui bloque l'enrichissement. Une table de correspondance contenant une clé en double duplique le salarié, comme dans l'existant, et ce cas est signalé par un contrôle.
#
# **Responsable et contact**
#
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

# ============= TRANSFORMATION =============
cell_log.start("TRANSFORM")
try:
    result = steps.run_transform(spark, tables, p_run_id)
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
