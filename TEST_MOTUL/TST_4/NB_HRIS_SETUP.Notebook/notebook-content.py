# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T3_NB_HRIS_SETUP. C'est le nom affiché dans Microsoft Fabric pour le notebook qui installe le socle du Lakehouse du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_SETUP.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Il n'est pas appelé par le pipeline : on le lance à la main lors de l'installation et après toute évolution du modèle de données.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/NB_HRIS_SETUP.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook prépare le Lakehouse dont dépendent tous les autres notebooks du hub. Il crée, s'ils n'existent pas, les six schémas cfg, stg, ref, pub, ctl et log, puis les quarante-trois tables du modèle de données, dont la table de journal des cellules log.cell_execution ajoutée par cette version. Il insère ensuite, uniquement si elles sont absentes, la ligne de paramétrage du processus et la ligne de verrou à l'état libéré. Une table existante n'est jamais supprimée, vidée, remplacée ni modifiée : son schéma est comparé à la définition attendue et tout écart est signalé comme incompatible, sans correction automatique. Les dix-sept tables de correspondance sont créées vides, car leur contenu validé n'est pas encore disponible.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise le modèle de données, les fonctions d'écriture Delta, les étapes du pipeline pour les graines et le journal des cellules. Il agit uniquement sur le Lakehouse désigné par le paramètre p_lakehouse_name dans le workspace courant, dont le nom réel est lu dans le contexte d'exécution de Fabric. Il lit cfg.process et ctl.process_lock pour décider des graines. Aucun secret n'est lu.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et applique les réglages de session Spark. La cellule de socle crée les schémas puis, table par table, crée ce qui manque ou contrôle la conformité de ce qui existe. Elle insère ensuite les graines absentes et calcule un résultat : le statut vaut INCOMPATIBLE dès qu'une table existante diffère de sa définition, OK sinon. Le résultat et l'action menée sur chaque table sont affichés. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_test_schema, vide par défaut, redirige toutes les tables vers un schéma de test isolé ; il reste vide pour installer les tables réelles. Le paramètre p_lakehouse_name donne le nom du Lakehouse cible, en pratique la variable lakehouse_name de la bibliothèque de variables VL_HRIS ; il est obligatoire puisqu'aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
# # **Sorties produites**
# # Le notebook crée les schémas et les tables manquants et les deux graines, puis ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui donne le statut, le nombre de tables créées et existantes, la liste des tables incompatibles et l'insertion éventuelle des graines.
# # **Limitations connues et points d'attention**
# # Le contrôle de conformité compare les noms et les types de colonnes, pas les contraintes ni les commentaires. Une table incompatible doit être traitée à la main, après validation, car le notebook ne la corrige jamais. Lors de la première exécution qui suit cette version, la table log.cell_execution est créée et apparaît dans le compteur des tables créées.
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
from hris import spark_io
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

p_test_schema = ""               # tests uniquement : schéma d'isolement (ex. "hris_tst") ; vide = tables réelles
p_lakehouse_name = ""            # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"      # true = trace aussi les cellules réussies ; false = échecs seulement

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
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "operation_id": cell_log.base["operation_id"],
                      "log_cell_success": cell_log.log_success}, ensure_ascii=False))
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

# ============= SOCLE : SCHÉMAS, TABLES, GRAINES =============
cell_log.start("SETUP")
try:
    report = spark_io.ensure_schemas_and_tables(spark, tables)
    seeded = steps.seed(spark, tables)
    incompatible = [entry["table"] for entry in report if entry["detail"].startswith("INCOMPATIBLE")]
    result = {"status": "INCOMPATIBLE" if incompatible else "OK",
              "created": sum(entry["action"] == "CREATED" for entry in report),
              "existing": sum(entry["action"] == "EXISTS" for entry in report), "incompatible": incompatible,
              "seed_process_inserted": seeded["process"], "seed_lock_inserted": seeded["lock"],
              "mapping_tables": "créées vides (D-REF-01 bloquante)"}
    print(json.dumps(result, ensure_ascii=False))
    for entry in report:
        print(entry["action"], entry["table"], entry["detail"])
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
