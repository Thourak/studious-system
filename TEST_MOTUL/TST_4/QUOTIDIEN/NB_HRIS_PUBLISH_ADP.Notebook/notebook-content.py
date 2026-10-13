# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T3_NB_HRIS_PUBLISH_ADP. C'est le nom affiché dans Microsoft Fabric pour le notebook de publication vers l'outil de paie ADP du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_PUBLISH_ADP.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T3_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Publish, après une promotion réussie, sans aucune reprise automatique.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/QUOTIDIEN/NB_HRIS_PUBLISH_ADP.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook décide si les salariés à publier peuvent être envoyés à l'outil de paie ADP et, dans cette version, n'envoie jamais rien. Le garde-fou lit le nom réel du workspace dans le contexte de ce notebook, au moment de la publication, et l'écrit dans un journal avant toute action. Si ce nom ne contient pas la chaîne PRD, abréviation de production, les éléments sont marqués comme non publiés hors production et aucun appel n'a lieu. Si le nom ne peut pas être lu, les éléments restent en attente et la décision pourra être réévaluée. Si le nom contient PRD, la condition d'environnement est remplie mais ne suffit pas : l'activation reste verrouillée dans le code tant que le contrat final de publication n'est pas validé. Avant tout envoi, l'intention est enregistrée durablement, ce qui garantit qu'un envoi incertain n'est jamais répété automatiquement.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules d'étapes, de garde-fou, de publication, de transport et de journal des cellules. Le contexte d'exécution de Fabric, relu au moment même de la décision, fournit le nom réel du workspace. Il lit ctl.run, la liste à publier pub.adp_outbox et stg.ts_employee_adp produits par les étapes précédentes. Il écrit le journal du garde-fou log.adp_gate_decision et, selon le cas, pub.adp_outbox, pub.adp_operation, pub.adp_attempt et log.adp_publication_log, met à jour ctl.run, consigne l'étape dans ctl.run_step et ajoute ses traces dans log.cell_execution. Les secrets de l'outil de paie, dont le coffre est désigné par p_kv_url_adp, ne sont lus que si le garde-fou et toutes les validations d'activation sont levés, ce qui est impossible dans cette version.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et applique les réglages de session Spark. Trois cellules définissent ensuite les fonctions transmises à la publication : la lecture d'un secret, la relecture du contexte réel et la planification des opérations d'un salarié. La cellule de publication construit la fabrique du transport, qui refuse de s'exécuter tant que l'activation n'est pas approuvée, puis lance la publication sous une sentinelle réseau. Cette sentinelle intercepte, enregistre et bloque toute connexion vers les serveurs de l'outil de paie et le service public d'adresses pendant toute l'étape. La cellule retire enfin le nom du workspace du résumé avant de l'afficher. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL par défaut, INIT_REFERENCE pour le premier chargement, sans publication, ou RETRY_PUBLICATION pour republier un run déjà promu. Le paramètre p_kv_url_adp donne l'adresse du coffre des secrets de l'outil de paie, vide hors production. Le paramètre p_max_attempts, qui vaut 3, fixe le nombre maximal de tentatives par opération. Le paramètre p_http_timeout_seconds, qui vaut 60, fixe le délai des appels. Le paramètre p_pause_seconds, qui vaut 1, fixe la pause entre deux envois, comme dans l'existant. Le paramètre p_test_schema redirige toutes les tables vers un schéma de test et reste vide en exploitation. Le paramètre p_lakehouse_name donne le Lakehouse cible et il est obligatoire, car aucun Lakehouse par défaut n'est attaché. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés. Aucun paramètre ne permet de forcer une publication.
# # **Sorties produites**
# # Le notebook écrit log.adp_gate_decision et, selon la décision, met à jour pub.adp_outbox ; les tables d'opérations et de tentatives ne reçoivent aucune ligne tant que l'activation est verrouillée. Il met à jour ctl.run et ctl.run_step et ajoute ses traces dans log.cell_execution. Sa valeur de sortie est un texte structuré qui contient le statut de la publication, le résumé sans nom de workspace et la preuve de non-publication : décision du garde-fou, nombre d'appels à la fabrique du transport et nombre de tentatives réseau interceptées.
# # **Limitations connues et points d'attention**
# # La règle du garde-fou est littérale et sensible à la casse : des noms comme PREPRD ou NOTPRD la satisfont, alors que prd en minuscules ne la satisfait pas. Ces effets sont à trancher avant toute activation. La preuve de non-publication est applicative et ne couvre pas un trafic émis hors de ce processus. Les listes de statuts de l'outil de paie restent provisoires tant que le contrat final n'est pas fourni.
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
import datetime as dt
import json
from typing import Any, Mapping

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB)
from hris import steps
from hris.adp_port import EmployeePlanner, TransportReader
from hris.adp_spark import RealAdpTransportFactory
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
p_run_mode = "NORMAL"            # NORMAL | INIT_REFERENCE | RETRY_PUBLICATION
p_kv_url_adp = ""                # VL_HRIS.kv_url_adp : coffre des secrets ADP (vide hors PRD, jamais le secret)
p_max_attempts = "3"             # VL_HRIS.adp_max_attempts : tentatives maximales par opération (D-PUB-02, à confirmer)
p_http_timeout_seconds = "60"    # délai des appels HTTP ADP, en secondes
p_pause_seconds = "1"            # pause entre deux envois, parité R-ADP-006 (D-ADP-10)
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

def get_adp_secret(kv_url: str, name: str) -> str:
    """Lit un secret ADP dans le coffre ; appelée seulement si le garde-fou et l'activation sont levés."""
    return notebookutils.credentials.getSecret(kv_url, name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_runtime_context() -> Mapping[str, Any]:
    """Relit le contexte RÉEL de ce notebook au moment de la décision du garde-fou (jamais une copie figée)."""
    return notebookutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plan_employee(row: Mapping[str, Any], execution: Any) -> Any:
    """Planifie les opérations ADP d'un salarié (portage de la Function ADP), lecture par le transport du run."""
    return EmployeePlanner(TransportReader(execution.transport), now=dt.datetime.now)(row, execution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= PUBLICATION ADP (GARDE-FOU, ACTIVATION VERROUILLÉE) =============
cell_log.start("PUBLISH")
try:
    factory = RealAdpTransportFactory(p_kv_url_adp, float(p_http_timeout_seconds), get_secret=get_adp_secret)
    result = steps.run_publish(spark, tables, p_run_id, p_run_mode,
                               context_provider=read_runtime_context,   # contexte RÉEL de ce notebook
                               transport_factory=factory, planner=plan_employee, max_attempts=int(p_max_attempts),
                               pause_seconds=float(p_pause_seconds))
    if "summary" in result:
        result["summary"] = {k: v for k, v in result["summary"].items() if k != "workspace_name"}
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
