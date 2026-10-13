# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T4_NB_HRIS_REMUNERATION. C'est le nom affiché dans Microsoft Fabric pour le notebook du flux de rémunération du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_REMUNERATION.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Le pipeline T4_PL_HRIS_ORCHESTRATOR l'appelle dans l'activité Remuneration, en parallèle de l'acquisition, comme l'existant lançait son flux de rémunération en parallèle du flux principal. On peut aussi le lancer à la main en mode VALIDATE.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/QUOTIDIEN/NB_HRIS_REMUNERATION.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook reprend le second flux de l'existant, le pipeline Synapse Pipeline_SIRH-SFTP, sans aucun fichier. L'existant lisait chaque jour trois exports CSV de TalentSoft sur le SFTP (salaire de base annuel, bonus en pourcentage et bonus en montant cible), calculait les changements par rapport à la veille et redéposait sur le SFTP deux fichiers CSV que TalentSoft importait dans son historique de rémunération. Ici, la rémunération est lue par l'API d'exports de TalentSoft (jeu Compensation), l'instantané du jour est écrit en table, les changements sont calculés par les règles des deux vues SQL de l'existant reproduites à l'identique, particularités comprises, et les lignes à importer sont écrites en table. Elles sont ensuite envoyées par l'API d'imports de TalentSoft, sous les mêmes types d'import et avec les mêmes colonnes que les fichiers de l'existant ; le contenu exigé par cette API est construit en mémoire et n'est écrit nulle part. La veille n'est plus l'archive précédente, mais la référence du dernier envoi accepté, ce qui évite de réémettre deux fois les mêmes changements ou d'en perdre après un échec.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise ses modules hris.remuneration, hris.ts_acquire (client de l'API TalentSoft et jeton), hris.adp_gate (règle d'environnement), l'écriture Delta et le journal des cellules. Il lit l'identifiant et le secret de l'application TalentSoft dans le coffre désigné par p_kv_url_ts. Il lit le registre ctl.run, la table de correspondance cfg.ts_remuneration_mapping et la référence ref.ts_remuneration_snapshot ; il écrit stg.ts_remuneration_snapshot, pub.ts_remuneration_change, log.ts_remuneration_import, ref.ts_remuneration_snapshot, ctl.run_step, log.anomaly et log.cell_execution.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables et ouvre le journal des cellules. Le pré-contrôle lit le run, les secrets TalentSoft et vérifie que le serveur est joignable. En mode RUN, la cellule de rémunération est sautée en reprise de publication ou si le flux est désactivé ; sinon elle lance un export Compensation, attend sa fin, lit son contenu en mémoire, construit l'instantané, calcule les changements contre la référence, puis, pour chacun des deux types d'import, décide : premier run (la référence est initialisée, rien n'est envoyé), aucun changement, workspace non PRD (calculé, non envoyé), envoi désactivé, ou envoi par l'API d'imports suivi jusqu'à son statut final. La référence d'un type n'avance que si ses changements ont été remis ou étaient sans objet. Si un envoi échoue, tout est journalisé puis le notebook échoue, ce qui fait échouer le pipeline comme dans l'existant. En mode VALIDATE, rien n'est écrit : le notebook compare la structure du contenu qu'il produirait, colonnes, codes et formes des valeurs, à celle du dernier import de l'existant relu par l'API, pour confirmer la table de correspondance avant d'activer l'envoi.
# # **Paramètres**
# # Le paramètre p_mode vaut RUN par défaut ; VALIDATE compare la structure sans rien écrire. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline ; il est obligatoire en RUN. Le paramètre p_kv_url_ts donne l'adresse du coffre des identifiants TalentSoft, la variable kv_url_ts de VL_HRIS. Le paramètre p_ts_base_url donne le tenant TalentSoft de production, seule valeur acceptée. Le paramètre p_flow_enabled, issu de la variable remuneration_enabled, vaut true ; à false, le flux est sauté. Le paramètre p_import_enabled, issu de la variable ts_remuneration_import_enabled, vaut false par défaut : les changements sont calculés et conservés, mais l'envoi n'a lieu que s'il vaut true et que le nom du workspace contient PRD. Le paramètre p_only_active_employees vaut false et restreint l'export aux salariés actifs s'il vaut true. Les paramètres p_test_schema, p_lakehouse_name et p_log_cell_success ont le même rôle que dans les autres notebooks.
# # **Sorties produites**
# # Le notebook écrit l'instantané du run, les lignes à importer, une ligne de suivi par type d'import (statut, nombre de lignes, empreinte du contenu, identifiant et statut de l'import TalentSoft, avance de la référence) et la référence mise à jour. Il consigne l'étape REMUNERATION dans ctl.run_step avec les compteurs de l'existant (lignes source et lignes émises pour le salaire et les bonus) et écrit le contrôle REMUNERATION_FLOW dans log.anomaly. Aucune valeur de matricule ni de montant n'est affichée ni journalisée ; les trois tables de données sont classées données personnelles.
# # **Limitations connues et points d'attention**
# # La correspondance entre les colonnes du jeu Compensation et celles des exports de l'existant, notamment les valeurs des colonnes type et calculation_method, la date d'effet du salaire de base et le format des dates, repose sur des hypothèses enregistrées dans cfg.ts_remuneration_mapping ; le mode VALIDATE permet de les confirmer ou de les corriger avant d'activer l'envoi. Les champs du formulaire de l'API d'imports ne sont pas documentés publiquement : le nom du contenu reprend celui des fichiers de l'existant, qui porte le type, la méthode et la culture. Le format exact du contenu d'export (JSON, CSV, classeur ou archive) est détecté à la lecture. Un jour sans changement n'envoie rien, alors que l'existant déposait un fichier réduit à l'en-tête. Tant que le coffre de secrets n'est pas lisible depuis le workspace, le flux échoue à la lecture des secrets ; la variable remuneration_enabled permet de le désactiver dans ce cas.
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
import logging
import socket
import uuid
from urllib.parse import urlparse

# Third-party libraries
import requests
from requests.adapters import HTTPAdapter

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB)
from hris import remuneration
from hris import spark_io
from hris import ts_acquire
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import (HrisError, SecretValue, TS_SECRET_CLIENT_ID, TS_SECRET_CLIENT_SECRET, as_bool, json_exit,
                         safe_error, sanitize)
from hris.model import tables_for_notebook
from hris.transform import ensure_session_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_mode = "RUN"                                   # RUN : flux complet ; VALIDATE : compare la structure au dernier import de l'existant, sans écrire
p_run_id = ""                                    # @pipeline().RunId (obligatoire en RUN)
p_kv_url_ts = ""                                 # VL_HRIS.kv_url_ts : coffre des identifiants TalentSoft (jamais le secret)
p_ts_base_url = "https://motul.talent-soft.com"  # VL_HRIS.ts_base_url : tenant TalentSoft de PRODUCTION (seule valeur acceptée)
p_flow_enabled = "true"                          # VL_HRIS.remuneration_enabled : false = flux sauté
p_import_enabled = "false"                       # VL_HRIS.ts_remuneration_import_enabled : envoi seulement si true ET workspace « PRD »
p_only_active_employees = "false"                # true : export limité aux salariés actifs
p_test_schema = ""                               # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""                            # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"                      # VL_HRIS.log_cell_success : true = trace aussi les cellules réussies

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
    for noisy_logger in ("urllib3", "requests", "py4j"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)            # aucune URL dans les sorties
    ensure_session_settings(spark)
    mode = str(p_mode).strip().upper()
    if mode not in ("RUN", "VALIDATE"):
        raise HrisError("PARAM_INVALID", "p_mode doit valoir RUN ou VALIDATE")
    if mode == "RUN" and not p_run_id:
        raise HrisError("PARAM_MISSING", "p_run_id")
    trace_id = p_run_id or ("REMVAL-" + uuid.uuid4().hex[:12])
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "mode": mode, "run_id": p_run_id,
                      "import_enabled": as_bool(p_import_enabled), "flow_enabled": as_bool(p_flow_enabled)},
                     ensure_ascii=False))
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

def read_ts_secrets(kv_url: str) -> dict:
    """Identifiant et secret de l'application TalentSoft, lus dans le coffre ; jamais affichés."""
    secrets = {}
    for name in (TS_SECRET_CLIENT_ID, TS_SECRET_CLIENT_SECRET):
        value = notebookutils.credentials.getSecret(kv_url, name)
        if not value:
            raise HrisError("TS_SECRET_EMPTY", name)
        secrets[name] = SecretValue(value)
    return secrets


def build_http_session(pool_size: int) -> requests.Session:
    """Session HTTP : aucune reprise automatique, pool à la taille de la concurrence."""
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0))
    return session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= PRÉ-CONTRÔLE : RUN, SECRETS, RÉSEAU =============
cell_log.start("PREFLIGHT")
try:
    run = spark_io.get_run(spark, tables, p_run_id) if p_run_id else None
    if mode == "RUN" and run is None:
        raise HrisError("RUN_UNKNOWN", "RunStart doit précéder le flux de rémunération")
    preflight = {"source": None, "secrets": None, "network": None}
    base_url = ts_acquire.assert_prd_source(p_ts_base_url)
    preflight["source"] = "PRD_OK"
    secrets = None
    if as_bool(p_flow_enabled):
        try:
            secrets = read_ts_secrets(p_kv_url_ts)
            preflight["secrets"] = "RESOLVED"
        except Exception as exc:  # aucune valeur de secret : la lecture a échoué ; URL et jetons masqués
            preflight["secrets"] = "UNAVAILABLE:" + type(exc).__name__
            preflight["secrets_error_head"] = sanitize(" ".join(str(exc).split()), 600, mask_numbers=False)
        try:
            with socket.create_connection((urlparse(base_url).hostname, 443), timeout=15):
                pass
            preflight["network"] = "TCP_443_OK"
        except Exception as exc:
            preflight["network"] = "UNREACHABLE:" + type(exc).__name__
    print(json.dumps({"preflight": preflight}, ensure_ascii=False))
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

# ============= FLUX RÉMUNÉRATION (EXPORT API -> TABLES -> IMPORT API, AUCUN FICHIER) =============
cell_log.start("REMUNERATION")
try:
    result = {"run_id": p_run_id or None, "mode": mode, "preflight": preflight}
    if not as_bool(p_flow_enabled):
        result["status"] = "SKIPPED_DISABLED"
    elif run is not None and run["run_mode"] == "RETRY_PUBLICATION":
        result["status"] = "SKIPPED_RETRY_PUBLICATION"
    else:
        step = spark_io.StepLogger(spark, tables, trace_id, "REMUNERATION" if mode == "RUN" else "REMUNERATION_VALIDATE")
        try:
            if secrets is None:
                raise HrisError("TS_SECRETS_UNAVAILABLE", preflight["secrets"])
            client = ts_acquire.TsClient(p_ts_base_url, secrets[TS_SECRET_CLIENT_ID], secrets[TS_SECRET_CLIENT_SECRET],
                                         session_factory=lambda: build_http_session(4),
                                         stats=ts_acquire.AcquisitionStats(),
                                         network_errors=(requests.exceptions.RequestException, OSError), max_concurrency=4)
            try:
                client.get_token()
                api = remuneration.TalentSoftApi(client)
                store = remuneration.SparkRemunerationStore(spark, tables)
                if mode == "RUN":
                    try:
                        outcome = remuneration.run_remuneration(
                            api, store, lambda: notebookutils.runtime.context, run_id=p_run_id,
                            business_date=run["business_date"], import_enabled=as_bool(p_import_enabled),
                            only_active_employees=as_bool(p_only_active_employees))
                    except remuneration.RemunerationFailed as failed:
                        outcome = failed.result
                        spark_io.write_anomalies(spark, tables, p_run_id, [{
                            "control_code": "REMUNERATION_FLOW", "severity": "WARNING", "metric_value": None,
                            "threshold_value": None, "outcome": "FAIL",
                            "message": "import TalentSoft en échec : " + json.dumps(outcome["imports"])}],
                            ["REMUNERATION_FLOW"])
                        raise
                    spark_io.write_anomalies(spark, tables, p_run_id, [{
                        "control_code": "REMUNERATION_FLOW", "severity": "WARNING", "metric_value": None,
                        "threshold_value": None, "outcome": "PASS", "message": json.dumps(outcome["imports"])}],
                        ["REMUNERATION_FLOW"])
                    step.succeeded(rows_out=sum(outcome["summary"][k] for k in ("baseSalary_nb_inserts", "bonus_nb_inserts")),
                                   message=remuneration.step_message(outcome), counters_only=True)
                    result.update(status=outcome["status"], summary=outcome["summary"], imports=outcome["imports"],
                                  first_run=outcome["first_run"], gate=outcome["gate"])
                else:   # VALIDATE : aucune écriture ; structure produite vs dernier import de l'existant
                    snapshot, stats = remuneration.build_snapshot(
                        remuneration.extract_compensation(api, only_active_employees=as_bool(p_only_active_employees)),
                        store.mapping())
                    produced = {
                        remuneration.IMPORT_SALARY: remuneration.base_salary_changes(snapshot[remuneration.BASE_SALARY], []),
                        remuneration.IMPORT_BONUS: remuneration.bonus_changes(
                            snapshot[remuneration.BONUS_PERCENT], [], snapshot[remuneration.BONUS_TARGET_VALUE], [],
                            snapshot[remuneration.BASE_SALARY])}
                    latest = api.latest_imports()
                    report = {}
                    for import_type, rows in produced.items():
                        mine = remuneration.content_structure(remuneration.import_content(import_type, rows))
                        legacy = (remuneration.content_structure(api.import_file(latest[import_type]["id"]))
                                  if import_type in latest else None)
                        report[import_type] = {"produced": mine, "legacy": legacy, "legacy_import_date": (
                            latest.get(import_type, {}).get("when", "")[:10] or None),
                            "gaps": remuneration.compare_structures(mine, legacy) if legacy else ["aucun import de l'existant trouvé"]}
                        print(f"== {import_type} : {len(report[import_type]['gaps'])} écart(s) de structure")
                        for gap in report[import_type]["gaps"]:
                            print("   -", gap)
                        print("   produit :", json.dumps(mine, ensure_ascii=False))
                        print("   existant :", json.dumps(legacy, ensure_ascii=False))
                    step.succeeded(counters_only=True, message=json.dumps(
                        {t: {"gaps": len(r["gaps"])} for t, r in report.items()}, ensure_ascii=False))
                    result.update(status="VALIDATED_STRUCTURE", snapshot_stats=stats,
                                  gaps={t: len(r["gaps"]) for t, r in report.items()})
            finally:
                client.close()
        except Exception as exc:
            step.failed(exc)
            raise
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
