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
# T5_NB_HRIS_PUBLISH_ADP
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_PUBLISH_ADP.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook transmet à l'outil de paie ADP les salariés nouveaux ou modifiés du run, avec la même logique que l'ancienne fonction Azure de publication : décision entre embauche et mise à jour, contrôle du numéro de sécurité sociale, lecture de chaque valeur en place dans ADP et envoi seulement de ce qui diffère. La publication n'a lieu que si le nom réel du workspace d'exécution contient PRD, lu au moment de la décision. Deux interrupteurs de la variable library permettent en plus d'activer la publication par étapes : désactivée, lecture seule, puis lecture et écriture. Un refus de publication ne bloque jamais les traitements du Lakehouse.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de garde, de transport, d'opérations, de logique ADP, de publication et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du coffre ADP, l'adresse d'ADP, l'adresse de l'interface des communes, les deux interrupteurs, le nombre de tentatives, le délai des appels et la pause entre deux salariés. Il lit dans le coffre les secrets client-id-adp, client-secret-adp, MOTUL-SA-cer et motulsa-auth-key, seulement si la garde et un interrupteur l'autorisent. Il lit ctl.run, pub.adp_outbox et stg.ts_employee_adp. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Publish, après une promotion réussie.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, la publication enregistre d'abord la décision de la garde. Hors production, les salariés à transmettre passent au statut NOT_PUSHED_NON_PRD sans aucun appel. En production avec les deux interrupteurs à false, ils passent au statut NOT_PUSHED_DISABLED, sans lecture de secret. En lecture seule, le notebook lit ADP et planifie chaque opération sans l'envoyer, dans log.adp_dry_run, pour la comparer au journal de l'existant pendant la non-régression. En lecture et écriture, il enregistre chaque opération de façon durable avant de l'envoyer, classe chaque réponse et ne renvoie jamais automatiquement une opération dont le résultat est incertain.
# 
# **Paramètres**
# 
# Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL pour publier le run en cours ou RETRY_PUBLICATION pour republier les éléments en échec d'un run déjà promu, sans recalcul. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric. Aucun paramètre ne permet de forcer une publication.
# 
# **Sorties produites**
# 
# Le notebook écrit log.adp_gate_decision, pub.adp_outbox, pub.adp_operation, pub.adp_attempt, log.adp_publication_log, log.adp_dry_run, ctl.run, ctl.run_step et son journal log.notebook_log. Les tables de publication contiennent des données personnelles et restent en accès restreint. Il renvoie au pipeline un résultat court de compteurs.
# 
# **Limitations connues et points d'attention**
# 
# Aucun workspace autre que celui de production ne doit contenir la suite de lettres PRD dans son nom, par exemple PREPRD. L'activation de l'écriture se fait par un commit tracé du jeu de valeurs PRD, après la non-régression et la bascule. Une réponse 429 d'ADP n'est pas reprise automatiquement : la reprise passe par le mode RETRY_PUBLICATION.
# 
# **Responsable et contact**
# 
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import datetime as dt
import json
from typing import Any, Mapping

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
        required_keys=("adp_base_url", "insee_api_url", "adp_max_attempts", "adp_http_timeout_seconds", "adp_pause_seconds"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    adp_base_url = context_values["adp_base_url"]
    insee_api_url = context_values["insee_api_url"]
    adp_max_attempts = context_values["adp_max_attempts"]
    adp_http_timeout_seconds = context_values["adp_http_timeout_seconds"]
    adp_pause_seconds = context_values["adp_pause_seconds"]
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

p_run_id = ""            # @pipeline().RunId (obligatoire)
p_run_mode = "NORMAL"    # NORMAL | RETRY_PUBLICATION (identique au mode enregistré)
p_test_schema = ""       # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""           # eventpath Fabric (ex. /p_run_id=.../p_run_mode=RETRY_PUBLICATION)

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
        p_run_id = parsed_path.get("p_run_id", p_run_id)
        p_run_mode = parsed_path.get("p_run_mode", p_run_mode)
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

def get_adp_secret(vault_url: str, secret_name: str) -> str:
    """Lit un secret ADP dans le coffre ; appelée seulement si la garde et un interrupteur l'autorisent."""
    return get_parameter(secret_name, vault_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_runtime_context() -> Mapping[str, Any]:
    """Relit le contexte RÉEL de ce notebook au moment de la décision de la garde (jamais une copie figée)."""
    return notebookutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plan_employee(row: Mapping[str, Any], execution: Any) -> Any:
    """Planifie les opérations ADP d'un salarié (logique de la Function de l'existant), lectures par le transport."""
    reader = hris.adp_port.TransportReader(execution.transport, insee_api_url)
    return hris.adp_port.EmployeePlanner(reader, now=dt.datetime.now)(row, execution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= PUBLICATION ADP (GARDE, INTERRUPTEURS, INTENTION DURABLE) =============
try:
    if not p_run_id:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id")
    activation = hris.notebook_context.activation_of(context_values)
    logger.info(f"✓ Interrupteurs ADP : {activation.mode}")
    factory = hris.adp_spark.RealAdpTransportFactory(
        context_values.get("key_vault_url_adp") or "", float(adp_http_timeout_seconds), get_secret=get_adp_secret,
        activation=activation, base_url=adp_base_url)
    result = hris.steps.run_publish(spark, tables, p_run_id, p_run_mode, context_provider=read_runtime_context,
                                    transport_factory=factory, planner=plan_employee,
                                    max_attempts=int(adp_max_attempts), pause_seconds=float(adp_pause_seconds),
                                    activation=activation)
    if "summary" in result:
        result["summary"] = {k: v for k, v in result["summary"].items() if k != "workspace_name"}
    logger.info(f"✓ Publication : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de la publication ADP: {e}"
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
