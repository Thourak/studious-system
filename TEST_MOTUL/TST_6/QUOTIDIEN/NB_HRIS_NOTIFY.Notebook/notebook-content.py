# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T6_NB_HRIS_NOTIFY
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_6/QUOTIDIEN/NB_HRIS_NOTIFY.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook prépare le message de fin de run destiné à l'équipe d'exploitation. Il lit le registre du run, ses étapes et ses contrôles, puis construit un objet de courriel et un corps HTML lisible dans les clients courants : un bandeau vert quand le run a réussi, rouge quand il a échoué, a été bloqué ou s'est arrêté avant sa fin, orange pour les cas à surveiller (publication partielle, publication désactivée ou en lecture seule, run ignoré). Chaque étape et chaque contrôle porte une pastille de la même couleur accompagnée d'un libellé écrit, afin que l'information ne repose jamais sur la couleur seule. Le notebook n'envoie rien lui-même : il renvoie au pipeline le sujet, la liste des destinataires et le HTML, et c'est l'activité Outlook d'un pipeline d'envoi dédié qui expédie le message.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par %run T6_NB_HRIS_LIB et lit les tables ctl.run, ctl.run_step et log.anomaly du Lakehouse T6_LH_HRIS, alimentées par les autres notebooks du run. Il lit dans la variable library T6_VL_HRIS les variables notify_enabled, notify_recipients et notify_subject_prefix. Il n'utilise aucun secret : l'adresse de destination est celle d'une liste de diffusion, pas celle d'une personne. Il est appelé par T6_PL_HRIS_MAIN, une fois T6_PL_HRIS_ORCHESTRATOR terminé, réussi ou non ; l'orchestrateur lui-même n'est pas modifié.
# # **Fonctionnement et logique de traitement**
# # Après la configuration et la préparation du Lakehouse, le notebook vérifie qu'il reçoit p_run_id ou p_since_utc, retrouve le run (le plus récent démarré depuis p_since_utc, avec cinq minutes de marge, quand p_run_id est vide), lit la ligne du run puis les étapes et les contrôles correspondants. Cette recherche évite de dépendre d'une propriété de sortie de l'activité de lancement, qui n'est pas documentée. Pour chaque étape, seule la dernière tentative est retenue. Il classe les statuts en succès, échec, attention ou incomplet, construit le HTML avec des styles en ligne et une mise en page en tableaux, seule technique fiable dans les clients de messagerie, puis vérifie que le message reste sous la limite de taille choisie ; si besoin, il réduit le nombre de lignes affichées. L'envoi n'est demandé que si la variable notify_enabled vaut true et si au moins une adresse valide est configurée ; sinon le notebook renvoie la raison de l'absence d'envoi. Tous les textes issus des tables sont échappés, tronqués et masqués par la fonction d'assainissement de la bibliothèque : le message ne contient ni donnée personnelle ni secret.
# # **Paramètres**
# # Le paramètre p_run_id reçoit l'identifiant du run HRIS à décrire. S'il est vide, le paramètre p_since_utc, reçu du pipeline sous la forme de l'heure de déclenchement, permet de retrouver le run le plus récent démarré depuis cet instant ; l'un des deux est obligatoire. Le paramètre p_test_schema isole les tables pendant les tests ; vide, il désigne les tables réelles.
# # **Sorties produites**
# # Aucune table n'est écrite, hors le journal du notebook dans log.notebook_log. Le notebook renvoie au pipeline un résultat JSON : status, send, reason, subject, recipients, html, run_status, category, html_bytes et run_id. Quand l'envoi n'est pas demandé, le champ html est vide pour garder la sortie courte.
# # **Limitations connues et points d'attention**
# # La taille maximale d'une valeur de sortie de notebook n'est pas documentée par Microsoft : le corps est donc limité à 40 000 octets, à valider sur un run réel. Le rendu des couleurs dans l'activité Outlook de Fabric n'est pas non plus documenté et doit être vérifié dans Outlook sur le web, sur le poste et sur mobile. Aucun enregistrement n'empêche encore un double envoi si le pipeline est rejoué avec le même run : la table de suivi des envois est prévue à l'étape suivante. Les valeurs réelles des colonnes outcome de log.anomaly autres que FAIL n'ont pas été observées dans Fabric.
# # **Responsable et contact**
# # Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import html
import json
import re
from datetime import timedelta
from typing import Any, Dict, List, Tuple

# Third-party libraries
from pyspark.sql import functions as F

# Azure libraries
# (aucune)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run T6_NB_HRIS_LIB

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
        required_keys=("lakehouse_name", "env_code"),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
    env_code = context_values["env_code"]
    notify_enabled = context_values["notify_enabled"] or "false"
    notify_recipients = context_values["notify_recipients"] or ""
    notify_subject_prefix = context_values["notify_subject_prefix"] or "[HRIS]"
    logger.info(f"✓ notify_enabled : {notify_enabled}")
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

p_run_id = ""            # identifiant du run HRIS (ctl.run) ; vide : utiliser p_since_utc
p_since_utc = ""         # @string(pipeline().TriggerTime) ; sans p_run_id, retient le run le plus récent démarré depuis cet instant
p_test_schema = ""       # tests uniquement : schéma d'isolement ; vide = tables réelles

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONSTANTES DU MESSAGE =============
# Statuts de ctl.run (hris.common) regroupés par catégorie d'affichage.
RUN_SUCCESS_STATUSES = ("SUCCEEDED", "PUBLISHED")
RUN_WARNING_STATUSES = ("PUBLISHED_PARTIAL", "NOT_PUBLISHED_NON_PRD", "NOT_PUBLISHED_DISABLED", "DRY_RUN",
                        "SKIPPED_CONCURRENT")
RUN_FAILURE_STATUSES = ("FAILED", "BLOCKED")

RUN_STATUS_LABELS = {
    "SUCCEEDED": "SUCCÈS", "PUBLISHED": "PUBLIÉ", "PUBLISHED_PARTIAL": "PUBLICATION PARTIELLE",
    "NOT_PUBLISHED_NON_PRD": "NON PUBLIÉ (HORS PRD)", "NOT_PUBLISHED_DISABLED": "NON PUBLIÉ (DÉSACTIVÉ)",
    "DRY_RUN": "LECTURE SEULE", "SKIPPED_CONCURRENT": "IGNORÉ (RUN CONCURRENT)", "FAILED": "ÉCHEC",
    "BLOCKED": "BLOQUÉ",
}
STEP_STATUS_LABELS = {"SUCCEEDED": "SUCCÈS", "FAILED": "ÉCHEC", "SKIPPED": "IGNORÉE", "STARTED": "INTERROMPUE"}

# Catégorie -> (couleur du texte, couleur de fond, libellé). Palette sobre à fort contraste.
CATEGORY_STYLES = {
    "SUCCESS": ("#05603a", "#d1fadf", "SUCCÈS"),
    "FAILURE": ("#b42318", "#fee4e2", "ÉCHEC"),
    "WARNING": ("#93370d", "#fef0c7", "ATTENTION"),
}
MAX_HTML_BYTES = 40000
ROW_LIMITS = ((40, 15), (10, 5), (0, 0))      # (étapes, contrôles) affichés, du plus complet au plus réduit
EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def classify_run_status(status: Any) -> str:
    """Catégorie d'affichage d'un statut de run : SUCCESS, WARNING ou FAILURE (statut inconnu ou intermédiaire = FAILURE)."""
    value = str(status or "").upper()
    if value in RUN_SUCCESS_STATUSES:
        return "SUCCESS"
    if value in RUN_WARNING_STATUSES:
        return "WARNING"
    return "FAILURE"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def classify_step_status(status: Any) -> str:
    """Catégorie d'affichage d'un statut d'étape : SUCCESS, WARNING (ignorée) ou FAILURE (échec ou interrompue)."""
    value = str(status or "").upper()
    if value == "SUCCEEDED":
        return "SUCCESS"
    if value == "SKIPPED":
        return "WARNING"
    return "FAILURE"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_text(value: Any, max_len: int = 160, mask_numbers: bool = True) -> str:
    """Texte issu d'une table, assaini par la bibliothèque, tronqué, puis échappé pour le HTML.

    Args:
        value: Valeur à afficher.
        max_len: Longueur maximale conservée.
        mask_numbers: True pour un texte libre (masque toute suite de 4 chiffres ou plus) ; False pour une valeur
            générée par le code (date, horodatage, identifiant de run, compteur), qui doit rester lisible.
    """
    if value is None:
        return ""
    return html.escape(hris.common.sanitize(str(value), max_len, mask_numbers), quote=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def badge_html(category: str, label: str) -> str:
    """Pastille colorée avec libellé écrit (la couleur n'est jamais le seul porteur de l'information)."""
    text_color, background, _ = CATEGORY_STYLES[category]
    return (f'<span style="display:inline-block;padding:2px 8px;border-radius:3px;font-weight:bold;'
            f'color:{text_color};background-color:{background};">{html.escape(label, quote=True)}</span>')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def latest_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dernière tentative de chaque étape, dans l'ordre de démarrage."""
    latest: Dict[str, Dict[str, Any]] = {}
    for step in steps:
        code = str(step.get("step_code") or "")
        current = latest.get(code)
        if current is None or int(step.get("attempt_no") or 0) >= int(current.get("attempt_no") or 0):
            latest[code] = step
    return sorted(latest.values(), key=lambda s: (str(s.get("started_at_utc") or ""), str(s.get("step_code") or "")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_subject(prefix: str, env: str, run: Dict[str, Any]) -> str:
    """Objet du courriel : préfixe, environnement, libellé du statut et date métier (une seule ligne, 120 caractères au plus)."""
    status = str(run.get("status") or "").upper()
    label = RUN_STATUS_LABELS.get(status, "INCOMPLET")
    text = f"{prefix} [{env}] {label} - {run.get('business_date') or 'date inconnue'}"
    return re.sub(r"[\r\n\t]+", " ", text)[:120]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_notification_html(run: Dict[str, Any], steps: List[Dict[str, Any]], anomalies: List[Dict[str, Any]],
                            env: str, max_steps: int, max_anomalies: int) -> str:
    """Corps HTML du message : bandeau de statut, informations du run, étapes, contrôles en écart."""
    category = classify_run_status(run.get("status"))
    text_color, background, _ = CATEGORY_STYLES[category]
    status = str(run.get("status") or "").upper()
    label = RUN_STATUS_LABELS.get(status, "INCOMPLET")
    cell = 'style="padding:6px 8px;border-bottom:1px solid #d0d5dd;font:13px Arial,Helvetica,sans-serif;color:#101828;"'
    head = 'style="padding:6px 8px;background-color:#f2f4f7;font:bold 12px Arial,Helvetica,sans-serif;color:#344054;text-align:left;"'

    def row(name: str, value: Any, free_text: bool = False) -> str:
        return (f"<tr><td {cell} width=\"35%\"><b>{html.escape(name)}</b></td>"
                f"<td {cell}>{safe_text(value, 200, mask_numbers=free_text)}</td></tr>")

    parts = [
        '<html><body style="margin:0;padding:0;background-color:#ffffff;">',
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="max-width:680px;"><tr><td>',
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td bgcolor="{background}" style="background-color:{background};color:{text_color};padding:14px 12px;'
        f'font:bold 18px Arial,Helvetica,sans-serif;">Run HRIS : {html.escape(label)}</td></tr></table>',
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">',
        row("Environnement", env), row("Date métier", run.get("business_date")), row("Mode", run.get("run_mode")),
        row("Statut du run", run.get("status")), row("Code d'erreur", run.get("error_code") or "aucun"),
        row("Résumé de l'erreur", run.get("error_summary") or "aucun", free_text=True),
        row("Début (UTC)", run.get("started_at_utc")), row("Fin (UTC)", run.get("ended_at_utc")),
        row("Identifiant du run", run.get("run_id")),
        "</table>",
    ]
    shown_steps = latest_steps(steps)[:max_steps] if max_steps else []
    if shown_steps:
        parts.append('<p style="font:bold 14px Arial,Helvetica,sans-serif;margin:16px 0 6px;">Étapes</p>')
        parts.append(f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
                     f'<td {head}>Étape</td><td {head}>Statut</td><td {head}>Lignes</td><td {head}>Message</td></tr>')
        for step in shown_steps:
            step_status = str(step.get("status") or "").upper()
            step_label = STEP_STATUS_LABELS.get(step_status, "INCONNU")
            badge = badge_html(classify_step_status(step_status), step_label)
            parts.append(f'<tr><td {cell}>{safe_text(step.get("step_code"), 60, mask_numbers=False)}</td>'
                         f'<td {cell}>{badge}</td>'
                         f'<td {cell}>{safe_text(step.get("rows_out"), 20, mask_numbers=False)}</td>'
                         f'<td {cell}>{safe_text(step.get("message"), 160)}</td></tr>')
        parts.append("</table>")
    gaps = [a for a in anomalies if str(a.get("outcome") or "").upper() == "FAIL"]
    if max_anomalies and gaps:
        parts.append('<p style="font:bold 14px Arial,Helvetica,sans-serif;margin:16px 0 6px;">Contrôles en écart</p>')
        parts.append(f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
                     f'<td {head}>Contrôle</td><td {head}>Gravité</td><td {head}>Message</td></tr>')
        for anomaly in gaps[:max_anomalies]:
            blocking = str(anomaly.get("severity") or "").upper() == "BLOCKING"
            badge = badge_html("FAILURE" if blocking else "WARNING", "BLOQUANT" if blocking else "À SURVEILLER")
            parts.append(f'<tr><td {cell}>{safe_text(anomaly.get("control_code"), 60, mask_numbers=False)}</td>'
                         f'<td {cell}>{badge}</td>'
                         f'<td {cell}>{safe_text(anomaly.get("message"), 160)}</td></tr>')
        parts.append("</table>")
        if len(gaps) > max_anomalies:
            parts.append(f'<p style="font:12px Arial,Helvetica,sans-serif;color:#475467;">{len(gaps) - max_anomalies} '
                         f'autre(s) contrôle(s) en écart non affiché(s) : voir log.anomaly.</p>')
    parts.append('<p style="font:12px Arial,Helvetica,sans-serif;color:#475467;margin-top:16px;">Message automatique '
                 "du hub HRIS. Détail dans ctl.run, ctl.run_step et log.anomaly. Aucune donnée personnelle n'est "
                 "reprise dans ce message.</p>")
    parts.append("</td></tr></table></body></html>")
    return "".join(parts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fit_html(run: Dict[str, Any], steps: List[Dict[str, Any]], anomalies: List[Dict[str, Any]], env: str) -> Tuple[str, int]:
    """Corps HTML le plus complet qui tient dans MAX_HTML_BYTES ; renvoie (html, nombre de niveaux de réduction appliqués)."""
    body = ""
    for level, (max_steps, max_anomalies) in enumerate(ROW_LIMITS):
        body = build_notification_html(run, steps, anomalies, env, max_steps, max_anomalies)
        if len(body.encode("utf-8")) <= MAX_HTML_BYTES:
            return body, level
    return body, len(ROW_LIMITS) - 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_recipients(raw: str) -> List[str]:
    """Adresses valides d'une liste séparée par « ; » ou « , » (les autres sont ignorées, sans être journalisées)."""
    candidates = [item.strip() for item in re.split(r"[;,]", raw or "")]
    return [item for item in candidates if item and EMAIL_PATTERN.match(item)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_steps(spark_session: Any, tables: Any, run_id: str) -> List[Dict[str, Any]]:
    """Étapes du run (ctl.run_step), lues par l'API DataFrame."""
    frame = (spark_session.table(tables("ctl.run_step"))
             .where(F.col("run_id") == hris.spark_io.check_run_id(run_id))
             .select("step_code", "attempt_no", "status", "rows_out", "message", "started_at_utc", "ended_at_utc"))
    return [row.asDict() for row in frame.collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_anomalies(spark_session: Any, tables: Any, run_id: str) -> List[Dict[str, Any]]:
    """Contrôles du run (log.anomaly), lus par l'API DataFrame."""
    frame = (spark_session.table(tables("log.anomaly"))
             .where(F.col("run_id") == hris.spark_io.check_run_id(run_id))
             .select("control_code", "severity", "outcome", "message"))
    return [row.asDict() for row in frame.collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_latest_run_id(spark_session: Any, tables: Any, since_utc: str) -> str:
    """Identifiant du run le plus récent démarré depuis since_utc, avec 5 minutes de marge (écart de déclenchement).

    Args:
        spark_session: Session Spark du notebook.
        tables: Noms complets des tables (tables_for_run).
        since_utc: Horodatage ISO 8601 UTC, par exemple @string(pipeline().TriggerTime).

    Returns:
        L'identifiant du run.

    Raises:
        HrisError: RUN_NOT_FOUND si aucun run n'a démarré depuis cet instant.
    """
    since = hris.common.to_utc_naive(hris.common.parse_trigger_time(since_utc)) - timedelta(minutes=5)
    frame = (spark_session.table(tables("ctl.run"))
             .where(F.col("started_at_utc") >= F.lit(since))
             .orderBy(F.col("started_at_utc").desc())
             .select("run_id")
             .limit(1))
    rows = frame.collect()
    if not rows:
        raise hris.common.HrisError("RUN_NOT_FOUND", "aucun run démarré depuis l'instant donné")
    return rows[0].asDict()["run_id"]

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

# ============= PRÉPARATION DU MESSAGE =============
try:
    if not p_run_id and not p_since_utc:
        raise hris.common.HrisError("PARAM_MISSING", "p_run_id ou p_since_utc")
    resolved_run_id = p_run_id or find_latest_run_id(spark, tables, p_since_utc)
    run = hris.spark_io.get_run(spark, tables, resolved_run_id)
    if run is None:
        raise hris.common.HrisError("RUN_NOT_FOUND", "aucune ligne ctl.run pour ce run")
    steps = read_steps(spark, tables, resolved_run_id)
    anomalies = read_anomalies(spark, tables, resolved_run_id)
    recipients = parse_recipients(notify_recipients)
    category = classify_run_status(run.get("status"))
    subject = build_subject(notify_subject_prefix, env_code, run)
    if not hris.common.as_bool(notify_enabled):
        send, reason, body, reduction = False, "NOTIFY_DISABLED", "", 0
    elif not recipients:
        send, reason, body, reduction = False, "NO_VALID_RECIPIENT", "", 0
    else:
        body, reduction = fit_html(run, steps, anomalies, env_code)
        send, reason = True, "OK"
    result = {
        "status": "OK", "send": send, "reason": reason, "subject": subject, "recipients": ";".join(recipients),
        "html": body, "run_status": str(run.get("status") or ""), "category": category,
        "html_bytes": len(body.encode("utf-8")), "run_id": resolved_run_id,
    }
    logger.info(f"✓ Message préparé : send={send}, raison={reason}, catégorie={category}, "
                f"étapes={len(latest_steps(steps))}, réduction={reduction}, destinataires={len(recipients)}")
except Exception as e:
    error_message = f"Erreur lors de la préparation du message: {e}"
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
notebookutils.notebook.exit(hris.common.json_exit(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
