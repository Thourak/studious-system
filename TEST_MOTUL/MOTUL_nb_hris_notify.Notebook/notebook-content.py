# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# **Nom de l'objet**
#
# MOTUL_nb_hris_notify, nom repris du champ displayName du fichier .platform.
#
# **Type d'objet**
#
# Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark, appelé par MOTUL_PL_HRIS_Orchestrateur au début de l'exécution pour vérifier les destinataires, puis à la fin ou en cas d'échec pour informer.
#
# **Chemin dans le dépôt**
#
# TEST_MOTUL/MOTUL_nb_hris_notify.Notebook/notebook-content.py.
#
# **Description fonctionnelle**
#
# Ce notebook informe la liste de distribution du résultat d'une exécution par un courriel HTML, distinct pour un succès et pour un échec. Il remplace les activités SendEmailResume, SendEmailErrorNotif et Web1, les trois Logic Apps d'envoi et la lecture du fichier emails.txt. Contrairement à l'existant, le courriel d'échec transporte le diagnostic réel journalisé dans ctl_execution_etape, et les destinataires sont résolus dès le début de l'exécution, de sorte qu'un échec précoce est lui aussi notifié. Le corps ne contient aucune donnée RH nominative : volumes, statuts, types de rejet et identifiant d'exécution uniquement.
#
# **Dépendances**
#
# Le notebook importe MOTUL_nb_hris_lib. Il lit ctl_execution, ctl_execution_etape et gld_resume, et rjt_ts_employe seulement si une pièce jointe de rejets est explicitement demandée. Il lit dans le coffre de l'environnement le secret dont le nom est porté par env_notif_liste_distribution, qui contient l'adresse de la liste de distribution. Il envoie le courriel par l'API Microsoft Graph sendMail depuis la boîte technique désignée par env_notif_bal_technique. L'identité d'exécution est choisie par env_notif_auth_mode : identite_workspace pour l'identité du workspace Fabric, ou certificat pour une application Entra ID dont le certificat est lu dans le secret nommé par env_notif_cert_secret, avec env_tenant_id et env_notif_client_id. Le lien du rapport est porté par env_powerbi_rapport_url.
#
# **Fonctionnement et logique de traitement**
#
# L'action verifier lit le secret des destinataires et contrôle les variables d'envoi, sans rien envoyer : le pipeline l'appelle avant toute étape susceptible d'échouer. L'action envoyer résout d'abord les destinataires, puis vérifie dans ctl_execution_etape qu'aucune notification du même type n'a déjà réussi pour cette exécution. Elle détermine le statut à partir du paramètre statut ou, à défaut, de ctl_execution, rassemble les statuts et volumes des flux, le résumé de gld_resume et, en cas d'échec, les diagnostics des étapes en échec. Elle construit le message HTML, obtient un jeton Graph et envoie le courriel. Seules les erreurs transitoires, codes 429 et 5xx ou erreurs réseau, sont réessayées, trois fois au plus avec un délai croissant ; une erreur 401 ou 403 n'est jamais réessayée car elle signale une configuration incorrecte. Chaque tentative laisse une ligne dans ctl_execution_etape avec son code retour. Un échec d'envoi laisse les données intactes, porte le statut Notification en échec et fait échouer l'activité pour déclencher l'alerte native Fabric.
#
# **Paramètres**
#
# Le paramètre action vaut envoyer par défaut, ou verifier. Le paramètre execution_id porte l'identifiant de l'exécution. Le paramètre statut force le statut à notifier, par exemple Échec sur le chemin d'erreur du pipeline ; vide, le statut de ctl_execution est utilisé. Le paramètre resume accepte un JSON de volumes à afficher en complément. Le paramètre pieces_jointes vaut aucune par défaut ; la valeur rejets joint le fichier des rejets de l'exécution, qui contient des données nominatives et exige une validation préalable du métier. Le paramètre message_erreur_pipeline transmet le diagnostic d'une activité du pipeline qui aurait échoué avant d'écrire dans les journaux. Les paramètres date_traitement et variables_env ont le même rôle que dans les autres notebooks.
#
# **Sorties produites**
#
# Un courriel HTML est envoyé à la liste de distribution. Une ligne par tentative est écrite dans ctl_execution_etape, pour le flux notification et l'étape notify:<type>, avec le code retour. Aucun jeton, aucun en-tête d'autorisation et aucune adresse de destinataire n'est journalisé.
#
# **Limitations connues et points d'attention**
#
# L'envoi reste non validé tant que les droits Exchange de la boîte technique, l'identité d'exécution et la liste de distribution ne sont pas configurés. L'obtention d'un jeton Graph par l'identité du workspace est à confirmer ; à défaut, le mode certificat doit être retenu. Le courriel de résumé avec pièce jointe de rejets reste un besoin à arbitrer : la pièce jointe n'est produite que sur demande explicite. Un diagnostic issu d'une erreur Spark est tronqué mais peut exceptionnellement citer une valeur ; il est limité à cinq cents caractères par étape.
#
# **Responsable et contact**
#
# Métier RH pour le contenu, référent technique HRIS pour le canal d'envoi.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import base64
import html
import json
import re
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

# Bibliothèques tierces
import requests
from pyspark.sql import functions as F

# Bibliothèques Azure
from azure.identity import CertificateCredential

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run MOTUL_nb_hris_lib

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

journal = obtenir_journal()
journal.info("✓ Imports réussis")
journal.info("✓ Journal initialisé")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

action = "envoyer"             # envoyer ou verifier
execution_id = ""              # GUID de l'exécution, produit par MOTUL_PL_HRIS_Orchestrateur
statut = ""                    # Statut à notifier ; vide pour reprendre celui de ctl_execution
resume = ""                    # JSON optionnel de volumes complémentaires à afficher
pieces_jointes = "aucune"      # aucune ou rejets ; rejets exige une validation préalable du métier
message_erreur_pipeline = ""   # Diagnostic d'une activité du pipeline en échec avant toute journalisation
date_traitement = ""           # Date au format aaaa-mm-jj ; vide pour la date du jour à Paris
variables_env = ""             # JSON des variables d'environnement résolues au lancement du pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION DE L'ENVIRONNEMENT =============
FLUX_NOTIFICATION = "notification"
ADRESSE_GRAPH = "https://graph.microsoft.com/v1.0"
PORTEE_GRAPH = "https://graph.microsoft.com/.default"
TENTATIVES_ENVOI = 3
try:
    contexte = initialiser_contexte_hris(variables_env)
    journal.info(f"✓ Environnement résolu : {contexte['env_nom']}")
    journal.info(f"✓ Workspace : {contexte['workspace_nom']}")

    if action not in ("envoyer", "verifier"):
        raise ErreurConfiguration(f"Action inconnue : {action!r} ; valeurs admises : envoyer, verifier.")
    if not execution_id:
        raise ErreurConfiguration("Le paramètre execution_id est obligatoire.")
    if pieces_jointes not in ("aucune", "rejets"):
        raise ErreurConfiguration(f"pieces_jointes invalide : {pieces_jointes!r} ; valeurs admises : aucune, rejets.")
    date_traitement_effective = resoudre_date_traitement(date_traitement)
    initialiser_socle()

except Exception as exc:
    journal.error(f"❌ Erreur lors de la configuration de l'environnement : {nettoyer_message_erreur(exc)}", exc_info=True)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyser_destinataires(valeur: str) -> List[str]:
    """Découpe la valeur du secret de liste de distribution en adresses et refuse une adresse mal formée."""
    adresses = [a.strip() for a in re.split(r"[;,]", valeur or "") if a.strip()]
    if not adresses:
        raise ErreurConfiguration("Le secret de liste de distribution ne contient aucune adresse.")
    invalides = sum(1 for a in adresses if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", a))
    if invalides:
        raise ErreurConfiguration(f"{invalides} adresse(s) mal formée(s) dans le secret de liste de distribution.")
    return adresses

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resoudre_destinataires() -> Tuple[List[str], str]:
    """Résout la liste de distribution, lue dans le coffre, et la boîte technique expéditrice ; rien n'est journalisé."""
    destinataires = analyser_destinataires(lire_secret("env_notif_liste_distribution"))
    expediteur = lire_variable("env_notif_bal_technique")
    mode_authentification = lire_variable("env_notif_auth_mode")
    if mode_authentification not in ("identite_workspace", "certificat"):
        raise ErreurConfiguration("env_notif_auth_mode doit valoir identite_workspace ou certificat.")
    if mode_authentification == "certificat":
        for nom in ("env_tenant_id", "env_notif_client_id", "env_notif_cert_secret"):
            lire_variable(nom)
    return destinataires, expediteur

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def obtenir_jeton_graph() -> str:
    """Obtient un jeton applicatif Microsoft Graph sous l'identité d'exécution de l'environnement."""
    mode_authentification = lire_variable("env_notif_auth_mode")
    if mode_authentification == "identite_workspace":
        return notebookutils.credentials.getToken("https://graph.microsoft.com")
    certificat = base64.b64decode(lire_secret("env_notif_cert_secret"))
    identite = CertificateCredential(lire_variable("env_tenant_id"), lire_variable("env_notif_client_id"), certificate_data=certificat)
    return identite.get_token(PORTEE_GRAPH).token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def rassembler_contenu(statut_force: str) -> Dict[str, Any]:
    """Rassemble statut, volumes par flux, résumés et diagnostics de l'exécution, sans donnée nominative."""
    execution = lire_execution(execution_id) or {}
    statut_effectif = statut_force or execution.get("statut") or STATUT_ECHEC
    if statut_effectif == STATUT_EN_COURS:
        statut_effectif = STATUT_ECHEC
    etapes = lire_etapes_consolidees(execution_id=execution_id)
    principales = etapes.where(~F.col("etape").contains(":") & ~F.col("flux_id").isin("orchestrateur", FLUX_NOTIFICATION))
    derniers = {}
    for ligne in principales.orderBy("horodatage").collect():
        derniers[ligne["flux_id"]] = ligne
    flux = [{"flux_id": f, "statut": l["statut"], "lues": l["lignes_lues"], "ecrites": l["lignes_ecrites"],
             "rejetees": l["lignes_rejetees"]} for f, l in sorted(derniers.items())]
    diagnostics = [
        {"flux_id": ligne["flux_id"], "etape": ligne["etape"], "message": (ligne["message_erreur"] or "Diagnostic absent")[:500]}
        for ligne in etapes.where(F.col("statut").isin(STATUT_ECHEC, STATUT_BLOQUE)).orderBy("horodatage").collect()
    ][-10:]
    if message_erreur_pipeline:
        diagnostics.append({"flux_id": "pipeline", "etape": "activité", "message": nettoyer_message_erreur(message_erreur_pipeline)[:500]})
    resumes = []
    if table_existe("gld_resume"):
        for ligne in lire_table("gld_resume").where(F.col("execution_id") == execution_id).orderBy("flux_id").collect():
            resumes.append({
                "flux_id": ligne["flux_id"], "lues": ligne["mt_lignes_lues"], "inserees": ligne["mt_lignes_inserts"],
                "rejetees": ligne["mt_lignes_rejets"],
                "taux": None if ligne["pc_lignes_rejets"] is None else f"{float(ligne['pc_lignes_rejets']) * 100:.2f} %",
                "types": ligne["ds_type_rejets"], "indicateurs": json.loads(ligne["indicateurs"]) if ligne["indicateurs"] else {},
            })
    complement = analyser_json_objet(resume, "paramètre resume") if resume else {}
    date_execution = execution.get("date_traitement") or date_traitement_effective
    return {"statut": statut_effectif, "environnement": contexte["env_nom"], "date_traitement": str(date_execution),
            "execution_id": execution_id, "flux": flux, "diagnostics": diagnostics, "resumes": resumes, "complement": complement}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_corps_html(contenu: Dict[str, Any], lien_rapport: Optional[str]) -> Tuple[str, str]:
    """
    Construit l'objet et le corps HTML du courriel, en échappant chaque valeur.

    Le modèle reprend la présentation du référentiel FR_BI_FABRIC : bandeau de statut, tableaux et avertissement
    de non-réponse ; le diagnostic réel n'apparaît que pour un échec.
    """
    succes = contenu["statut"] in STATUTS_REUSSIS
    couleur = "#2e7d32" if succes else "#c62828"
    e = lambda valeur: html.escape("" if valeur is None else str(valeur))
    libelle = "Statistiques disponibles" if succes else "Échec de l'exécution"
    objet = f"[HRIS][{contenu['environnement']}] {libelle} — {contenu['date_traitement']}"
    style_cellule = "border:1px solid #ddd;padding:6px 10px;text-align:left;"
    lignes_flux = "".join(
        f"<tr><td style='{style_cellule}'>{e(f['flux_id'])}</td><td style='{style_cellule}'>{e(f['statut'])}</td>"
        f"<td style='{style_cellule}'>{e(f['lues'])}</td><td style='{style_cellule}'>{e(f['ecrites'])}</td>"
        f"<td style='{style_cellule}'>{e(f['rejetees'])}</td></tr>"
        for f in contenu["flux"]
    )
    blocs = [
        f"<div style='background:{couleur};color:#fff;padding:14px 18px;font-size:18px;font-weight:bold;'>"
        f"{e(libelle)} : {e(contenu['statut'])}</div>",
        f"<p>Environnement : <strong>{e(contenu['environnement'])}</strong><br/>"
        f"Date de traitement : <strong>{e(contenu['date_traitement'])}</strong></p>",
    ]
    if contenu["flux"]:
        blocs.append(
            "<table style='border-collapse:collapse;font-size:13px;'><tr>"
            + "".join(f"<th style='{style_cellule}background:#f3f3f3;'>{t}</th>" for t in ("Flux", "Statut", "Lues", "Écrites", "Rejetées"))
            + f"</tr>{lignes_flux}</table>"
        )
    for r in contenu["resumes"]:
        details = [f"Lignes lues : {e(r['lues'])}", f"Lignes insérées : {e(r['inserees'])}", f"Lignes rejetées : {e(r['rejetees'])}"]
        if r["taux"] is not None:
            details.append(f"Taux de rejet : {e(r['taux'])}")
        if r["types"]:
            details.append(f"Types de rejet : {e(r['types'])}")
        details += [f"{e(cle)} : {e(valeur)}" for cle, valeur in sorted(r["indicateurs"].items())]
        blocs.append(f"<p><strong>Résumé {e(r['flux_id'])}</strong><br/>" + "<br/>".join(details) + "</p>")
    if contenu["complement"]:
        blocs.append("<p>" + "<br/>".join(f"{e(k)} : {e(v)}" for k, v in sorted(contenu["complement"].items())) + "</p>")
    if not succes:
        diagnostics = contenu["diagnostics"] or [{"flux_id": "-", "etape": "-", "message": "Aucun diagnostic journalisé."}]
        blocs.append(
            "<p><strong>Diagnostic</strong></p><ul>"
            + "".join(f"<li>{e(d['flux_id'])} / {e(d['etape'])} : <code>{e(d['message'])}</code></li>" for d in diagnostics)
            + "</ul>"
        )
    elif lien_rapport:
        blocs.append(f"<p><a href='{e(lien_rapport)}'>Ouvrir le rapport</a> (accès soumis à vos habilitations).</p>")
    blocs.append(f"<p style='color:#6c757d;font-size:12px;'>Identifiant d'exécution : {e(contenu['execution_id'])}</p>")
    blocs.append(
        "<div style='margin-top:30px;padding:15px;background:#f8f9fa;border-left:4px solid #dc3545;font-size:12px;color:#6c757d;'>"
        "<strong>⚠️ Ceci est un courriel automatique – Ne pas répondre</strong></div>"
    )
    corps = "<div style='font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#222;'>" + "".join(blocs) + "</div>"
    return objet, corps

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_piece_jointe_rejets() -> Optional[Dict[str, str]]:
    """Produit en mémoire le CSV des rejets de l'exécution, sans fichier persistant ; données nominatives."""
    tables = sorted({
        f["parametres"].get("table_rejet") for f in lire_tous_flux()
        if f["parametres"].get("table_rejet") and f["parametres"].get("traitement", "").startswith("resume")
    })
    lignes, colonnes = [], None
    for nom in tables:
        if not table_existe(nom):
            continue
        rejets = lire_table(nom).where(F.col("execution_id") == execution_id).drop("execution_id", "date_traitement")
        colonnes = colonnes or rejets.columns
        lignes.extend([[ligne[c] for c in colonnes] for ligne in rejets.collect()])
    if not lignes:
        return None
    contenu = formater_csv(colonnes, lignes, ",", '"', None, "\r\n").encode("utf-8")
    return {"@odata.type": "#microsoft.graph.fileAttachment", "name": f"rejet_{date_traitement_effective.strftime('%Y%m%d')}.csv",
            "contentType": "text/csv", "contentBytes": base64.b64encode(contenu).decode("ascii")}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def envoyer_courriel(expediteur: str, destinataires: List[str], objet: str, corps: str, pieces: List[Dict[str, str]], etape: str) -> None:
    """
    Envoie le courriel par Microsoft Graph sendMail ; réessaie uniquement les erreurs transitoires.

    Une ligne par tentative est journalisée avec le code retour ; ni jeton ni destinataire n'est écrit.
    """
    message = {
        "subject": objet,
        "body": {"contentType": "HTML", "content": corps},
        "toRecipients": [{"emailAddress": {"address": adresse}} for adresse in destinataires],
        "attachments": pieces,
    }
    jeton = obtenir_jeton_graph()
    for numero in range(1, TENTATIVES_ENVOI + 1):
        etat = demarrer_etape(execution_id, FLUX_NOTIFICATION, etape, date_traitement_effective)
        try:
            reponse = requests.post(
                f"{ADRESSE_GRAPH}/users/{expediteur}/sendMail",
                headers={"Authorization": f"Bearer {jeton}", "Content-Type": "application/json"},
                json={"message": message, "saveToSentItems": True}, timeout=30,
            )
        except requests.RequestException as exc:
            terminer_etape(etat, STATUT_NOTIFICATION_ECHEC, message_erreur=f"Erreur réseau : {type(exc).__name__}")
            if numero == TENTATIVES_ENVOI:
                raise ErreurHris(f"Notification en échec après {numero} tentative(s) : erreur réseau.") from None
            time.sleep(2 ** numero)
            continue
        identifiant_requete = reponse.headers.get("request-id", "inconnu")
        if reponse.status_code == 202:
            terminer_etape(etat, STATUT_SUCCES, lignes_ecrites=len(destinataires),
                           details={"code_retour": 202, "request_id": identifiant_requete, "pieces_jointes": len(pieces)})
            return
        transitoire = reponse.status_code == 429 or reponse.status_code >= 500
        terminer_etape(etat, STATUT_NOTIFICATION_ECHEC,
                       message_erreur=f"Graph HTTP {reponse.status_code}, request-id {identifiant_requete}",
                       details={"code_retour": reponse.status_code, "request_id": identifiant_requete})
        if not transitoire or numero == TENTATIVES_ENVOI:
            raise ErreurHris(f"Notification en échec : Graph HTTP {reponse.status_code}, request-id {identifiant_requete}.")
        attente = int(reponse.headers.get("Retry-After", 2 ** numero)) if reponse.status_code == 429 else 2 ** numero
        time.sleep(min(attente, 120))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= NOTIFICATION =============
resultat = {}
try:
    journal.info("=" * 80)
    journal.info(f"🚀 NOTIFICATION : ACTION {action.upper()}")
    journal.info("=" * 80)
    destinataires, expediteur = resoudre_destinataires()
    journal.info(f"✓ Destinataires résolus : {len(destinataires)} adresse(s)")

    if action == "verifier":
        etat = demarrer_etape(execution_id, FLUX_NOTIFICATION, "notify:verification", date_traitement_effective)
        terminer_etape(etat, STATUT_SUCCES, lignes_lues=len(destinataires))
        resultat = {"execution_id": execution_id, "statut": STATUT_SUCCES, "destinataires": len(destinataires)}
    else:
        contenu = rassembler_contenu(statut)
        type_notification = "resume" if contenu["statut"] in STATUTS_REUSSIS else "erreur"
        etape = f"notify:{type_notification}"
        if etape_deja_reussie(execution_id, FLUX_NOTIFICATION, etape):
            journal.info("✓ Notification déjà émise pour cette exécution : aucun nouvel envoi.")
            resultat = {"execution_id": execution_id, "statut": STATUT_IGNORE, "type": type_notification}
        else:
            lien = lire_variable("env_powerbi_rapport_url") if type_notification == "resume" else None
            objet, corps = construire_corps_html(contenu, lien)
            pieces = []
            if pieces_jointes == "rejets":
                piece = construire_piece_jointe_rejets()
                pieces = [piece] if piece else []
            envoyer_courriel(expediteur, destinataires, objet, corps, pieces, etape)
            resultat = {"execution_id": execution_id, "statut": STATUT_SUCCES, "type": type_notification,
                        "statut_execution": contenu["statut"], "pieces_jointes": len(pieces)}
    journal.info(f"✓ Notification terminée : {json.dumps(resultat, ensure_ascii=False)}")

except Exception as exc:
    message = nettoyer_message_erreur(exc)
    journal.error(f"❌ Échec de la notification : {message}", exc_info=True)
    if not isinstance(exc, ErreurHris) or not str(exc).startswith("Notification en échec"):
        try:
            etat = demarrer_etape(execution_id, FLUX_NOTIFICATION, f"notify:{action}", date_traitement_effective)
            terminer_etape(etat, STATUT_NOTIFICATION_ECHEC, message_erreur=message)
        except Exception:
            journal.error("❌ Impossible de journaliser l'échec de notification dans ctl_execution_etape.", exc_info=True)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps(resultat, ensure_ascii=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
