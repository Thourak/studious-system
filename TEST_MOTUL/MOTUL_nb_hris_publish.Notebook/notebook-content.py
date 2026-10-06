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
# # MOTUL_nb_hris_publish, nom repris du champ displayName du fichier .platform.
# # **Type d'objet**
# # Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark, appelé par MOTUL_PL_HRIS_Orchestrateur pour les flux de publication pub_sirh et pub_remuneration, après la réussite des contrôles.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/MOTUL_nb_hris_publish.Notebook/notebook-content.py.
# # **Description fonctionnelle**
# # Ce notebook consolide puis met à disposition le lot validé de l'exécution courante. Pour chaque jeu de données, il gère deux tables aux rôles distincts. La table cumulative, suffixée _cumul, regroupe les données des traitements successifs : elle est alimentée exclusivement par upsert, conserve les enregistrements absents du lot courant et n'est jamais vidée ni remplacée. La table finale, de nom stable, contient uniquement les données du dernier lot validé : son contenu est remplacé à chaque alimentation réussie, sans que la table soit supprimée ni recréée. La publication lit la table finale et non la table cumulative. La publication externe n'a lieu que si l'environnement résolu vaut exactement PRD et si le nom du workspace porte le marqueur PRD : les évolutions de rémunération partent alors vers le SFTP de TalentSoft, et l'envoi vers ADP reste bloqué tant que la tâche MIG-027 n'a pas fourni le contrat ADP. En DEV et en UAT, la table finale du Lakehouse local constitue la publication.
# # **Dépendances**
# # Le notebook importe MOTUL_nb_hris_lib. Il lit cfg_flux pour la description des jeux de données, des clés, des politiques et des fichiers, les tables de lot slv_*_lot produites par MOTUL_nb_hris_transform, ctl_execution pour l'horodatage du lot, ctl_execution_etape pour vérifier la réussite de la transformation et du contrôle et pour la protection contre le double envoi, et ctl_qualite pour l'état du contrat ADP. En PRD, il utilise le SFTP désigné par env_sftp_hote, env_sftp_port, env_sftp_utilisateur et env_sftp_import_chemin, avec le mot de passe lu dans le coffre par le nom porté par env_secret_sftp.
# # **Fonctionnement et logique de traitement**
# # Le notebook vérifie d'abord que la transformation et le contrôle du flux ont réussi pour cette exécution et que la source n'était pas vide : un lot non validé, ou issu d'une source vide, ne remplace pas le dernier résultat valide et ne déclenche aucune publication. Pour chaque jeu, il lit le lot de l'exécution, supprime les doublons exacts et refuse des lignes contradictoires partageant la même clé. Il consolide ensuite la table cumulative : une clé nouvelle est insérée, une clé existante est mise à jour seulement si ses valeurs changent et si le lot n'est pas plus ancien que celui qui l'a écrite, une clé absente est conservée. Il alimente ensuite la table finale avec toutes les lignes du lot, y compris celles dont les valeurs n'ont pas changé dans la table cumulative, par l'instruction INSERT OVERWRITE ; l'identité du lot est inscrite dans le même commit Delta, et un lot plus ancien que le dernier lot écrit est refusé, ce qui protège contre une exécution concurrente ou tardive. Un lot valide mais vide vide la table finale ou conserve le résultat précédent selon la politique déclarée. Une fois toutes les écritures réussies, la publication lit la table finale : dépôt SFTP en PRD, protégé contre le double envoi, trace de l'état bloqué pour ADP en PRD, et trace de la mise à disposition dans le Lakehouse local hors PRD.
# # **Paramètres**
# # Le paramètre execution_id porte l'identifiant de l'exécution. Le paramètre flux_id vaut pub_sirh ou pub_remuneration. Le paramètre date_traitement, au format aaaa-mm-jj, date le lot et le nom des fichiers déposés. Le paramètre variables_env porte le JSON des variables d'environnement. Le paramètre mode est accepté pour homogénéité et n'a pas d'effet ici.
# # **Sorties produites**
# # Les tables cumulatives gld_ts_employe_adp_cumul, gld_base_salary_changes_cumul et gld_bonus_changes_cumul portent les colonnes cumul_empreinte, cumul_execution_id, cumul_date_traitement, cumul_horodatage_lot, cumul_date_insertion et cumul_date_maj. Les tables finales gld_ts_employe_adp, gld_base_salary_changes et gld_bonus_changes contiennent le dernier lot validé avec execution_id, date_traitement et horodatage_lot. En PRD, les fichiers RemunSalaryHistoIE_InsertAndUpdate_fr-FR_1_<jjmmaaaa>.csv et RemunTargetBonusHistoIE_InsertAndUpdate_fr-FR_2_<jjmmaaaa>.csv sont déposés dans le dossier d'import TalentSoft. Chaque consolidation, remplacement, dépôt ou refus laisse une sous-étape dans ctl_execution_etape.
# # **Limitations connues et points d'attention**
# # Plusieurs choix restent à confirmer par le métier : les clés de consolidation de chaque jeu, la stratégie de cumul, upsert de type SCD 1 retenu par défaut ou historisation de type SCD 2 disponible par configuration, la règle à appliquer à des lignes contradictoires, et le comportement d'un lot valide mais vide, réglé sur vider comme le faisait l'existant. Aucune transaction ne couvre les deux tables : si la consolidation réussit et que le remplacement de la table finale échoue, l'étape échoue sans publier, et une reprise de la même exécution rejoue la consolidation sans effet puis remplace la table finale. Tout dépôt SFTP destiné à être réintégré et tout envoi vers ADP exigent une validation humaine avant la première exécution en PRD. La durée de conservation des tables de lot et de la table cumulative reste une question ouverte.
# # **Responsable et contact**
# # Équipe data du projet HRIS Motul ; validation humaine par le métier RH et l'administrateur HRIS avant toute publication externe.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

# Bibliothèques tierces
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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

execution_id = ""            # GUID de l'exécution, produit par MOTUL_PL_HRIS_Orchestrateur
flux_id = ""                 # pub_sirh ou pub_remuneration
mode = "incremental"         # Accepté pour homogénéité ; sans effet sur la publication
date_traitement = ""         # Date au format aaaa-mm-jj ; vide pour la date du jour à Paris
variables_env = ""           # JSON des variables d'environnement résolues au lancement du pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION DE L'ENVIRONNEMENT =============
COLONNES_TECHNIQUES_LOT = ("execution_id", "date_traitement", "dt_transforme")
etat_etape = None
blocage = None
try:
    contexte = initialiser_contexte_hris(variables_env)
    journal.info(f"✓ Environnement résolu : {contexte['env_nom']}")
    journal.info(f"✓ Workspace : {contexte['workspace_nom']}")
    journal.info(f"✓ Lakehouse : {contexte['lakehouse_nom']}")

    if not execution_id or not flux_id:
        raise ErreurConfiguration("Les paramètres execution_id et flux_id sont obligatoires.")
    date_traitement_effective = resoudre_date_traitement(date_traitement)
    publication_autorisee = publication_externe_autorisee(contexte["env_nom"], contexte["workspace_nom"])
    journal.info(f"✓ Publication externe autorisée : {'oui' if publication_autorisee else 'non'}")

    initialiser_socle()
    flux = lire_flux(flux_id)
    if flux.get("notebook") != contexte["notebook_nom"]:
        raise ErreurConfiguration(f"Le flux {flux_id} est routé vers {flux.get('notebook')} et non vers {contexte['notebook_nom']}.")
    execution = lire_execution(execution_id)
    if not execution or not execution.get("debut"):
        raise ErreurConfiguration(f"Exécution {execution_id} absente de ctl_execution : le lot ne peut pas être horodaté.")
    horodatage_lot = execution["debut"]
    etat_etape = demarrer_etape(execution_id, flux_id, "publish", date_traitement_effective,
                                {"publication_externe_autorisee": publication_autorisee})
    blocage = verifier_dependances_execution(execution_id, flux_id)

except Exception as exc:
    journal.error(f"❌ Erreur lors de la configuration de l'environnement : {nettoyer_message_erreur(exc)}", exc_info=True)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def derniere_etape(flux_cible: str, etape: str) -> Optional[Dict[str, Any]]:
    """Renvoie la tentative la plus récente d'une étape d'un flux pour l'exécution courante, ou None."""
    lignes = (
        lire_etapes_consolidees(execution_id=execution_id)
        .where((F.col("flux_id") == flux_cible) & (F.col("etape") == etape))
        .orderBy(F.col("horodatage").desc())
        .limit(1)
        .collect()
    )
    return lignes[0].asDict() if lignes else None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def verifier_lot_valide(parametres: Dict[str, Any]) -> int:
    """
    Vérifie que la transformation et le contrôle ont réussi pour cette exécution et que la source n'était pas vide.

    Un lot vide issu d'une source non vide est un lot valide ; une source vide est traitée comme une anomalie
    d'ingestion probable et ne remplace pas le dernier résultat valide.

    Returns:
        Le volume de la source déclaré par la transformation.
    """
    flux_transformation = exiger_parametre(parametres, "flux_transformation", flux_id)
    flux_controle = exiger_parametre(parametres, "flux_controle", flux_id)
    transformation = derniere_etape(flux_transformation, "transform")
    controle = derniere_etape(flux_controle, "control")
    for nom, etape in ((flux_transformation, transformation), (flux_controle, controle)):
        if not etape or etape["statut"] not in STATUTS_REUSSIS:
            raise ErreurDonnees(
                f"Lot non validé : {nom} n'a pas réussi pour l'exécution {execution_id} ; "
                "aucune consolidation, aucun remplacement de la table finale et aucune publication."
            )
    details = analyser_json_objet(transformation.get("details"), f"détails de {flux_transformation}")
    if "lignes_source" not in details:
        raise ErreurDonnees(f"Volume de la source non journalisé par {flux_transformation} : lot non validé.")
    if int(details["lignes_source"]) == 0:
        raise ErreurDonnees(
            f"Source vide pour {flux_transformation} : anomalie d'ingestion probable ; le résultat final précédent est conservé."
        )
    return int(details["lignes_source"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tracer_sous_etape(nom: str, statut: str, message: Optional[str] = None, lignes: Optional[int] = None,
                      details: Optional[Dict[str, Any]] = None) -> None:
    """Journalise une sous-étape de publication : publish:cumul, publish:final, publish:sftp, publish:adp ou publish:lakehouse."""
    etat = demarrer_etape(execution_id, flux_id, nom, date_traitement_effective)
    terminer_etape(etat, statut, lignes_ecrites=lignes, message_erreur=message, details=details)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_lot(jeu: Dict[str, Any]) -> DataFrame:
    """Lit le lot de l'exécution courante, sans doublon exact, et refuse des lignes contradictoires sur une même clé."""
    nom_table = valider_nom_table(exiger_parametre(jeu, "table_lot", jeu.get("nom")))
    if not table_existe(nom_table):
        raise ErreurDonnees(f"La table de lot {nom_table} n'existe pas : la transformation n'a pas produit de lot.")
    lot = lire_table(nom_table).where(F.col("execution_id") == execution_id)
    return dedoublonner_lot(lot, exiger_parametre(jeu, "cles", jeu["nom"]), f"Lot {jeu['nom']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def consolider_jeu(jeu: Dict[str, Any], lot: DataFrame) -> Dict[str, Any]:
    """Alimente la table cumulative du jeu par upsert, ou par historisation SCD 2 si cette stratégie est déclarée."""
    table_cumul = valider_nom_table(exiger_parametre(jeu, "table_cumul", jeu["nom"]))
    cles = exiger_parametre(jeu, "cles", jeu["nom"])
    metier = [colonne for colonne in lot.columns if colonne not in COLONNES_TECHNIQUES_LOT]
    strategie = exiger_parametre(jeu, "strategie_cumul", jeu["nom"])
    if strategie == "upsert":
        bilan = consolider_upsert(lot, table_cumul, cles, metier, execution_id, date_traitement_effective, horodatage_lot)
    elif strategie == "scd2":
        bilan = {"versions_presentees": historiser_scd2(lot.select(*metier), table_cumul, cles, metier,
                                                         date_traitement_effective, execution_id)}
    else:
        raise ErreurConfiguration(f"Stratégie de cumul inconnue pour {jeu['nom']} : {strategie!r} ; valeurs admises : upsert, scd2.")
    tracer_sous_etape(f"publish:cumul:{table_cumul}", STATUT_SUCCES, lignes=sum(bilan.values()),
                      details={"strategie": strategie, **bilan})
    return {"table": table_cumul, "strategie": strategie, **bilan}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lot_plus_ancien(courant: Dict[str, Any], precedent: Optional[Dict[str, Any]]) -> bool:
    """Indique si le lot courant est antérieur au dernier lot écrit, par date de traitement puis horodatage d'exécution."""
    if not precedent or precedent.get("execution_id") == courant["execution_id"]:
        return False
    cle = lambda lot: (date.fromisoformat(lot["date_traitement"]), datetime.fromisoformat(lot["horodatage_lot"]))
    return cle(courant) < cle(precedent)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def alimenter_table_finale(jeu: Dict[str, Any], lot: DataFrame) -> Dict[str, Any]:
    """
    Remplace le contenu de la table finale par toutes les lignes du lot courant, y compris les lignes inchangées.

    Un lot plus ancien que le dernier lot écrit est refusé ; la vérification est rejouée avec l'écriture en cas de
    conflit de concurrence. Un lot vide applique la politique déclarée : vider ou conserver.
    """
    table_finale = valider_nom_table(exiger_parametre(jeu, "table_finale", jeu["nom"]))
    politique = exiger_parametre(jeu, "politique_lot_vide", jeu["nom"])
    if politique not in ("vider", "conserver"):
        raise ErreurConfiguration(f"Politique de lot vide inconnue pour {jeu['nom']} : {politique!r} ; valeurs admises : vider, conserver.")
    identite = {"hris_lot": True, "flux_id": flux_id, "jeu": jeu["nom"], "execution_id": execution_id,
                "date_traitement": date_traitement_effective.isoformat(), "horodatage_lot": horodatage_lot.isoformat()}
    donnees = lot.withColumn("horodatage_lot", F.lit(horodatage_lot).cast("timestamp"))
    lignes = donnees.count()

    def operation() -> Dict[str, Any]:
        precedent = lire_metadonnees_dernier_lot(table_finale)
        if lot_plus_ancien(identite, precedent):
            return {"statut": "ignoree_plus_ancien", "lignes": 0,
                    "message": f"Lot plus ancien que le lot {precedent['execution_id']} déjà en place : table finale inchangée."}
        if lignes == 0 and politique == "conserver":
            return {"statut": "conservee", "lignes": 0, "message": "Lot valide mais vide : résultat final précédent conservé."}
        mode_ecriture = ecraser_contenu_table(donnees, table_finale, identite)
        return {"statut": "remplacee", "lignes": lignes, "message": None, "mode": mode_ecriture}

    resultat = executer_avec_reprise_concurrence(operation, f"table finale {table_finale}")
    statut_trace = STATUT_SUCCES if resultat["statut"] == "remplacee" else STATUT_IGNORE
    tracer_sous_etape(f"publish:final:{table_finale}", statut_trace, resultat["message"], resultat["lignes"],
                      details={"resultat": resultat["statut"], **identite})
    return {"table": table_finale, **resultat}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_table_finale_du_lot(table_finale: str) -> DataFrame:
    """Relit la table finale et vérifie qu'elle ne contient que le lot courant avant toute publication."""
    finale = lire_table(table_finale)
    if finale.where(F.col("execution_id") != execution_id).limit(1).count() > 0:
        raise ErreurDonnees(f"La table finale {table_finale} contient des lignes d'un autre lot : publication refusée.")
    return finale

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def publier_fichier_sftp(client: Any, dossier: str, jeu: Dict[str, Any], finale: DataFrame, parametres: Dict[str, Any]) -> Optional[str]:
    """Dépose sur le SFTP TalentSoft le fichier construit depuis la table finale ; un fichier déjà déposé n'est jamais renvoyé."""
    fichier = exiger_parametre(jeu, "fichier", jeu["nom"])
    nom = fichier["modele_nom"].replace("{ddMMyyyy}", date_traitement_effective.strftime("%d%m%Y"))
    sous_etape = f"publish:sftp:{nom}"
    if etape_deja_reussie(execution_id, flux_id, sous_etape):
        journal.info(f"✓ {nom} déjà déposé pour cette exécution : aucun nouvel envoi.")
        return None
    sources = [source for source, _ in fichier["colonnes"]]
    lignes = [[ligne[source] for source in sources] for ligne in finale.select(*sources).orderBy(*jeu["cles"]).collect()]
    contenu = formater_csv(
        [entete for _, entete in fichier["colonnes"]], lignes, exiger_parametre(parametres, "separateur", flux_id),
        exiger_parametre(parametres, "guillemet", flux_id), parametres.get("echappement"),
        exiger_parametre(parametres, "saut_ligne", flux_id),
    ).encode(exiger_parametre(parametres, "encodage", flux_id))
    etat = demarrer_etape(execution_id, flux_id, sous_etape, date_traitement_effective)
    try:
        deposer_fichier_sftp(client, dossier, nom, contenu)
    except Exception as exc:
        terminer_etape(etat, STATUT_ECHEC, message_erreur=nettoyer_message_erreur(exc))
        raise
    terminer_etape(etat, STATUT_SUCCES, lignes_ecrites=len(lignes),
                   details={"fichier": nom, "table_finale": jeu["table_finale"], "empreinte_sha256": empreinte_sha256(contenu)})
    return nom

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def publier_jeux(parametres: Dict[str, Any], finales: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> Dict[str, str]:
    """
    Publie chaque jeu depuis sa table finale, selon l'environnement : SFTP ou ADP en PRD, Lakehouse local ailleurs.

    Un jeu dont la table finale n'a pas été remplacée par le lot courant n'est pas publié.
    """
    publications, a_deposer = {}, []
    for jeu, finale_info in finales:
        if finale_info["statut"] != "remplacee":
            publications[jeu["nom"]] = "non publié : table finale non remplacée par ce lot"
            continue
        finale = lire_table_finale_du_lot(finale_info["table"])
        if not publication_autorisee:
            tracer_sous_etape(f"publish:lakehouse:{finale_info['table']}", STATUT_SUCCES,
                              lignes=finale.count(), details={"canal_prd": jeu["canal"], "table_finale": finale_info["table"]})
            publications[jeu["nom"]] = "Lakehouse local"
        elif jeu["canal"] == "adp":
            controle = (
                lire_table("ctl_qualite")
                .where((F.col("execution_id") == execution_id) & (F.col("flux_id") == parametres["flux_controle"])
                       & (F.col("regle_id") == "CONTRAT_ADP"))
                .collect()
            )
            motif = controle[0]["commentaire"] if controle else "Contrôle du contrat ADP absent pour cette exécution."
            tracer_sous_etape("publish:adp", STATUT_BLOQUE,
                              f"Publication ADP non réalisée : MIG-027 bloquée, règles et comparatif ADP à fournir. {motif}", 0)
            publications[jeu["nom"]] = STATUT_BLOQUE
        elif jeu["canal"] == "sftp":
            a_deposer.append((jeu, finale))
        else:
            raise ErreurConfiguration(f"Canal de publication inconnu pour {jeu['nom']} : {jeu['canal']!r}.")
    if a_deposer:
        transport, client = ouvrir_session_sftp()
        try:
            dossier = lire_variable(flux["source_ref"])
            for jeu, finale in a_deposer:
                nom = publier_fichier_sftp(client, dossier, jeu, finale, parametres)
                publications[jeu["nom"]] = f"SFTP {nom}" if nom else "SFTP déjà déposé"
        finally:
            fermer_session_sftp(transport, client)
    return publications

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONSOLIDATION, TABLE FINALE ET PUBLICATION =============
resultat = {}
try:
    parametres = flux["parametres"]
    if parametres.get("traitement") != "publication":
        raise ErreurConfiguration(f"Traitement de publication inconnu pour {flux_id} : {parametres.get('traitement')!r}.")
    journal.info("=" * 80)
    journal.info(f"🚀 PUBLICATION DU FLUX {flux_id}")
    journal.info("=" * 80)
    if blocage:
        journal.warning(f"⚠️ {blocage}")
        terminer_etape(etat_etape, STATUT_BLOQUE, message_erreur=blocage)
        resultat = {"execution_id": execution_id, "flux_id": flux_id, "statut": STATUT_BLOQUE, "motif": blocage}
    else:
        lignes_source = verifier_lot_valide(parametres)

        consolidations, finales, lues = {}, [], 0
        for jeu in exiger_parametre(parametres, "jeux", flux_id):
            lot = lire_lot(jeu).cache()
            lues += lot.count()
            consolidations[jeu["nom"]] = consolider_jeu(jeu, lot)
            finales.append((jeu, alimenter_table_finale(jeu, lot)))
        journal.info(f"✓ Consolidation et tables finales : {json.dumps({j['nom']: f['statut'] for j, f in finales}, ensure_ascii=False)}")

        publications = publier_jeux(parametres, finales)
        remplacees = sum(1 for _, f in finales if f["statut"] == "remplacee")
        statut_final = STATUT_SUCCES if remplacees else STATUT_IGNORE
        details = {"lignes_source": lignes_source, "consolidations": consolidations,
                   "tables_finales": {j["nom"]: f["statut"] for j, f in finales}, "publications": publications}
        terminer_etape(etat_etape, statut_final, lignes_lues=lues, lignes_ecrites=sum(f["lignes"] for _, f in finales),
                       lignes_rejetees=0, details=details)
        resultat = {"execution_id": execution_id, "flux_id": flux_id, "statut": statut_final, **details}
        journal.info(f"✓ Publication terminée : {json.dumps(resultat, ensure_ascii=False, default=str)}")

except Exception as exc:
    message = nettoyer_message_erreur(exc)
    journal.error(f"❌ Échec de la publication du flux {flux_id} : {message}", exc_info=True)
    if etat_etape is not None:
        terminer_etape(etat_etape, STATUT_ECHEC, message_erreur=message)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps(resultat, ensure_ascii=False, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
