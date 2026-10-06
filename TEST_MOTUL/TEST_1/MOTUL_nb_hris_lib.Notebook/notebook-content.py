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
# # MOTUL_nb_hris_lib. Ce nom est celui du champ displayName du fichier .platform et c'est celui qu'utilisent les autres notebooks dans leur instruction %run.
# # **Type d'objet**
# # Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark. Il s'agit d'une bibliothèque : elle n'est jamais exécutée seule et n'est jamais appelée comme activité du pipeline.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/MOTUL_nb_hris_lib.Notebook/notebook-content.py.
# # **Description fonctionnelle**
# # Cette bibliothèque regroupe les fonctions communes aux cinq notebooks métier du hub d'intégration RH entre TalentSoft et ADP. Elle remplace les quarante et un datasets Synapse, les expressions qui déduisaient l'environnement du nom de la fabrique et les activités qui lisaient des liens dans le coffre de secrets. Elle ne contient aucune règle métier : les règles de transformation vivent dans MOTUL_nb_hris_transform et les règles de contrôle sont déclarées dans la table cfg_qualite.
# # **Dépendances**
# # La bibliothèque s'appuie sur le Lakehouse de l'environnement, dont le nom est fourni par la variable env_lakehouse_nom, et sur le coffre de secrets de l'environnement, dont l'adresse est fournie par la variable env_kv_url. Elle lit les tables cfg_flux, cfg_dependance, cfg_mapping et cfg_qualite et écrit dans les tables ctl_execution et ctl_execution_etape. Les échanges SFTP utilisent la bibliothèque paramiko lorsqu'elle est présente dans le runtime Spark.
# # **Fonctionnement et logique de traitement**
# # Les variables d'environnement sont résolues une seule fois par le pipeline MOTUL_PL_HRIS_Orchestrateur, qui les transmet à chaque notebook dans le paramètre variables_env sous forme de texte JSON. La fonction initialiser_contexte_hris lit ce paramètre, contrôle que l'environnement résolu vaut exactement DEV, UAT ou PRD et refuse l'exécution si le nom du workspace courant porte le marqueur d'un autre environnement. La fonction lire_variable lève une exception explicite lorsqu'une variable est absente ou vide : il n'existe aucune valeur par défaut et aucun repli vers un autre environnement. Les tables sont adressées par un nom complet composé du workspace courant, du Lakehouse, du schéma dbo et du nom de table, ce qui évite d'attacher un Lakehouse par défaut dont l'identifiant changerait d'un workspace à l'autre. Les secrets sont lus à l'exécution par notebookutils.credentials.getSecret à partir du nom porté par une variable, et leur valeur n'est jamais journalisée. Les primitives de qualité reproduisent la sémantique du T-SQL d'origine, notamment le fait qu'une chaîne vide n'est pas une valeur nulle et que les espaces de fin sont ignorés dans les comparaisons. La publication externe n'est autorisée que si l'environnement résolu vaut exactement PRD et si le nom du workspace porte le marqueur PRD. La fonction consolider_upsert alimente une table cumulative par fusion uniquement : elle insère les clés nouvelles, met à jour une clé existante seulement si ses valeurs changent et si le lot n'est pas plus ancien que celui qui l'a écrite, et conserve les clés absentes du lot ; elle ne supprime et ne remplace jamais globalement et ne conserve pas les anciennes valeurs, ce qui correspond à une SCD de type 1. La fonction historiser_scd2 reste disponible si une historisation de toutes les versions est retenue. La fonction ecraser_contenu_table remplace le contenu d'une table finale par l'instruction INSERT OVERWRITE, sans la supprimer ni la recréer, et inscrit l'identité du lot dans le même commit Delta, que relit lire_metadonnees_dernier_lot.
# # **Paramètres**
# # Ce notebook n'a aucun paramètre. Il est importé par l'instruction %run MOTUL_nb_hris_lib placée dans la troisième cellule de chaque notebook métier.
# # **Sorties produites**
# # La bibliothèque crée de façon idempotente les tables de socle cfg_flux, cfg_dependance, cfg_mapping, cfg_qualite, ctl_execution, ctl_execution_etape, ctl_qualite, ctl_fichier_traite et gld_resume. Elle écrit une ligne dans ctl_execution_etape au début et à la fin de chaque tentative d'étape, ce qui permet de détecter une étape restée en cours au-delà de son délai.
# # **Limitations connues et points d'attention**
# # Les noms de fonctions et de colonnes sont en français pour rester alignés sur le plan de migration, qui prime sur la règle de nommage en anglais du référentiel FR_BI_FABRIC. La fonction tsql_isdate reproduit la fonction ISDATE pour les formats ISO, compacts et avec barres obliques en année, mois et jour ; un format dépendant de la langue de session SQL Server, comme mois, jour et année, est considéré comme invalide et doit être vérifié lors de la réconciliation. L'écriture parallèle de plusieurs flux dans une même table Delta est protégée par une reprise automatique en cas de conflit de concurrence. La fonction executer_tests_unitaires vérifie les fonctions pures ; elle doit être lancée manuellement dans le workspace DEV.
# # **Responsable et contact**
# # Équipe data du projet HRIS Motul, référent technique HRIS pour les évolutions.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import csv
import hashlib
import io
import json
import logging
import posixpath
import re
import stat
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import reduce
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

# Bibliothèques tierces
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    import paramiko
except ImportError:  # paramiko n'est requis que pour les échanges SFTP
    paramiko = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONSTANTES DE LA BIBLIOTHEQUE =============
# Aucune valeur propre à un environnement : uniquement des conventions communes aux trois workspaces.
VERSION_CODE_HRIS = "1.0.0"
FUSEAU_HORAIRE_HRIS = "Europe/Paris"
SCHEMA_LAKEHOUSE_HRIS = "dbo"
ENVIRONNEMENTS_AUTORISES = ("DEV", "UAT", "PRD")
MARQUEUR_PRD = "PRD"
ZONES_FICHIERS = ("landing", "archive", "rejet", "reference", "sortie")
DOMAINE_ONELAKE = "onelake.dfs.fabric.microsoft.com"
DOMAINE_ADLS = "dfs.core.windows.net"

STATUT_EN_COURS = "En cours"
STATUT_SUCCES = "Succès"
STATUT_SUCCES_REJETS = "Succès avec rejets"
STATUT_ECHEC = "Échec"
STATUT_ECHEC_PARTIEL = "Échec partiel"
STATUT_BLOQUE = "Bloqué"
STATUT_IGNORE = "Ignoré"
STATUT_NOTIFICATION_ECHEC = "Notification en échec"
STATUTS_ETAPE = (
    STATUT_EN_COURS, STATUT_SUCCES, STATUT_SUCCES_REJETS, STATUT_ECHEC,
    STATUT_BLOQUE, STATUT_IGNORE, STATUT_NOTIFICATION_ECHEC,
)
STATUTS_REUSSIS = (STATUT_SUCCES, STATUT_SUCCES_REJETS)

# Colonnes techniques ajoutées par MOTUL_nb_hris_ingest, exclues des comparaisons métier.
COLONNES_TECHNIQUES_INGESTION = (
    "execution_id", "date_traitement", "fichier_source", "date_fichier_source", "horodatage_ingestion",
)
TAILLE_MAX_MESSAGE_ERREUR = 2000
DATE_VIDE_TSQL = date(1900, 1, 1)  # CONVERT(DATE, '') renvoie le 1er janvier 1900 en T-SQL

_MOTIF_NOM_TABLE = re.compile(r"^(brz|stg|slv|gld|rjt|hst|cfg|ctl)_[a-z0-9_]+$")
_MOTIF_IDENTIFIANT = re.compile(r"^[a-z0-9_]+$")
_MOTIF_NUMERIQUE_TSQL = re.compile(r"^\s*[+-]?(\d+(\.\d*)?|\.\d+)\s*$")

_VARIABLES_ENV: Dict[str, str] = {}
_CONTEXTE_HRIS: Dict[str, str] = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= EXCEPTIONS EXPLICITES =============
class ErreurHris(Exception):
    """Erreur explicite levée par les notebooks HRIS ; son message ne contient jamais de secret."""


class VariableEnvironnementManquante(ErreurHris):
    """Variable d'environnement absente ou vide : l'exécution doit échouer sans valeur par défaut."""


class ErreurConfiguration(ErreurHris):
    """Configuration de flux, de table ou de paramètre incomplète ou incohérente."""


class ErreurGardeEnvironnement(ErreurHris):
    """Croisement d'environnements ou publication externe refusée hors PRD."""


class ErreurDonnees(ErreurHris):
    """Donnée source inexploitable, reproduisant un échec de l'existant ou un contrôle bloquant."""


class CycleDependances(ErreurHris):
    """Cycle détecté dans cfg_dependance : aucun traitement ne doit démarrer."""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def obtenir_journal(nom: str = "hris") -> logging.Logger:
    """Renvoie le journal Python commun aux notebooks HRIS, configuré une seule fois."""
    journal = logging.getLogger(nom)
    if not journal.handlers:
        gestionnaire = logging.StreamHandler()
        gestionnaire.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        journal.addHandler(gestionnaire)
    journal.setLevel(logging.INFO)
    journal.propagate = False
    return journal

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyser_variables_env(variables_env: Optional[str]) -> Dict[str, str]:
    """
    Analyse le texte JSON des variables d'environnement transmis par le pipeline.

    Args:
        variables_env: Objet JSON dont les clés sont les noms de variables env_*.

    Returns:
        Dictionnaire nom de variable vers valeur textuelle.

    Raises:
        VariableEnvironnementManquante: si le texte est vide, invalide ou n'est pas un objet JSON.
    """
    if variables_env is None or not str(variables_env).strip():
        raise VariableEnvironnementManquante(
            "Le paramètre variables_env est vide : les variables d'environnement doivent être "
            "résolues au lancement par MOTUL_PL_HRIS_Orchestrateur puis transmises au notebook."
        )
    try:
        valeurs = json.loads(variables_env)
    except json.JSONDecodeError as exc:
        raise VariableEnvironnementManquante(
            f"Le paramètre variables_env n'est pas un JSON valide (position {exc.pos})."
        ) from None
    if not isinstance(valeurs, dict):
        raise VariableEnvironnementManquante("Le paramètre variables_env doit être un objet JSON.")
    return {str(cle): ("" if valeur is None else str(valeur)) for cle, valeur in valeurs.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_variable_depuis(variables: Dict[str, str], nom: str) -> str:
    """
    Lit une variable d'environnement dans un dictionnaire, sans aucune valeur par défaut.

    Raises:
        ErreurConfiguration: si le nom ne respecte pas la convention env_*.
        VariableEnvironnementManquante: si la variable est absente ou vide.
    """
    if not nom or not re.match(r"^env_[a-z0-9_]+$", nom):
        raise ErreurConfiguration(f"Nom de variable d'environnement invalide : {nom!r}.")
    if nom not in variables:
        raise VariableEnvironnementManquante(
            f"Variable d'environnement absente : {nom}. Aucune valeur par défaut ni aucun repli "
            "sur un autre environnement n'est appliqué ; compléter le jeu de valeurs de VL_HRIS."
        )
    valeur = variables[nom]
    if valeur is None or not str(valeur).strip():
        raise VariableEnvironnementManquante(
            f"Variable d'environnement vide : {nom}. Compléter le jeu de valeurs actif de VL_HRIS."
        )
    return str(valeur).strip()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialiser_variables(variables_env: Optional[str]) -> Dict[str, str]:
    """Analyse et mémorise les variables d'environnement transmises au notebook."""
    variables = analyser_variables_env(variables_env)
    _VARIABLES_ENV.clear()
    _VARIABLES_ENV.update(variables)
    return dict(_VARIABLES_ENV)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_variable(nom: str) -> str:
    """
    Lit une variable d'environnement mémorisée ; lève une exception explicite si elle est absente ou vide.

    C'est le seul point de lecture des variables d'environnement dans les notebooks HRIS.
    """
    if not _VARIABLES_ENV:
        raise VariableEnvironnementManquante(
            "Les variables d'environnement ne sont pas initialisées : appeler initialiser_contexte_hris."
        )
    return lire_variable_depuis(_VARIABLES_ENV, nom)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_contexte_execution() -> Dict[str, str]:
    """Lit le contexte d'exécution Fabric (workspace, notebook, activité) sans valeur codée en dur."""
    contexte = notebookutils.runtime.context
    correspondance = {
        "workspace_nom": "currentWorkspaceName",
        "workspace_id": "currentWorkspaceId",
        "notebook_nom": "currentNotebookName",
        "notebook_id": "currentNotebookId",
        "activite_id": "activityId",
    }
    valeurs = {cle: contexte.get(source) for cle, source in correspondance.items()}
    manquantes = sorted(cle for cle, valeur in valeurs.items() if not valeur)
    if manquantes:
        raise ErreurConfiguration("Contexte d'exécution Fabric incomplet : " + ", ".join(manquantes))
    return {cle: str(valeur) for cle, valeur in valeurs.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extraire_marqueurs_environnement(nom_workspace: Optional[str]) -> Set[str]:
    """Renvoie les marqueurs DEV, UAT ou PRD présents comme mots dans un nom de workspace."""
    jetons = {jeton for jeton in re.split(r"[^A-Z0-9]+", (nom_workspace or "").upper()) if jeton}
    return jetons.intersection(ENVIRONNEMENTS_AUTORISES)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def verifier_coherence_environnement(env_nom: str, nom_workspace: str) -> str:
    """
    Vérifie l'environnement résolu et l'absence de croisement avec le workspace courant.

    Raises:
        ErreurGardeEnvironnement: si env_nom n'est pas exactement DEV, UAT ou PRD, ou si le nom du
        workspace porte le marqueur d'un autre environnement.
    """
    if env_nom not in ENVIRONNEMENTS_AUTORISES:
        raise ErreurGardeEnvironnement(
            f"env_nom vaut {env_nom!r} ; valeurs admises, casse exacte : DEV, UAT, PRD."
        )
    autres = sorted(extraire_marqueurs_environnement(nom_workspace) - {env_nom})
    if autres:
        raise ErreurGardeEnvironnement(
            f"Le workspace courant porte le marqueur {', '.join(autres)} alors que l'environnement "
            f"résolu est {env_nom}. Exécution refusée pour éviter tout croisement d'environnements."
        )
    return env_nom

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def publication_externe_autorisee(env_nom: str, nom_workspace: str) -> bool:
    """Indique si une publication externe est permise : environnement PRD exact et workspace marqué PRD."""
    return env_nom == MARQUEUR_PRD and MARQUEUR_PRD in extraire_marqueurs_environnement(nom_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def exiger_contexte() -> Dict[str, str]:
    """Renvoie le contexte HRIS initialisé ou lève une erreur explicite."""
    if not _CONTEXTE_HRIS:
        raise ErreurConfiguration(
            "Contexte HRIS non initialisé : appeler initialiser_contexte_hris avant tout accès au Lakehouse."
        )
    return dict(_CONTEXTE_HRIS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialiser_contexte_hris(variables_env: Optional[str]) -> Dict[str, str]:
    """
    Initialise les variables d'environnement et le contexte d'exécution du notebook courant.

    Returns:
        Dictionnaire contenant env_nom, lakehouse_nom et le contexte Fabric du workspace courant.
    """
    initialiser_variables(variables_env)
    contexte_runtime = lire_contexte_execution()
    env_nom = verifier_coherence_environnement(lire_variable("env_nom"), contexte_runtime["workspace_nom"])
    lakehouse_nom = lire_variable("env_lakehouse_nom")
    _CONTEXTE_HRIS.clear()
    _CONTEXTE_HRIS.update(contexte_runtime)
    _CONTEXTE_HRIS.update({"env_nom": env_nom, "lakehouse_nom": lakehouse_nom})
    return dict(_CONTEXTE_HRIS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def valider_nom_table(nom: str) -> str:
    """Contrôle qu'un nom de table respecte la convention snake_case à préfixe de zone."""
    if not nom or not _MOTIF_NOM_TABLE.match(nom):
        raise ErreurConfiguration(
            f"Nom de table invalide : {nom!r}. Préfixes admis : brz_, stg_, slv_, gld_, rjt_, hst_, cfg_, ctl_."
        )
    return nom

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def nom_complet_table(nom: str, workspace_nom: str, lakehouse_nom: str, schema: str = SCHEMA_LAKEHOUSE_HRIS) -> str:
    """Construit le nom complet à quatre parties d'une table du Lakehouse, protégé par des accents graves."""
    def proteger(identifiant: str) -> str:
        return "`" + str(identifiant).replace("`", "``") + "`"

    return ".".join(proteger(partie) for partie in (workspace_nom, lakehouse_nom, schema, valider_nom_table(nom)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table(nom: str) -> str:
    """Renvoie le nom complet d'une table du Lakehouse de l'environnement courant."""
    contexte = exiger_contexte()
    return nom_complet_table(nom, contexte["workspace_nom"], contexte["lakehouse_nom"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_table(nom: str) -> DataFrame:
    """Lit une table Delta du Lakehouse de l'environnement courant."""
    return spark.table(table(nom))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_existe(nom: str) -> bool:
    """Indique si une table existe dans le Lakehouse courant, sans déclencher de lecture de données."""
    try:
        _ = spark.table(table(nom)).schema
        return True
    except AnalysisException as exc:
        message = str(exc)
        if "TABLE_OR_VIEW_NOT_FOUND" in message or "not found" in message.lower():
            return False
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_chemin_relatif(
    zone: str,
    flux_id: Optional[str] = None,
    date_traitement: Optional[date] = None,
    nom_fichier: Optional[str] = None,
) -> str:
    """
    Construit un chemin relatif de la partie Files du Lakehouse : Files/<zone>/<flux>/<aaaammjj>/<fichier>.

    La zone reference n'est ni datée ni rattachée à un flux. Remplace les datasets Synapse de dossiers datés.
    """
    if zone not in ZONES_FICHIERS:
        raise ErreurConfiguration(f"Zone de fichiers inconnue : {zone!r} ; zones admises : {', '.join(ZONES_FICHIERS)}.")
    parties = ["Files", zone]
    if zone != "reference":
        if not flux_id or not _MOTIF_IDENTIFIANT.match(flux_id):
            raise ErreurConfiguration(f"Identifiant de flux invalide pour un chemin de fichiers : {flux_id!r}.")
        if not isinstance(date_traitement, date):
            raise ErreurConfiguration("Une date de traitement est obligatoire pour une zone de fichiers datée.")
        parties += [flux_id, date_traitement.strftime("%Y%m%d")]
    if nom_fichier is not None:
        if not nom_fichier or "/" in nom_fichier or "\\" in nom_fichier or nom_fichier in (".", ".."):
            raise ErreurConfiguration(f"Nom de fichier invalide : {nom_fichier!r}.")
        parties.append(nom_fichier)
    return "/".join(parties)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_chemin_onelake(chemin_relatif: str, workspace_id: str, lakehouse_nom: str) -> str:
    """Construit l'adresse OneLake d'un chemin relatif du Lakehouse."""
    return f"abfss://{workspace_id}@{DOMAINE_ONELAKE}/{lakehouse_nom}.Lakehouse/{chemin_relatif.lstrip('/')}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def chemin_onelake(chemin_relatif: str) -> str:
    """Renvoie l'adresse OneLake d'un chemin relatif dans le Lakehouse de l'environnement courant."""
    contexte = exiger_contexte()
    return construire_chemin_onelake(chemin_relatif, contexte["workspace_id"], contexte["lakehouse_nom"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_chemin_adls(compte: str, conteneur: str, chemin: str) -> str:
    """Construit l'adresse d'un dossier du compte ADLS de transition à partir des variables d'environnement."""
    if not compte or not conteneur:
        raise ErreurConfiguration("Le compte et le conteneur ADLS sont obligatoires.")
    return f"abfss://{conteneur}@{compte}.{DOMAINE_ADLS}/{chemin.strip('/')}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resoudre_date_traitement(valeur: Optional[str]) -> date:
    """Convertit le paramètre date_traitement au format aaaa-mm-jj ; vide signifie la date du jour à Paris."""
    if valeur is None or not str(valeur).strip():
        return datetime.now(ZoneInfo(FUSEAU_HORAIRE_HRIS)).date()
    try:
        return datetime.strptime(str(valeur).strip(), "%Y-%m-%d").date()
    except ValueError:
        raise ErreurConfiguration(f"date_traitement invalide : {valeur!r} ; format attendu aaaa-mm-jj.") from None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyser_booleen(valeur: Any, nom: str) -> bool:
    """Convertit un paramètre textuel en booléen ; une valeur vide vaut faux, une valeur inconnue est refusée."""
    if isinstance(valeur, bool):
        return valeur
    texte = "" if valeur is None else str(valeur).strip().lower()
    if texte in ("", "false", "faux", "non", "no", "0"):
        return False
    if texte in ("true", "vrai", "oui", "yes", "1"):
        return True
    raise ErreurConfiguration(f"Valeur booléenne invalide pour {nom} : {valeur!r}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def maintenant_utc() -> datetime:
    """Renvoie l'horodatage courant en temps universel, avec fuseau explicite."""
    return datetime.now(timezone.utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def nettoyer_message_erreur(erreur: Any) -> str:
    """
    Produit un diagnostic exploitable et borné pour ctl_execution_etape.message_erreur.

    Les jetons porteurs et les signatures d'URL sont masqués ; le texte est tronqué.
    """
    if isinstance(erreur, BaseException):
        texte = f"{type(erreur).__name__}: {erreur}"
    else:
        texte = "" if erreur is None else str(erreur)
    texte = re.sub(r"\s+", " ", texte).strip()
    texte = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9\-\._~\+/]+=*", r"\1***", texte)
    texte = re.sub(r"(?i)((?:sig|code|client_secret|password)=)[^&\s]+", r"\1***", texte)
    return texte[:TAILLE_MAX_MESSAGE_ERREUR] if texte else "Erreur sans message"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executer_avec_reprise_concurrence(operation: Callable[[], Any], description: str, tentatives: int = 4) -> Any:
    """
    Exécute une écriture Delta en la rejouant uniquement en cas de conflit de concurrence.

    Args:
        operation: Fonction sans argument réalisant l'écriture.
        description: Libellé journalisé en cas de reprise.
        tentatives: Nombre maximal de tentatives.
    """
    marqueurs = (
        "ConcurrentAppendException", "ConcurrentDeleteReadException", "ConcurrentDeleteDeleteException",
        "ConcurrentTransactionException", "MetadataChangedException", "ProtocolChangedException",
        "DELTA_CONCURRENT",
    )
    for numero in range(1, tentatives + 1):
        try:
            return operation()
        except Exception as exc:
            signature = f"{type(exc).__name__} {str(exc)[:1000]}"
            if numero == tentatives or not any(marqueur in signature for marqueur in marqueurs):
                raise
            obtenir_journal().warning(
                f"Conflit d'écriture Delta concurrent sur {description} ; tentative {numero + 1}/{tentatives}."
            )
            time.sleep(2 ** numero)
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= DEFINITION DES TABLES DE SOCLE =============
# Chaque colonne est décrite par (nom, type Spark SQL, nullable). La partition n'est posée que là où le plan la justifie.
DEFINITIONS_TABLES_SOCLE: Dict[str, Dict[str, Any]] = {
    "cfg_flux": {"partition": None, "colonnes": [
        ("flux_id", "STRING", False), ("actif", "BOOLEAN", False), ("domaine", "STRING", True),
        ("type_chargement", "STRING", True), ("source_type", "STRING", True), ("source_ref", "STRING", True),
        ("format_source", "STRING", True), ("destination_table", "STRING", True), ("notebook", "STRING", True),
        ("parametres", "STRING", True), ("ordre", "INT", True), ("parallelisme_max", "INT", True),
        ("watermark_colonne", "STRING", True), ("watermark_strategie", "STRING", True),
        ("strategie_ecriture", "STRING", True), ("cles_fusion", "STRING", True), ("retries", "INT", True),
        ("timeout_minutes", "INT", True), ("seuil_rejet_pct", "DECIMAL(5,2)", True),
        ("politique_archivage", "STRING", True), ("politique_rejet", "STRING", True),
        ("criticite", "STRING", True), ("regle_reprise", "STRING", True), ("date_maj", "TIMESTAMP", True),
    ]},
    "cfg_dependance": {"partition": None, "colonnes": [
        ("flux_id", "STRING", False), ("depend_de_flux_id", "STRING", False),
        ("type_dependance", "STRING", False), ("date_maj", "TIMESTAMP", True),
    ]},
    "cfg_mapping": {"partition": None, "colonnes": [
        ("domaine", "STRING", False), ("code_source", "STRING", False), ("attribut_cible", "STRING", False),
        ("code_cible", "STRING", True), ("libelle", "STRING", True), ("date_debut", "DATE", False),
        ("date_fin", "DATE", True), ("fichier_source", "STRING", True), ("execution_id", "STRING", True),
        ("date_maj", "TIMESTAMP", True),
    ]},
    "cfg_qualite": {"partition": None, "colonnes": [
        ("flux_id", "STRING", False), ("regle_id", "STRING", False), ("type_regle", "STRING", False),
        ("colonne", "STRING", False), ("expression", "STRING", False), ("motif_rejet", "STRING", True),
        ("bloquant", "BOOLEAN", False), ("date_maj", "TIMESTAMP", True),
    ]},
    "ctl_execution": {"partition": None, "colonnes": [
        ("execution_id", "STRING", False), ("environnement", "STRING", False), ("workspace_nom", "STRING", True),
        ("declencheur", "STRING", True), ("parametres", "STRING", True), ("plan_execution", "STRING", True),
        ("version_code", "STRING", True), ("debut", "TIMESTAMP", True), ("fin", "TIMESTAMP", True),
        ("statut", "STRING", True), ("message", "STRING", True), ("execution_id_reprise", "STRING", True),
        ("date_traitement", "DATE", True),
    ]},
    "ctl_execution_etape": {"partition": "date_traitement", "colonnes": [
        ("execution_id", "STRING", False), ("environnement", "STRING", False), ("flux_id", "STRING", False),
        ("etape", "STRING", False), ("tentative", "INT", False), ("statut", "STRING", False),
        ("debut", "TIMESTAMP", True), ("fin", "TIMESTAMP", True), ("duree_secondes", "DOUBLE", True),
        ("lignes_lues", "BIGINT", True), ("lignes_ecrites", "BIGINT", True), ("lignes_rejetees", "BIGINT", True),
        ("watermark", "STRING", True), ("message_erreur", "STRING", True), ("details", "STRING", True),
        ("notebook", "STRING", True), ("activite_id", "STRING", True), ("horodatage", "TIMESTAMP", False),
        ("date_traitement", "DATE", False),
    ]},
    "ctl_qualite": {"partition": None, "colonnes": [
        ("execution_id", "STRING", False), ("environnement", "STRING", False), ("flux_id", "STRING", False),
        ("date_traitement", "DATE", False), ("regle_id", "STRING", False), ("type_regle", "STRING", True),
        ("colonne", "STRING", True), ("nb_lignes_controlees", "BIGINT", True),
        ("nb_lignes_en_echec", "BIGINT", True), ("bloquant", "BOOLEAN", True), ("statut", "STRING", True),
        ("commentaire", "STRING", True), ("horodatage", "TIMESTAMP", True),
    ]},
    "ctl_fichier_traite": {"partition": None, "colonnes": [
        ("flux_id", "STRING", False), ("nom_fichier", "STRING", False), ("chemin", "STRING", True),
        ("taille_octets", "BIGINT", True), ("empreinte_sha256", "STRING", False), ("horodatage", "TIMESTAMP", True),
        ("execution_id", "STRING", False), ("date_traitement", "DATE", True), ("statut", "STRING", False),
        ("motif", "STRING", True),
    ]},
    "gld_resume": {"partition": None, "colonnes": [
        ("execution_id", "STRING", False), ("environnement", "STRING", False), ("flux_id", "STRING", False),
        ("date_traitement", "DATE", False), ("dt_resume", "DATE", True), ("cd_source", "STRING", True),
        ("cd_target", "STRING", True), ("mt_lignes_inserts", "INT", True), ("mt_lignes_rejets", "INT", True),
        ("mt_lignes_lues", "INT", True), ("pc_lignes_rejets", "DECIMAL(12,4)", True),
        ("ds_type_rejets", "STRING", True), ("ds_rejets", "STRING", True), ("indicateurs", "STRING", True),
        ("horodatage", "TIMESTAMP", True),
    ]},
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def schema_table_socle(nom: str) -> T.StructType:
    """Renvoie le schéma Spark d'une table de socle à partir de sa définition."""
    if nom not in DEFINITIONS_TABLES_SOCLE:
        raise ErreurConfiguration(f"Table de socle inconnue : {nom}.")
    correspondance_types = {
        "STRING": T.StringType(), "BOOLEAN": T.BooleanType(), "INT": T.IntegerType(), "BIGINT": T.LongType(),
        "DOUBLE": T.DoubleType(), "DATE": T.DateType(), "TIMESTAMP": T.TimestampType(),
    }
    champs = []
    for colonne, type_sql, nullable in DEFINITIONS_TABLES_SOCLE[nom]["colonnes"]:
        decimal_match = re.match(r"^DECIMAL\((\d+),(\d+)\)$", type_sql)
        type_spark = T.DecimalType(int(decimal_match.group(1)), int(decimal_match.group(2))) if decimal_match else correspondance_types[type_sql]
        champs.append(T.StructField(colonne, type_spark, nullable))
    return T.StructType(champs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialiser_socle() -> List[str]:
    """
    Crée de façon idempotente le schéma et les tables de socle, puis ajoute les colonnes manquantes.

    Rejouer cette fonction ne modifie ni ne duplique aucune donnée.
    """
    contexte = exiger_contexte()
    schema_complet = ".".join(
        "`" + partie.replace("`", "``") + "`"
        for partie in (contexte["workspace_nom"], contexte["lakehouse_nom"], SCHEMA_LAKEHOUSE_HRIS)
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_complet}")
    tables_traitees = []
    for nom, definition in DEFINITIONS_TABLES_SOCLE.items():
        colonnes_sql = ", ".join(
            f"`{colonne}` {type_sql}{'' if nullable else ' NOT NULL'}"
            for colonne, type_sql, nullable in definition["colonnes"]
        )
        partition = f" PARTITIONED BY (`{definition['partition']}`)" if definition["partition"] else ""
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table(nom)} ({colonnes_sql}) USING DELTA{partition}")
        existantes = {champ.name for champ in spark.table(table(nom)).schema.fields}
        manquantes = [
            f"`{colonne}` {type_sql}"
            for colonne, type_sql, _ in definition["colonnes"] if colonne not in existantes
        ]
        if manquantes:
            spark.sql(f"ALTER TABLE {table(nom)} ADD COLUMNS ({', '.join(manquantes)})")
        tables_traitees.append(nom)
    return tables_traitees

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyser_json_objet(texte: Optional[str], description: str) -> Dict[str, Any]:
    """Analyse un texte JSON qui doit représenter un objet ; vide renvoie un objet vide."""
    if texte is None or not str(texte).strip():
        return {}
    try:
        valeur = json.loads(texte)
    except json.JSONDecodeError as exc:
        raise ErreurConfiguration(f"{description} : JSON invalide (position {exc.pos}).") from None
    if not isinstance(valeur, dict):
        raise ErreurConfiguration(f"{description} : un objet JSON est attendu.")
    return valeur

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def exiger_parametre(parametres: Dict[str, Any], cle: str, description: str) -> Any:
    """Renvoie un paramètre de flux obligatoire ou lève une erreur de configuration explicite."""
    valeur = parametres.get(cle)
    if valeur is None or (isinstance(valeur, str) and not valeur.strip()) or (isinstance(valeur, (list, dict)) and not valeur):
        raise ErreurConfiguration(f"Paramètre obligatoire absent ou vide : {cle} ({description}).")
    return valeur

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_tous_flux() -> List[Dict[str, Any]]:
    """Lit toutes les lignes de cfg_flux et décode leur colonne parametres."""
    flux = []
    for ligne in lire_table("cfg_flux").collect():
        valeurs = ligne.asDict()
        valeurs["parametres"] = analyser_json_objet(valeurs.get("parametres"), f"cfg_flux.parametres de {valeurs['flux_id']}")
        flux.append(valeurs)
    identifiants = [valeurs["flux_id"] for valeurs in flux]
    doublons = sorted({identifiant for identifiant in identifiants if identifiants.count(identifiant) > 1})
    if doublons:
        raise ErreurConfiguration("Identifiants de flux en double dans cfg_flux : " + ", ".join(doublons))
    return flux

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_flux(flux_id: str, exiger_actif: bool = True) -> Dict[str, Any]:
    """Lit la configuration d'un flux ; un flux inconnu ou inactif provoque une erreur explicite."""
    correspondants = [valeurs for valeurs in lire_tous_flux() if valeurs["flux_id"] == flux_id]
    if not correspondants:
        raise ErreurConfiguration(f"Flux inconnu dans cfg_flux : {flux_id!r}.")
    flux = correspondants[0]
    if exiger_actif and not flux.get("actif"):
        raise ErreurConfiguration(f"Le flux {flux_id} est inactif dans cfg_flux.")
    return flux

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_dependances() -> List[Dict[str, Any]]:
    """Lit le graphe d'exécution déclaré dans cfg_dependance."""
    return [ligne.asDict() for ligne in lire_table("cfg_dependance").collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_regles_qualite(flux_id: str) -> List[Dict[str, Any]]:
    """Lit les règles de cfg_qualite d'un flux, triées par identifiant, avec leur expression décodée."""
    regles = []
    for ligne in lire_table("cfg_qualite").where(F.col("flux_id") == flux_id).orderBy("regle_id").collect():
        regle = ligne.asDict()
        regle["expression"] = analyser_json_objet(regle["expression"], f"cfg_qualite.expression de {regle['regle_id']}")
        regles.append(regle)
    return regles

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_mapping_actif(date_reference: date) -> DataFrame:
    """Renvoie les correspondances TalentSoft vers ADP actives à la date de référence."""
    return (
        lire_table("cfg_mapping")
        .where(F.col("date_debut") <= F.lit(date_reference))
        .where(F.col("date_fin").isNull() | (F.col("date_fin") >= F.lit(date_reference)))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ecrire_ligne_etape(etat: Dict[str, Any]) -> None:
    """Ajoute une ligne à ctl_execution_etape par simple ajout, sans conflit possible entre flux parallèles."""
    if etat.get("statut") not in STATUTS_ETAPE:
        raise ErreurConfiguration(f"Statut d'étape inconnu : {etat.get('statut')!r}.")
    schema = schema_table_socle("ctl_execution_etape")
    ligne = {champ.name: etat.get(champ.name) for champ in schema.fields}
    ligne["horodatage"] = ligne.get("horodatage") or maintenant_utc()
    donnees = spark.createDataFrame([tuple(ligne[champ.name] for champ in schema.fields)], schema)
    executer_avec_reprise_concurrence(
        lambda: donnees.write.format("delta").mode("append").saveAsTable(table("ctl_execution_etape")),
        "ctl_execution_etape",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prochaine_tentative(execution_id: str, flux_id: str, etape: str) -> int:
    """Calcule le numéro de la prochaine tentative d'une étape pour une exécution donnée."""
    maximum = (
        lire_table("ctl_execution_etape")
        .where((F.col("execution_id") == execution_id) & (F.col("flux_id") == flux_id) & (F.col("etape") == etape))
        .agg(F.max("tentative").alias("maximum"))
        .collect()[0]["maximum"]
    )
    return 1 if maximum is None else int(maximum) + 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def demarrer_etape(
    execution_id: str,
    flux_id: str,
    etape: str,
    date_traitement: date,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Journalise le début d'une tentative d'étape dans ctl_execution_etape avec le statut En cours.

    Returns:
        L'état de l'étape, à transmettre à terminer_etape.
    """
    contexte = exiger_contexte()
    etat = {
        "execution_id": execution_id,
        "environnement": contexte["env_nom"],
        "flux_id": flux_id,
        "etape": etape,
        "tentative": prochaine_tentative(execution_id, flux_id, etape),
        "statut": STATUT_EN_COURS,
        "debut": maintenant_utc(),
        "date_traitement": date_traitement,
        "notebook": contexte["notebook_nom"],
        "activite_id": contexte["activite_id"],
        "details": json.dumps(details, ensure_ascii=False, default=str) if details else None,
    }
    ecrire_ligne_etape(dict(etat, horodatage=etat["debut"]))
    return etat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def terminer_etape(
    etat: Dict[str, Any],
    statut: str,
    lignes_lues: Optional[int] = None,
    lignes_ecrites: Optional[int] = None,
    lignes_rejetees: Optional[int] = None,
    message_erreur: Optional[str] = None,
    watermark: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Journalise la fin d'une tentative d'étape : statut, durée, volumes et diagnostic réel.

    Returns:
        L'état final de l'étape.
    """
    fin = maintenant_utc()
    final = dict(etat)
    final.update({
        "statut": statut,
        "fin": fin,
        "duree_secondes": round((fin - etat["debut"]).total_seconds(), 3),
        "lignes_lues": None if lignes_lues is None else int(lignes_lues),
        "lignes_ecrites": None if lignes_ecrites is None else int(lignes_ecrites),
        "lignes_rejetees": None if lignes_rejetees is None else int(lignes_rejetees),
        "message_erreur": None if message_erreur is None else nettoyer_message_erreur(message_erreur),
        "watermark": watermark,
        "horodatage": fin,
    })
    if details is not None:
        final["details"] = json.dumps(details, ensure_ascii=False, default=str)
    ecrire_ligne_etape(final)
    return final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_etapes_consolidees(execution_id: Optional[str] = None, date_traitement: Optional[date] = None) -> DataFrame:
    """Renvoie une ligne par tentative d'étape : la plus récente, c'est-à-dire la ligne de fin si elle existe."""
    etapes = lire_table("ctl_execution_etape")
    if execution_id:
        etapes = etapes.where(F.col("execution_id") == execution_id)
    if date_traitement:
        etapes = etapes.where(F.col("date_traitement") == F.lit(date_traitement))
    fenetre = Window.partitionBy("execution_id", "flux_id", "etape", "tentative").orderBy(
        F.col("fin").desc_nulls_last(), F.col("horodatage").desc()
    )
    return etapes.withColumn("_rang", F.row_number().over(fenetre)).where(F.col("_rang") == 1).drop("_rang")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def etape_deja_reussie(execution_id: str, flux_id: str, etape: str) -> bool:
    """Indique si une étape a déjà abouti pour une exécution : base de l'anti-doublon et de la reprise."""
    return (
        lire_etapes_consolidees(execution_id=execution_id)
        .where((F.col("flux_id") == flux_id) & (F.col("etape") == etape) & F.col("statut").isin(*STATUTS_REUSSIS))
        .limit(1)
        .count()
        > 0
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ecrire_table(
    donnees: DataFrame,
    nom: str,
    mode: str,
    predicat_remplacement: Optional[str] = None,
    partition: Optional[Sequence[str]] = None,
    evolution_schema: bool = False,
) -> None:
    """
    Écrit une table Delta du Lakehouse courant de façon transactionnelle.

    Args:
        donnees: Données à écrire.
        nom: Nom court de la table, à préfixe de zone.
        mode: append, overwrite (remplacement complet) ou remplacer (remplacement du seul périmètre du prédicat).
        predicat_remplacement: Prédicat replaceWhere obligatoire en mode remplacer.
        partition: Colonnes de partition éventuelles.
        evolution_schema: Autorise l'ajout de colonnes nouvelles.
    """
    cible = table(nom)
    if mode == "append":
        ecrivain = donnees.write.format("delta").mode("append")
        if evolution_schema:
            ecrivain = ecrivain.option("mergeSchema", "true")
    elif mode == "overwrite":
        ecrivain = donnees.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    elif mode == "remplacer":
        if not predicat_remplacement:
            raise ErreurConfiguration(f"Un prédicat de remplacement est obligatoire pour réécrire {nom}.")
        ecrivain = donnees.write.format("delta").mode("overwrite").option("replaceWhere", predicat_remplacement)
        if evolution_schema:
            ecrivain = ecrivain.option("mergeSchema", "true")
    else:
        raise ErreurConfiguration(f"Mode d'écriture inconnu : {mode!r}.")
    if partition:
        ecrivain = ecrivain.partitionBy(*partition)
    executer_avec_reprise_concurrence(lambda: ecrivain.saveAsTable(cible), f"écriture de {nom}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def litteral_sql(valeur: Any) -> str:
    """Convertit une valeur Python simple en littéral Spark SQL échappé."""
    if valeur is None:
        return "NULL"
    if isinstance(valeur, bool):
        return "true" if valeur else "false"
    if isinstance(valeur, (int, float, Decimal)):
        return str(valeur)
    if isinstance(valeur, datetime):
        return f"TIMESTAMP'{valeur.isoformat(sep=' ')}'"
    if isinstance(valeur, date):
        return f"DATE'{valeur.isoformat()}'"
    texte = str(valeur).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{texte}'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def remplacer_perimetre(donnees: DataFrame, nom: str, filtres: Dict[str, Any], partition: Optional[Sequence[str]] = None) -> None:
    """
    Remplace atomiquement les lignes d'une table correspondant à des égalités de colonnes.

    Rejouer la même exécution remplace ses propres lignes sans toucher celles des autres exécutions.
    """
    if not filtres:
        raise ErreurConfiguration(f"Aucun filtre de remplacement fourni pour {nom}.")
    predicat = " AND ".join(f"`{colonne}` = {litteral_sql(valeur)}" for colonne, valeur in filtres.items())
    ecrire_table(donnees, nom, "remplacer", predicat_remplacement=predicat, partition=partition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fusionner_table(donnees: DataFrame, nom: str, cles: Sequence[str]) -> None:
    """Fusionne des données dans une table Delta par clés, en mise à jour et insertion, de façon idempotente."""
    if not cles:
        raise ErreurConfiguration(f"Aucune clé de fusion fournie pour {nom}.")
    if not table_existe(nom):
        ecrire_table(donnees, nom, "append")
        return
    vue = f"hris_source_{uuid.uuid4().hex}"
    donnees.createOrReplaceTempView(vue)
    condition = " AND ".join(f"cible.`{cle}` <=> source.`{cle}`" for cle in cles)
    requete = (
        f"MERGE INTO {table(nom)} AS cible USING {vue} AS source ON {condition} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    try:
        executer_avec_reprise_concurrence(lambda: spark.sql(requete), f"fusion dans {nom}")
    finally:
        spark.catalog.dropTempView(vue)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def historiser_scd2(
    donnees: DataFrame,
    nom_table_historique: str,
    cles: Sequence[str],
    colonnes_suivies: Sequence[str],
    date_effet: date,
    execution_id: str,
) -> int:
    """
    Historise des données selon une stratégie SCD de type 2, option de cumul à retenir explicitement par configuration.

    Une version courante dont l'empreinte change est clôturée la veille de la date d'effet et une nouvelle
    version est insérée. Rejouer la même exécution ne crée aucune version supplémentaire.

    Returns:
        Le nombre de lignes présentées à l'historisation.
    """
    valider_nom_table(nom_table_historique)
    doublons = donnees.groupBy(*cles).count().where(F.col("count") > 1).limit(1).count()
    if doublons:
        raise ErreurDonnees(
            f"Clé d'historisation non unique pour {nom_table_historique} ({', '.join(cles)}) : "
            "la stratégie SCD doit être précisée avant d'historiser ces données."
        )
    empreinte = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in colonnes_suivies]), 256)
    source = (
        donnees.withColumn("hst_empreinte", empreinte)
        .withColumn("hst_date_debut", F.lit(date_effet).cast("date"))
        .withColumn("hst_date_fin", F.lit(None).cast("date"))
        .withColumn("hst_est_courant", F.lit(True))
        .withColumn("hst_execution_id", F.lit(execution_id))
    )
    volume = source.count()
    if not table_existe(nom_table_historique):
        ecrire_table(source, nom_table_historique, "append")
        return volume
    cle_source = F.concat_ws("#~#", *[F.coalesce(F.col(c).cast("string"), F.lit("#NULL#")) for c in cles])
    courant = lire_table(nom_table_historique).where(F.col("hst_est_courant")).select(
        *cles, F.col("hst_empreinte").alias("_empreinte_courante")
    )
    nouvelles_versions = (
        source.join(courant, on=[source[c].eqNullSafe(courant[c]) for c in cles], how="inner")
        .where(F.col("hst_empreinte") != F.col("_empreinte_courante"))
        .select(*[source[c] for c in source.columns])
        .withColumn("_cle_fusion", F.lit(None).cast("string"))
    )
    preparation = source.withColumn("_cle_fusion", cle_source).unionByName(nouvelles_versions)
    vue = f"hris_scd_{uuid.uuid4().hex}"
    preparation.createOrReplaceTempView(vue)
    cle_cible = "concat_ws('#~#', " + ", ".join(f"coalesce(cast(cible.`{c}` as string), '#NULL#')" for c in cles) + ")"
    colonnes = ", ".join(f"`{c}`" for c in source.columns)
    valeurs = ", ".join(f"source.`{c}`" for c in source.columns)
    requete = (
        f"MERGE INTO {table(nom_table_historique)} AS cible USING {vue} AS source "
        f"ON {cle_cible} = source._cle_fusion AND cible.hst_est_courant = true "
        "WHEN MATCHED AND cible.hst_empreinte <> source.hst_empreinte THEN UPDATE SET "
        f"cible.hst_est_courant = false, cible.hst_date_fin = {litteral_sql(date_effet - timedelta(days=1))} "
        f"WHEN NOT MATCHED THEN INSERT ({colonnes}) VALUES ({valeurs})"
    )
    try:
        executer_avec_reprise_concurrence(lambda: spark.sql(requete), f"historisation dans {nom_table_historique}")
    finally:
        spark.catalog.dropTempView(vue)
    return volume

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dedoublonner_lot(lot: DataFrame, cles: Sequence[str], description: str) -> DataFrame:
    """
    Supprime les doublons exacts d'un lot et refuse des lignes contradictoires partageant la même clé.

    Aucune ligne n'est choisie arbitrairement : la règle de déduplication d'un conflit reste à confirmer par le métier.
    """
    if not cles:
        raise ErreurConfiguration(f"{description} : aucune clé de consolidation déclarée.")
    distinct = lot.dropDuplicates()
    conflits = distinct.groupBy(*cles).count().where(F.col("count") > 1).count()
    if conflits:
        raise ErreurDonnees(
            f"{description} : {conflits} clé(s) ({', '.join(cles)}) portent des valeurs contradictoires dans le lot ; "
            "la règle de déduplication est à confirmer."
        )
    return distinct

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def consolider_upsert(
    lot: DataFrame,
    nom_table_cumul: str,
    cles: Sequence[str],
    colonnes_metier: Sequence[str],
    execution_id: str,
    date_traitement: date,
    horodatage_lot: datetime,
) -> Dict[str, int]:
    """
    Consolide un lot validé dans une table cumulative par upsert exclusivement, sur le modèle d'une SCD de type 1.

    Une clé nouvelle est insérée ; une clé existante n'est mise à jour que si ses valeurs changent et si le lot n'est
    pas plus ancien que celui qui l'a écrite ; une clé absente du lot est conservée. Il n'y a ni suppression, ni
    remplacement global, ni historique des anciennes valeurs. Rejouer le même lot ne modifie rien.

    Returns:
        Les nombres de lignes insérées, mises à jour, inchangées et ignorées car issues d'un lot plus ancien.
    """
    manquantes = [c for c in list(cles) + list(colonnes_metier) if c not in lot.columns]
    if manquantes:
        raise ErreurConfiguration(f"Colonnes absentes du lot pour {nom_table_cumul} : {', '.join(manquantes)}.")
    empreinte = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in colonnes_metier]), 256)
    source = (
        lot.select(*colonnes_metier)
        .withColumn("cumul_empreinte", empreinte)
        .withColumn("cumul_execution_id", F.lit(execution_id))
        .withColumn("cumul_date_traitement", F.lit(date_traitement).cast("date"))
        .withColumn("cumul_horodatage_lot", F.lit(horodatage_lot).cast("timestamp"))
        .withColumn("cumul_date_insertion", F.lit(horodatage_lot).cast("timestamp"))
        .withColumn("cumul_date_maj", F.lit(horodatage_lot).cast("timestamp"))
    )
    if not table_existe(nom_table_cumul):
        volume = source.count()
        ecrire_table(source, nom_table_cumul, "append")
        return {"inserees": volume, "mises_a_jour": 0, "inchangees": 0, "ignorees_plus_anciennes": 0}

    cible = lire_table(nom_table_cumul)
    differentes = sorted(set(source.columns) ^ set(cible.columns))
    if differentes:
        raise ErreurConfiguration(f"Structure de {nom_table_cumul} différente du lot : {', '.join(differentes)}.")
    existant = cible.select(*cles, "cumul_empreinte", "cumul_date_traitement", "cumul_horodatage_lot").alias("c")
    jointure = source.alias("s").join(existant, on=[F.col(f"s.{k}").eqNullSafe(F.col(f"c.{k}")) for k in cles], how="left")
    trouve = F.col("c.cumul_empreinte").isNotNull()
    change = F.col("s.cumul_empreinte") != F.col("c.cumul_empreinte")
    recent = (F.col("s.cumul_date_traitement") > F.col("c.cumul_date_traitement")) | (
        (F.col("s.cumul_date_traitement") == F.col("c.cumul_date_traitement"))
        & (F.col("s.cumul_horodatage_lot") >= F.col("c.cumul_horodatage_lot"))
    )
    bilan = jointure.agg(
        F.sum(F.when(~trouve, 1).otherwise(0)).alias("inserees"),
        F.sum(F.when(trouve & change & recent, 1).otherwise(0)).alias("mises_a_jour"),
        F.sum(F.when(trouve & ~change, 1).otherwise(0)).alias("inchangees"),
        F.sum(F.when(trouve & change & ~recent, 1).otherwise(0)).alias("ignorees_plus_anciennes"),
    ).collect()[0].asDict()

    vue = f"hris_cumul_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(vue)
    condition = " AND ".join(f"cible.`{k}` <=> source.`{k}`" for k in cles)
    plus_recent = (
        "(source.cumul_date_traitement > cible.cumul_date_traitement OR (source.cumul_date_traitement = "
        "cible.cumul_date_traitement AND source.cumul_horodatage_lot >= cible.cumul_horodatage_lot))"
    )
    mises_a_jour = [f"cible.`{c}` = source.`{c}`" for c in colonnes_metier if c not in cles] + [
        f"cible.`{c}` = source.`{c}`"
        for c in ("cumul_empreinte", "cumul_execution_id", "cumul_date_traitement", "cumul_horodatage_lot", "cumul_date_maj")
    ]
    colonnes = ", ".join(f"`{c}`" for c in source.columns)
    valeurs = ", ".join(f"source.`{c}`" for c in source.columns)
    requete = (
        f"MERGE INTO {table(nom_table_cumul)} AS cible USING {vue} AS source ON {condition} "
        f"WHEN MATCHED AND cible.cumul_empreinte <> source.cumul_empreinte AND {plus_recent} "
        f"THEN UPDATE SET {', '.join(mises_a_jour)} "
        f"WHEN NOT MATCHED THEN INSERT ({colonnes}) VALUES ({valeurs})"
    )
    try:
        executer_avec_reprise_concurrence(lambda: spark.sql(requete), f"consolidation dans {nom_table_cumul}")
    finally:
        spark.catalog.dropTempView(vue)
    return {cle: int(valeur or 0) for cle, valeur in bilan.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ecraser_contenu_table(donnees: DataFrame, nom: str, metadonnees: Dict[str, Any]) -> str:
    """
    Remplace tout le contenu d'une table par INSERT OVERWRITE, sans la supprimer ni la recréer.

    Le nom, l'identifiant Delta, les propriétés et la structure de la table sont conservés ; une structure différente
    est refusée plutôt que modifiée. Les métadonnées du lot sont écrites dans le même commit Delta, ce qui identifie
    le dernier lot même lorsque la table est vide. Seule la première alimentation crée la table.

    Returns:
        creation ou ecrasement.
    """
    texte_metadonnees = json.dumps(metadonnees, ensure_ascii=False, sort_keys=True, default=str)
    if not table_existe(nom):
        ecrivain = donnees.write.format("delta").mode("append").option("userMetadata", texte_metadonnees)
        executer_avec_reprise_concurrence(lambda: ecrivain.saveAsTable(table(nom)), f"création de {nom}")
        return "creation"
    colonnes = [champ.name for champ in lire_table(nom).schema.fields]
    differentes = sorted(set(colonnes) ^ set(donnees.columns))
    if differentes:
        raise ErreurConfiguration(f"Structure de {nom} différente des données à écrire : {', '.join(differentes)}.")
    vue = f"hris_final_{uuid.uuid4().hex}"
    donnees.select(*colonnes).createOrReplaceTempView(vue)
    cle_configuration = "spark.databricks.delta.commitInfo.userMetadata"
    try:
        spark.conf.set(cle_configuration, texte_metadonnees)
        selection = ", ".join(f"`{c}`" for c in colonnes)
        spark.sql(f"INSERT OVERWRITE TABLE {table(nom)} SELECT {selection} FROM {vue}")
    finally:
        spark.conf.unset(cle_configuration)
        spark.catalog.dropTempView(vue)
    return "ecrasement"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_metadonnees_dernier_lot(nom: str) -> Optional[Dict[str, Any]]:
    """Lit, dans l'historique Delta d'une table, les métadonnées du dernier lot écrit par ecraser_contenu_table."""
    if not table_existe(nom):
        return None
    historique = spark.sql(f"DESCRIBE HISTORY {table(nom)}").select("version", "userMetadata").orderBy(F.col("version").desc()).collect()
    for ligne in historique:
        if not ligne["userMetadata"]:
            continue
        try:
            valeur = json.loads(ligne["userMetadata"])
        except json.JSONDecodeError:
            continue
        if isinstance(valeur, dict) and valeur.get("hris_lot"):
            return valeur
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def enregistrer_execution(valeurs: Dict[str, Any]) -> None:
    """Crée ou met à jour la ligne d'une exécution dans ctl_execution, en conservant les champs non fournis."""
    if not valeurs.get("execution_id"):
        raise ErreurConfiguration("execution_id est obligatoire pour journaliser une exécution.")
    schema = schema_table_socle("ctl_execution")
    existantes = lire_table("ctl_execution").where(F.col("execution_id") == valeurs["execution_id"]).collect()
    ligne = existantes[0].asDict() if existantes else {champ.name: None for champ in schema.fields}
    ligne.update({cle: valeur for cle, valeur in valeurs.items() if cle in ligne})
    donnees = spark.createDataFrame([tuple(ligne[champ.name] for champ in schema.fields)], schema)
    fusionner_table(donnees, "ctl_execution", ["execution_id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_execution(execution_id: str) -> Optional[Dict[str, Any]]:
    """Lit la ligne ctl_execution d'une exécution, ou None si elle n'existe pas."""
    lignes = lire_table("ctl_execution").where(F.col("execution_id") == execution_id).collect()
    return lignes[0].asDict() if lignes else None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def verifier_dependances_execution(execution_id: str, flux_id: str) -> Optional[str]:
    """
    Vérifie que les dépendances planifiées d'un flux ont abouti dans l'exécution courante.

    Le pipeline poursuit les vagues suivantes après un échec pour laisser avancer les flux indépendants : chaque flux
    vérifie donc ses propres dépendances et se déclare bloqué sans rien traiter si l'une d'elles n'a pas abouti.

    Returns:
        Le motif de blocage, ou None si le flux peut être traité ou s'il est lancé hors plan d'exécution.
    """
    execution = lire_execution(execution_id)
    if not execution or not execution.get("plan_execution"):
        return None
    dependances = json.loads(execution["plan_execution"]).get("dependances", {}).get(flux_id, [])
    if not dependances:
        return None
    statuts = {}
    principales = lire_etapes_consolidees(execution_id=execution_id).where(
        ~F.col("etape").contains(":") & F.col("flux_id").isin(*dependances)
    )
    for ligne in principales.orderBy("horodatage").collect():
        statuts[ligne["flux_id"]] = ligne["statut"]
    satisfaits = STATUTS_REUSSIS + (STATUT_IGNORE,)
    non_satisfaites = [d for d in dependances if statuts.get(d) not in satisfaits]
    if not non_satisfaites:
        return None
    return "Non traité : dépendance non aboutie (" + ", ".join(f"{d} : {statuts.get(d, 'non exécuté')}" for d in non_satisfaites) + ")."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_secret(nom_variable_secret: str) -> str:
    """
    Lit un secret du coffre de l'environnement courant à partir de la variable qui porte son nom.

    La valeur n'est jamais journalisée ni écrite ; seuls l'adresse du coffre et le nom du secret circulent.
    """
    adresse_coffre = lire_variable("env_kv_url")
    nom_secret = lire_variable(nom_variable_secret)
    valeur = notebookutils.credentials.getSecret(adresse_coffre, nom_secret)
    if valeur is None or not str(valeur).strip():
        raise ErreurConfiguration(f"Le secret {nom_secret}, référencé par {nom_variable_secret}, est vide ou illisible.")
    return valeur

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ouvrir_session_sftp() -> Tuple[Any, Any]:
    """
    Ouvre une session SFTP avec l'hôte, le port, le compte et le secret de l'environnement courant.

    Comme le service lié Synapse d'origine, la clé d'hôte n'est pas vérifiée : ce point est à durcir dès
    que son empreinte sera fournie.

    Returns:
        Le transport et le client SFTP, à fermer par fermer_session_sftp.
    """
    if paramiko is None:
        raise ErreurConfiguration(
            "La bibliothèque paramiko est absente du runtime Spark : l'échange SFTP est impossible."
        )
    hote = lire_variable("env_sftp_hote")
    try:
        port = int(lire_variable("env_sftp_port"))
    except ValueError:
        raise ErreurConfiguration("La variable env_sftp_port doit être un entier.") from None
    utilisateur = lire_variable("env_sftp_utilisateur")
    transport = paramiko.Transport((hote, port))
    try:
        transport.connect(username=utilisateur, password=lire_secret("env_secret_sftp"))
        client = paramiko.SFTPClient.from_transport(transport)
    except Exception:
        transport.close()
        raise
    return transport, client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fermer_session_sftp(transport: Any, client: Any) -> None:
    """Ferme une session SFTP sans masquer une erreur antérieure."""
    for ressource in (client, transport):
        try:
            if ressource is not None:
                ressource.close()
        except Exception:
            pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lister_fichiers_sftp(client: Any, dossier: str) -> List[str]:
    """Liste les seuls fichiers réguliers d'un dossier SFTP, triés par nom."""
    return sorted(
        entree.filename for entree in client.listdir_attr(dossier)
        if entree.st_mode is not None and stat.S_ISREG(entree.st_mode)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def telecharger_fichier_sftp(client: Any, dossier: str, nom_fichier: str) -> bytes:
    """Télécharge un fichier SFTP en mémoire, sans copie disque intermédiaire."""
    tampon = io.BytesIO()
    client.getfo(posixpath.join(dossier, nom_fichier), tampon)
    return tampon.getvalue()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deposer_fichier_sftp(client: Any, dossier: str, nom_fichier: str, contenu: bytes) -> None:
    """Dépose un fichier SFTP sous un nom temporaire puis le renomme, comme le faisait la copie Synapse."""
    chemin_final = posixpath.join(dossier, nom_fichier)
    chemin_temporaire = chemin_final + f".{uuid.uuid4().hex}.tmp"
    client.putfo(io.BytesIO(contenu), chemin_temporaire)
    try:
        client.posix_rename(chemin_temporaire, chemin_final)
    except Exception:
        client.remove(chemin_temporaire)
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def analyser_csv(
    contenu: bytes,
    separateur: str,
    guillemet: str,
    echappement: Optional[str],
    encodage: str,
    entete: bool,
    vide_comme_null: bool = True,
) -> Tuple[List[str], List[List[Optional[str]]]]:
    """
    Analyse un fichier CSV sans aucune inférence de type : toutes les valeurs restent textuelles.

    Une valeur vide devient nulle par défaut, comme dans la copie Synapse d'origine. Les lignes vides sont ignorées.

    Returns:
        Les noms de colonnes, ou des noms positionnels sans entête, et les lignes de valeurs.
    """
    try:
        texte = contenu.decode(encodage)
    except (UnicodeDecodeError, LookupError) as exc:
        raise ErreurDonnees(f"Le fichier ne peut pas être décodé avec l'encodage {encodage} : {exc.__class__.__name__}.") from None
    lecteur = csv.reader(
        io.StringIO(texte, newline=""),
        delimiter=separateur,
        quotechar=guillemet,
        escapechar=echappement or None,
        doublequote=not echappement,
        strict=True,
    )
    try:
        lignes = [ligne for ligne in lecteur if ligne and any(valeur != "" for valeur in ligne)]
    except csv.Error as exc:
        raise ErreurDonnees(f"Fichier CSV mal formé à la ligne {lecteur.line_num} : {exc}.") from None
    if entete:
        if not lignes:
            raise ErreurDonnees("Fichier CSV sans ligne d'entête.")
        colonnes, lignes = [nom.strip() for nom in lignes[0]], lignes[1:]
    else:
        largeur = max((len(ligne) for ligne in lignes), default=0)
        colonnes = [f"colonne_{indice + 1}" for indice in range(largeur)]
    valeurs = [[None if (vide_comme_null and valeur == "") else valeur for valeur in ligne] for ligne in lignes]
    return colonnes, valeurs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def formater_csv(
    colonnes: Sequence[str],
    lignes: Iterable[Sequence[Any]],
    separateur: str,
    guillemet: str = '"',
    echappement: Optional[str] = None,
    saut_ligne: str = "\r\n",
) -> str:
    """
    Produit un texte CSV dont toutes les valeurs non nulles sont entre guillemets ; une valeur nulle reste vide.

    Avec un caractère d'échappement, guillemets et échappements internes sont préfixés ; sinon ils sont doublés.
    """
    def champ(valeur: Any) -> str:
        if valeur is None:
            return ""
        texte = str(valeur)
        if echappement:
            texte = texte.replace(echappement, echappement * 2).replace(guillemet, echappement + guillemet)
        else:
            texte = texte.replace(guillemet, guillemet * 2)
        return f"{guillemet}{texte}{guillemet}"

    sortie = [separateur.join(champ(nom) for nom in colonnes)]
    sortie.extend(separateur.join(champ(valeur) for valeur in ligne) for ligne in lignes)
    return saut_ligne.join(sortie) + saut_ligne

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def empreinte_sha256(contenu: bytes) -> str:
    """Calcule l'empreinte SHA-256 hexadécimale d'un contenu, pour la traçabilité des fichiers."""
    return hashlib.sha256(contenu).hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_est_vide(valeur: Optional[str]) -> bool:
    """Reproduit le prédicat T-SQL colonne = '' : vrai pour une chaîne vide ou d'espaces, faux pour NULL."""
    return valeur is not None and valeur.rstrip(" ") == ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_isdate(valeur: Optional[str]) -> int:
    """
    Reproduit ISDATE pour les formats aaaa-mm-jj, aaaammjj, aaaa/mm/jj et les horodatages ISO à l'heure près.

    Renvoie 0 pour NULL, une chaîne vide, une date de calendrier invalide ou une année hors de 1753 à 9999.
    """
    return 0 if _tsql_analyser_date(valeur) is None else 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _tsql_analyser_date(valeur: Optional[str]) -> Optional[date]:
    """Analyse une date selon les formats reconnus par tsql_isdate ; renvoie None si elle est invalide."""
    if valeur is None:
        return None
    texte = valeur.strip()
    motifs = (
        r"^(\d{4})-(\d{2})-(\d{2})$",
        r"^(\d{4})(\d{2})(\d{2})$",
        r"^(\d{4})/(\d{2})/(\d{2})$",
        r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$",
        r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$",
    )
    for motif in motifs:
        correspondance = re.match(motif, texte)
        if not correspondance:
            continue
        groupes = correspondance.groups()
        annee, mois, jour = int(groupes[0]), int(groupes[1]), int(groupes[2])
        if not 1753 <= annee <= 9999:
            return None
        if len(groupes) > 3:
            heure, minute = int(groupes[3]), int(groupes[4])
            seconde = int(groupes[5]) if groupes[5] else 0
            if heure > 23 or minute > 59 or seconde > 59:
                return None
        try:
            return date(annee, mois, jour)
        except ValueError:
            return None
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_convertir_date(valeur: Optional[str]) -> Optional[date]:
    """
    Reproduit CONVERT(DATE, valeur) : NULL reste NULL et une chaîne vide donne le 1er janvier 1900.

    Raises:
        ErreurDonnees: si la valeur n'est pas convertible, cas où le T-SQL d'origine échouait.
    """
    if valeur is None:
        return None
    if valeur.strip() == "":
        return DATE_VIDE_TSQL
    resultat = _tsql_analyser_date(valeur)
    if resultat is None:
        raise ErreurDonnees("Conversion de date impossible : valeur hors des formats reconnus.")
    return resultat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_isnumeric(valeur: Optional[str]) -> int:
    """Reproduit ISNUMERIC pour les nombres décimaux usuels ; les notations monétaires ou exponentielles valent 0."""
    return 1 if valeur is not None and _MOTIF_NUMERIQUE_TSQL.match(valeur) else 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_hors_bornes_numeriques(valeur: Optional[str], minimum: str, maximum: str) -> int:
    """
    Reproduit CASE WHEN ISNUMERIC(v)=0 THEN 0 ELSE CASE WHEN CAST(v AS NUMERIC(12,2)) BETWEEN min AND max THEN 0 ELSE 1 END END.

    Une valeur non numérique n'est donc jamais signalée, comme dans la procédure d'origine.
    """
    if not tsql_isnumeric(valeur):
        return 0
    try:
        arrondi = Decimal(valeur.strip()).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return 0
    return 0 if Decimal(str(minimum)) <= arrondi <= Decimal(str(maximum)) else 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= FONCTIONS SPARK DE SEMANTIQUE T-SQL =============
udf_tsql_isdate = F.udf(tsql_isdate, T.IntegerType())
udf_tsql_convertir_date = F.udf(tsql_convertir_date, T.DateType())
udf_tsql_hors_bornes_numeriques = F.udf(tsql_hors_bornes_numeriques, T.IntegerType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_colonne_motif(motif: Optional[str], nom_colonne_rejet: str, valeur: Column) -> Column:
    """
    Construit le motif de rejet à partir d'un modèle contenant {colonne} et {valeur}.

    Comme la concaténation T-SQL, le motif devient NULL si la valeur insérée est NULL.
    """
    if motif is None or motif == "":
        return F.lit(None).cast("string")
    elements = []
    for morceau in re.split(r"(\{colonne\}|\{valeur\})", motif):
        if morceau == "{colonne}":
            elements.append(F.lit(nom_colonne_rejet))
        elif morceau == "{valeur}":
            elements.append(valeur.cast("string"))
        elif morceau:
            elements.append(F.lit(morceau))
    return F.concat(*elements)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_indicateur_regle(regle: Dict[str, Any]) -> Column:
    """
    Construit l'indicateur 0 ou 1 d'une règle déclarée dans cfg_qualite.

    Tests disponibles : valeur_vide, longueur_differente, date_invalide, numerique_hors_bornes, reference_absente.
    """
    expression = regle["expression"]
    test = expression.get("test")
    colonne = F.col(regle["colonne"])
    vide = F.rtrim(colonne) == F.lit("")
    if test == "valeur_vide":
        return F.when(vide, F.lit(1)).otherwise(F.lit(0))
    if test == "longueur_differente":
        longueur = int(exiger_parametre(expression, "longueur", regle["regle_id"]))
        return F.when(F.length(F.rtrim(colonne)) == F.lit(longueur), F.lit(0)).otherwise(F.lit(1))
    if test == "date_invalide":
        base = F.substring(colonne, 1, int(expression["sous_chaine"])) if expression.get("sous_chaine") else colonne
        return F.when(vide | (udf_tsql_isdate(base) == F.lit(1)), F.lit(0)).otherwise(F.lit(1))
    if test == "numerique_hors_bornes":
        minimum = str(exiger_parametre(expression, "minimum", regle["regle_id"]))
        maximum = str(exiger_parametre(expression, "maximum", regle["regle_id"]))
        return udf_tsql_hors_bornes_numeriques(colonne, F.lit(minimum), F.lit(maximum))
    if test == "reference_absente":
        absente = F.col(exiger_parametre(expression, "colonne_correspondance", regle["regle_id"])).isNull()
        for cible in expression.get("colonnes_cible", []):
            absente = absente | F.col(cible).isNull()
        return F.when((F.rtrim(colonne) != F.lit("")) & absente, F.lit(1)).otherwise(F.lit(0))
    raise ErreurConfiguration(f"Test de qualité inconnu {test!r} pour la règle {regle.get('regle_id')}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def appliquer_regles_rejet(
    donnees: DataFrame,
    regles: List[Dict[str, Any]],
    colonnes_identite: Sequence[str],
    date_rejet: date,
) -> DataFrame:
    """
    Applique des règles déclaratives et produit une ligne de rejet par colonne en défaut.

    Pour chaque famille de règles, une ligne en défaut n'est rejetée que si au moins une règle déclencheuse de la
    même famille est en défaut, ce qui reproduit les clauses WHERE de la procédure de rejet d'origine.

    Returns:
        Les rejets avec dt_rejet, les colonnes d'identité, cd_type_rejet, lb_nom_colonne_rejet,
        lb_valeur_rejet, fl_rejet, lb_fichier_mapping, ds_rejet, regle_id et bloquant.
    """
    schema_rejet = T.StructType(
        [T.StructField("dt_rejet", T.DateType())]
        + [T.StructField(nom, donnees.schema[nom].dataType) for nom in colonnes_identite]
        + [
            T.StructField("cd_type_rejet", T.StringType()), T.StructField("lb_nom_colonne_rejet", T.StringType()),
            T.StructField("lb_valeur_rejet", T.StringType()), T.StructField("fl_rejet", T.IntegerType()),
            T.StructField("lb_fichier_mapping", T.StringType()), T.StructField("ds_rejet", T.StringType()),
            T.StructField("regle_id", T.StringType()), T.StructField("bloquant", T.BooleanType()),
        ]
    )
    if not regles:
        return spark.createDataFrame([], schema_rejet)
    colonnes_controlees = sorted({regle["colonne"] for regle in regles})
    # Première projection : un indicateur par règle ; seconde : une porte par famille. Le plan reste compact.
    indicateurs = donnees.select(
        *[F.col(nom) for nom in colonnes_identite],
        *[F.col(nom).cast("string").alias(f"_val_{nom}") for nom in colonnes_controlees],
        *[construire_indicateur_regle(regle).alias(f"_fl_{indice}") for indice, regle in enumerate(regles)],
    )
    familles = sorted({regle["type_regle"] for regle in regles})
    portes = []
    for numero, famille in enumerate(familles):
        declencheurs = [
            F.col(f"_fl_{indice}") for indice, regle in enumerate(regles)
            if regle["type_regle"] == famille and regle["expression"].get("declencheur", True)
        ]
        porte = (reduce(lambda gauche, droite: gauche + droite, declencheurs) > F.lit(0)) if declencheurs else F.lit(False)
        portes.append(porte.alias(f"_porte_{numero}"))
    indicateurs = indicateurs.select("*", *portes)
    elements = []
    for indice, regle in enumerate(regles):
        expression = regle["expression"]
        nom_rejet = expression.get("nom_colonne_rejet") or regle["colonne"]
        valeur = F.col(f"_val_{regle['colonne']}")
        condition = F.col(f"_porte_{familles.index(regle['type_regle'])}") & (F.col(f"_fl_{indice}") == F.lit(1))
        elements.append(F.when(condition, F.struct(
            F.lit(exiger_parametre(expression, "type_rejet", regle["regle_id"])).alias("cd_type_rejet"),
            F.lit(nom_rejet).alias("lb_nom_colonne_rejet"),
            valeur.alias("lb_valeur_rejet"),
            F.lit(expression.get("fichier_mapping", "")).alias("lb_fichier_mapping"),
            construire_colonne_motif(regle.get("motif_rejet"), nom_rejet, valeur).alias("ds_rejet"),
            F.lit(regle["regle_id"]).alias("regle_id"),
            F.lit(bool(regle["bloquant"])).alias("bloquant"),
        )))
    # Une ligne de rejet par règle en défaut, en une seule passe sur les données.
    depliees = indicateurs.select(
        *[F.col(nom) for nom in colonnes_identite],
        F.explode(F.filter(F.array(*elements), lambda element: element.isNotNull())).alias("_rejet"),
    )
    return depliees.select(
        F.lit(date_rejet).cast("date").alias("dt_rejet"),
        *[F.col(nom) for nom in colonnes_identite],
        F.col("_rejet.cd_type_rejet").alias("cd_type_rejet"),
        F.col("_rejet.lb_nom_colonne_rejet").alias("lb_nom_colonne_rejet"),
        F.col("_rejet.lb_valeur_rejet").alias("lb_valeur_rejet"),
        F.lit(1).alias("fl_rejet"),
        F.col("_rejet.lb_fichier_mapping").alias("lb_fichier_mapping"),
        F.col("_rejet.ds_rejet").alias("ds_rejet"),
        F.col("_rejet.regle_id").alias("regle_id"),
        F.col("_rejet.bloquant").alias("bloquant"),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extraire_date_fichier(nom_fichier: str, motif: str) -> Optional[date]:
    """Extrait la date aaaammjj capturée par le premier groupe du motif ; None si le nom ne correspond pas."""
    correspondance = re.match(motif, nom_fichier or "")
    if not correspondance or not correspondance.groups():
        return None
    try:
        return datetime.strptime(correspondance.group(1), "%Y%m%d").date()
    except ValueError:
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def selectionner_fichier_plus_recent(noms: Sequence[str], motif: str, date_maximale: date) -> Tuple[str, date, List[str]]:
    """
    Sélectionne le fichier le plus récent dont la date, lue dans le nom, ne dépasse pas la date de traitement.

    La date est analysée, et non comparée lexicographiquement comme dans l'existant.

    Returns:
        Le nom retenu, sa date et la liste des noms ignorés car non conformes au motif.
    """
    candidats, ignores = [], []
    for nom in noms:
        date_fichier = extraire_date_fichier(nom, motif)
        if date_fichier is None:
            ignores.append(nom)
        elif date_fichier <= date_maximale:
            candidats.append((date_fichier, nom))
    if not candidats:
        raise ErreurDonnees(f"Aucun fichier conforme au motif {motif} et daté au plus tard du {date_maximale.isoformat()}.")
    date_retenue, nom_retenu = max(candidats)
    return nom_retenu, date_retenue, ignores

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_watermark(flux_id: str, etape: str) -> Optional[str]:
    """Renvoie le watermark le plus élevé atteint par une étape réussie d'un flux, ou None."""
    ligne = (
        lire_etapes_consolidees()
        .where((F.col("flux_id") == flux_id) & (F.col("etape") == etape) & F.col("statut").isin(*STATUTS_REUSSIS))
        .agg(F.max("watermark").alias("watermark"))
        .collect()[0]
    )
    return ligne["watermark"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_vagues(
    flux_ids: Sequence[str],
    dependances: Sequence[Tuple[str, str]],
    ordre: Optional[Dict[str, int]] = None,
) -> List[List[str]]:
    """
    Trie topologiquement des flux en vagues exécutables ; un cycle arrête tout avant traitement.

    Args:
        flux_ids: Flux à planifier.
        dependances: Couples (flux, flux dont il dépend). Une dépendance vers un flux hors sélection est ignorée.
        ordre: Rang d'affichage optionnel au sein d'une vague.

    Raises:
        CycleDependances: si un cycle, y compris une dépendance d'un flux à lui-même, est détecté.
    """
    rang = ordre or {}
    selection = set(flux_ids)
    predecesseurs = {flux: set() for flux in selection}
    for flux, depend_de in dependances:
        if flux in selection and depend_de in selection:
            predecesseurs[flux].add(depend_de)
    vagues = []
    while predecesseurs:
        prets = sorted((flux for flux, preds in predecesseurs.items() if not preds), key=lambda f: (rang.get(f, 0), f))
        if not prets:
            raise CycleDependances(
                "Cycle détecté dans cfg_dependance entre les flux : " + ", ".join(sorted(predecesseurs))
                + ". Aucun traitement n'est lancé."
            )
        vagues.append(prets)
        for flux in prets:
            del predecesseurs[flux]
        for preds in predecesseurs.values():
            preds.difference_update(prets)
    return vagues

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executer_tests_unitaires() -> Dict[str, int]:
    """
    Exécute les tests unitaires des fonctions pures de la bibliothèque ; lève AssertionError au premier échec.

    À lancer manuellement dans DEV après %run MOTUL_nb_hris_lib. Couvre notamment le test T-14.
    """
    resultats = {"reussis": 0}

    def verifier(condition: bool, libelle: str) -> None:
        assert condition, f"Test en échec : {libelle}"
        resultats["reussis"] += 1

    def leve(exception: type, fonction: Callable[[], Any]) -> bool:
        try:
            fonction()
        except exception:
            return True
        return False

    variables = {"env_nom": "DEV", "env_vide": "  "}
    verifier(lire_variable_depuis(variables, "env_nom") == "DEV", "lecture d'une variable présente")
    verifier(leve(VariableEnvironnementManquante, lambda: lire_variable_depuis(variables, "env_absente")), "T-14 variable absente")
    verifier(leve(VariableEnvironnementManquante, lambda: lire_variable_depuis(variables, "env_vide")), "T-14 variable vide")
    verifier(leve(VariableEnvironnementManquante, lambda: analyser_variables_env("")), "T-14 paramètre vide")
    verifier(leve(VariableEnvironnementManquante, lambda: analyser_variables_env("[1]")), "variables_env non objet")
    verifier(analyser_variables_env('{"env_nom": "UAT"}') == {"env_nom": "UAT"}, "analyse JSON des variables")

    verifier(verifier_coherence_environnement("DEV", "ESPACE-TEST-DEV") == "DEV", "workspace cohérent")
    verifier(leve(ErreurGardeEnvironnement, lambda: verifier_coherence_environnement("DEV", "ESPACE-TEST-PRD")), "croisement DEV dans PRD")
    verifier(leve(ErreurGardeEnvironnement, lambda: verifier_coherence_environnement("dev", "ESPACE-TEST-DEV")), "casse exacte")
    verifier(publication_externe_autorisee("PRD", "ESPACE-TEST-PRD"), "publication PRD autorisée")
    verifier(not publication_externe_autorisee("PRD", "ESPACE-TEST-UAT"), "publication refusée sans marqueur PRD")
    verifier(not publication_externe_autorisee("UAT", "ESPACE-TEST-PRD"), "publication refusée hors PRD")
    verifier(not publication_externe_autorisee("PRD", "ESPACE-TEST-PRDX"), "marqueur PRD exact")

    verifier(nom_complet_table("cfg_flux", "ESPACE-TEST-DEV", "LH_TEST") == "`ESPACE-TEST-DEV`.`LH_TEST`.`dbo`.`cfg_flux`", "nom complet")
    verifier(leve(ErreurConfiguration, lambda: valider_nom_table("lu_hr_emp_sexe")), "préfixe de table interdit")
    verifier(construire_chemin_relatif("landing", "import_csv_ponctuel", date(2026, 10, 6), "a.csv") == "Files/landing/import_csv_ponctuel/20261006/a.csv", "chemin daté")
    verifier(construire_chemin_relatif("reference", nom_fichier="sexe.csv") == "Files/reference/sexe.csv", "chemin référentiel")
    verifier(leve(ErreurConfiguration, lambda: construire_chemin_relatif("landing", "x", date(2026, 1, 1), "../a")), "nom de fichier dangereux")
    verifier(construire_chemin_adls("compte", "files", "/talentsoft/import/") == "abfss://files@compte.dfs.core.windows.net/talentsoft/import", "chemin ADLS")

    verifier(resoudre_date_traitement("2026-10-06") == date(2026, 10, 6), "date de traitement")
    verifier(leve(ErreurConfiguration, lambda: resoudre_date_traitement("06/10/2026")), "date de traitement invalide")
    verifier(analyser_booleen("oui", "x") and not analyser_booleen("", "x"), "booléens")

    verifier(tsql_est_vide("") and tsql_est_vide("   ") and not tsql_est_vide(None) and not tsql_est_vide("a"), "chaîne vide T-SQL")
    verifier(tsql_isdate("2024-07-12") == 1 and tsql_isdate("20240712") == 1 and tsql_isdate("2024-07-12T10:00:00") == 1, "ISDATE valide")
    verifier(tsql_isdate("2024-02-30") == 0 and tsql_isdate(None) == 0 and tsql_isdate("") == 0 and tsql_isdate("1700-01-01") == 0, "ISDATE invalide")
    verifier(tsql_convertir_date("") == date(1900, 1, 1) and tsql_convertir_date(None) is None, "CONVERT DATE vide et NULL")
    verifier(tsql_convertir_date("2024-07-12 10:30") == date(2024, 7, 12), "CONVERT DATE horodatage")
    verifier(leve(ErreurDonnees, lambda: tsql_convertir_date("abc")), "CONVERT DATE impossible")
    verifier(tsql_isnumeric(" 12.5 ") == 1 and tsql_isnumeric("1e5") == 0 and tsql_isnumeric(None) == 0, "ISNUMERIC")
    verifier(tsql_hors_bornes_numeriques("100.004", "0", "100") == 0 and tsql_hors_bornes_numeriques("100.005", "0", "100") == 1, "arrondi NUMERIC(12,2)")
    verifier(tsql_hors_bornes_numeriques("abc", "0", "100") == 0, "non numérique non signalé")

    colonnes, lignes = analyser_csv(b'a;b\r\n"x;1";\r\n\r\n"y\\"z";2\r\n', ";", '"', "\\", "utf-8", True)
    verifier(colonnes == ["a", "b"] and lignes == [["x;1", None], ['y"z', "2"]], "analyse CSV")
    verifier(leve(ErreurDonnees, lambda: analyser_csv(b"\xff\xfe", ",", '"', None, "utf-8", True)), "encodage invalide")
    texte = formater_csv(["a", "b"], [['x"1', None]], ";", echappement="\\")
    verifier(texte == '"a";"b"\r\n"x\\"1";\r\n', "formatage CSV")

    verifier(extraire_date_fichier("json_motul_talentsoft_20261006.json", r"^json_motul_talentsoft_(\d{8})\.json$") == date(2026, 10, 6), "date de fichier")
    nom, date_fichier, ignores = selectionner_fichier_plus_recent(
        ["json_motul_talentsoft_20261005.json", "json_motul_talentsoft_20261007.json", "autre.txt"],
        r"^json_motul_talentsoft_(\d{8})\.json$", date(2026, 10, 6),
    )
    verifier(nom == "json_motul_talentsoft_20261005.json" and ignores == ["autre.txt"], "fichier le plus récent")

    verifier(calculer_vagues(["a", "b", "c"], [("b", "a"), ("c", "b")]) == [["a"], ["b"], ["c"]], "tri topologique")
    verifier(calculer_vagues(["a", "b"], [("b", "x")]) == [["a", "b"]], "dépendance hors sélection")
    verifier(leve(CycleDependances, lambda: calculer_vagues(["a", "b"], [("a", "b"), ("b", "a")])), "détection de cycle")
    verifier(leve(CycleDependances, lambda: calculer_vagues(["a"], [("a", "a")])), "auto-dépendance")

    verifier("***" in nettoyer_message_erreur("Authorization: Bearer abc.def") and len(nettoyer_message_erreur("x" * 5000)) == TAILLE_MAX_MESSAGE_ERREUR, "nettoyage du diagnostic")
    verifier(litteral_sql("l'a") == "'l\\'a'" and litteral_sql(date(2026, 1, 2)) == "DATE'2026-01-02'", "littéraux SQL")
    return resultats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
