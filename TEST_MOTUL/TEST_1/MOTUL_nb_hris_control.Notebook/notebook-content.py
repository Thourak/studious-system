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
# # MOTUL_nb_hris_control, nom repris du champ displayName du fichier .platform.
# # **Type d'objet**
# # Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark, appelé par MOTUL_PL_HRIS_Orchestrateur au début de chaque exécution, pour chaque flux de contrôle, puis à la fin de l'exécution.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/MOTUL_nb_hris_control.Notebook/notebook-content.py.
# # **Description fonctionnelle**
# # Ce notebook porte les contrôles qualité et le bilan d'exécution du hub RH. Il remplace le script ScriptResume, la requête mail_variables, la procédure de résumé et les contrôles de format de matricule de la branche SFTP, qui étaient inactifs. Il ouvre aussi chaque exécution : il applique la configuration de référence versionnée, vérifie que les variables d'environnement requises existent, détecte un cycle dans le graphe des flux avant tout traitement et calcule le plan d'exécution par vagues. Il clôture enfin l'exécution en calculant le statut global et en marquant bloqués les flux dont une dépendance a échoué.
# # **Dépendances**
# # Le notebook importe MOTUL_nb_hris_lib. Il écrit cfg_flux, cfg_dependance et cfg_qualite à partir de la configuration de référence, puis lit ces tables pour planifier. Il lit les tables de rejets et de sortie produites par MOTUL_nb_hris_transform, et la table ctl_execution_etape alimentée par tous les notebooks. Il écrit ctl_execution, ctl_qualite et gld_resume, que consultent MOTUL_nb_hris_publish et MOTUL_nb_hris_notify.
# # **Fonctionnement et logique de traitement**
# # L'action ouvrir crée ou complète les tables de socle, remplace le contenu de cfg_flux, cfg_dependance et cfg_qualite par la configuration de référence de ce notebook, afin que les trois environnements aient exactement la même configuration, puis contrôle chaque variable d'environnement citée par un flux actif. Elle trie topologiquement tous les flux actifs ; un cycle arrête l'exécution. Elle retient ensuite les flux demandés, écarte ceux qui ont déjà réussi pour la même date sauf demande de rejeu, et journalise l'environnement, les paramètres et le plan dans ctl_execution. En reprise, elle recharge le plan de l'exécution visée et n'en conserve que les flux non aboutis. L'action controler calcule le résumé : pour les salariés, lignes rejetées et insérées en salariés distincts, lignes lues, taux de rejet et types de rejet, comme la procédure de résumé ; pour la rémunération, les volumes source et les évolutions, comme mail_variables. Elle trace dans ctl_qualite le nombre de rejets par règle et l'état des contrôles du contrat ADP. Aucune règle ADP n'étant fournie, la publication ADP est déclarée bloquée en PRD et non applicable ailleurs ; aucun seuil de rejet n'est appliqué tant qu'il n'est pas déclaré dans cfg_flux. L'action cloturer calcule le statut global Succès, Succès avec rejets, Échec partiel ou Échec.
# # **Paramètres**
# # Le paramètre action vaut ouvrir, controler ou cloturer. Le paramètre execution_id porte l'identifiant de l'exécution ; vide à l'ouverture, il est généré. Le paramètre flux_id désigne le flux de contrôle pour l'action controler. Le paramètre date_traitement, au format aaaa-mm-jj, vaut par défaut la date du jour à Paris. Le paramètre variables_env porte le JSON des variables d'environnement. À l'ouverture, p_flux vaut une étoile pour tous les flux récurrents actifs, ou une liste d'identifiants séparés par des virgules ; p_rejouer_reussis vaut faux par défaut ; p_execution_id_reprise désigne une exécution à reprendre ; declencheur et parametres_pipeline sont journalisés tels quels.
# # **Sorties produites**
# # L'action ouvrir renvoie le plan d'exécution au pipeline sous forme de JSON : identifiant d'exécution, environnement, date et vagues de flux avec leur notebook, leurs reprises et leur délai. L'action controler écrit une ligne dans gld_resume et une ligne par règle dans ctl_qualite. L'action cloturer met à jour ctl_execution avec la fin, le statut global et un message de synthèse, et renvoie ce statut au pipeline.
# # **Limitations connues et points d'attention**
# # La configuration de référence est la source de vérité : une modification manuelle de cfg_flux, cfg_dependance ou cfg_qualite est remplacée à l'ouverture suivante ; toute évolution passe par Git et le pipeline de déploiement. Le tri topologique est réalisé dans ce notebook parce qu'un Data Pipeline ne sait pas le calculer seul ; le pipeline consomme le plan renvoyé. Chaque vague est découpée pour ne jamais dépasser le parallélisme déclaré dans cfg_flux, puis le plan est complété par des vagues vides jusqu'au nombre de vagues prévu par le pipeline, transmis par nb_vagues_max ; un plan plus profond fait échouer l'ouverture. Les valeurs de parallélisme de la configuration de référence sont provisoires. Les nombres de reprises et les délais des étapes de transformation, de contrôle et de publication sont des valeurs provisoires à valider en exploitation. Le résumé de rémunération ne contient pas de taux de rejet, car les contrôles SFTP de l'existant étaient inactifs. Pour la publication, la configuration déclare une table cumulative et une table finale par jeu de données ; les clés de consolidation, la stratégie de cumul upsert et la politique vider pour un lot vide sont des choix à confirmer par le métier.
# # **Responsable et contact**
# # Équipe data du projet HRIS Motul ; le métier RH valide les règles de contrôle et le futur contrat ADP.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import json
import uuid
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple

# Bibliothèques tierces
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

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

action = "controler"         # ouvrir, controler ou cloturer
execution_id = ""            # GUID de l'exécution ; vide à l'ouverture pour le générer
flux_id = ""                 # Flux de contrôle pour l'action controler
mode = "incremental"         # Mode transmis par le pipeline, journalisé à l'ouverture
date_traitement = ""         # Date au format aaaa-mm-jj ; vide pour la date du jour à Paris
variables_env = ""           # JSON des variables d'environnement résolues au lancement du pipeline
p_flux = "*"                 # Ouverture : * pour tous les flux récurrents actifs, ou liste séparée par des virgules
p_rejouer_reussis = "false"  # Ouverture : true pour rejouer les flux déjà réussis à la même date
p_execution_id_reprise = ""  # Ouverture : exécution à reprendre, seules ses étapes non abouties sont replanifiées
declencheur = ""             # Ouverture : origine du lancement (planification, manuel), journalisée
parametres_pipeline = ""     # Ouverture : JSON des paramètres du pipeline, journalisé
nb_vagues_max = ""           # Ouverture : nombre de vagues prévues par le pipeline ; vide hors pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION DE L'ENVIRONNEMENT =============
etat_etape = None
try:
    contexte = initialiser_contexte_hris(variables_env)
    journal.info(f"✓ Environnement résolu : {contexte['env_nom']}")
    journal.info(f"✓ Workspace : {contexte['workspace_nom']}")
    journal.info(f"✓ Lakehouse : {contexte['lakehouse_nom']}")

    if action not in ("ouvrir", "controler", "cloturer"):
        raise ErreurConfiguration(f"Action inconnue : {action!r} ; valeurs admises : ouvrir, controler, cloturer.")
    if action == "ouvrir" and not execution_id and not p_execution_id_reprise:
        execution_id = str(uuid.uuid4())
    if action == "ouvrir" and p_execution_id_reprise:
        execution_id = p_execution_id_reprise
    if not execution_id:
        raise ErreurConfiguration("Le paramètre execution_id est obligatoire.")
    if action == "controler" and not flux_id:
        raise ErreurConfiguration("Le paramètre flux_id est obligatoire pour l'action controler.")
    date_traitement_effective = resoudre_date_traitement(date_traitement)
    journal.info(f"✓ Action {action}, exécution {execution_id}, date {date_traitement_effective}")

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

# ============= CONFIGURATION DE REFERENCE VERSIONNEE =============
# Identique dans DEV, UAT et PRD : aucune valeur propre à un environnement, uniquement des noms de variables.
NOTEBOOK_INGEST = "MOTUL_nb_hris_ingest"
NOTEBOOK_TRANSFORM = "MOTUL_nb_hris_transform"
NOTEBOOK_CONTROL = "MOTUL_nb_hris_control"
NOTEBOOK_PUBLISH = "MOTUL_nb_hris_publish"
ETAPE_PRINCIPALE = {NOTEBOOK_INGEST: "ingest", NOTEBOOK_TRANSFORM: "transform", NOTEBOOK_CONTROL: "control", NOTEBOOK_PUBLISH: "publish"}
FLUX_ORCHESTRATION = "orchestrateur"

_CSV_EXPORT = {"separateur": ",", "guillemet": "\"", "echappement": "\\", "encodage": "utf-8-sig", "entete": True}
_COLONNES_BONUS = ["employeenumber", "type", "start_date", "end_date", "calculation_method", "value", "currency",
                   "base_salary_effective_date", "periodicity"]
_TABLES_STAGING_REMUNERATION = {"base_salary": "stg_sftp_base_salary", "bonus_percentage": "stg_sftp_bonus_percentage",
                                "bonus_target": "stg_sftp_bonus_target"}
_TABLES_ETAT_REMUNERATION = {"base_salary": "slv_sftp_base_salary_etat", "bonus_percentage": "slv_sftp_bonus_percentage_etat",
                             "bonus_target": "slv_sftp_bonus_target_etat"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_ligne_flux(flux_id: str, notebook: str, domaine: Optional[str], type_chargement: str, ordre: int, retries: int,
                          timeout: int, parametres: Dict[str, Any], **autres: Any) -> Dict[str, Any]:
    """Construit une ligne de cfg_flux de la configuration de référence, avec des valeurs communes explicites."""
    ligne = {
        "flux_id": flux_id, "actif": True, "domaine": domaine, "type_chargement": type_chargement, "source_type": "lakehouse",
        "source_ref": "env_lakehouse_nom", "format_source": "delta", "destination_table": None, "notebook": notebook,
        "parametres": json.dumps(parametres, ensure_ascii=False), "ordre": ordre, "parallelisme_max": 1,
        "watermark_colonne": None, "watermark_strategie": "aucun", "strategie_ecriture": "remplacer_execution",
        "cles_fusion": None, "retries": retries, "timeout_minutes": timeout, "seuil_rejet_pct": None,
        "politique_archivage": "aucune", "politique_rejet": "table_rjt", "criticite": "bloquant",
        "regle_reprise": "reprendre_etape",
    }
    ligne.update(autres)
    return ligne

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= FLUX ET DEPENDANCES DE REFERENCE =============
CONFIGURATION_FLUX: List[Dict[str, Any]] = [
    construire_ligne_flux("ref_correspondances", NOTEBOOK_INGEST, "reference", "complet", 10, 3, 30,
          {"traitement": "referentiels", **_CSV_EXPORT, "separateur": ";"},
          source_type="sftp", source_ref="env_sftp_reference_chemin", format_source="csv", destination_table="cfg_mapping",
          strategie_ecriture="merge", cles_fusion="domaine,code_source,attribut_cible", regle_reprise="rejouer_flux"),
    construire_ligne_flux("ts_employe", NOTEBOOK_INGEST, "sirh", "incremental", 20, 2, 45,
          {"traitement": "export_json_ts", "variable_compte": "env_adls_compte", "dossier": "talentsoft/import",
           "motif_fichier": r"^json_motul_talentsoft_(\d{8})\.json$"},
          source_type="adls", source_ref="env_adls_filesystem", format_source="json", destination_table="stg_ts_employe",
          parallelisme_max=4, watermark_strategie="fichier_date", strategie_ecriture="overwrite"),
    construire_ligne_flux("sftp_base_salary", NOTEBOOK_INGEST, "remuneration", "incremental", 30, 2, 30,
          {"traitement": "export_csv_sftp", **_CSV_EXPORT, "fichier": "COMPENSATION_HUB_ADMIN_ANNUALBASESALARY.csv",
           "colonnes": ["employeenumber", "date", "type", "amount", "currency", "periodicity"]},
          source_type="sftp", source_ref="env_sftp_export_chemin", format_source="csv",
          destination_table="stg_sftp_base_salary", parallelisme_max=4, strategie_ecriture="overwrite"),
    construire_ligne_flux("sftp_bonus_percentage", NOTEBOOK_INGEST, "remuneration", "incremental", 30, 2, 30,
          {"traitement": "export_csv_sftp", **_CSV_EXPORT, "fichier": "COMPENSATION_HUB_ADMIN_BONUSPERCENT.csv",
           "colonnes": _COLONNES_BONUS},
          source_type="sftp", source_ref="env_sftp_export_chemin", format_source="csv",
          destination_table="stg_sftp_bonus_percentage", parallelisme_max=4, strategie_ecriture="overwrite"),
    construire_ligne_flux("sftp_bonus_target", NOTEBOOK_INGEST, "remuneration", "incremental", 30, 2, 30,
          {"traitement": "export_csv_sftp", **_CSV_EXPORT, "fichier": "COMPENSATION_HUB_ADMIN_BONUSTARGETVALUE.csv",
           "colonnes": _COLONNES_BONUS},
          source_type="sftp", source_ref="env_sftp_export_chemin", format_source="csv",
          destination_table="stg_sftp_bonus_target", parallelisme_max=4, strategie_ecriture="overwrite"),
    construire_ligne_flux("import_csv_ponctuel", NOTEBOOK_INGEST, None, "ponctuel", 15, 0, 30,
          {"traitement": "depot_csv_ponctuel"},
          actif=False, source_type="depot_csv", format_source="csv", strategie_ecriture="append",
          politique_archivage="archive", politique_rejet="rejet"),
    construire_ligne_flux("trf_sirh", NOTEBOOK_TRANSFORM, "sirh", "incremental", 40, 0, 60,
          {"traitement": "sirh_employe", "table_staging": "stg_ts_employe", "table_etat": "slv_ts_employe_etat",
           "table_prepare": "slv_ts_employe_prepare", "table_rejet": "rjt_ts_employe", "table_sortie": "slv_ts_employe_adp_lot"},
          destination_table="slv_ts_employe_adp_lot", parallelisme_max=2),
    construire_ligne_flux("trf_remuneration", NOTEBOOK_TRANSFORM, "remuneration", "incremental", 40, 0, 60,
          {"traitement": "remuneration", "tables_staging": _TABLES_STAGING_REMUNERATION, "tables_etat": _TABLES_ETAT_REMUNERATION,
           "table_sortie_base_salary": "slv_base_salary_changes_lot", "table_sortie_bonus": "slv_bonus_changes_lot"},
          destination_table="slv_bonus_changes_lot", parallelisme_max=2),
    construire_ligne_flux("ctl_sirh", NOTEBOOK_CONTROL, "sirh", "incremental", 50, 0, 30,
          {"traitement": "resume_sirh", "flux_transformation": "trf_sirh", "table_rejet": "rjt_ts_employe",
           "table_sortie": "slv_ts_employe_adp_lot", "cd_source": "TalentSoft", "cd_target": "ADP", "cible_adp": True},
          destination_table="gld_resume", parallelisme_max=2),
    construire_ligne_flux("ctl_remuneration", NOTEBOOK_CONTROL, "remuneration", "incremental", 50, 0, 30,
          {"traitement": "resume_remuneration", "flux_transformation": "trf_remuneration",
           "tables_staging": _TABLES_STAGING_REMUNERATION, "table_sortie_base_salary": "slv_base_salary_changes_lot",
           "table_sortie_bonus": "slv_bonus_changes_lot"},
          destination_table="gld_resume", parallelisme_max=2),
    # Publication : une paire de tables par jeu de données, cumulative alimentée par upsert et finale remplacée par
    # le dernier lot validé. Clés, stratégie de cumul et politique de lot vide sont des choix À CONFIRMER par le métier.
    construire_ligne_flux("pub_sirh", NOTEBOOK_PUBLISH, "sirh", "incremental", 60, 0, 30,
          {"traitement": "publication", "flux_controle": "ctl_sirh", "flux_transformation": "trf_sirh",
           "jeux": [
               {"nom": "ts_employe_adp", "canal": "adp", "table_lot": "slv_ts_employe_adp_lot",
                "table_cumul": "gld_ts_employe_adp_cumul", "table_finale": "gld_ts_employe_adp", "cles": ["id_unique"],
                "strategie_cumul": "upsert", "politique_lot_vide": "vider"},
           ]},
          destination_table="gld_ts_employe_adp", parallelisme_max=2),
    construire_ligne_flux("pub_remuneration", NOTEBOOK_PUBLISH, "remuneration", "incremental", 60, 0, 30,
          {"traitement": "publication", "flux_controle": "ctl_remuneration", "flux_transformation": "trf_remuneration",
           "separateur": ";", "guillemet": "\"", "echappement": "\\", "encodage": "utf-8", "saut_ligne": "\r\n",
           "jeux": [
               {"nom": "base_salary_changes", "canal": "sftp", "table_lot": "slv_base_salary_changes_lot",
                "table_cumul": "gld_base_salary_changes_cumul", "table_finale": "gld_base_salary_changes",
                "cles": ["employeenumber", "type"], "strategie_cumul": "upsert", "politique_lot_vide": "vider",
                "fichier": {"modele_nom": "RemunSalaryHistoIE_InsertAndUpdate_fr-FR_1_{ddMMyyyy}.csv",
                            "colonnes": [["employeenumber", "employeenumber"], ["date", "date"], ["type", "type"],
                                         ["amount", "amount"], ["currency", "currency"], ["periodicity", "periodicity"],
                                         ["extra_augm_n_1", "extra_augm_n-1"]]}},
               {"nom": "bonus_changes", "canal": "sftp", "table_lot": "slv_bonus_changes_lot",
                "table_cumul": "gld_bonus_changes_cumul", "table_finale": "gld_bonus_changes",
                "cles": ["employeenumber", "type", "start_date"], "strategie_cumul": "upsert", "politique_lot_vide": "vider",
                "fichier": {"modele_nom": "RemunTargetBonusHistoIE_InsertAndUpdate_fr-FR_2_{ddMMyyyy}.csv",
                            "colonnes": [[nom, nom] for nom in _COLONNES_BONUS]}},
           ]},
          source_ref="env_sftp_import_chemin", destination_table="gld_bonus_changes", parallelisme_max=2),
]

CONFIGURATION_DEPENDANCES: List[Tuple[str, str, str]] = [
    ("ts_employe", "ref_correspondances", "succes"),
    ("sftp_base_salary", "ref_correspondances", "succes"),
    ("sftp_bonus_percentage", "ref_correspondances", "succes"),
    ("sftp_bonus_target", "ref_correspondances", "succes"),
    ("trf_sirh", "ts_employe", "succes"), ("trf_sirh", "ref_correspondances", "succes"),
    ("trf_remuneration", "sftp_base_salary", "succes"), ("trf_remuneration", "sftp_bonus_percentage", "succes"),
    ("trf_remuneration", "sftp_bonus_target", "succes"),
    ("ctl_sirh", "trf_sirh", "succes"), ("ctl_remuneration", "trf_remuneration", "succes"),
    ("pub_sirh", "ctl_sirh", "succes"), ("pub_remuneration", "ctl_remuneration", "succes"),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= REGLES DE REJET DECLARATIVES DU FLUX trf_sirh =============
# Transcription de dwh.ps_hr_ts_LoadDataToExternalRejetTable. Le champ declencheur reproduit les clauses WHERE :
# une règle non déclencheuse n'émet de rejet que si une règle déclencheuse de la même famille est en défaut.
_MOTIF_NULL = "{colonne} ne peut pas être NULL"
_MOTIF_DATE = "Format Date non respecté au YYYY-MM-DD pour {colonne} = {valeur}"
_MOTIF_POURCENTAGE = "Format Pourcentage non respecté du 0 au 100 pour {colonne} = {valeur}"
_MOTIF_REFERENCE = "Mapping non trouvé entre TS et ADP pour {colonne} = {valeur}"
_DATES_CONTROLEES = [
    ("dt_naissance", None), ("dt_situation_start", None), ("dt_entree_societe", 10), ("dt_entree_groupe", None),
    ("dt_anciennete", None), ("dt_sortie", None), ("dt_company_start", None), ("dt_ppc_debut_contrat", None),
    ("dt_ppc_fin_contrat", None), ("dt_ppc_debut_periode_essaie", None), ("dt_ppc_fin_periode_essaie", None),
    ("dt_debut_mt_salaire_base", None), ("dt_debut_pc_prime_annuelle", None), ("dt_debut_prime_annuelle", None),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_regle_qualite(regle_id: str, type_regle: str, colonne: str, expression: Dict[str, Any], motif: Optional[str]) -> Dict[str, Any]:
    """Construit une ligne de cfg_qualite pour le flux trf_sirh ; tout rejet exclut le salarié du format paie."""
    return {"flux_id": "trf_sirh", "regle_id": regle_id, "type_regle": type_regle, "colonne": colonne,
            "expression": json.dumps(expression, ensure_ascii=False), "motif_rejet": motif, "bloquant": True}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_regle_correspondance(
    numero: int, fichier: str, nom_rejet: str, colonne: str, colonne_lu: str, cibles: List[str], declencheur: bool = True,
) -> Dict[str, Any]:
    """Construit une règle de correspondance absente, reprise de la famille Référence_Rejet de la procédure d'origine."""
    return construire_regle_qualite(
        f"SIRH_REF_{numero:02d}", "reference", colonne,
        {"test": "reference_absente", "type_rejet": "Référence_Rejet", "nom_colonne_rejet": nom_rejet,
         "fichier_mapping": fichier, "colonne_correspondance": colonne_lu, "colonnes_cible": cibles,
         "declencheur": declencheur},
        _MOTIF_REFERENCE,
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONFIGURATION_QUALITE: List[Dict[str, Any]] = (
    [construire_regle_qualite(f"SIRH_NUL_{i:02d}", "obligatoire", colonne, {"test": "valeur_vide", "type_rejet": "Null_Rejet"}, _MOTIF_NULL)
     for i, colonne in enumerate(["id_payroll", "cd_matricule_it", "id_numero_securite_sociale"], start=1)]
    + [construire_regle_qualite("SIRH_FMT_01", "format", "id_payroll", {"test": "longueur_differente", "longueur": 8, "type_rejet": "Format_Rejet"}, None)]
    + [construire_regle_qualite(f"SIRH_FMT_{i:02d}", "format", colonne,
              {"test": "date_invalide", "type_rejet": "Format_Rejet", **({"sous_chaine": sous_chaine} if sous_chaine else {})},
              _MOTIF_DATE)
       for i, (colonne, sous_chaine) in enumerate(_DATES_CONTROLEES, start=2)]
    + [construire_regle_qualite("SIRH_FMT_16", "format", "pc_ppc_fte",
              {"test": "numerique_hors_bornes", "minimum": 0, "maximum": 100, "type_rejet": "Format_Rejet", "declencheur": False},
              _MOTIF_POURCENTAGE),
       construire_regle_qualite("SIRH_FMT_17", "format", "pc_prime_annuelle",
              {"test": "numerique_hors_bornes", "minimum": 0, "maximum": 100, "type_rejet": "Format_Rejet"}, _MOTIF_POURCENTAGE),
       construire_regle_qualite("SIRH_FMT_18", "format", "prime_annuelle",
              {"test": "numerique_hors_bornes", "minimum": 0, "maximum": 1000000, "type_rejet": "Format_Rejet"}, None)]
    + [construire_regle_correspondance(1, "titre.csv", "cd_emp_titre", "cd_emp_titre_ts", "cd_emp_titre_ts_lu", ["cd_emp_titre_adp"]),
       construire_regle_correspondance(2, "sexe.csv", "cd_emp_sexe", "cd_emp_sexe_ts", "cd_emp_sexe_ts_lu", ["cd_emp_sexe_adp"]),
       construire_regle_correspondance(3, "nationalite.csv", "cd_emp_nationalite", "cd_emp_nationalite_ts", "cd_emp_nationalite_ts_lu", ["cd_emp_nationalite_adp"]),
       construire_regle_correspondance(4, "situation_matrimoniale.csv", "cd_emp_situation_matrimoniale", "cd_emp_situation_matrimoniale_ts",
                  "cd_emp_situation_matrimoniale_ts_lu", ["cd_emp_situation_matrimoniale_adp"]),
       construire_regle_correspondance(5, "pays.csv", "cd_naissance_pays", "cd_naissance_pays_ts", "cd_naissance_pays_ts_lu", ["id_naissance_pays_adp"]),
       construire_regle_correspondance(6, "pays.csv", "cd_adresse_pays", "cd_adresse_pays_ts", "cd_adresse_pays_ts_lu", ["id_adresse_pays_adp"]),
       construire_regle_correspondance(7, "raison_debut_contrat.csv", "cd_ppc_raison_debut_contrat", "cd_ppc_raison_debut_contrat_ts",
                  "cd_ppc_raison_debut_contrat_ts_lu", ["cd_ppc_raison_debut_contrat_adp"]),
       construire_regle_correspondance(8, "contrat_v2.csv", "cd_nature_contrat", "cd_ppc_contrat_ts", "cd_ppc_contrat_ts_lu", ["cd_nature_contract_adp"]),
       construire_regle_correspondance(9, "contrat_v2.csv", "cd_type_contrat", "cd_ppc_contrat_ts", "cd_ppc_contrat_ts_lu", ["cd_type_contract_adp"]),
       construire_regle_correspondance(10, "contrat_v2.csv", "cd_type_collaboration_adp", "cd_ppc_contrat_ts", "cd_ppc_contrat_ts_lu", ["cd_type_collaboration_adp"]),
       construire_regle_correspondance(11, "classes.csv", "cd_occupational_category", "cd_occupational_category", "cd_occupational_category_ts_lu", [], declencheur=False),
       construire_regle_correspondance(12, "entite.csv", "cd_geo_entite", "cd_geo_entite_ts", "cd_geo_entite_ts_lu", ["cd_geo_entite_adp"]),
       construire_regle_correspondance(13, "location_country.csv", "cd_ppc_location_country", "cd_ppc_location_country_ts",
                  "cd_ppc_location_country_ts_lu", ["cd_ppc_location_country_adp"]),
       construire_regle_correspondance(14, "cost_center.csv", "cd_ppc_cost_center", "cd_ppc_cost_center_ts", "cd_ppc_cost_center_ts_lu", ["cd_ppc_cost_center_adp"]),
       construire_regle_correspondance(15, "organisationalstructure.csv", "cd_organisationalstructure", "cd_organisationalstructure_ts",
                  "cd_organisationalstructure_ts_lu", ["cd_organisationalstructure_adp"])]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def appliquer_configuration_reference() -> Dict[str, int]:
    """Remplace atomiquement cfg_flux, cfg_dependance et cfg_qualite par la configuration de référence versionnée."""
    horodatage = maintenant_utc()
    jeux = {
        "cfg_flux": CONFIGURATION_FLUX,
        "cfg_dependance": [{"flux_id": f, "depend_de_flux_id": d, "type_dependance": t} for f, d, t in CONFIGURATION_DEPENDANCES],
        "cfg_qualite": CONFIGURATION_QUALITE,
    }
    volumes = {}
    for nom, lignes in jeux.items():
        schema = schema_table_socle(nom)
        valeurs = [tuple(horodatage if c.name == "date_maj" else ligne.get(c.name) for c in schema.fields) for ligne in lignes]
        ecrire_table(spark.createDataFrame(valeurs, schema), nom, "overwrite")
        volumes[nom] = len(valeurs)
    return volumes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def variables_requises(flux_actifs: List[Dict[str, Any]]) -> Set[str]:
    """Liste les variables d'environnement nécessaires aux flux actifs, pour échouer avant tout traitement."""
    requises = {"env_nom", "env_lakehouse_nom", "env_kv_url"}
    for flux in flux_actifs:
        for valeur in (flux.get("source_ref"), flux["parametres"].get("variable_compte")):
            if isinstance(valeur, str) and valeur.startswith("env_"):
                requises.add(valeur)
        canaux = {jeu.get("canal") for jeu in flux["parametres"].get("jeux", [])}
        if flux.get("source_type") == "sftp" or "sftp" in canaux:
            requises.update({"env_sftp_hote", "env_sftp_port", "env_sftp_utilisateur", "env_secret_sftp"})
    return requises

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def statuts_par_flux(etapes: DataFrame) -> Dict[str, Dict[str, Any]]:
    """Renvoie, pour chaque flux, la tentative la plus récente de son étape principale : statut et diagnostic."""
    principales = etapes.where(~F.col("etape").contains(":") & (F.col("flux_id") != FLUX_ORCHESTRATION))
    resultat = {}
    for ligne in principales.orderBy("horodatage").collect():
        resultat[ligne["flux_id"]] = {"statut": ligne["statut"], "message_erreur": ligne["message_erreur"],
                                     "etape": ligne["etape"], "tentative": ligne["tentative"]}
    return resultat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def decouper_vague(vague: List[str], parallelisme: Dict[str, int]) -> List[List[str]]:
    """
    Découpe une vague topologique en groupes consécutifs dont la taille ne dépasse jamais le plus petit
    cfg_flux.parallelisme_max des flux du groupe.
    """
    groupes, courant = [], []
    for flux_id in vague:
        limite = min(max(int(parallelisme.get(f) or 1), 1) for f in courant + [flux_id])
        if courant and len(courant) + 1 > limite:
            groupes.append(courant)
            courant = [flux_id]
        else:
            courant.append(flux_id)
    if courant:
        groupes.append(courant)
    return groupes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ouvrir_execution() -> Dict[str, Any]:
    """
    Ouvre l'exécution : configuration de référence, contrôle des variables, détection de cycle et plan par vagues.
    """
    volumes = appliquer_configuration_reference()
    tous_flux = {flux["flux_id"]: flux for flux in lire_tous_flux()}
    actifs = {identifiant: flux for identifiant, flux in tous_flux.items() if flux["actif"]}
    dependances = [(d["flux_id"], d["depend_de_flux_id"]) for d in lire_dependances()]
    calculer_vagues(list(actifs), dependances)

    if p_execution_id_reprise:
        precedente = lire_execution(p_execution_id_reprise)
        if not precedente or not precedente.get("plan_execution"):
            raise ErreurConfiguration(f"Exécution à reprendre introuvable ou sans plan : {p_execution_id_reprise}.")
        plan_initial = json.loads(precedente["plan_execution"])
        deja = statuts_par_flux(lire_etapes_consolidees(execution_id=p_execution_id_reprise))
        selection = [f for vague in plan_initial["vagues"] for f in vague
                     if deja.get(f, {}).get("statut") not in STATUTS_REUSSIS]
        date_plan = date.fromisoformat(plan_initial["date_traitement"])
    else:
        if p_flux.strip() in ("", "*"):
            selection = [i for i, f in actifs.items() if f["type_chargement"] != "ponctuel"]
        else:
            selection = [i.strip() for i in p_flux.split(",") if i.strip()]
            inconnus = [i for i in selection if i not in actifs]
            if inconnus:
                raise ErreurConfiguration("Flux demandé(s) inconnu(s) ou inactif(s) : " + ", ".join(inconnus))
        date_plan = date_traitement_effective
        if not analyser_booleen(p_rejouer_reussis, "p_rejouer_reussis"):
            reussis = statuts_par_flux(lire_etapes_consolidees(date_traitement=date_plan))
            selection = [i for i in selection if reussis.get(i, {}).get("statut") not in STATUTS_REUSSIS]

    manquantes = []
    for nom in sorted(variables_requises([actifs[i] for i in selection if i in actifs])):
        try:
            lire_variable(nom)
        except VariableEnvironnementManquante:
            manquantes.append(nom)
    if manquantes:
        raise VariableEnvironnementManquante(
            "Variable(s) d'environnement requise(s) absente(s) ou vide(s) dans le jeu de valeurs actif : " + ", ".join(manquantes)
        )

    parallelisme = {i: actifs[i].get("parallelisme_max") or 1 for i in selection}
    vagues = []
    for vague in calculer_vagues(selection, dependances, {i: actifs[i].get("ordre") or 0 for i in selection}):
        vagues.extend(decouper_vague(vague, parallelisme))
    try:
        capacite = int(nb_vagues_max) if str(nb_vagues_max).strip() else None
    except ValueError:
        raise ErreurConfiguration(f"nb_vagues_max doit être un entier : {nb_vagues_max!r}.") from None
    if capacite is not None and len(vagues) > capacite:
        raise ErreurConfiguration(
            f"Le plan exige {len(vagues)} vagues alors que le pipeline en prévoit {capacite} : ajouter des vagues au pipeline."
        )
    dependances_plan = {i: sorted({d for f, d in dependances if f == i and d in selection}) for i in selection}
    vagues_pipeline = [[{"flux_id": i, "notebook": actifs[i]["notebook"]} for i in vague] for vague in vagues]
    if capacite is not None:
        vagues_pipeline += [[] for _ in range(capacite - len(vagues))]
    plan = {
        "execution_id": execution_id, "environnement": contexte["env_nom"], "date_traitement": date_plan.isoformat(),
        "vagues": vagues_pipeline,
        "flux": {
            i: {"notebook": actifs[i]["notebook"], "retries": actifs[i]["retries"], "timeout_minutes": actifs[i]["timeout_minutes"],
                "parallelisme_max": actifs[i]["parallelisme_max"], "criticite": actifs[i]["criticite"],
                "depend_de": dependances_plan[i]}
            for vague in vagues for i in vague
        },
    }
    enregistrer_execution({
        "execution_id": execution_id, "environnement": contexte["env_nom"], "workspace_nom": contexte["workspace_nom"],
        "declencheur": declencheur or None, "parametres": parametres_pipeline or json.dumps(
            {"p_flux": p_flux, "mode": mode, "p_rejouer_reussis": p_rejouer_reussis, "p_execution_id_reprise": p_execution_id_reprise}),
        "plan_execution": json.dumps({"date_traitement": date_plan.isoformat(), "vagues": vagues, "dependances": dependances_plan}),
        "version_code": VERSION_CODE_HRIS, "fin": None, "statut": STATUT_EN_COURS,
        # En reprise, l'horodatage de début est conservé : il ordonne les lots dans les tables cumulative et finale.
        "debut": (precedente.get("debut") or maintenant_utc()) if p_execution_id_reprise else maintenant_utc(),
        "message": "Reprise de l'exécution" if p_execution_id_reprise else None,
        "execution_id_reprise": p_execution_id_reprise or None, "date_traitement": date_plan,
    })
    journal.info(f"✓ Configuration appliquée : {volumes}")
    journal.info(f"✓ Plan : {len(selection)} flux en {len(vagues)} vague(s)")
    return plan

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_resume_sirh(rejets: DataFrame, sorties: DataFrame) -> Dict[str, Any]:
    """
    Reproduit dwh.ps_hr_ts_LoadDataToExternalResumeTable : volumes en salariés distincts, taux et types de rejet.

    L'ordre des types et des motifs, indéterminé dans l'existant, est trié ; les textes sont tronqués à 50 et 500.
    """
    rejetes = rejets.where(F.col("id_unique").isNotNull()).select("id_unique").distinct().count()
    inseres = sorties.where(F.col("id_unique").isNotNull()).select("id_unique").distinct().count()
    lues = rejetes + inseres
    taux = Decimal(0) if lues == 0 else (Decimal(rejetes) / Decimal(lues)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    paires = sorted(
        ((ligne["cd_type_rejet"], ligne["ds_rejet"]) for ligne in rejets.select("cd_type_rejet", "ds_rejet").distinct().collect()),
        key=lambda paire: (paire[0] or "", paire[1] or ""),
    )
    types = ",".join(t for t, _ in paires if t is not None)
    motifs = ",".join(m for _, m in paires if m is not None)
    return {
        "mt_lignes_inserts": inseres, "mt_lignes_rejets": rejetes, "mt_lignes_lues": lues, "pc_lignes_rejets": taux,
        "ds_type_rejets": types[:50] if paires else None, "ds_rejets": motifs[:500] if motifs else None,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ecrire_resume(flux_controle: str, valeurs: Dict[str, Any], indicateurs: Optional[Dict[str, Any]] = None) -> None:
    """Écrit la ligne de gld_resume d'un flux pour l'exécution courante, en remplaçant une ligne antérieure."""
    schema = schema_table_socle("gld_resume")
    ligne = {"execution_id": execution_id, "environnement": contexte["env_nom"], "flux_id": flux_controle,
             "date_traitement": date_traitement_effective, "dt_resume": date_traitement_effective,
             "indicateurs": json.dumps(indicateurs, ensure_ascii=False) if indicateurs else None, "horodatage": maintenant_utc()}
    ligne.update(valeurs)
    donnees = spark.createDataFrame([tuple(ligne.get(c.name) for c in schema.fields)], schema)
    remplacer_perimetre(donnees, "gld_resume", {"execution_id": execution_id, "flux_id": flux_controle})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ecrire_controles_qualite(flux_controle: str, lignes: List[Dict[str, Any]]) -> None:
    """Écrit les lignes de ctl_qualite d'un flux pour l'exécution courante, en remplaçant les lignes antérieures."""
    schema = schema_table_socle("ctl_qualite")
    horodatage = maintenant_utc()
    valeurs = []
    for ligne in lignes:
        complete = {"execution_id": execution_id, "environnement": contexte["env_nom"], "flux_id": flux_controle,
                    "date_traitement": date_traitement_effective, "horodatage": horodatage}
        complete.update(ligne)
        valeurs.append(tuple(complete.get(c.name) for c in schema.fields))
    remplacer_perimetre(spark.createDataFrame(valeurs, schema), "ctl_qualite", {"execution_id": execution_id, "flux_id": flux_controle})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controle_contrat_adp(flux_transformation: str) -> Dict[str, Any]:
    """
    Décrit l'état des contrôles du contrat ADP, sans inventer de règle.

    Aucune règle n'étant fournie, la publication ADP est bloquée en PRD et non applicable hors PRD.
    """
    regles_adp = [r for r in lire_regles_qualite(flux_transformation) if r["type_regle"] == "contrat_adp"]
    if regles_adp:
        raise ErreurConfiguration(
            "Des règles contrat_adp sont déclarées mais leur application n'est pas encore implémentée (MIG-018) : "
            "valider les règles exhaustives avant d'activer ce contrôle."
        )
    if contexte["env_nom"] == MARQUEUR_PRD:
        return {"regle_id": "CONTRAT_ADP", "type_regle": "contrat_adp", "colonne": None, "bloquant": True,
                "statut": STATUT_BLOQUE,
                "commentaire": "Règles exhaustives du contrat ADP non fournies : publication ADP refusée (MIG-018)."}
    return {"regle_id": "CONTRAT_ADP", "type_regle": "contrat_adp", "colonne": None, "bloquant": False,
            "statut": "Non applicable",
            "commentaire": "Hors PRD : aucun appel ADP ; comparatif de référence non disponible."}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controler_sirh(flux: Dict[str, Any]) -> Dict[str, Any]:
    """Calcule le résumé SIRH, trace les rejets par règle et l'état du contrat ADP, puis applique un seuil s'il est déclaré."""
    parametres = flux["parametres"]
    flux_transformation = exiger_parametre(parametres, "flux_transformation", flux["flux_id"])
    rejets = lire_table(valider_nom_table(exiger_parametre(parametres, "table_rejet", flux["flux_id"]))).where(F.col("execution_id") == execution_id)
    sorties = lire_table(valider_nom_table(exiger_parametre(parametres, "table_sortie", flux["flux_id"]))).where(F.col("execution_id") == execution_id)
    resume = calculer_resume_sirh(rejets, sorties)
    resume.update({"cd_source": parametres.get("cd_source"), "cd_target": parametres.get("cd_target")})
    ecrire_resume(flux["flux_id"], resume)

    par_regle = {ligne["regle_id"]: ligne["count"] for ligne in rejets.groupBy("regle_id").count().collect()}
    controles = [
        {"regle_id": regle["regle_id"], "type_regle": regle["type_regle"], "colonne": regle["colonne"],
         "nb_lignes_controlees": resume["mt_lignes_lues"], "nb_lignes_en_echec": par_regle.get(regle["regle_id"], 0),
         "bloquant": regle["bloquant"], "statut": "Rejets" if par_regle.get(regle["regle_id"]) else "Conforme", "commentaire": None}
        for regle in lire_regles_qualite(flux_transformation)
    ]
    if parametres.get("cible_adp"):
        controles.append(controle_contrat_adp(flux_transformation))

    seuil = lire_flux(flux_transformation, exiger_actif=False).get("seuil_rejet_pct")
    depassement = seuil is not None and resume["pc_lignes_rejets"] * 100 > Decimal(seuil)
    if seuil is not None:
        controles.append({"regle_id": "SEUIL_REJET", "type_regle": "seuil", "colonne": None,
                          "nb_lignes_controlees": resume["mt_lignes_lues"], "nb_lignes_en_echec": resume["mt_lignes_rejets"],
                          "bloquant": True, "statut": STATUT_ECHEC if depassement else "Conforme",
                          "commentaire": f"Seuil déclaré : {seuil} %"})
    ecrire_controles_qualite(flux["flux_id"], controles)
    if depassement:
        raise ErreurDonnees(f"Taux de rejet {resume['pc_lignes_rejets'] * 100:.2f} % supérieur au seuil déclaré de {seuil} %.")
    return {
        "statut": STATUT_SUCCES_REJETS if resume["mt_lignes_rejets"] else STATUT_SUCCES,
        "lignes_lues": resume["mt_lignes_lues"], "lignes_ecrites": resume["mt_lignes_inserts"],
        "lignes_rejetees": resume["mt_lignes_rejets"],
        "details": {"pc_lignes_rejets": str(resume["pc_lignes_rejets"]), "ds_type_rejets": resume["ds_type_rejets"]},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def controler_remuneration(flux: Dict[str, Any]) -> Dict[str, Any]:
    """Reproduit les indicateurs de mail_variables pour la branche rémunération et les trace dans gld_resume."""
    parametres = flux["parametres"]
    stagings = exiger_parametre(parametres, "tables_staging", flux["flux_id"])
    salaire = lire_table(valider_nom_table(exiger_parametre(parametres, "table_sortie_base_salary", flux["flux_id"])))
    bonus = lire_table(valider_nom_table(exiger_parametre(parametres, "table_sortie_bonus", flux["flux_id"])))
    indicateurs = {
        "baseSalary_nb_source": lire_table(valider_nom_table(stagings["base_salary"])).count(),
        "baseSalary_nb_inserts": salaire.where(F.col("execution_id") == execution_id).count(),
        "bonusPercentage_nb_source": lire_table(valider_nom_table(stagings["bonus_percentage"])).count(),
        "bonusTargetValue_nb_source": lire_table(valider_nom_table(stagings["bonus_target"])).count(),
        "bonus_nb_inserts": bonus.where(F.col("execution_id") == execution_id).count(),
    }
    ecrire_resume(flux["flux_id"], {}, indicateurs)
    ecrire_controles_qualite(flux["flux_id"], [{
        "regle_id": "CONTROLES_SFTP", "type_regle": "format", "colonne": "employeenumber", "bloquant": False,
        "statut": "Non applicable",
        "commentaire": "Contrôles de format de matricule inactifs dans l'existant ; règles exhaustives à fournir (MIG-018).",
    }])
    lues = indicateurs["baseSalary_nb_source"] + indicateurs["bonusPercentage_nb_source"] + indicateurs["bonusTargetValue_nb_source"]
    return {"statut": STATUT_SUCCES, "lignes_lues": lues,
            "lignes_ecrites": indicateurs["baseSalary_nb_inserts"] + indicateurs["bonus_nb_inserts"],
            "lignes_rejetees": 0, "details": indicateurs}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_statut_global(statuts: Dict[str, str]) -> str:
    """Calcule le statut global à partir du statut final de chaque flux planifié."""
    if not statuts:
        return STATUT_SUCCES
    echecs = [f for f, s in statuts.items() if s in (STATUT_ECHEC, STATUT_BLOQUE, STATUT_EN_COURS)]
    if not echecs:
        return STATUT_SUCCES_REJETS if STATUT_SUCCES_REJETS in statuts.values() else STATUT_SUCCES
    return STATUT_ECHEC if len(echecs) == len(statuts) else STATUT_ECHEC_PARTIEL

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cloturer_execution() -> Dict[str, Any]:
    """Marque bloqués les flux non lancés dont une dépendance a échoué, calcule le statut global et clôture ctl_execution."""
    execution = lire_execution(execution_id)
    if not execution or not execution.get("plan_execution"):
        raise ErreurConfiguration(f"Exécution inconnue ou sans plan : {execution_id}.")
    plan = json.loads(execution["plan_execution"])
    date_plan = date.fromisoformat(plan["date_traitement"])
    planifies = [f for vague in plan["vagues"] for f in vague]
    flux_config = {f["flux_id"]: f for f in lire_tous_flux()}
    dependances = {}
    for d in lire_dependances():
        dependances.setdefault(d["flux_id"], []).append((d["depend_de_flux_id"], d["type_dependance"]))

    resultats = statuts_par_flux(lire_etapes_consolidees(execution_id=execution_id))
    statuts = {}
    for identifiant in planifies:
        if identifiant in resultats:
            statuts[identifiant] = resultats[identifiant]["statut"]
            continue
        en_echec = [d for d, t in dependances.get(identifiant, [])
                    if t == "succes" and statuts.get(d) in (STATUT_ECHEC, STATUT_BLOQUE, STATUT_EN_COURS)]
        statut = STATUT_BLOQUE if en_echec else STATUT_ECHEC
        message = (f"Non lancé : dépendance en échec ({', '.join(en_echec)})." if en_echec
                   else "Non lancé alors qu'aucune dépendance n'a échoué : vérifier l'exécution du pipeline.")
        etape = ETAPE_PRINCIPALE.get(flux_config.get(identifiant, {}).get("notebook"), "inconnue")
        etat = demarrer_etape(execution_id, identifiant, etape, date_plan)
        terminer_etape(etat, statut, message_erreur=message)
        statuts[identifiant] = statut

    statut_global = calculer_statut_global(statuts)
    synthese = {s: sum(1 for v in statuts.values() if v == s) for s in sorted(set(statuts.values()))}
    enregistrer_execution({"execution_id": execution_id, "fin": maintenant_utc(), "statut": statut_global,
                           "message": json.dumps(synthese, ensure_ascii=False)})
    return {"statut": statut_global, "flux": statuts, "synthese": synthese}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= TRAITEMENT DE L'ACTION =============
resultat = {}
try:
    journal.info("=" * 80)
    journal.info(f"🚀 CONTROLE : ACTION {action.upper()}")
    journal.info("=" * 80)
    if action == "ouvrir":
        etat_etape = demarrer_etape(execution_id, FLUX_ORCHESTRATION, "ouvrir", date_traitement_effective)
        resultat = ouvrir_execution()
        terminer_etape(etat_etape, STATUT_SUCCES, lignes_lues=len(resultat["flux"]), details={"vagues": resultat["vagues"]})
    elif action == "cloturer":
        etat_etape = demarrer_etape(execution_id, FLUX_ORCHESTRATION, "cloturer", date_traitement_effective)
        bilan = cloturer_execution()
        terminer_etape(etat_etape, STATUT_SUCCES, details=bilan["synthese"])
        resultat = {"execution_id": execution_id, "environnement": contexte["env_nom"], **bilan}
    else:
        flux = lire_flux(flux_id)
        if flux.get("notebook") != contexte["notebook_nom"]:
            raise ErreurConfiguration(f"Le flux {flux_id} est routé vers {flux.get('notebook')} et non vers {contexte['notebook_nom']}.")
        etat_etape = demarrer_etape(execution_id, flux_id, "control", date_traitement_effective)
        traitement = flux["parametres"].get("traitement")
        blocage = verifier_dependances_execution(execution_id, flux_id)
        if blocage:
            journal.warning(f"⚠️ {blocage}")
            bilan = {"statut": STATUT_BLOQUE, "lignes_lues": 0, "lignes_ecrites": 0, "lignes_rejetees": 0,
                     "message": blocage, "details": {"motif": blocage}}
        elif traitement == "resume_sirh":
            bilan = controler_sirh(flux)
        elif traitement == "resume_remuneration":
            bilan = controler_remuneration(flux)
        else:
            raise ErreurConfiguration(f"Traitement de contrôle inconnu pour {flux_id} : {traitement!r}.")
        terminer_etape(etat_etape, bilan["statut"], lignes_lues=bilan["lignes_lues"], lignes_ecrites=bilan["lignes_ecrites"],
                       lignes_rejetees=bilan["lignes_rejetees"], details=bilan["details"], message_erreur=bilan.get("message"))
        resultat = {"execution_id": execution_id, "flux_id": flux_id, "statut": bilan["statut"],
                    "lignes_lues": bilan["lignes_lues"], "lignes_rejetees": bilan["lignes_rejetees"]}
    journal.info(f"✓ Action {action} terminée")

except Exception as exc:
    message = nettoyer_message_erreur(exc)
    journal.error(f"❌ Échec de l'action {action} : {message}", exc_info=True)
    if etat_etape is not None:
        terminer_etape(etat_etape, STATUT_ECHEC, message_erreur=message)
    else:
        try:
            echec = demarrer_etape(execution_id, flux_id or FLUX_ORCHESTRATION, action if action != "controler" else "control", date_traitement_effective)
            terminer_etape(echec, STATUT_ECHEC, message_erreur=message)
        except Exception:
            journal.error("❌ Impossible de journaliser l'échec dans ctl_execution_etape.", exc_info=True)
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
