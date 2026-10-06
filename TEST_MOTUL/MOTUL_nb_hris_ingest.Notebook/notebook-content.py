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
# MOTUL_nb_hris_ingest, nom repris du champ displayName du fichier .platform.
#
# **Type d'objet**
#
# Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark, appelé par le pipeline MOTUL_PL_HRIS_Orchestrateur pour chaque flux d'ingestion déclaré dans cfg_flux.
#
# **Chemin dans le dépôt**
#
# TEST_MOTUL/MOTUL_nb_hris_ingest.Notebook/notebook-content.py.
#
# **Description fonctionnelle**
#
# Ce notebook récupère les données entrantes du hub RH sans leur appliquer de règle métier. Il charge l'export des salariés produit par TalentSoft, les trois exports de rémunération déposés sur le SFTP de TalentSoft, les dix-sept référentiels de correspondance entre TalentSoft et ADP, et les fichiers CSV déposés ponctuellement. Les exports récurrents ne sont plus traités comme des fichiers datés : le jeu le plus récent est chargé dans une table de staging stg_*, que MOTUL_nb_hris_transform compare ensuite à la table d'état non-STG. Il remplace les activités de copie Synapse, la recherche du fichier de la veille par boucle et les quarante et un datasets.
#
# **Dépendances**
#
# Le notebook importe MOTUL_nb_hris_lib. Il lit cfg_flux pour connaître la source, le format et la table de destination de chaque flux, et cfg_qualite pour les contrôles minimaux d'un import ponctuel. Il lit le SFTP TalentSoft avec l'hôte, le port, le compte et le dossier portés par les variables d'environnement, et le mot de passe lu dans le coffre par le nom porté par env_secret_sftp. Pendant la transition, l'export JSON des salariés est lu sur le compte ADLS désigné par env_adls_compte et env_adls_filesystem. Il écrit dans les tables stg_*, cfg_mapping, brz_*, ctl_execution_etape et ctl_fichier_traite.
#
# **Fonctionnement et logique de traitement**
#
# Après la lecture de sa configuration, le notebook choisit un traitement selon le champ traitement des paramètres du flux. Le traitement export_json_ts sélectionne le fichier daté le plus récent qui ne dépasse pas la date de traitement, en lisant la date dans le nom plutôt qu'en comparant les noms, puis reproduit l'analyse JSON de la vue Synapse d'origine : chaque valeur scalaire devient un texte tronqué à cinquante caractères, un objet ou un tableau devient nul, le téléphone professionnel prend le mobile puis le fixe, et la valeur None de la date de fin de contrat devient une chaîne vide. Le traitement export_csv_sftp télécharge en mémoire un export de rémunération, contrôle ses colonnes et le charge tel quel en staging, une valeur vide devenant nulle comme dans la copie d'origine. Le traitement referentiels télécharge les dix-sept fichiers de correspondance, en conserve une copie brute dans Files/reference, lit les colonnes par position comme le faisaient les tables externes, refuse un entier invalide ou un code en double, puis recalcule cfg_mapping en clôturant les correspondances modifiées ou disparues. Le traitement depot_csv_ponctuel applique le processus d'import ponctuel : validation du nom avant lecture, refus d'un fichier déjà chargé, lecture sans inférence, validation du schéma et des types, contrôles de cfg_qualite, chargement Delta transactionnel, puis archivage ou rejet du fichier avec un motif.
#
# **Paramètres**
#
# Le paramètre execution_id porte l'identifiant unique de l'exécution produit par le pipeline. Le paramètre flux_id désigne la ligne de cfg_flux à traiter. Le paramètre mode vaut complet, incremental ou ponctuel. Le paramètre date_traitement, au format aaaa-mm-jj, vaut par défaut la date du jour à Paris. Le paramètre variables_env porte le JSON des variables d'environnement résolues par le pipeline ; il n'a aucune valeur par défaut exploitable. Le paramètre fichier désigne le fichier d'un import ponctuel. Le paramètre rejouer_reussis, faux par défaut, autorise le rechargement d'un fichier ponctuel déjà chargé.
#
# **Sorties produites**
#
# Les tables stg_ts_employe, stg_sftp_base_salary, stg_sftp_bonus_percentage et stg_sftp_bonus_target contiennent le jeu courant de chaque export avec l'identifiant d'exécution, la date de traitement et le fichier source. La table cfg_mapping contient les correspondances historisées par date de début et de fin. Un import ponctuel alimente la table brz_* déclarée dans cfg_flux. Chaque exécution laisse une trace dans ctl_execution_etape avec les volumes lus, écrits et rejetés.
#
# **Limitations connues et points d'attention**
#
# La connectivité de Fabric vers le compte de stockage privé et vers le SFTP reste à établir par la tâche MIG-006 ; sans elle, le notebook échoue explicitement. L'encodage réel des fichiers SFTP n'est pas documenté ; l'UTF-8 de la copie d'origine est déclaré dans cfg_flux. La clé d'hôte SFTP n'est pas vérifiée, comme dans l'existant. Les variables env_sftp_port et env_sftp_utilisateur s'ajoutent à l'inventaire du plan pour ne coder en dur ni le port ni le compte. La configuration du flux import_csv_ponctuel reste à fournir : sa table cible et ses colonnes ne sont pas connues, le flux est donc inactif. Les paramètres sont placés avant la configuration de l'environnement, car les variables arrivent par paramètre.
#
# **Responsable et contact**
#
# Équipe data du projet HRIS Motul.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

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

execution_id = ""            # GUID de l'exécution, produit par MOTUL_PL_HRIS_Orchestrateur
flux_id = ""                 # Identifiant du flux dans cfg_flux
mode = "incremental"         # complet, incremental ou ponctuel
date_traitement = ""         # Date au format aaaa-mm-jj ; vide pour la date du jour à Paris
variables_env = ""           # JSON des variables d'environnement résolues au lancement du pipeline
fichier = ""                 # Nom du fichier déposé, pour un import ponctuel uniquement
rejouer_reussis = "false"    # true pour recharger un fichier ponctuel déjà chargé avec succès

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION DE L'ENVIRONNEMENT =============
etat_etape = None
blocage = None
try:
    contexte = initialiser_contexte_hris(variables_env)
    journal.info(f"✓ Environnement résolu : {contexte['env_nom']}")
    journal.info(f"✓ Workspace : {contexte['workspace_nom']}")
    journal.info(f"✓ Lakehouse : {contexte['lakehouse_nom']}")
    journal.info(f"✓ Notebook : {contexte['notebook_nom']}")

    if not execution_id or not flux_id:
        raise ErreurConfiguration("Les paramètres execution_id et flux_id sont obligatoires.")
    if mode not in ("complet", "incremental", "ponctuel"):
        raise ErreurConfiguration(f"Mode inconnu : {mode!r} ; valeurs admises : complet, incremental, ponctuel.")
    date_traitement_effective = resoudre_date_traitement(date_traitement)
    rejouer = analyser_booleen(rejouer_reussis, "rejouer_reussis")

    initialiser_socle()
    flux = lire_flux(flux_id)
    if flux.get("notebook") != contexte["notebook_nom"]:
        raise ErreurConfiguration(
            f"Le flux {flux_id} est routé vers {flux.get('notebook')} et non vers {contexte['notebook_nom']}."
        )
    etat_etape = demarrer_etape(execution_id, flux_id, "ingest", date_traitement_effective, {"mode": mode})
    blocage = verifier_dependances_execution(execution_id, flux_id)
    journal.info(f"✓ Flux {flux_id} : traitement {flux['parametres'].get('traitement')}, date {date_traitement_effective}")

except Exception as exc:
    journal.error(f"❌ Erreur lors de la configuration de l'environnement : {nettoyer_message_erreur(exc)}", exc_info=True)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CATALOGUE DES REFERENTIELS DE CORRESPONDANCE =============
# Transcription des dix-sept tables externes de SYN/sqlscript/Script_Init.json : fichier, colonnes par position,
# type entier ou texte, code TalentSoft servant de clé et attributs ADP avec leur libellé.
CATALOGUE_REFERENTIELS: List[Dict[str, Any]] = [
    {"domaine": "lu_hr_emp_diplome", "fichier": "diplome.csv", "cle": "cd_emp_diplome_ts",
     "colonnes": [("cd_emp_diplome_ts", "texte"), ("ds_emp_diplome_ts", "texte"), ("cd_emp_diplome_adp", "texte"),
                  ("ds_emp_diplome_adp", "texte"), ("or_emp_diplome", "entier"), ("cd_emp_diplome_api_ts", "texte"),
                  ("ds_emp_diplome_api_ts", "texte")],
     "attributs": [("cd_emp_diplome_adp", "ds_emp_diplome_adp")]},
    {"domaine": "lu_hr_emp_nationalite", "fichier": "nationalite.csv", "cle": "cd_emp_nationalite_ts",
     "colonnes": [("id_emp_nationalite_ts", "entier"), ("cd_emp_nationalite_ts", "texte"), ("ds_emp_nationalite_ts", "texte"),
                  ("cd_emp_nationalite_adp", "texte"), ("ds_emp_nationalite_adp", "texte"), ("or_emp_nationalite", "entier")],
     "attributs": [("cd_emp_nationalite_adp", "ds_emp_nationalite_adp")]},
    {"domaine": "lu_hr_emp_pays", "fichier": "pays.csv", "cle": "cd_emp_pays_ts",
     "colonnes": [("id_emp_pays_ts", "entier"), ("cd_emp_pays_ts", "texte"), ("ds_emp_pays_ts", "texte"),
                  ("id_emp_pays_adp", "texte"), ("ds_emp_pays_adp", "texte"), ("or_emp_pays", "entier")],
     "attributs": [("id_emp_pays_adp", "ds_emp_pays_adp")]},
    {"domaine": "lu_hr_emp_situation_matrimoniale", "fichier": "situation_matrimoniale.csv", "cle": "cd_emp_situation_matrimoniale_ts",
     "colonnes": [("cd_emp_situation_matrimoniale_ts", "texte"), ("ds_emp_situation_matrimoniale_ts", "texte"),
                  ("cd_emp_situation_matrimoniale_adp", "texte"), ("ds_emp_situation_matrimoniale_adp", "texte"),
                  ("or_emp_situation_matrimoniale", "entier")],
     "attributs": [("cd_emp_situation_matrimoniale_adp", "ds_emp_situation_matrimoniale_adp")]},
    {"domaine": "lu_hr_emp_titre", "fichier": "titre.csv", "cle": "cd_emp_titre_ts",
     "colonnes": [("cd_emp_titre_ts", "texte"), ("lb_emp_titre_ts", "texte"), ("ds_emp_titre_ts", "texte"),
                  ("cd_emp_titre_adp", "texte"), ("ds_emp_titre_adp", "texte"), ("or_emp_titre", "entier")],
     "attributs": [("cd_emp_titre_adp", "ds_emp_titre_adp")]},
    {"domaine": "lu_hr_emp_sexe", "fichier": "sexe.csv", "cle": "cd_emp_sexe_ts",
     "colonnes": [("cd_emp_sexe_ts", "texte"), ("ds_emp_sexe_ts", "texte"), ("cd_emp_sexe_adp", "texte"),
                  ("ds_emp_sexe_adp", "texte"), ("or_emp_sexe", "entier")],
     "attributs": [("cd_emp_sexe_adp", "ds_emp_sexe_adp")]},
    {"domaine": "lu_hr_emp_work_accident", "fichier": "work_accident.csv", "cle": "cd_emp_work_accident_ts",
     "colonnes": [("cd_emp_work_accident_ts", "texte"), ("ds_emp_work_accident_ts", "texte"), ("cd_emp_work_accident_adp", "texte"),
                  ("ds_emp_work_accident_adp", "texte"), ("or_emp_work_accident", "entier")],
     "attributs": [("cd_emp_work_accident_adp", "ds_emp_work_accident_adp")]},
    {"domaine": "lu_hr_ppc_convention_collective", "fichier": "convention_collective.csv", "cle": "cd_ppc_convention_collective_ts",
     "colonnes": [("cd_ppc_convention_collective_ts", "texte"), ("ds_ppc_convention_collective_ts", "texte"),
                  ("cd_ppc_convention_collective_adp", "texte"), ("ds_ppc_convention_collective_adp", "texte"),
                  ("or_ppc_convention_collective", "entier")],
     "attributs": [("cd_ppc_convention_collective_adp", "ds_ppc_convention_collective_adp")]},
    {"domaine": "lu_hr_ppc_classification", "fichier": "classification.csv", "cle": "cd_convention_collective_lvl2_ts",
     "colonnes": [("cd_convention_collective_lvl2_ts", "texte"), ("lb_convention_collective_lvl2_ts", "texte"),
                  ("cd_classification_adp", "texte"), ("lb_classification_adp", "texte"), ("cd_coefficient_adp", "texte"),
                  ("cd_ppc_convention_collective_adp", "texte"), ("cd_ppc_convention_collective_ts", "texte")],
     "attributs": [("cd_classification_adp", "lb_classification_adp"), ("cd_coefficient_adp", None),
                   ("cd_ppc_convention_collective_adp", None)]},
    {"domaine": "lu_hr_ppc_classes", "fichier": "classes.csv", "cle": "cd_occupational_category_ts",
     "colonnes": [("cd_occupational_category_ts", "texte"), ("lb_fr_occupational_category_ts", "texte"),
                  ("lb_en_occupational_category_ts", "texte"), ("cd_categorie_cotisant_adp", "texte"),
                  ("lb_categorie_cotisant_adp", "texte"), ("cd_classe_remuneration_adp", "texte"),
                  ("lb_classe_remuneration_adp", "texte")],
     "attributs": [("cd_categorie_cotisant_adp", "lb_categorie_cotisant_adp"),
                   ("cd_classe_remuneration_adp", "lb_classe_remuneration_adp")]},
    {"domaine": "lu_hr_geo_entite", "fichier": "entite.csv", "cle": "cd_geo_entite_ts",
     "colonnes": [("cd_geo_entite_ts", "texte"), ("ds_geo_entite_ts", "texte"), ("cd_geo_parent_entite_ts", "texte"),
                  ("cd_geo_entite_adp", "texte"), ("ds_geo_entite_adp", "texte"), ("or_geo_entite_adp", "entier")],
     "attributs": [("cd_geo_entite_adp", "ds_geo_entite_adp")]},
    {"domaine": "lu_hr_ppc_contrat", "fichier": "contrat.csv", "cle": "cd_ppc_contrat_ts",
     "colonnes": [("cd_ppc_contrat_ts", "texte"), ("ds_ppc_contrat_ts", "texte"), ("cd_ppc_contrat_adp", "texte"),
                  ("ds_ppc_contrat_adp", "texte"), ("cd_ppc_contrat_type", "texte"), ("ds_ppc_contrat_type", "texte"),
                  ("or_ppc_contrat", "entier")],
     "attributs": [("cd_ppc_contrat_adp", "ds_ppc_contrat_adp"), ("cd_ppc_contrat_type", "ds_ppc_contrat_type")]},
    {"domaine": "lu_hr_ppc_contrat2", "fichier": "contrat_v2.csv", "cle": "cd_contract_ts",
     "colonnes": [("cd_contract_ts", "texte"), ("lb_contract_ts", "texte"), ("cd_nature_contract_adp", "texte"),
                  ("lb_nature_contract_adp", "texte"), ("cd_type_contract_adp", "texte"), ("lb_type_contract_adp", "texte"),
                  ("cd_type_collaboration_adp", "texte"), ("lb_type_collaboration_adp", "texte")],
     "attributs": [("cd_nature_contract_adp", "lb_nature_contract_adp"), ("cd_type_contract_adp", "lb_type_contract_adp"),
                   ("cd_type_collaboration_adp", "lb_type_collaboration_adp")]},
    {"domaine": "lu_hr_ppc_cost_center", "fichier": "cost_center.csv", "cle": "cd_ppc_cost_center_ts",
     "colonnes": [("cd_ppc_cost_center_ts", "texte"), ("ds_ppc_cost_center_ts", "texte"), ("cd_ppc_cost_center_adp", "texte"),
                  ("ds_ppc_cost_center_adp", "texte"), ("or_ppc_cost_center", "entier")],
     "attributs": [("cd_ppc_cost_center_adp", "ds_ppc_cost_center_adp")]},
    {"domaine": "lu_hr_ppc_location_country", "fichier": "location_country.csv", "cle": "cd_ppc_location_country_ts",
     "colonnes": [("cd_ppc_location_country_ts", "texte"), ("ds_ppc_location_country_ts", "texte"),
                  ("cd_ppc_location_country_adp", "texte"), ("ds_ppc_location_country_adp", "texte"),
                  ("or_ppc_location_country", "entier")],
     "attributs": [("cd_ppc_location_country_adp", "ds_ppc_location_country_adp")]},
    {"domaine": "lu_hr_ppc_raison_debut_contrat", "fichier": "raison_debut_contrat.csv", "cle": "cd_ppc_raison_debut_contrat_ts",
     "colonnes": [("cd_ppc_raison_debut_contrat_ts", "texte"), ("ds_ppc_raison_debut_contrat_ts", "texte"),
                  ("cd_ppc_raison_debut_contrat_adp", "entier"), ("ds_ppc_raison_debut_contrat_adp", "texte"),
                  ("or_ppc_raison_debut_contrat", "entier")],
     "attributs": [("cd_ppc_raison_debut_contrat_adp", "ds_ppc_raison_debut_contrat_adp")]},
    {"domaine": "lu_hr_organisationalStructure", "fichier": "organisationalstructure.csv", "cle": "cd_organisationalstructure_ts",
     "colonnes": [("cd_organisationalstructure_ts", "texte"), ("ds_organisationalstructure_ts", "texte"),
                  ("cd_organisationalstructure_adp", "texte"), ("ds_organisationalstructure_adp", "texte"),
                  ("or_organisationalstructure", "entier")],
     "attributs": [("cd_organisationalstructure_adp", None), ("ds_organisationalstructure_adp", None)]},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CHAMPS DE L'EXPORT JSON DES SALARIES =============
# Transcription de la vue ods.vw_hr_ts_employee_j de SYN/sqlscript/Script_Create_Procedures.json, dans le même ordre.
# Une source sous forme de tuple reproduit un COALESCE ; l'option remplacer_none reproduit REPLACE(..., 'None', '').
CHAMPS_EXPORT_TS_EMPLOYE: List[Tuple[str, Any, Dict[str, Any]]] = [
    ("id_unique", "employeeNumber", {}), ("id_payroll", "LegacyID", {}), ("cd_matricule_it", "Matricule_IT", {}),
    ("dt_debut_identite", "Identity_startDate", {}), ("lb_nom", "LastName", {}), ("lb_prenom", "FirstName", {}),
    ("lb_nom_naissance", "BirthName", {}), ("cd_emp_titre_ts", "Title", {}), ("cd_emp_sexe_ts", "Gender", {}),
    ("cd_emp_nationalite_ts", "Nationality", {}), ("id_numero_securite_sociale", "hubSocialSecurityNumber", {}),
    ("dt_debut_situation_matrimoniale", "MaritalStatus_startDate", {}),
    ("cd_emp_situation_matrimoniale_ts", "MaritalStatus", {}),
    ("dt_debut_civil_registration", "CivilRegistration_startDate", {}), ("dt_naissance", "Date_de_naissance", {}),
    ("lb_naissance_ville", "Ville_de_naissance", {}), ("lb_naissance_cp", "code_postal_lieu_de_naissance", {}),
    ("lb_naissance_departement", "region_ou_etat_lieu_de_naissance", {}), ("cd_naissance_pays_ts", "Pays_de_naissance", {}),
    ("lb_fiscal_ville", "LegalTownOrCity", {}), ("lb_fiscal_country", "LegalCountry", {}),
    ("lb_fiscal_cp", "LegalPostalCode", {}), ("lb_fiscal_street", "LegalStreet", {}),
    ("lb_fiscal_streetnumber", "LegalStreetNumber", {}),
    ("lb_fiscal_additional_address_information", "LegalAdditionalAddressInformation", {}),
    ("lb_fiscal_streetnumber_complement", "LegalStreetNumberComplement", {}),
    ("lb_adresse_cp", "address_postalCode", {}), ("lb_adresse_ville", "adresse_city", {}),
    ("cd_adresse_pays_ts", "adresse_countryCode", {}), ("dt_debut_contatct_info", "ContactInformation_startDate", {}),
    ("lb_email_professionnel", "BusinessEmail", {}), ("lb_email_personnel", "PersonalEmail", {}),
    ("lb_telephone_professionnel", ("BusinessMobile", "BusinessPhone"), {}), ("lb_telephone_personnel", "PersonalMobile", {}),
    ("dt_entree_societe", "referenceAdmissionDate", {}), ("dt_entree_groupe", "GroupStartDate", {}),
    ("dt_anciennete", "RecalculatedSeniorityDate", {}), ("dt_sortie", "GroupEndDate", {}),
    ("dt_company_start", "CompanyStartDate", {}), ("cd_ppc_raison_debut_contrat_ts", "HiringReason", {}),
    ("cd_ppc_raison_fin_contrat_ts", "TerminationReason", {}), ("dt_ppc_debut_contrat", "date_debut_contract", {}),
    ("dt_ppc_fin_contrat", "date_fin_contract", {"remplacer_none": True}), ("cd_ppc_contrat_ts", "NatureOfContract", {}),
    ("cd_recours_cdd", "FixedTermContractReason", {}), ("cd_occupational_category", "OccupationalCategory", {}),
    ("dt_ppc_debut_periode_essaie", "FirstPeriodStartDate", {}), ("dt_ppc_fin_periode_essaie", "FirstPeriodEndDate", {}),
    ("dt_situation_start", "Situation_starDate", {}), ("cd_ppc_convention_collective_lvl2_ts", "Coefficient", {}),
    ("cd_geo_entite_ts", "LegalStructure", {}), ("cd_ppc_location_country_ts", "GeographicOrganisationStructure", {}),
    ("pc_ppc_fte", "PPC_FTE", {}), ("cd_organisationalstructure_ts", "OrganisationalStructure", {}),
    ("id_numero_ordre", "numero_ordre", {}), ("cd_ppc_cost_center_ts", "CostCenter", {}),
    ("pc_imputation", "pourcentage_imputation", {}), ("lb_account_holder", "AccountHolder", {}),
    ("lb_iban", "IBANOrABA", {}), ("lb_bic", "BicOrSwift", {}), ("lb_account_number", "AccountNumber", {}),
    ("lb_bank_name", "BankName", {}), ("dt_debut_mt_salaire_base", "base_startDate", {}),
    ("mt_salaire_base", "salary_base", {}), ("dt_debut_pc_prime_annuelle", "pourcentage_prime_annuelles_startDate", {}),
    ("pc_prime_annuelle", "Pourcentage_prime_annuelles", {}), ("dt_debut_prime_annuelle", "prime_annuelles_startDate", {}),
    ("prime_annuelle", "prime_annuelles", {}), ("lb_job_title", "WorkingType", {}),
]
LONGUEUR_NVARCHAR_EXPORT = 50

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def valeur_json_tsql(objet: Any, cle: str) -> Optional[str]:
    """
    Reproduit JSON_VALUE(objet, '$.cle') en mode lax : un scalaire devient texte, un objet ou un tableau devient NULL.

    Les nombres conservent leur écriture d'origine car le JSON est analysé avec parse_float et parse_int textuels.
    """
    if not isinstance(objet, dict) or cle not in objet:
        return None
    valeur = objet[cle]
    if valeur is None or isinstance(valeur, (dict, list)):
        return None
    if isinstance(valeur, bool):
        return "true" if valeur else "false"
    texte = str(valeur)
    return None if len(texte) > 4000 else texte

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extraire_objets_export_json(contenu: bytes) -> List[Any]:
    """
    Reproduit OPENJSON('[' + contenu + ']') suivi d'OPENJSON(valeur) : renvoie les éléments de premier niveau.

    Un export sous forme d'objet donne ses valeurs, un export sous forme de tableau donne ses éléments.
    """
    try:
        texte = contenu.decode("utf-8-sig")
        racine = json.loads(texte, parse_float=str, parse_int=str, parse_constant=str)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ErreurDonnees(f"Export JSON illisible : {exc.__class__.__name__}.") from None
    if isinstance(racine, dict):
        return list(racine.values())
    if isinstance(racine, list):
        return list(racine)
    raise ErreurDonnees("Export JSON inattendu : un objet ou un tableau est attendu au premier niveau.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convertir_salarie_export(objet: Any) -> Tuple[Optional[str], ...]:
    """Convertit un salarié de l'export JSON en ligne de la vue d'origine, chaque valeur étant tronquée à 50 caractères."""
    valeurs = []
    for _, source, options in CHAMPS_EXPORT_TS_EMPLOYE:
        cles = source if isinstance(source, tuple) else (source,)
        valeur = None
        for cle in cles:
            valeur = valeur_json_tsql(objet, cle)
            if valeur is not None:
                break
        if valeur is not None and options.get("remplacer_none"):
            valeur = valeur.replace("None", "")
        valeurs.append(None if valeur is None else valeur[:LONGUEUR_NVARCHAR_EXPORT])
    return tuple(valeurs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_octets(chemin: str) -> bytes:
    """Lit le contenu binaire complet d'un fichier OneLake ou ADLS avec Spark."""
    lignes = spark.read.format("binaryFile").load(chemin).select("content").collect()
    if len(lignes) != 1:
        raise ErreurDonnees(f"Fichier introuvable ou ambigu : {len(lignes)} correspondance(s).")
    return bytes(lignes[0]["content"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ajouter_colonnes_techniques(
    donnees: DataFrame,
    execution: str,
    date_effective: date,
    fichier_source: Optional[str],
    date_fichier_source: Optional[date],
) -> DataFrame:
    """Ajoute les colonnes techniques de traçabilité de l'ingestion."""
    return (
        donnees.withColumn("execution_id", F.lit(execution))
        .withColumn("date_traitement", F.lit(date_effective).cast("date"))
        .withColumn("fichier_source", F.lit(fichier_source).cast("string"))
        .withColumn("date_fichier_source", F.lit(date_fichier_source).cast("date"))
        .withColumn("horodatage_ingestion", F.current_timestamp())
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingerer_export_json_ts(flux: Dict[str, Any], date_effective: date) -> Dict[str, Any]:
    """
    Charge en staging l'export JSON TalentSoft le plus récent du compte ADLS de transition.

    Remplace Validation_File_J, Get Metadata FilesImport, la boucle de recherche de la veille et la vue d'analyse JSON.
    """
    parametres = flux["parametres"]
    compte = lire_variable(exiger_parametre(parametres, "variable_compte", flux["flux_id"]))
    conteneur = lire_variable(flux["source_ref"])
    dossier = construire_chemin_adls(compte, conteneur, exiger_parametre(parametres, "dossier", flux["flux_id"]))
    motif = exiger_parametre(parametres, "motif_fichier", flux["flux_id"])
    noms = [entree.name for entree in notebookutils.fs.ls(dossier) if entree.isFile]
    nom_retenu, date_fichier, ignores = selectionner_fichier_plus_recent(noms, motif, date_effective)
    if date_fichier < date_effective:
        journal.warning(f"Export du {date_effective} absent : le jeu le plus récent, du {date_fichier}, est utilisé.")
    objets = extraire_objets_export_json(lire_octets(f"{dossier}/{nom_retenu}"))
    schema = T.StructType([T.StructField(colonne, T.StringType(), True) for colonne, _, _ in CHAMPS_EXPORT_TS_EMPLOYE])
    donnees = spark.createDataFrame([convertir_salarie_export(objet) for objet in objets], schema)
    donnees = ajouter_colonnes_techniques(donnees, execution_id, date_effective, nom_retenu, date_fichier)
    ecrire_table(donnees, valider_nom_table(flux["destination_table"]), "overwrite")
    return {
        "lignes_lues": len(objets), "lignes_ecrites": len(objets), "lignes_rejetees": 0,
        "watermark": date_fichier.isoformat(),
        "details": {"fichier": nom_retenu, "date_fichier": date_fichier.isoformat(), "fichiers_non_conformes": len(ignores)},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def aligner_colonnes_csv(
    colonnes_lues: Sequence[str],
    lignes: List[List[Optional[str]]],
    colonnes_attendues: Sequence[str],
    description: str,
) -> List[Tuple[Optional[str], ...]]:
    """
    Aligne les lignes CSV sur les colonnes attendues, par nom et sans tenir compte de la casse.

    Une colonne attendue absente ou une ligne plus longue que l'entête provoque un rejet explicite ; une colonne
    supplémentaire est ignorée, comme le faisait la table externe Parquet d'origine.
    """
    index = {nom.lower(): position for position, nom in enumerate(colonnes_lues)}
    manquantes = [nom for nom in colonnes_attendues if nom.lower() not in index]
    if manquantes:
        raise ErreurDonnees(f"{description} : colonne(s) attendue(s) absente(s) : {', '.join(manquantes)}.")
    largeur = len(colonnes_lues)
    resultat = []
    for numero, ligne in enumerate(lignes, start=2):
        if len(ligne) > largeur:
            raise ErreurDonnees(f"{description} : la ligne {numero} compte plus de valeurs que l'entête.")
        complete = list(ligne) + [None] * (largeur - len(ligne))
        resultat.append(tuple(complete[index[nom.lower()]] for nom in colonnes_attendues))
    return resultat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingerer_export_csv_sftp(flux: Dict[str, Any], date_effective: date) -> Dict[str, Any]:
    """
    Charge en staging, sans transformation, un export de rémunération du SFTP TalentSoft.

    Remplace Copy data BaseSalary, Copy data BonusPercentage et Copy data BonusTargetValue.
    """
    parametres = flux["parametres"]
    dossier = lire_variable(flux["source_ref"])
    nom_fichier = exiger_parametre(parametres, "fichier", flux["flux_id"])
    colonnes_attendues = exiger_parametre(parametres, "colonnes", flux["flux_id"])
    transport, client = ouvrir_session_sftp()
    try:
        contenu = telecharger_fichier_sftp(client, dossier, nom_fichier)
    finally:
        fermer_session_sftp(transport, client)
    colonnes, lignes = analyser_csv(
        contenu,
        exiger_parametre(parametres, "separateur", flux["flux_id"]),
        exiger_parametre(parametres, "guillemet", flux["flux_id"]),
        parametres.get("echappement"),
        exiger_parametre(parametres, "encodage", flux["flux_id"]),
        bool(exiger_parametre(parametres, "entete", flux["flux_id"])),
    )
    valeurs = aligner_colonnes_csv(colonnes, lignes, colonnes_attendues, f"{flux['flux_id']} / {nom_fichier}")
    schema = T.StructType([T.StructField(nom, T.StringType(), True) for nom in colonnes_attendues])
    donnees = ajouter_colonnes_techniques(spark.createDataFrame(valeurs, schema), execution_id, date_effective, nom_fichier, None)
    ecrire_table(donnees, valider_nom_table(flux["destination_table"]), "overwrite")
    return {
        "lignes_lues": len(valeurs), "lignes_ecrites": len(valeurs), "lignes_rejetees": 0,
        "watermark": date_effective.isoformat(),
        "details": {"fichier": nom_fichier, "empreinte_sha256": empreinte_sha256(contenu),
                    "colonnes_ignorees": len(colonnes) - len(colonnes_attendues)},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_correspondances(
    definition: Dict[str, Any],
    lignes: List[List[Optional[str]]],
) -> Tuple[List[Dict[str, Optional[str]]], int]:
    """
    Convertit les lignes d'un fichier de référence, lues par position, en correspondances pivot.

    Une valeur entière invalide ou un code TalentSoft en double, espaces de fin ignorés, provoque un rejet explicite.

    Returns:
        Les correspondances (domaine, code_source, attribut_cible, code_cible, libelle) et le nombre de lignes sans code.
    """
    noms = [nom for nom, _ in definition["colonnes"]]
    types = dict(definition["colonnes"])
    correspondances, sans_code, codes_vus = [], 0, set()
    for numero, ligne in enumerate(lignes, start=2):
        valeurs = dict(zip(noms, list(ligne) + [None] * (len(noms) - len(ligne))))
        for nom, valeur in valeurs.items():
            if types[nom] == "entier" and valeur is not None:
                if not re.match(r"^\s*[+-]?\d+\s*$", valeur):
                    raise ErreurDonnees(f"{definition['fichier']} ligne {numero} : valeur non entière dans {nom}.")
                valeurs[nom] = str(int(valeur.strip()))
        code = valeurs[definition["cle"]]
        if code is None:
            sans_code += 1
            continue
        code_normalise = code.rstrip(" ")
        if code_normalise in codes_vus:
            raise ErreurDonnees(f"{definition['fichier']} ligne {numero} : code TalentSoft en double dans {definition['cle']}.")
        codes_vus.add(code_normalise)
        for attribut, libelle in definition["attributs"]:
            correspondances.append({
                "domaine": definition["domaine"], "code_source": code_normalise, "attribut_cible": attribut,
                "code_cible": valeurs[attribut], "libelle": valeurs[libelle] if libelle else None,
                "fichier_source": definition["fichier"],
            })
    return correspondances, sans_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def recalculer_mapping_historise(
    existantes: List[Dict[str, Any]],
    nouvelles: List[Dict[str, Any]],
    domaines_rafraichis: Sequence[str],
    date_effective: date,
    execution: str,
) -> List[Dict[str, Any]]:
    """
    Recalcule cfg_mapping avec historisation par date de début et de fin.

    Une correspondance inchangée est conservée ; modifiée, elle est clôturée la veille et remplacée ; disparue, elle
    est clôturée ; nouvelle, elle est insérée. Une version ouverte le jour même est remplacée sur place.
    """
    cle = lambda ligne: (ligne["domaine"], ligne["code_source"], ligne["attribut_cible"])
    actives = {cle(l): l for l in existantes if l["date_fin"] is None and l["domaine"] in domaines_rafraichis}
    resultat = [dict(l) for l in existantes if l["date_fin"] is not None or l["domaine"] not in domaines_rafraichis]
    vues = set()
    for nouvelle in nouvelles:
        identifiant = cle(nouvelle)
        vues.add(identifiant)
        active = actives.get(identifiant)
        if active and active["code_cible"] == nouvelle["code_cible"] and active["libelle"] == nouvelle["libelle"]:
            resultat.append(dict(active))
            continue
        if active and active["date_debut"] < date_effective:
            resultat.append(dict(active, date_fin=date_effective - timedelta(days=1)))
        resultat.append(dict(nouvelle, date_debut=date_effective, date_fin=None, execution_id=execution))
    for identifiant, active in actives.items():
        if identifiant in vues:
            continue
        if active["date_debut"] < date_effective:
            resultat.append(dict(active, date_fin=date_effective - timedelta(days=1)))
    return resultat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingerer_referentiels(flux: Dict[str, Any], date_effective: date) -> Dict[str, Any]:
    """
    Recharge les dix-sept référentiels de correspondance depuis le SFTP et recalcule cfg_mapping.

    Remplace Get Metadata SFTP Reference, la boucle de copie des fichiers de référence et les tables externes lu_hr_*.
    """
    parametres = flux["parametres"]
    dossier = lire_variable(flux["source_ref"])
    encodage = exiger_parametre(parametres, "encodage", flux["flux_id"])
    transport, client = ouvrir_session_sftp()
    try:
        disponibles = set(lister_fichiers_sftp(client, dossier))
        manquants = sorted(d["fichier"] for d in CATALOGUE_REFERENTIELS if d["fichier"] not in disponibles)
        if manquants:
            raise ErreurDonnees("Référentiel(s) absent(s) du SFTP : " + ", ".join(manquants))
        contenus = {d["fichier"]: telecharger_fichier_sftp(client, dossier, d["fichier"]) for d in CATALOGUE_REFERENTIELS}
    finally:
        fermer_session_sftp(transport, client)

    nouvelles, volumes = [], {}
    for definition in CATALOGUE_REFERENTIELS:
        _, lignes = analyser_csv(
            contenus[definition["fichier"]],
            exiger_parametre(parametres, "separateur", flux["flux_id"]),
            exiger_parametre(parametres, "guillemet", flux["flux_id"]),
            parametres.get("echappement"),
            encodage,
            bool(exiger_parametre(parametres, "entete", flux["flux_id"])),
        )
        correspondances, sans_code = construire_correspondances(definition, lignes)
        nouvelles.extend(correspondances)
        volumes[definition["domaine"]] = {"lignes": len(lignes), "sans_code": sans_code}

    for nom_fichier, contenu in contenus.items():
        chemin = chemin_onelake(construire_chemin_relatif("reference", nom_fichier=nom_fichier))
        notebookutils.fs.put(chemin, contenu.decode(encodage), True)

    schema = schema_table_socle("cfg_mapping")
    existantes = [ligne.asDict() for ligne in lire_table("cfg_mapping").collect()]
    recalcule = recalculer_mapping_historise(
        existantes, nouvelles, [d["domaine"] for d in CATALOGUE_REFERENTIELS], date_effective, execution_id
    )
    horodatage = maintenant_utc()
    lignes_mapping = [
        tuple((horodatage if champ.name == "date_maj" else ligne.get(champ.name)) for champ in schema.fields)
        for ligne in recalcule
    ]
    ecrire_table(spark.createDataFrame(lignes_mapping, schema), "cfg_mapping", "overwrite")
    lues = sum(v["lignes"] for v in volumes.values())
    return {
        "lignes_lues": lues, "lignes_ecrites": len(nouvelles), "lignes_rejetees": 0,
        "watermark": date_effective.isoformat(),
        "details": {"domaines": volumes, "fichiers_non_catalogues": len(disponibles) - len(CATALOGUE_REFERENTIELS)},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deplacer_fichier_ponctuel(chemin_source: str, zone: str, flux_cible: str, date_effective: date, nom: str, motif: Optional[str]) -> str:
    """Déplace un fichier ponctuel vers l'archive ou le rejet, avec un fichier .motif.json adjacent en cas de rejet."""
    destination = chemin_onelake(construire_chemin_relatif(zone, flux_cible, date_effective, nom))
    notebookutils.fs.mv(chemin_source, destination, True, True)
    if motif is not None:
        contenu = json.dumps({"fichier": nom, "execution_id": execution_id, "motif": motif}, ensure_ascii=False, indent=2)
        notebookutils.fs.put(destination + ".motif.json", contenu, True)
    return destination

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def journaliser_fichier_traite(flux_cible: str, nom: str, chemin: str, contenu: bytes, date_effective: date, statut: str, motif: Optional[str]) -> None:
    """Trace un fichier ponctuel dans ctl_fichier_traite : nom, taille, empreinte, exécution, statut et motif."""
    schema = schema_table_socle("ctl_fichier_traite")
    ligne = {
        "flux_id": flux_cible, "nom_fichier": nom, "chemin": chemin, "taille_octets": len(contenu),
        "empreinte_sha256": empreinte_sha256(contenu), "horodatage": maintenant_utc(), "execution_id": execution_id,
        "date_traitement": date_effective, "statut": statut, "motif": motif,
    }
    donnees = spark.createDataFrame([tuple(ligne[champ.name] for champ in schema.fields)], schema)
    ecrire_table(donnees, "ctl_fichier_traite", "append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingerer_depot_csv_ponctuel(flux: Dict[str, Any], date_effective: date, nom_fichier: str, forcer: bool) -> Dict[str, Any]:
    """
    Charge un fichier CSV déposé ponctuellement dans Files/landing, selon le processus d'import ponctuel du plan.

    Toute anomalie déplace le fichier en rejet avec son motif ; en cas d'échec aucune ligne n'est publiée.
    """
    parametres = flux["parametres"]
    identifiant = flux["flux_id"]
    if not flux.get("destination_table"):
        raise ErreurConfiguration(f"cfg_flux.destination_table est vide pour {identifiant} : la table cible reste à fournir.")
    destination = valider_nom_table(flux["destination_table"])
    colonnes_attendues: Dict[str, str] = exiger_parametre(parametres, "colonnes", identifiant)
    if not nom_fichier:
        raise ErreurConfiguration("Le paramètre fichier est obligatoire pour un import ponctuel.")
    chemin_source = chemin_onelake(construire_chemin_relatif("landing", identifiant, date_effective, nom_fichier))

    if not re.match(exiger_parametre(parametres, "motif_nom_fichier", identifiant), nom_fichier):
        motif = "Nom de fichier non conforme au motif déclaré dans cfg_flux."
        deplacer_fichier_ponctuel(chemin_source, "rejet", identifiant, date_effective, nom_fichier, motif)
        raise ErreurDonnees(motif)

    contenu = lire_octets(chemin_source)
    empreinte = empreinte_sha256(contenu)
    deja_charge = (
        lire_table("ctl_fichier_traite")
        .where((F.col("flux_id") == identifiant) & (F.col("empreinte_sha256") == empreinte) & (F.col("statut") == "succes"))
        .limit(1).count() > 0
    )
    if deja_charge and not forcer:
        motif = "Fichier déjà chargé avec succès : rechargement refusé sans rejouer_reussis=true."
        chemin = deplacer_fichier_ponctuel(chemin_source, "rejet", identifiant, date_effective, nom_fichier, motif)
        journaliser_fichier_traite(identifiant, nom_fichier, chemin, contenu, date_effective, "refuse_doublon", motif)
        return {"statut": STATUT_IGNORE, "lignes_lues": 0, "lignes_ecrites": 0, "lignes_rejetees": 0,
                "details": {"fichier": nom_fichier, "motif": motif}}

    try:
        colonnes, lignes = analyser_csv(
            contenu,
            exiger_parametre(parametres, "separateur", identifiant),
            exiger_parametre(parametres, "guillemet", identifiant),
            parametres.get("echappement"),
            exiger_parametre(parametres, "encodage", identifiant),
            bool(exiger_parametre(parametres, "entete", identifiant)),
        )
        permissif = parametres.get("evolution_schema") == "permissive"
        connues = {nom.lower() for nom in colonnes_attendues}
        supplementaires = [nom for nom in colonnes if nom.lower() not in connues]
        if supplementaires and not permissif:
            raise ErreurDonnees("Colonne(s) non déclarée(s) refusée(s) : " + ", ".join(supplementaires))
        noms_charges = list(colonnes_attendues) + supplementaires
        valeurs = aligner_colonnes_csv(colonnes, lignes, noms_charges, f"{identifiant} / {nom_fichier}")
        texte = spark.createDataFrame(valeurs, T.StructType([T.StructField(n, T.StringType(), True) for n in noms_charges]))
        typees = texte.select(
            *[F.col(n).cast(colonnes_attendues[n]).alias(n) for n in colonnes_attendues],
            *[F.col(n) for n in supplementaires],
        )
        incompatibles = [
            n for n in colonnes_attendues
            if texte.where(F.col(n).isNotNull() & F.col(n).cast(colonnes_attendues[n]).isNull()).limit(1).count() > 0
        ]
        if incompatibles:
            raise ErreurDonnees("Type incompatible pour la ou les colonnes : " + ", ".join(incompatibles))

        cles = [c.strip() for c in (flux.get("cles_fusion") or "").split(",") if c.strip()]
        if cles and typees.groupBy(*cles).count().where(F.col("count") > 1).limit(1).count() > 0:
            raise ErreurDonnees("Clé de chargement non unique : " + ", ".join(cles))
        rejets = appliquer_regles_rejet(texte, lire_regles_qualite(identifiant), cles or [], date_effective)
        nb_rejets = rejets.count()
        if nb_rejets and rejets.where(F.col("bloquant")).limit(1).count() > 0:
            raise ErreurDonnees(f"{nb_rejets} anomalie(s) de qualité dont au moins une bloquante.")

        lues = len(valeurs)
        if parametres.get("mode_test") is True:
            journaliser_fichier_traite(identifiant, nom_fichier, chemin_source, contenu, date_effective, "validation_a_blanc", None)
            return {"statut": STATUT_SUCCES, "lignes_lues": lues, "lignes_ecrites": 0, "lignes_rejetees": nb_rejets,
                    "details": {"fichier": nom_fichier, "mode_test": True}}

        chargees = ajouter_colonnes_techniques(typees, execution_id, date_effective, nom_fichier, None)
        strategie = flux.get("strategie_ecriture")
        if strategie == "append":
            # Un rechargement forcé remplace les lignes du même fichier pour la même date, sans doublon.
            remplacer_perimetre(chargees, destination, {"fichier_source": nom_fichier, "date_traitement": date_effective},
                                partition=["date_traitement"])
        elif strategie == "overwrite":
            ecrire_table(chargees, destination, "overwrite", partition=["date_traitement"])
        elif strategie == "merge":
            if not cles:
                raise ErreurConfiguration("La stratégie merge exige cfg_flux.cles_fusion.")
            fusionner_table(chargees, destination, cles)
        else:
            raise ErreurConfiguration(f"Stratégie d'écriture inconnue pour {identifiant} : {strategie!r}.")
    except Exception as exc:
        motif = nettoyer_message_erreur(exc)
        chemin = deplacer_fichier_ponctuel(chemin_source, "rejet", identifiant, date_effective, nom_fichier, motif)
        journaliser_fichier_traite(identifiant, nom_fichier, chemin, contenu, date_effective, "rejete", motif)
        raise

    chemin = deplacer_fichier_ponctuel(chemin_source, "archive", identifiant, date_effective, nom_fichier, None)
    journaliser_fichier_traite(identifiant, nom_fichier, chemin, contenu, date_effective, "succes", None)
    return {
        "statut": STATUT_SUCCES_REJETS if nb_rejets else STATUT_SUCCES,
        "lignes_lues": lues, "lignes_ecrites": lues, "lignes_rejetees": nb_rejets,
        "details": {"fichier": nom_fichier, "empreinte_sha256": empreinte, "table": destination},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= INGESTION DU FLUX =============
resultat = {}
try:
    traitement = flux["parametres"].get("traitement")
    journal.info("=" * 80)
    journal.info(f"🚀 INGESTION DU FLUX {flux_id} ({traitement})")
    journal.info("=" * 80)
    if blocage:
        journal.warning(f"⚠️ {blocage}")
        bilan = {"statut": STATUT_BLOQUE, "lignes_lues": 0, "lignes_ecrites": 0, "lignes_rejetees": 0,
                 "message": blocage, "details": {"motif": blocage}}
    elif traitement == "export_json_ts":
        bilan = ingerer_export_json_ts(flux, date_traitement_effective)
    elif traitement == "export_csv_sftp":
        bilan = ingerer_export_csv_sftp(flux, date_traitement_effective)
    elif traitement == "referentiels":
        bilan = ingerer_referentiels(flux, date_traitement_effective)
    elif traitement == "depot_csv_ponctuel":
        bilan = ingerer_depot_csv_ponctuel(flux, date_traitement_effective, fichier, rejouer)
    else:
        raise ErreurConfiguration(f"Traitement d'ingestion inconnu pour {flux_id} : {traitement!r}.")

    statut_final = bilan.get("statut", STATUT_SUCCES)
    terminer_etape(
        etat_etape, statut_final,
        lignes_lues=bilan.get("lignes_lues"), lignes_ecrites=bilan.get("lignes_ecrites"),
        lignes_rejetees=bilan.get("lignes_rejetees"), watermark=bilan.get("watermark"), details=bilan.get("details"),
        message_erreur=bilan.get("message"),
    )
    resultat = {
        "execution_id": execution_id, "flux_id": flux_id, "statut": statut_final,
        "lignes_lues": bilan.get("lignes_lues"), "lignes_ecrites": bilan.get("lignes_ecrites"),
        "lignes_rejetees": bilan.get("lignes_rejetees"),
    }
    journal.info(f"✓ Ingestion terminée : {json.dumps(resultat, ensure_ascii=False)}")

except Exception as exc:
    message = nettoyer_message_erreur(exc)
    journal.error(f"❌ Échec de l'ingestion du flux {flux_id} : {message}", exc_info=True)
    if etat_etape is not None:
        terminer_etape(etat_etape, STATUT_ECHEC, message_erreur=message)
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
