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
# # MOTUL_nb_hris_transform, nom repris du champ displayName du fichier .platform.
# # **Type d'objet**
# # Notebook Microsoft Fabric exécuté avec le moteur Synapse PySpark, appelé par MOTUL_PL_HRIS_Orchestrateur pour les flux de transformation trf_sirh et trf_remuneration.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/MOTUL_nb_hris_transform.Notebook/notebook-content.py.
# # **Description fonctionnelle**
# # Ce notebook met les données RH au format attendu par les systèmes cibles. Il reporte en PySpark, sans altération fonctionnelle volontaire, les cinq procédures stockées ps_hr_ts_LoadDataToExternal*Table pour le flux des salariés vers la paie ADP, ainsi que les vues vw_BaseSalary_Changes et vw_Bonus_Changes pour les évolutions de rémunération renvoyées à TalentSoft. Il remplace la recherche du fichier de la veille : le jeu courant chargé en staging est comparé à la table d'état non-STG, le delta est calculé, transformé et contrôlé, puis la table d'état est mise à jour en dernier.
# # **Dépendances**
# # Le notebook importe MOTUL_nb_hris_lib. Il lit cfg_flux pour connaître les tables du flux, cfg_mapping pour les correspondances TalentSoft vers ADP actives à la date de traitement et cfg_qualite pour les règles de rejet du flux trf_sirh. Il lit les tables de staging alimentées par MOTUL_nb_hris_ingest et les tables d'état slv_*_etat qu'il maintient lui-même. Il ne dépend d'aucune variable d'environnement autre que celles qui désignent le Lakehouse.
# # **Fonctionnement et logique de traitement**
# # Pour le flux des salariés, le notebook calcule d'abord le delta par différence ensembliste entre la staging et l'état, comme le faisait l'instruction EXCEPT entre l'export du jour et celui de la veille ; en mode complet, toute la staging est retenue. Il joint ensuite les quinze correspondances de la procédure de staging, en ignorant les espaces de fin comme le faisait SQL Server, et produit la table préparée slv_ts_employe_prepare. Il applique les règles déclarées dans cfg_qualite, qui reprennent les trois familles de rejets d'origine : information obligatoire vide, format de matricule, de date ou de pourcentage, et correspondance absente. Un salarié qui porte au moins un rejet est exclu du lot au format paie slv_ts_employe_adp_lot, dans laquelle les dates sont converties comme par CONVERT et le salaire mensuel vaut le salaire de base arrondi au centime divisé par douze. Pour la rémunération, le notebook reproduit la comparaison entre le jour et la veille : évolution de montant ou de date du salaire de base avec le pourcentage d'augmentation, nouvelles entrées de bonus, clôture de l'ancienne entrée la veille de la nouvelle date de début, et clôture d'une entrée disparue. Les montants sont arrondis au centime et écrits avec une virgule décimale.
# # **Paramètres**
# # Le paramètre execution_id porte l'identifiant de l'exécution. Le paramètre flux_id vaut trf_sirh ou trf_remuneration. Le paramètre mode vaut incremental par défaut ; la valeur complet ignore la table d'état et retraite toute la staging. Le paramètre date_traitement, au format aaaa-mm-jj, sert de date de rejet, de transformation et de validité des correspondances. Le paramètre variables_env porte le JSON des variables d'environnement.
# # **Sorties produites**
# # Le flux des salariés produit slv_ts_employe_prepare, rjt_ts_employe, partitionnée par date de traitement, et le lot au format paie slv_ts_employe_adp_lot. Le flux de rémunération produit les lots slv_base_salary_changes_lot et slv_bonus_changes_lot. Chaque table de lot porte l'identifiant d'exécution : rejouer une exécution remplace ses propres lignes, sans doublon. Le volume de la source est journalisé dans ctl_execution_etape pour distinguer un lot vide d'une ingestion vide. Les tables d'état slv_ts_employe_etat et slv_sftp_*_etat ne sont réécrites qu'après le succès de toutes les sorties. La consolidation cumulative, la table finale et la publication sont réalisées par MOTUL_nb_hris_publish après les contrôles.
# # **Limitations connues et points d'attention**
# # La reproduction de la fonction ISDATE couvre les formats ISO ; un format dépendant de la langue SQL Server serait rejeté ici alors qu'il était accepté par l'existant. Plusieurs comportements surprenants de l'existant sont reproduits volontairement et doivent être validés par le métier : une valeur nulle n'est pas un rejet d'information obligatoire, le rejet de pourcentage d'équivalent temps plein et celui de catégorie d'emploi ne sont émis que si une autre règle de la même famille est en défaut, la correspondance du pays fiscal n'est jamais contrôlée, une date vide devient le 1er janvier 1900, le motif d'un rejet de matricule ou de prime annuelle est vide, et la clôture d'un bonus disparu porte une date de fin vide alors que le commentaire d'origine annonçait la veille du jour. Comme dans l'existant, un salaire ou un montant non numérique fait échouer le traitement. Les codes ADP de type caractère ne sont pas complétés par des espaces de fin. L'ordre des types de rejet dans le résumé était indéterminé dans l'existant ; il est trié ici.
# # **Responsable et contact**
# # Équipe data du projet HRIS Motul ; validation fonctionnelle par le métier RH pour tout écart de réconciliation.

# CELL ********************

# ============= IMPORTS =============
# Bibliothèques standard
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

# Bibliothèques tierces
from pyspark.sql import Column, DataFrame
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
flux_id = ""                 # trf_sirh ou trf_remuneration
mode = "incremental"         # incremental : delta contre l'état ; complet : toute la staging
date_traitement = ""         # Date au format aaaa-mm-jj ; vide pour la date du jour à Paris
variables_env = ""           # JSON des variables d'environnement résolues au lancement du pipeline

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

    if not execution_id or not flux_id:
        raise ErreurConfiguration("Les paramètres execution_id et flux_id sont obligatoires.")
    if mode not in ("complet", "incremental"):
        raise ErreurConfiguration(f"Mode inconnu pour une transformation : {mode!r} ; valeurs admises : complet, incremental.")
    date_traitement_effective = resoudre_date_traitement(date_traitement)

    initialiser_socle()
    flux = lire_flux(flux_id)
    if flux.get("notebook") != contexte["notebook_nom"]:
        raise ErreurConfiguration(f"Le flux {flux_id} est routé vers {flux.get('notebook')} et non vers {contexte['notebook_nom']}.")
    etat_etape = demarrer_etape(execution_id, flux_id, "transform", date_traitement_effective, {"mode": mode})
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

# ============= JOINTURES DE LA PROCEDURE DE STAGING =============
# Transcription de dwh.ps_hr_ts_LoadDataToExternalStagingTable : colonne de l'export, domaine de cfg_mapping,
# colonne de contrôle _lu et attributs ADP renommés. L'ordre des colonnes produites est celui de la procédure.
JOINTURES_SIRH: List[Dict[str, Any]] = [
    {"source": "cd_emp_titre_ts", "domaine": "lu_hr_emp_titre", "lu": "cd_emp_titre_ts_lu",
     "attributs": [("cd_emp_titre_adp", "cd_emp_titre_adp", "string")]},
    {"source": "cd_emp_sexe_ts", "domaine": "lu_hr_emp_sexe", "lu": "cd_emp_sexe_ts_lu",
     "attributs": [("cd_emp_sexe_adp", "cd_emp_sexe_adp", "string")]},
    {"source": "cd_emp_nationalite_ts", "domaine": "lu_hr_emp_nationalite", "lu": "cd_emp_nationalite_ts_lu",
     "attributs": [("cd_emp_nationalite_adp", "cd_emp_nationalite_adp", "string")]},
    {"source": "cd_emp_situation_matrimoniale_ts", "domaine": "lu_hr_emp_situation_matrimoniale",
     "lu": "cd_emp_situation_matrimoniale_ts_lu",
     "attributs": [("cd_emp_situation_matrimoniale_adp", "cd_emp_situation_matrimoniale_adp", "string")]},
    {"source": "cd_naissance_pays_ts", "domaine": "lu_hr_emp_pays", "lu": "cd_naissance_pays_ts_lu",
     "attributs": [("id_emp_pays_adp", "id_naissance_pays_adp", "string")]},
    {"source": "cd_adresse_pays_ts", "domaine": "lu_hr_emp_pays", "lu": "cd_adresse_pays_ts_lu",
     "attributs": [("id_emp_pays_adp", "id_adresse_pays_adp", "string")]},
    {"source": "lb_fiscal_country", "domaine": "lu_hr_emp_pays", "lu": "cd_fiscal_pays_ts_lu",
     "attributs": [("id_emp_pays_adp", "id_fiscal_pays_adp", "string")]},
    {"source": "cd_ppc_raison_debut_contrat_ts", "domaine": "lu_hr_ppc_raison_debut_contrat",
     "lu": "cd_ppc_raison_debut_contrat_ts_lu",
     "attributs": [("cd_ppc_raison_debut_contrat_adp", "cd_ppc_raison_debut_contrat_adp", "int")]},
    {"source": "cd_ppc_contrat_ts", "domaine": "lu_hr_ppc_contrat2", "lu": "cd_ppc_contrat_ts_lu",
     "attributs": [("cd_nature_contract_adp", "cd_nature_contract_adp", "string"),
                   ("cd_type_contract_adp", "cd_type_contract_adp", "string"),
                   ("cd_type_collaboration_adp", "cd_type_collaboration_adp", "string")]},
    {"source": "cd_ppc_convention_collective_lvl2_ts", "domaine": "lu_hr_ppc_classification",
     "lu": "cd_ppc_convention_collective_lvl2_ts_lu",
     "attributs": [("cd_classification_adp", "cd_classification_adp", "string"),
                   ("cd_coefficient_adp", "cd_coefficient_adp", "string"),
                   ("cd_ppc_convention_collective_adp", "cd_ppc_convention_collective_adp", "string")]},
    {"source": "cd_occupational_category", "domaine": "lu_hr_ppc_classes", "lu": "cd_occupational_category_ts_lu",
     "attributs": [("cd_categorie_cotisant_adp", "cd_categorie_cotisant_adp", "string"),
                   ("cd_classe_remuneration_adp", "cd_classe_remuneration_adp", "string")]},
    {"source": "cd_geo_entite_ts", "domaine": "lu_hr_geo_entite", "lu": "cd_geo_entite_ts_lu",
     "attributs": [("cd_geo_entite_adp", "cd_geo_entite_adp", "string")]},
    {"source": "cd_ppc_location_country_ts", "domaine": "lu_hr_ppc_location_country", "lu": "cd_ppc_location_country_ts_lu",
     "attributs": [("cd_ppc_location_country_adp", "cd_ppc_location_country_adp", "string")]},
    {"source": "cd_ppc_cost_center_ts", "domaine": "lu_hr_ppc_cost_center", "lu": "cd_ppc_cost_center_ts_lu",
     "attributs": [("cd_ppc_cost_center_adp", "cd_ppc_cost_center_adp", "string")]},
    {"source": "cd_organisationalstructure_ts", "domaine": "lu_hr_organisationalStructure",
     "lu": "cd_organisationalstructure_ts_lu",
     "attributs": [("cd_organisationalstructure_adp", "cd_organisationalstructure_adp", "string"),
                   ("ds_organisationalstructure_adp", "ds_organisationalstructure_adp", "string")]},
]

# Colonnes de dwh.ps_hr_ts_LoadDataToExternalTransformeTable, dans l'ordre : nom, conversion appliquée.
# conversion : None (repris tel quel), "date" (CONVERT(DATE, x)), "date10" (CONVERT(DATE, SUBSTRING(x,1,10))).
COLONNES_TRANSFORME_SIRH: List[tuple] = [
    ("id_unique", None), ("id_payroll", None), ("cd_matricule_it", None), ("lb_nom", None), ("lb_prenom", None),
    ("lb_nom_naissance", None), ("cd_emp_titre_adp", None), ("cd_emp_sexe_adp", None), ("cd_emp_nationalite_adp", None),
    ("id_numero_securite_sociale", None), ("dt_debut_situation_matrimoniale", None),
    ("cd_emp_situation_matrimoniale_adp", None), ("dt_naissance", None), ("lb_naissance_ville", None),
    ("lb_naissance_cp", None), ("lb_naissance_departement", None), ("id_naissance_pays_adp", None),
    ("lb_adresse_cp", None), ("lb_adresse_ville", None), ("id_adresse_pays_adp", None),
    ("lb_email_professionnel", None), ("lb_email_personnel", None), ("lb_telephone_professionnel", None),
    ("lb_telephone_personnel", None), ("lb_fiscal_ville", None), ("id_fiscal_pays_adp", None), ("lb_fiscal_cp", None),
    ("lb_fiscal_street", None), ("lb_fiscal_streetnumber", None), ("lb_fiscal_additional_address_information", None),
    ("lb_fiscal_streetnumber_complement", None), ("dt_entree_societe", "date10"), ("dt_entree_groupe", "date"),
    ("dt_anciennete", "date"), ("dt_sortie", "date"), ("dt_company_start", "date"),
    ("cd_ppc_raison_debut_contrat_adp", None), ("cd_ppc_raison_fin_contrat_ts", None),
    ("dt_ppc_debut_contrat", "date"), ("dt_ppc_fin_contrat", "date"), ("cd_nature_contract_adp", None),
    ("cd_type_contract_adp", None), ("cd_type_collaboration_adp", None), ("cd_recours_cdd", None),
    ("cd_categorie_cotisant_adp", None), ("cd_classe_remuneration_adp", None), ("dt_ppc_debut_periode_essaie", "date"),
    ("dt_ppc_fin_periode_essaie", "date"), ("cd_classification_adp", None), ("cd_coefficient_adp", None),
    ("cd_organisationalstructure_adp", None), ("dt_situation_start", "date"), ("cd_ppc_convention_collective_adp", None),
    ("cd_geo_entite_adp", None), ("cd_ppc_location_country_adp", None), ("pc_ppc_fte", None), ("id_numero_ordre", None),
    ("cd_ppc_cost_center_adp", None), ("pc_imputation", None), ("dt_debut_mt_salaire_base", "date"),
    ("mt_salaire_mensuel", "salaire_mensuel"), ("dt_debut_pc_prime_annuelle", "date"), ("pc_prime_annuelle", None),
    ("dt_debut_prime_annuelle", "date"), ("prime_annuelle", None), ("lb_job_title", None), ("lb_account_holder", None),
    ("lb_iban", None), ("lb_bic", None), ("lb_account_number", None), ("lb_bank_name", None),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def colonnes_metier(donnees: DataFrame) -> List[str]:
    """Renvoie les colonnes métier d'une table de staging ou d'état, sans les colonnes techniques d'ingestion."""
    return [colonne for colonne in donnees.columns if colonne not in COLONNES_TECHNIQUES_INGESTION]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_delta(staging: DataFrame, nom_table_etat: str, colonnes: Sequence[str], mode_traitement: str) -> DataFrame:
    """
    Compare la staging à la table d'état non-STG et renvoie les lignes nouvelles ou modifiées, sans doublon.

    Reproduit SELECT * FROM jour EXCEPT SELECT * FROM veille ; en mode complet ou sans état, toute la staging est retenue.
    """
    courant = staging.select(*colonnes)
    if mode_traitement == "complet" or not table_existe(nom_table_etat):
        return courant.distinct()
    etat = lire_table(nom_table_etat)
    manquantes = [colonne for colonne in colonnes if colonne not in etat.columns]
    if manquantes:
        raise ErreurConfiguration(
            f"La table d'état {nom_table_etat} n'a pas les colonnes {', '.join(manquantes)} : relancer en mode complet."
        )
    return courant.subtract(etat.select(*colonnes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def joindre_correspondances(donnees: DataFrame, correspondances: DataFrame, jointures: List[Dict[str, Any]]) -> DataFrame:
    """
    Joint à gauche chaque correspondance de cfg_mapping, en ignorant les espaces de fin du code source.

    Pour chaque jointure, la colonne _lu reçoit le code TalentSoft trouvé et chaque attribut ADP est renommé et typé.
    """
    resultat = donnees
    for numero, jointure in enumerate(jointures):
        attributs = [attribut for attribut, _, _ in jointure["attributs"]]
        reference = (
            correspondances.where((F.col("domaine") == jointure["domaine"]) & F.col("attribut_cible").isin(*attributs))
            .groupBy("code_source")
            .pivot("attribut_cible", attributs)
            .agg(F.first("code_cible"))
        )
        cle = f"_code_{numero}"
        reference = reference.select(
            F.col("code_source").alias(cle),
            *[F.col(f"`{attribut}`").alias(f"_attr_{numero}_{indice}") for indice, attribut in enumerate(attributs)],
        )
        resultat = resultat.join(F.broadcast(reference), F.rtrim(resultat[jointure["source"]]) == reference[cle], "left")
        resultat = resultat.withColumn(jointure["lu"], F.col(cle)).drop(cle)
        for indice, (_, nom_sortie, type_sortie) in enumerate(jointure["attributs"]):
            resultat = resultat.withColumn(nom_sortie, F.col(f"_attr_{numero}_{indice}").cast(type_sortie)).drop(f"_attr_{numero}_{indice}")
    return resultat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def verifier_conversions_sirh(retenus: DataFrame) -> None:
    """
    Vérifie que les dates à convertir et le salaire de base sont convertibles, comme l'exigeait le T-SQL.

    Raises:
        ErreurDonnees: avec le nombre de salariés concernés, sans aucune valeur individuelle.
    """
    comptages = []
    for colonne, conversion in COLONNES_TRANSFORME_SIRH:
        if conversion in ("date", "date10"):
            base = F.substring(F.col(colonne), 1, 10) if conversion == "date10" else F.col(colonne)
            invalide = base.isNotNull() & (F.trim(base) != F.lit("")) & (udf_tsql_isdate(base) == F.lit(0))
            comptages.append(F.sum(F.when(invalide, 1).otherwise(0)).alias(colonne))
    salaire_invalide = F.col("mt_salaire_base").isNotNull() & F.col("mt_salaire_base").cast("decimal(12,2)").isNull()
    comptages.append(F.sum(F.when(salaire_invalide, 1).otherwise(0)).alias("mt_salaire_base"))
    resultats = retenus.agg(*comptages).collect()[0].asDict()
    anomalies = [
        f"{colonne} non numérique ou hors NUMERIC(12,2) ({nombre})" if colonne == "mt_salaire_base" else f"{colonne} ({nombre})"
        for colonne, nombre in resultats.items() if nombre
    ]
    if anomalies:
        raise ErreurDonnees(
            "Conversion impossible pour des salariés non rejetés, cas où la procédure d'origine échouait : " + ", ".join(anomalies)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def construire_transforme_sirh(retenus: DataFrame, date_effective: date) -> DataFrame:
    """Sélectionne et convertit les colonnes au format paie, comme dwh.ps_hr_ts_LoadDataToExternalTransformeTable."""
    colonnes = [F.lit(date_effective).cast("date").alias("dt_transforme")]
    for colonne, conversion in COLONNES_TRANSFORME_SIRH:
        if conversion == "date":
            colonnes.append(udf_tsql_convertir_date(F.col(colonne)).alias(colonne))
        elif conversion == "date10":
            colonnes.append(udf_tsql_convertir_date(F.substring(F.col(colonne), 1, 10)).alias(colonne))
        elif conversion == "salaire_mensuel":
            colonnes.append((F.col("mt_salaire_base").cast("decimal(12,2)") / F.lit(12)).alias(colonne))
        else:
            colonnes.append(F.col(colonne))
    return retenus.select(*colonnes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transformer_sirh(flux: Dict[str, Any], date_effective: date, mode_traitement: str) -> Dict[str, Any]:
    """
    Enchaîne delta, staging avec correspondances, rejets, transformation au format paie et mise à jour de l'état.

    Les sorties sont remplacées pour l'exécution courante ; l'état n'est réécrit qu'en dernier.
    """
    parametres = flux["parametres"]
    tables = {cle: valider_nom_table(exiger_parametre(parametres, cle, flux["flux_id"]))
              for cle in ("table_staging", "table_etat", "table_prepare", "table_rejet", "table_sortie")}
    regles = lire_regles_qualite(flux["flux_id"])
    if not regles:
        raise ErreurConfiguration(f"Aucune règle de rejet dans cfg_qualite pour {flux['flux_id']} : traitement refusé.")

    staging = lire_table(tables["table_staging"])
    colonnes = colonnes_metier(staging)
    delta = calculer_delta(staging, tables["table_etat"], colonnes, mode_traitement)

    techniques = {"execution_id": execution_id, "date_traitement": date_effective}
    def tracer(donnees: DataFrame) -> DataFrame:
        return donnees.withColumn("execution_id", F.lit(execution_id)).withColumn("date_traitement", F.lit(date_effective).cast("date"))

    def relire(nom: str, colonnes_lues: List[str]) -> DataFrame:
        # La relecture de la table Delta écrite coupe la lignée Spark et garde un plan d'exécution compact.
        return lire_table(nom).where(F.col("execution_id") == execution_id).select(*colonnes_lues)

    prepare = joindre_correspondances(delta, lire_mapping_actif(date_effective), JOINTURES_SIRH)
    prepare = prepare.select(F.lit(date_effective).cast("date").alias("dt_staging"), *prepare.columns)
    colonnes_prepare = prepare.columns
    remplacer_perimetre(tracer(prepare), tables["table_prepare"], {"execution_id": execution_id})
    prepare = relire(tables["table_prepare"], colonnes_prepare)
    lignes_delta = prepare.count()

    rejets = appliquer_regles_rejet(prepare, regles, ["id_unique", "id_payroll"], date_effective)
    colonnes_rejet = rejets.columns
    remplacer_perimetre(tracer(rejets), tables["table_rejet"], techniques, partition=["date_traitement"])
    rejets = relire(tables["table_rejet"], colonnes_rejet)

    retenus = prepare.join(rejets.select("id_unique").where(F.col("id_unique").isNotNull()).distinct(), "id_unique", "left_anti")
    retenus = retenus.select(*colonnes_prepare)
    verifier_conversions_sirh(retenus)
    remplacer_perimetre(tracer(construire_transforme_sirh(retenus, date_effective)), tables["table_sortie"],
                        {"execution_id": execution_id})
    ecrire_table(staging.select(*colonnes), tables["table_etat"], "overwrite")

    salaries_rejetes = rejets.where(F.col("id_unique").isNotNull()).select("id_unique").distinct().count()
    lignes_ecrites = lire_table(tables["table_sortie"]).where(F.col("execution_id") == execution_id).count()
    return {
        "statut": STATUT_SUCCES_REJETS if salaries_rejetes else STATUT_SUCCES,
        "lignes_lues": lignes_delta, "lignes_ecrites": lignes_ecrites, "lignes_rejetees": salaries_rejetes,
        "details": {"lignes_source": staging.count(), "lignes_rejet": rejets.count(), "mode": mode_traitement},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tsql_veille_style_103(valeur: Optional[str]) -> Optional[str]:
    """
    Reproduit CONVERT(varchar(19), DATEADD(day, -1, CONVERT(date, valeur, 103)), 103) + ' 00:00:00'.

    Une valeur nulle donne NULL ; une valeur hors du format jj/mm/aaaa fait échouer le traitement, comme en T-SQL.
    """
    if valeur is None:
        return None
    correspondance = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,7})?)?)?\s*$", valeur)
    if not correspondance:
        raise ErreurDonnees("Date de début de bonus hors du format jj/mm/aaaa : conversion impossible.")
    try:
        jour = date(int(correspondance.group(3)), int(correspondance.group(2)), int(correspondance.group(1)))
    except ValueError:
        raise ErreurDonnees("Date de début de bonus invalide : conversion impossible.") from None
    return (jour - timedelta(days=1)).strftime("%d/%m/%Y") + " 00:00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

udf_tsql_veille_style_103 = F.udf(tsql_veille_style_103, T.StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def montant_virgule(colonne: Column) -> Column:
    """Reproduit REPLACE(CAST(CAST(x AS DECIMAL(20,2)) AS VARCHAR(50)), '.', ',')."""
    return F.regexp_replace(colonne.cast("decimal(20,2)").cast("string"), r"\.", ",")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def verifier_montants(donnees: DataFrame, colonne: str, description: str) -> None:
    """Refuse un montant non convertible en DECIMAL(20,2), cas où la vue d'origine échouait."""
    nombre = donnees.where(F.col(colonne).isNotNull() & F.col(colonne).cast("decimal(20,2)").isNull()).count()
    if nombre:
        raise ErreurDonnees(f"{nombre} valeur(s) non numérique(s) dans {description}.{colonne} : la vue d'origine échouait sur ce cas.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_changements_base_salary(jour: DataFrame, veille: DataFrame) -> DataFrame:
    """
    Reproduit SFTP.vw_BaseSalary_Changes : évolution du montant ou de la date, avec le pourcentage d'augmentation.
    """
    j, j1 = jour.alias("j"), veille.alias("j1")
    jointe = j.join(j1, F.rtrim(F.col("j.employeenumber")) == F.rtrim(F.col("j1.employeenumber")), "left")
    montant_j = F.col("j.amount").cast("decimal(20,2)")
    montant_j1 = F.col("j1.amount").cast("decimal(20,2)")
    zero = F.lit(0).cast("decimal(20,2)")
    condition = (
        ((F.coalesce(montant_j, zero) != F.coalesce(montant_j1, zero)) & F.col("j.amount").isNotNull())
        | (F.rtrim(F.col("j.date")) != F.rtrim(F.col("j1.date")))
    )
    diviseur = F.when(montant_j1 == zero, F.lit(None)).otherwise(montant_j1)
    evolution = F.when(montant_j == montant_j1, F.lit(0)).otherwise(F.round(((montant_j - montant_j1) / diviseur) * F.lit(100), 2))
    return jointe.where(condition).select(
        F.col("j.employeenumber").alias("employeenumber"), F.col("j.date").alias("date"), F.col("j.type").alias("type"),
        montant_virgule(F.col("j.amount")).alias("amount"), F.col("j.currency").alias("currency"),
        F.col("j.periodicity").alias("periodicity"), montant_virgule(evolution).alias("extra_augm_n_1"),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculer_blocs_bonus(jour: DataFrame, veille: DataFrame, salaire_jour: DataFrame) -> List[DataFrame]:
    """
    Produit les trois blocs de SFTP.vw_Bonus_Changes pour un domaine de bonus : nouvelles entrées, clôture sur
    changement de date et clôture d'une entrée disparue. La devise provient du salaire de base du jour.
    """
    c, p = jour.alias("c"), veille.alias("p")
    s = salaire_jour.select("employeenumber", "currency").alias("s")
    jointe = (
        c.join(p, F.rtrim(F.col("c.employeenumber")) == F.rtrim(F.col("p.employeenumber")), "left")
        .join(s, F.rtrim(F.col("c.employeenumber")) == F.rtrim(F.col("s.employeenumber")), "left")
    )
    valeur_changee = F.rtrim(F.coalesce(F.col("c.value"), F.lit("0"))) != F.rtrim(F.coalesce(F.col("p.value"), F.lit("0")))
    date_changee = F.rtrim(F.col("c.start_date")) != F.rtrim(F.col("p.start_date"))
    valeur_presente = F.col("c.value").isNotNull()

    def projeter(alias: str, fin: Column) -> List[Column]:
        return [
            F.col(f"{alias}.employeenumber").alias("employeenumber"), F.col(f"{alias}.type").alias("type"),
            F.col(f"{alias}.start_date").alias("start_date"), fin.alias("end_date"),
            F.col(f"{alias}.calculation_method").alias("calculation_method"),
            montant_virgule(F.col(f"{alias}.value")).alias("value"), F.col("s.currency").alias("currency"),
            F.col(f"{alias}.base_salary_effective_date").alias("base_salary_effective_date"),
            F.col(f"{alias}.periodicity").alias("periodicity"),
        ]

    nouvelles = jointe.where((valeur_changee & valeur_presente) | (date_changee & valeur_presente)).select(
        *projeter("c", F.col("c.end_date"))
    )
    cloture_date = jointe.where(date_changee & valeur_presente).select(
        *projeter("p", udf_tsql_veille_style_103(F.col("c.start_date")))
    )
    cloture_disparue = jointe.where(
        F.col("p.start_date").isNotNull() & F.col("p.value").isNotNull() & F.col("c.start_date").isNull() & F.col("c.value").isNull()
    ).select(*projeter("p", udf_tsql_veille_style_103(F.col("c.start_date"))))
    return [nouvelles, cloture_date, cloture_disparue]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lire_veille(nom_table_etat: str, colonnes: Sequence[str], mode_traitement: str) -> DataFrame:
    """Lit l'état de la veille ; en mode complet ou sans état, renvoie un jeu vide de même structure."""
    if mode_traitement == "complet" or not table_existe(nom_table_etat):
        return spark.createDataFrame([], T.StructType([T.StructField(colonne, T.StringType(), True) for colonne in colonnes]))
    return lire_table(nom_table_etat).select(*colonnes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transformer_remuneration(flux: Dict[str, Any], date_effective: date, mode_traitement: str) -> Dict[str, Any]:
    """
    Calcule les évolutions du salaire de base et des bonus entre la staging et l'état, puis met à jour l'état.

    Remplace CreateBaseSalaryTables, CreateBonusPercentageTables, CreateBonusTargetValueTables, CreateBaseSalaryView
    et CreateBonusView, ainsi que la recherche du fichier de la veille par comparaison de noms.
    """
    parametres = flux["parametres"]
    stagings = exiger_parametre(parametres, "tables_staging", flux["flux_id"])
    etats = exiger_parametre(parametres, "tables_etat", flux["flux_id"])
    domaines = ("base_salary", "bonus_percentage", "bonus_target")
    jour, veille, colonnes = {}, {}, {}
    for domaine in domaines:
        staging = lire_table(valider_nom_table(exiger_parametre(stagings, domaine, "tables_staging")))
        colonnes[domaine] = colonnes_metier(staging)
        jour[domaine] = staging.select(*colonnes[domaine]).cache()
        veille[domaine] = lire_veille(valider_nom_table(exiger_parametre(etats, domaine, "tables_etat")), colonnes[domaine], mode_traitement)
    verifier_montants(jour["base_salary"], "amount", "salaire de base du jour")
    verifier_montants(veille["base_salary"], "amount", "salaire de base de la veille")
    for domaine in ("bonus_percentage", "bonus_target"):
        verifier_montants(jour[domaine], "value", f"{domaine} du jour")
        verifier_montants(veille[domaine], "value", f"{domaine} de la veille")

    salaire = calculer_changements_base_salary(jour["base_salary"], veille["base_salary"])
    blocs = (
        calculer_blocs_bonus(jour["bonus_percentage"], veille["bonus_percentage"], jour["base_salary"])
        + calculer_blocs_bonus(jour["bonus_target"], veille["bonus_target"], jour["base_salary"])
    )
    bonus = blocs[0]
    for bloc in blocs[1:]:
        bonus = bonus.unionByName(bloc)
    bonus = bonus.distinct()

    def tracer(donnees: DataFrame) -> DataFrame:
        return donnees.withColumn("execution_id", F.lit(execution_id)).withColumn("date_traitement", F.lit(date_effective).cast("date"))

    table_salaire = valider_nom_table(exiger_parametre(parametres, "table_sortie_base_salary", flux["flux_id"]))
    table_bonus = valider_nom_table(exiger_parametre(parametres, "table_sortie_bonus", flux["flux_id"]))
    remplacer_perimetre(tracer(salaire), table_salaire, {"execution_id": execution_id})
    remplacer_perimetre(tracer(bonus), table_bonus, {"execution_id": execution_id})
    for domaine in domaines:
        ecrire_table(jour[domaine], etats[domaine], "overwrite")

    lignes_salaire = lire_table(table_salaire).where(F.col("execution_id") == execution_id).count()
    lignes_bonus = lire_table(table_bonus).where(F.col("execution_id") == execution_id).count()
    lues = sum(jour[domaine].count() for domaine in domaines)
    return {
        "statut": STATUT_SUCCES, "lignes_lues": lues, "lignes_ecrites": lignes_salaire + lignes_bonus, "lignes_rejetees": 0,
        "details": {"lignes_source": lues, "base_salary_changes": lignes_salaire, "bonus_changes": lignes_bonus, "mode": mode_traitement},
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= TRANSFORMATION DU FLUX =============
resultat = {}
try:
    traitement = flux["parametres"].get("traitement")
    journal.info("=" * 80)
    journal.info(f"🚀 TRANSFORMATION DU FLUX {flux_id} ({traitement})")
    journal.info("=" * 80)
    if blocage:
        journal.warning(f"⚠️ {blocage}")
        bilan = {"statut": STATUT_BLOQUE, "lignes_lues": 0, "lignes_ecrites": 0, "lignes_rejetees": 0,
                 "message": blocage, "details": {"motif": blocage}}
    elif traitement == "sirh_employe":
        bilan = transformer_sirh(flux, date_traitement_effective, mode)
    elif traitement == "remuneration":
        bilan = transformer_remuneration(flux, date_traitement_effective, mode)
    else:
        raise ErreurConfiguration(f"Traitement de transformation inconnu pour {flux_id} : {traitement!r}.")

    terminer_etape(
        etat_etape, bilan["statut"], lignes_lues=bilan["lignes_lues"], lignes_ecrites=bilan["lignes_ecrites"],
        lignes_rejetees=bilan["lignes_rejetees"], watermark=date_traitement_effective.isoformat(), details=bilan["details"],
        message_erreur=bilan.get("message"),
    )
    resultat = {
        "execution_id": execution_id, "flux_id": flux_id, "statut": bilan["statut"],
        "lignes_lues": bilan["lignes_lues"], "lignes_ecrites": bilan["lignes_ecrites"], "lignes_rejetees": bilan["lignes_rejetees"],
    }
    journal.info(f"✓ Transformation terminée : {json.dumps(resultat, ensure_ascii=False)}")

except Exception as exc:
    message = nettoyer_message_erreur(exc)
    journal.error(f"❌ Échec de la transformation du flux {flux_id} : {message}", exc_info=True)
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
