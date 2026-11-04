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
# T6_NB_HRIS_REFERENCE_IMPORT
#
# **Type d'objet**
#
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
#
# **Chemin dans le dépôt**
#
# TEST_MOTUL/TST_6/ONESHOT/NB_HRIS_REFERENCE_IMPORT.Notebook/notebook-content.py
#
# **Description fonctionnelle**
#
# Ce notebook reprend une seule fois, dans les tables de correspondance du Lakehouse, le contenu des dix-sept fichiers de référentiels de l'existant (classes, classification, contrat, contrat_v2, convention_collective, cost_center, diplome, entite, location_country, nationalite, organisationalstructure, pays, raison_debut_contrat, sexe, situation_matrimoniale, titre, work_accident). Ces fichiers portent les codes ADP choisis par le métier, qui n'existent nulle part ailleurs. Le notebook lit les fichiers que l'utilisateur a déposés à la main dans la zone Files du Lakehouse, contrôle leur contenu, puis renseigne les codes ADP des tables cfg.lu_hr_ et ajoute les lignes qui n'existent pas encore. Il passe par les mêmes contrôles que la saisie manuelle de T6_NB_HRIS_REFERENCE_ADMIN. Il sert à la mise en service de chaque environnement ; les fichiers déposés sont supprimés ensuite.
#
# **Dépendances**
#
# Le notebook charge la bibliothèque commune par %run T6_NB_HRIS_LIB et utilise ses modules de correspondances, d'écriture dans les tables, de contrôle du run, de modèle de données et de journal. Il lit les fichiers du dossier indiqué par p_folder dans la zone Files du Lakehouse T6_LH_HRIS, lit et écrit les tables cfg.lu_hr_, lit le verrou ctl.process_lock et trace l'opération dans ctl.run_step. Il est lancé à la main par le développeur ; il n'est appelé par aucun pipeline.
#
# **Fonctionnement et logique de traitement**
#
# Après la configuration et la préparation du Lakehouse, le notebook contrôle les paramètres, puis lit tous les fichiers CSV du dossier. Chaque fichier est décodé en UTF-8, sinon en Windows-1252, et son séparateur (virgule, point-virgule ou tabulation) est reconnu sur la ligne d'en-têtes. Les colonnes sont lues par position, comme le font les tables externes de l'existant, car les en-têtes des fichiers diffèrent du modèle pour trois d'entre eux. Le nombre de colonnes doit être celui de la table. Un code TalentSoft en double avec des valeurs différentes est une erreur, sauf si p_conflict_policy demande de garder la première ou la dernière occurrence. Pour un code déjà présent dans la table, le notebook renseigne les colonnes métier du fichier, sans jamais remplacer une valeur déjà saisie, sauf si p_overwrite vaut true ; la partie TalentSoft de la table n'est pas modifiée. Un code absent de la table est ajouté avec les valeurs du fichier, s'il porte au moins une valeur métier ; l'acquisition mettra ensuite à jour ses libellés TalentSoft sans jamais toucher aux colonnes ADP. En mode CHECK, rien n'est écrit et chaque changement est affiché. En mode APPLY, le notebook refuse d'écrire si un run tient le verrou ; une seule erreur, sur n'importe quelle table, fait tout refuser.
#
# **Paramètres**
#
# Le paramètre p_mode vaut CHECK par défaut ou APPLY. Le paramètre p_folder désigne le dossier des fichiers dans la zone Files du Lakehouse et vaut Files/reference_import par défaut. Le paramètre p_table limite l'import à une table de correspondance, par exemple lu_hr_emp_titre, et reste vide pour traiter tous les fichiers présents. Le paramètre p_overwrite vaut false par défaut : une valeur déjà saisie est conservée. Le paramètre p_conflict_policy vaut ERROR par défaut, FIRST ou LAST pour trancher les codes en double. Le paramètre p_change_ref porte la référence du changement et il est obligatoire en mode APPLY. Le paramètre p_max_lines limite le nombre de changements affichés par table, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
#
# **Sorties produites**
#
# En mode APPLY, le notebook remplace les tables de correspondance modifiées, chaque écriture créant une version Delta qui permet un retour arrière, et ajoute une trace dans ctl.run_step. Dans tous les modes, il écrit son journal dans log.notebook_log et renvoie un résultat court de compteurs par table, avec les fichiers absents ou inattendus.
#
# **Limitations connues et points d'attention**
#
# Les fichiers sont des données métier déposées dans la zone Files du Lakehouse : ils doivent y rester le temps de l'import seulement, puis être supprimés. La lecture des fichiers par Spark en mode binaire n'a pas encore été essayée dans Fabric ; en cas d'échec, le notebook retombe sur une lecture en texte UTF-8. Le nom du workspace est utilisé dans le chemin OneLake, comme dans les autres notebooks du projet. Une clé de situation matrimoniale porte deux codes ADP différents dans l'existant : le métier doit choisir le bon code, dans le fichier ou par p_conflict_policy. Un import rejoué ne remplace jamais une valeur saisie depuis ; p_overwrite à true est réservé à une reprise volontaire.
#
# **Responsable et contact**
#
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import csv
import io
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple

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
        required_keys=("lakehouse_name",),
    )
    workspace_name = context_values["workspace_name"]
    workspace_id = context_values["workspace_id"]
    notebook_id = context_values["notebook_id"]
    item_name = context_values["item_name"]
    operation_id = context_values["operation_id"]
    lakehouse_name = context_values["lakehouse_name"]
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

p_mode = "CHECK"             # CHECK : contrôle et affiche, n'écrit rien ; APPLY : écrit dans les tables
p_folder = "Files/reference_import"  # dossier des fichiers CSV dans la zone Files du Lakehouse
p_table = ""                 # une seule table (ex. lu_hr_emp_titre) ; vide = tous les fichiers présents
p_overwrite = "false"        # false : une valeur ADP déjà saisie est conservée ; true : le fichier la remplace
p_conflict_policy = "ERROR"  # ERROR : un code en double avec des valeurs différentes est une erreur ; FIRST | LAST : garde cette occurrence
p_change_ref = ""            # APPLY : référence du changement (ticket, validation métier), obligatoire
p_max_lines = "20"           # nombre maximal de changements affichés par table
p_test_schema = ""           # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""               # eventpath Fabric (ex. /p_mode=CHECK/p_table=lu_hr_emp_titre)

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
        p_mode = parsed_path.get("p_mode", p_mode)
        p_folder = parsed_path.get("p_folder", p_folder)
        p_table = parsed_path.get("p_table", p_table)
        p_overwrite = parsed_path.get("p_overwrite", p_overwrite)
        p_conflict_policy = parsed_path.get("p_conflict_policy", p_conflict_policy)
        p_change_ref = parsed_path.get("p_change_ref", p_change_ref)
        p_max_lines = parsed_path.get("p_max_lines", p_max_lines)
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
    attach_logger_to_lakehouse(logger, tables, context_values, run_id="")
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

# ============= CONSTANTES : FICHIERS DE L'EXISTANT ET TABLES DE CORRESPONDANCE =============
# Même association que les tables externes dwh.lu_hr_* de Synapse (Script_Init de Pipeline INIT, dossier talentsoft/reference).
FILE_TO_TABLE = {
    "classes.csv": "lu_hr_ppc_classes",
    "classification.csv": "lu_hr_ppc_classification",
    "contrat.csv": "lu_hr_ppc_contrat",
    "contrat_v2.csv": "lu_hr_ppc_contrat2",
    "convention_collective.csv": "lu_hr_ppc_convention_collective",
    "cost_center.csv": "lu_hr_ppc_cost_center",
    "diplome.csv": "lu_hr_emp_diplome",
    "entite.csv": "lu_hr_geo_entite",
    "location_country.csv": "lu_hr_ppc_location_country",
    "nationalite.csv": "lu_hr_emp_nationalite",
    "organisationalstructure.csv": "lu_hr_organisationalstructure",
    "pays.csv": "lu_hr_emp_pays",
    "raison_debut_contrat.csv": "lu_hr_ppc_raison_debut_contrat",
    "sexe.csv": "lu_hr_emp_sexe",
    "situation_matrimoniale.csv": "lu_hr_emp_situation_matrimoniale",
    "titre.csv": "lu_hr_emp_titre",
    "work_accident.csv": "lu_hr_emp_work_accident",
}
CONFLICT_POLICIES = ("ERROR", "FIRST", "LAST")
FOLDER_PREFIX = "Files/"
assert sorted(FILE_TO_TABLE.values()) == sorted(hris.model.LOOKUP_TABLES), "association fichiers / tables incomplète"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def is_filled(value: Any) -> bool:
    """Vrai si la valeur contient autre chose que des espaces."""
    return value is not None and str(value).strip() != ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def norm_code(value: Any) -> Optional[str]:
    """Clé comparée comme dans la jointure de la transformation : espaces de fin ignorés, vide = pas de clé."""
    if value is None:
        return None
    text = str(value).rstrip(" ")
    return text or None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def decode_bytes(raw: bytes) -> Tuple[str, str]:
    """Décode un fichier : UTF-8 (avec ou sans marque d'ordre des octets), sinon Windows-1252. Renvoie (texte, encodage)."""
    try:
        return raw.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace"), "cp1252"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def detect_delimiter(first_line: str) -> str:
    """Séparateur le plus fréquent de la ligne d'en-têtes : virgule, point-virgule ou tabulation (virgule par défaut)."""
    counts = {delimiter: first_line.count(delimiter) for delimiter in (",", ";", "\t")}
    best = max(counts, key=lambda delimiter: counts[delimiter])
    return best if counts[best] > 0 else ","

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_csv(text: str) -> Tuple[List[str], List[List[str]], str]:
    """Lit un fichier CSV (guillemets doubles, première ligne = en-têtes). Renvoie (en-têtes, lignes, séparateur).
    Les lignes entièrement vides sont ignorées ; les cellules vides en fin d'en-tête sont retirées."""
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = detect_delimiter(first_line)
    parsed = [row for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"')
              if any(cell.strip() for cell in row)]
    if not parsed:
        return [], [], delimiter
    header = [cell.strip() for cell in parsed[0]]
    while header and header[-1] == "":
        header.pop()
    return header, parsed[1:], delimiter

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_rows_from_csv(table: str, header: List[str], rows: List[List[str]]) -> Tuple[List[Dict[str, Optional[str]]], List[str], List[str]]:
    """Associe les colonnes du fichier à celles de la table PAR POSITION (comme les tables externes de l'existant).
    Renvoie (lignes {colonne: valeur ou None}, erreurs, remarques). Une cellule vide devient None."""
    columns = hris.model.LOOKUP_TABLES[table]
    errors: List[str] = []
    notes: List[str] = []
    if len(header) != len(columns):
        errors.append(f"{len(header)} colonne(s) dans le fichier, {len(columns)} attendue(s) ({', '.join(columns)})")
        return [], errors, notes
    if [name.lower() for name in header] != [name.lower() for name in columns]:
        notes.append("en-têtes différents du modèle : colonnes lues par position, comme l'existant")
    out: List[Dict[str, Optional[str]]] = []
    for number, cells in enumerate(rows, start=2):
        cells = list(cells)
        while len(cells) > len(columns) and cells[-1].strip() == "":
            cells.pop()
        if len(cells) > len(columns):
            errors.append(f"ligne {number} : {len(cells)} cellules pour {len(columns)} colonnes")
            continue
        cells += [""] * (len(columns) - len(cells))
        out.append({column: (cell.strip() or None) for column, cell in zip(columns, cells)})
    return out, errors, notes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dedupe_by_code(table: str, rows: List[Dict[str, Optional[str]]], policy: str) -> Tuple[List[Dict[str, Optional[str]]], List[str], int, int, List[str]]:
    """Une ligne par code TalentSoft. Doublon identique : ignoré. Doublon avec valeurs différentes : erreur (ERROR),
    ou occurrence gardée (FIRST / LAST). Renvoie (lignes, erreurs, doublons identiques, lignes sans code, codes tranchés)."""
    code_column = hris.reference_api.CATALOGUE[table].code
    kept: Dict[str, Dict[str, Optional[str]]] = {}
    order: List[str] = []
    errors: List[str] = []
    resolved: List[str] = []
    identical = 0
    without_code = 0
    for row in rows:
        key = norm_code(row.get(code_column))
        if key is None:
            without_code += 1
            continue
        if key not in kept:
            kept[key] = row
            order.append(key)
        elif kept[key] == row:
            identical += 1
        elif policy == "ERROR":
            differing = sorted(column for column in row if kept[key].get(column) != row[column])
            errors.append(f"code {key} en double avec des valeurs différentes ({', '.join(differing)}) : "
                          "corriger le fichier ou choisir p_conflict_policy")
        else:
            resolved.append(key)
            if policy == "LAST":
                kept[key] = row
    return [kept[key] for key in order], errors, identical, without_code, resolved

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def plan_table(table: str, state_rows: Any, csv_rows: List[Dict[str, Optional[str]]], overwrite: bool,
               technical: Dict[str, Any]) -> Tuple[List[dict], Dict[str, int], List[str], List[Tuple[str, str, Optional[str], Optional[str]]]]:
    """Calcule le contenu de la table après import, sans rien écrire. Code présent : colonnes métier du fichier, sans
    remplacer une valeur déjà saisie (sauf overwrite). Code absent : ligne ajoutée si elle porte une valeur métier.
    Renvoie (lignes de la table, compteurs, erreurs, changements (code, colonne, avant, après))."""
    spec = hris.reference_api.CATALOGUE[table]
    business = hris.reference_api.business_columns(table)
    existing = {norm_code(row.get(spec.code)): row for row in state_rows if norm_code(row.get(spec.code))}
    assignments: Dict[str, Dict[str, str]] = {}
    inserts: List[dict] = []
    kept_existing = 0
    ignored_empty = 0
    for row in csv_rows:
        key = norm_code(row.get(spec.code))
        if key in existing:
            values: Dict[str, str] = {}
            for column in business:
                new = row.get(column)
                if new is None:
                    continue
                old = existing[key].get(column)
                if is_filled(old) and str(old) != new and not overwrite:
                    kept_existing += 1
                    continue
                values[column] = new
            if values:
                assignments[key] = values
        elif any(is_filled(row.get(column)) for column in business):
            new_row = {column: row.get(column) for column in hris.model.LOOKUP_TABLES[table]}
            new_row.update(technical)
            inserts.append(new_row)
        else:
            ignored_empty += 1
    rows, outcome = hris.reference_api.apply_assignments(table, state_rows, assignments, technical)
    counters = {"file_codes": len(csv_rows), "inserted": len(inserts), "updated_cells": len(outcome.changes),
                "kept_existing": kept_existing, "ignored_without_value": ignored_empty}
    return list(rows) + inserts, counters, list(outcome.errors), list(outcome.changes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_csv_files(folder_url: str) -> Dict[str, bytes]:
    """Lit les fichiers *.csv du dossier de la zone Files (Spark, lecture binaire pour décider de l'encodage).
    Repli sur une lecture texte UTF-8 si la lecture binaire échoue. Renvoie {nom en minuscules: contenu}."""
    try:
        frame = spark.read.format("binaryFile").option("pathGlobFilter", "*.csv").load(folder_url)
        return {row.path.rsplit("/", 1)[-1].lower(): bytes(row.content) for row in frame.select("path", "content").collect()}
    except Exception as first_error:
        if "does not exist" in str(first_error).lower() or "path_not_found" in str(first_error).lower():
            raise hris.common.HrisError("IMPORT_FOLDER_ABSENT", f"dossier introuvable : {folder_url}") from None
        logger.warning(f"⚠️ Lecture binaire impossible ({type(first_error).__name__}) : repli sur une lecture texte UTF-8")
        from pyspark.sql import functions as F
        frame = spark.read.option("wholetext", True).text(folder_url.rstrip("/") + "/*.csv")
        rows = frame.select(F.input_file_name().alias("path"), F.col("value")).collect()
        return {row.path.rsplit("/", 1)[-1].lower(): row.value.encode("utf-8") for row in rows}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONTRÔLE DES PARAMÈTRES ET DU VERROU =============
try:
    mode = str(p_mode).strip().upper()
    if mode not in ("CHECK", "APPLY"):
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir CHECK ou APPLY")
    if p_table and p_table not in hris.reference_api.CATALOGUE:
        raise hris.common.HrisError("PARAM_INVALID", "p_table inconnue : " + p_table)
    folder = str(p_folder).strip().strip("/")
    if not (folder + "/").startswith(FOLDER_PREFIX) or ".." in folder.split("/"):
        raise hris.common.HrisError("PARAM_INVALID", "p_folder doit être un dossier de la zone Files (Files/...)")
    policy = str(p_conflict_policy).strip().upper()
    if policy not in CONFLICT_POLICIES:
        raise hris.common.HrisError("PARAM_INVALID", "p_conflict_policy doit valoir ERROR, FIRST ou LAST")
    overwrite = hris.common.as_bool(p_overwrite)
    if mode == "APPLY" and not p_change_ref.strip():
        raise hris.common.HrisError("PARAM_MISSING", "p_change_ref (obligatoire pour écrire)")
    if mode == "APPLY":
        lock = hris.run_control.lock_state(spark, tables)
        if lock["held"]:
            raise hris.common.HrisError("RUN_IN_PROGRESS", "un run tient le verrou jusqu'à " + str(lock["lease_expires_at_utc"])
                                        + " : relancer APPLY après sa fin")
    max_lines = int(str(p_max_lines).strip())
    import_id = "REFIMPORT-" + uuid.uuid4().hex[:12]
    folder_url = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/{folder}"
    logger.info(f"✓ Import des codes ADP {mode}, trace {import_id}, dossier {folder}")
except Exception as e:
    error_message = f"Erreur lors de le contrôle des paramètres: {e}"
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

# ============= IMPORT DES CODES ADP DEPUIS LES FICHIERS DE L'EXISTANT =============
try:
    store = hris.reference_api.SparkReferenceStore(spark, tables)
    technical = {"_loaded_at_utc": hris.common.utc_now(), "_loaded_by": item_name + ":" + import_id,
                 "_change_ref": p_change_ref.strip() or None}
    files = read_csv_files(folder_url)
    wanted = sorted((name for name, table in FILE_TO_TABLE.items() if not p_table or table == p_table),
                    key=lambda name: FILE_TO_TABLE[name])
    unexpected = sorted(set(files) - set(FILE_TO_TABLE))
    missing = [name for name in wanted if name not in files]
    if len(missing) == len(wanted):
        raise hris.common.HrisError("IMPORT_NO_FILE", f"aucun des fichiers attendus dans {folder}")
    if missing:
        logger.warning(f"⚠️ {len(missing)} fichier(s) attendu(s) absent(s) : {', '.join(missing)}")
    if unexpected:
        logger.warning(f"⚠️ {len(unexpected)} fichier(s) ignoré(s) car inattendu(s) : {', '.join(unexpected)}")
    errors: List[str] = []
    plans = []
    counters_by_table: Dict[str, Dict[str, Any]] = {}
    for name in (n for n in wanted if n in files):
        table = FILE_TO_TABLE[name]
        text, encoding = decode_bytes(files[name])
        header, raw_rows, delimiter = parse_csv(text)
        csv_rows, problems, notes = table_rows_from_csv(table, header, raw_rows)
        errors += [f"{table} : {message}" for message in problems]
        if problems:
            continue
        unique_rows, conflicts, identical, without_code, resolved = dedupe_by_code(table, csv_rows, policy)
        errors += [f"{table} : {message}" for message in conflicts]
        state = store.read(table)
        if not state.exists or not state.schema_ok:
            errors.append(f"{table} : table indisponible, {state.schema_detail[:200]}")
            continue
        rows, counters, plan_errors, changes = plan_table(table, state.rows, unique_rows, overwrite, technical)
        errors += [f"{table} : {message}" for message in plan_errors]
        counters.update(file=name, encoding=encoding, delimiter=repr(delimiter), duplicates_identical=identical,
                        duplicates_resolved=len(resolved), rows_without_code=without_code)
        for note in notes:
            logger.info(f"   {table} : {note}")
        if resolved:
            logger.info(f"   {table} : {len(resolved)} code(s) en double tranché(s) par {policy} : " + ", ".join(resolved[:max_lines]))
        for code, column, old, new in changes[:max_lines]:
            print(f"   {table:<34} {code:<30} {column:<36} {old!r} -> {new!r}")
        if len(changes) > max_lines:
            print(f"   {table} : {len(changes) - max_lines} autre(s) changement(s) non affiché(s) (p_max_lines)")
        logger.info(f"   {table} : {counters['file_codes']} code(s) dans le fichier ({encoding}, séparateur {delimiter!r}), "
                    f"{counters['inserted']} ajouté(s), {counters['updated_cells']} cellule(s) renseignée(s), "
                    f"{counters['kept_existing']} valeur(s) déjà saisie(s) conservée(s)")
        plans.append((table, rows, counters))
        counters_by_table[table] = counters
    for message in errors[:max_lines * 5]:
        logger.error(f"   ERREUR : {message}")
    if errors:
        raise hris.common.HrisError("IMPORT_INVALID", f"{len(errors)} erreur(s) : rien n'est écrit")
    written: List[str] = []
    if mode == "APPLY":
        for table, rows, counters in plans:
            if counters["inserted"] or counters["updated_cells"]:
                store.write(table, rows)
                written.append(table)
                logger.info(f"   {table} : écrite")
    changed = {table: counters["inserted"] + counters["updated_cells"] for table, _, counters in plans}
    hris.spark_io.StepLogger(spark, tables, import_id, "REFERENCE_IMPORT").succeeded(
        rows_out=sum(changed[table] for table in written), counters_only=True,
        message=json.dumps({"mode": mode, "folder": folder, "changes": changed, "written": written,
                            "missing_files": missing, "change_ref": p_change_ref.strip()}, ensure_ascii=False)[:7800])
    result = {"import_id": import_id, "mode": mode, "tables": counters_by_table, "written": written,
              "missing_files": missing, "unexpected_files": unexpected}
    logger.info(f"✓ Import : {json.dumps({k: v for k, v in result.items() if k != 'tables'}, ensure_ascii=False)}")
except Exception as e:
    error_message = f"Erreur lors de l'import des codes ADP: {e}"
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
