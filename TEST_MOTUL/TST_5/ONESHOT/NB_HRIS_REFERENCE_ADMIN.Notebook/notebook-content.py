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
# T5_NB_HRIS_REFERENCE_ADMIN
# 
# **Type d'objet**
# 
# Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.
# 
# **Chemin dans le dépôt**
# 
# TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_REFERENCE_ADMIN.Notebook/notebook-content.py
# 
# **Description fonctionnelle**
# 
# Ce notebook permet de saisir, de façon contrôlée et tracée, les codes ADP des tables de correspondance entre les codes de TalentSoft et ceux de l'outil de paie ADP. Ces tables remplacent les dix-sept fichiers de référentiels de l'existant : aucun fichier n'est lu. Leur partie TalentSoft est mise à jour automatiquement par l'acquisition ; leur partie ADP n'existe pas dans TalentSoft et se saisit ici. Le notebook affiche d'abord les codes TalentSoft sans code ADP, les plus employés en premier, sous la forme d'une grille prête à compléter, puis contrôle et applique la saisie.
# 
# **Dépendances**
# 
# Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de correspondances, d'écriture dans les tables, de contrôle du run, de modèle de données et de journal. Il lit et écrit les tables cfg.lu_hr_, lit la dernière extraction stg.ts_employee_raw pour compter l'usage des codes et lit le verrou ctl.process_lock. Il est lancé à la main par le métier ou le développeur ; il n'est appelé par aucun pipeline.
# 
# **Fonctionnement et logique de traitement**
# 
# Après la configuration et la préparation du Lakehouse, le notebook contrôle les paramètres. En mode REPORT, il affiche la grille des codes sans code ADP. En mode CHECK, il lit la grille collée ou la saisie en format JSON, rapproche chaque code de la table, applique la saisie en mémoire et affiche chaque changement, sans rien écrire. En mode APPLY, il refuse d'écrire si un run tient le verrou, pour ne pas croiser la mise à jour des codes TalentSoft faite par l'acquisition ; sinon il écrit la saisie, table par table, avec la référence du changement, et trace l'opération dans ctl.run_step. Une seule erreur, sur n'importe quelle table, fait tout refuser.
# 
# **Paramètres**
# 
# Le paramètre p_mode vaut REPORT par défaut, CHECK ou APPLY. Le paramètre p_table désigne une table de correspondance, ou reste vide pour traiter les treize tables utilisées ou les tables nommées dans la grille. Le paramètre p_grid reçoit une grille collée, avec ses en-têtes, depuis un tableur ou le résultat d'une requête sur les tables de l'existant ; une cellule vide laisse la valeur inchangée. Le paramètre p_assignments reçoit, à la place de la grille, une saisie en format JSON pour une seule table. Le paramètre p_change_ref porte la référence du changement et il est obligatoire en mode APPLY. Le paramètre p_run_id choisit l'extraction utilisée pour compter les codes, p_max_lines limite le nombre de codes affichés, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.
# 
# **Sorties produites**
# 
# En mode APPLY, le notebook remplace les tables de correspondance modifiées, chaque écriture créant une version qui permet un retour arrière, et ajoute une trace dans ctl.run_step. Dans tous les modes, il écrit son journal dans log.notebook_log et renvoie un résultat court de compteurs.
# 
# **Limitations connues et points d'attention**
# 
# Pour reprendre les codes ADP de l'existant, il faut coller le résultat des requêtes sur les tables externes de Synapse, dont les colonnes portent les noms du modèle : les fichiers de référentiels de l'existant ont des en-têtes différents pour les nationalités, les pays et les entités, et une grille collée telle quelle est refusée. Une clé de situation matrimoniale porte deux codes ADP différents dans l'existant : le métier doit choisir le bon code avant la saisie. Après APPLY, il faut vider p_grid pour ne pas enregistrer la grille dans le contrôle de source.
# 
# **Responsable et contact**
# 
# Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import json
import uuid

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

p_mode = "REPORT"        # REPORT : codes TalentSoft sans code ADP ; CHECK : contrôle une saisie ; APPLY : écrit la saisie
p_table = ""             # table de correspondance (ex. lu_hr_emp_titre) ; vide = 13 tables (REPORT) ou tables de la grille
p_grid = ""              # CHECK/APPLY : grille collée (en-têtes compris), entre triples guillemets pour plusieurs lignes ; cellule vide = inchangée
p_assignments = ""       # CHECK/APPLY, à la place de p_grid : JSON {"<code TalentSoft>": {"<colonne>": "<valeur>"}}
p_change_ref = ""        # APPLY : référence du changement (ticket, validation métier), obligatoire
p_run_id = ""            # REPORT : run dont l'extraction sert au comptage ; vide = dernière extraction
p_max_lines = "100"      # REPORT : nombre maximal de codes affichés par table
p_test_schema = ""       # tests uniquement : schéma d'isolement ; vide = tables réelles
eventpath = ""           # eventpath Fabric (ex. /p_mode=REPORT/p_table=lu_hr_emp_titre)

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
        p_table = parsed_path.get("p_table", p_table)
        p_grid = parsed_path.get("p_grid", p_grid)
        p_assignments = parsed_path.get("p_assignments", p_assignments)
        p_change_ref = parsed_path.get("p_change_ref", p_change_ref)
        p_run_id = parsed_path.get("p_run_id", p_run_id)
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

# ============= CONTRÔLE DES PARAMÈTRES ET DU VERROU =============
try:
    mode = str(p_mode).strip().upper()
    if mode not in ("REPORT", "CHECK", "APPLY"):
        raise hris.common.HrisError("PARAM_INVALID", "p_mode doit valoir REPORT, CHECK ou APPLY")
    if p_table and p_table not in hris.reference_api.CATALOGUE:
        raise hris.common.HrisError("PARAM_INVALID", "p_table inconnue : " + p_table)
    use_grid = bool(str(p_grid).strip())
    if mode != "REPORT" and use_grid == bool(str(p_assignments).strip()):
        raise hris.common.HrisError("PARAM_INVALID", "renseigner p_grid ou p_assignments (un seul des deux)")
    if mode != "REPORT" and not use_grid and not p_table:
        raise hris.common.HrisError("PARAM_MISSING", "p_table (obligatoire avec p_assignments)")
    if mode == "APPLY" and not p_change_ref.strip():
        raise hris.common.HrisError("PARAM_MISSING", "p_change_ref (obligatoire pour écrire)")
    if mode == "APPLY":
        lock = hris.run_control.lock_state(spark, tables)
        if lock["held"]:
            raise hris.common.HrisError("RUN_IN_PROGRESS", "un run tient le verrou jusqu'à " + str(lock["lease_expires_at_utc"])
                                        + " : relancer APPLY après sa fin")
    max_lines = int(str(p_max_lines).strip())
    admin_id = "REFADMIN-" + uuid.uuid4().hex[:12]
    logger.info(f"✓ Administration des codes ADP {mode}, trace {admin_id}")
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

# ============= ADMINISTRATION DES CODES ADP (AUCUN FICHIER) =============
try:
    store = hris.reference_api.SparkReferenceStore(spark, tables)
    result = {"admin_id": admin_id, "mode": mode}
    if mode == "REPORT":
        report_run_id = p_run_id or store.latest_run()
        result["run_id"] = report_run_id
        summary, grid = hris.reference_api.adp_grid_report(
            store, [p_table] if p_table else hris.model.USED_LOOKUP_TABLES,
            lambda table: store.usage(report_run_id, table) if report_run_id else {}, max_lines)
        result["tables"] = summary
        print("\n".join(grid))              # grille à copier : codes TalentSoft et libellés, aucune donnée de salarié
    else:
        technical = {"_loaded_at_utc": hris.common.utc_now(), "_loaded_by": item_name + ":" + admin_id,
                     "_change_ref": p_change_ref.strip() or None}
        errors, plans = [], []
        if use_grid:
            grids, errors = hris.reference_api.parse_grid(p_grid, p_table)
            requested = {table: grid.assignments for table, grid in grids.items()}
            for table, grid in grids.items():
                logger.info(f"   {table} : {grid.rows} ligne(s) lue(s), {len(grid.assignments)} code(s) avec valeur, "
                            f"{grid.rows_without_value} sans valeur ; colonnes : {', '.join(grid.columns) or 'aucune'}")
        else:
            try:
                requested = {p_table: json.loads(p_assignments)}
            except ValueError:
                raise hris.common.HrisError("PARAM_INVALID", "p_assignments n'est pas un JSON valide") from None
        for table, assignments in requested.items():
            state = store.read(table)
            if not state.exists or not state.schema_ok:
                errors.append(f"{table} : table indisponible, {state.schema_detail[:200]}")
                continue
            if use_grid:
                resolution = hris.reference_api.resolve_grid(table, state.rows, assignments)
                if resolution.unknown:
                    logger.info(f"   {table} : {len(resolution.unknown)} code(s) absent(s) de la table, ignoré(s) : "
                                + ", ".join(resolution.unknown[:max_lines]))
                if resolution.case_matched:
                    logger.info(f"   {table} : rapprochés à la casse près : "
                                + ", ".join(f"{a} -> {b}" for a, b in resolution.case_matched[:max_lines]))
                errors += [f"{table} : {message}" for message in resolution.errors]
                assignments = resolution.assignments
            rows, outcome = hris.reference_api.apply_assignments(table, state.rows, assignments, technical)
            for code, column, old, new in outcome.changes:
                print(f"   {table:<34} {code:<30} {column:<36} {old!r} -> {new!r}")
            errors += [f"{table} : {message}" for message in outcome.errors]
            remaining = len(hris.reference_api.missing_adp(table, rows, {}))
            logger.info(f"   {table} : {len(outcome.changes)} changement(s) ; codes TalentSoft encore sans code ADP : {remaining}")
            plans.append((table, rows, outcome.changes))
        for message in errors:
            logger.error(f"   ERREUR : {message}")
        if errors:
            raise hris.common.HrisError("ASSIGNMENTS_INVALID", f"{len(errors)} erreur(s) : rien n'est écrit")
        written = []
        if mode == "APPLY":
            for table, rows, changes in plans:
                if changes:
                    store.write(table, rows)
                    written.append(table)
                    logger.info(f"   {table} : écrite")
        counts = {table: len(changes) for table, _, changes in plans}
        hris.spark_io.StepLogger(spark, tables, admin_id, "REFERENCE_ADMIN").succeeded(
            rows_out=sum(counts[t] for t in written), counters_only=True,
            message=json.dumps({"mode": mode, "input": "p_grid" if use_grid else "p_assignments", "changes": counts,
                                "written": written, "change_ref": p_change_ref.strip()}, ensure_ascii=False)[:7800])
        result.update(tables=counts, changes=sum(counts.values()), written=written)
    logger.info(f"✓ Administration : {json.dumps(result, ensure_ascii=False, default=str)}")
except Exception as e:
    error_message = f"Erreur lors de l'administration des codes ADP: {e}"
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
