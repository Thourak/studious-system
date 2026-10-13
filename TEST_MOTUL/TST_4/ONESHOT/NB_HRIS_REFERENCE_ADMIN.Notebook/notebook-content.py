# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T4_NB_HRIS_REFERENCE_ADMIN. C'est le nom affiché dans Microsoft Fabric pour le notebook d'administration des tables de correspondance entre les codes de TalentSoft et ceux d'ADP ; le dossier du dépôt s'appelle NB_HRIS_REFERENCE_ADMIN.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Il n'est pas appelé par le pipeline : le métier ou le développeur le lance à la main.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/ONESHOT/NB_HRIS_REFERENCE_ADMIN.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Les tables de correspondance cfg.lu_hr_* sont la seule source des codes ADP : aucun fichier n'est utilisé. Leur partie TalentSoft, c'est-à-dire les codes et les libellés, est mise à jour automatiquement à chaque run par le notebook d'acquisition, à partir des référentiels du Core HR et de l'extraction. Leur partie ADP n'existe pas dans TalentSoft, ce qui a été vérifié sur le tenant de production : elle est saisie ici. Le notebook liste d'abord, table par table, les codes TalentSoft qui n'ont encore aucun code ADP, les plus employés en premier, sous la forme d'une grille prête à compléter. La saisie la plus simple consiste à coller une grille avec ses en-têtes : soit cette grille complétée dans Excel, soit le résultat d'une requête SELECT sur une table dwh.lu_hr_* de l'existant, dont les noms de colonnes sont les mêmes. Plusieurs tables peuvent être collées en une fois. Le notebook contrôle ensuite la saisie sans rien écrire, puis l'écrit. Une saisie n'est écrite que si elle est entièrement valide, sur toutes les tables : colonne modifiable par le métier, valeur textuelle, aucun conflit. Les colonnes TalentSoft et les colonnes techniques ne sont jamais modifiables ici. Aucun fichier n'est lu : la grille est un texte collé dans le paramètre p_grid.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise son module hris.reference_api, ainsi que l'écriture Delta, le modèle de données et le journal des cellules. Il lit les tables cfg.lu_hr_* et l'extraction stg.ts_employee_raw, écrit les tables cfg.lu_hr_* en mode APPLY, consigne les saisies dans ctl.run_step et ajoute ses traces dans log.cell_execution. Aucun secret n'est lu et aucun appel réseau n'est fait.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et valide les paramètres. En mode REPORT, la cellule d'administration retient le run demandé ou, à défaut, la dernière extraction, puis affiche une grille séparée par des tabulations : pour chaque table, une ligne de titre qui commence par # et donne le nombre de codes TalentSoft et de codes sans code ADP, puis une ligne d'en-têtes et une ligne par code sans code ADP, avec son libellé, son nombre d'occurrences dans l'extraction et les colonnes ADP à remplir. En mode CHECK, elle lit la grille collée, ou la saisie JSON, et rapproche chaque code de la grille du code de la table. Le rapprochement se fait à l'identique, ou à la casse près quand un seul code correspond. Un code absent de la table est ignoré et listé, ce qui arrive avec une grille reprise de l'existant. La cellule applique ensuite la saisie en mémoire et affiche chaque changement prévu, avec l'ancienne et la nouvelle valeur, ainsi que le nombre de codes encore sans code ADP, sans rien écrire. En mode APPLY, elle fait la même chose, puis remplace le contenu de chaque table modifiée, ce qui crée une nouvelle version Delta. Si la saisie contient une erreur, quelle que soit la table concernée, toutes les erreurs sont affichées et rien n'est écrit. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_mode vaut REPORT par défaut ; CHECK contrôle une saisie sans l'écrire et APPLY l'écrit. Le paramètre p_table désigne la table, par exemple lu_hr_emp_titre. En REPORT, il peut rester vide pour parcourir les treize tables utilisées par la transformation. Avec p_grid, il peut aussi rester vide si chaque grille est précédée de sa ligne # lu_hr_.... Il est obligatoire avec p_assignments. Le paramètre p_grid reçoit la grille collée entre les triples guillemets. La première ligne donne les en-têtes : la colonne du code TalentSoft, par exemple cd_emp_titre_ts, ou code, puis les colonnes à renseigner. Les cellules sont séparées par des tabulations, comme dans un copier-coller depuis Excel ou un éditeur SQL ; le point-virgule est accepté. Une cellule vide ou NULL laisse la valeur inchangée. Les colonnes TalentSoft et les colonnes d'aide sont ignorées, alors qu'une colonne en _adp ou en or_ inconnue est une erreur. Le paramètre p_assignments contient, à la place de p_grid, la saisie au format JSON d'une seule table : pour chaque code TalentSoft, les colonnes à renseigner et leurs valeurs. Une chaîne vide y efface une valeur, et un code absent de la table y est une erreur. Le paramètre p_change_ref, obligatoire en APPLY, donne la référence du changement, par exemple un ticket ou une validation métier ; il est écrit dans la colonne _change_ref des lignes modifiées. Le paramètre p_run_id choisit le run dont l'extraction sert à compter les occurrences en REPORT ; vide, la dernière extraction est retenue. Le paramètre p_max_lines vaut 100 et limite le nombre de codes affichés par table. Les paramètres p_test_schema, p_lakehouse_name et p_log_cell_success ont le même rôle que dans les autres notebooks.
# # **Sorties produites**
# # En mode APPLY, le notebook remplace le contenu de chaque table modifiée. Les lignes modifiées reçoivent _loaded_at_utc, _loaded_by (notebook et identifiant REFADMIN) et _change_ref. Il consigne chaque contrôle ou saisie dans ctl.run_step, étape REFERENCE_ADMIN, avec le nombre de changements par table, les tables écrites et la référence du changement, sans aucune valeur. Sa valeur de sortie est un texte structuré qui résume le mode, les changements par table et les tables écrites.
# # **Limitations connues et points d'attention**
# # Les codes ADP ne peuvent venir que du métier : TalentSoft ne les stocke pas, et l'ancien flux les lisait dans des fichiers déposés à la main sur le SFTP, qui ne sont plus utilisés. Tant qu'une table utilisée n'a aucun code ADP, la transformation bloque sur MAPPING_TABLE_EMPTY ; tant qu'un code TalentSoft employé n'a pas de code ADP, les salariés concernés sont rejetés avec le motif Référence_Rejet. Excel transforme les codes numériques : un code 01 devient 1. Il faut donc mettre les colonnes de code au format Texte avant d'y coller ou d'y saisir. Coller directement le résultat d'une requête SQL dans le notebook évite ce piège. Les tables d'une grille sont écrites l'une après l'autre : si une écriture échoue, les tables déjà écrites le restent, et relancer APPLY ne réécrit que ce qui diffère encore. La grille collée reste dans la cellule de paramètres : il faut la vider après APPLY, ou annuler la modification du notebook dans le contrôle de source, pour ne pas la valider dans Git. Le notebook affiche des codes et des libellés de référentiel, qui ne sont pas des données personnelles, mais aucune donnée de salarié.
# # **Responsable et contact**
# # Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance des correspondances reste à confirmer par l'équipe en charge des ressources humaines.

# CELL ********************

%run T4_NB_HRIS_LIB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import json
import uuid

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB)
from hris import reference_api
from hris import spark_io
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import HrisError, as_bool, json_exit, utc_now
from hris.model import USED_LOOKUP_TABLES, tables_for_notebook
from hris.transform import ensure_session_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_mode = "REPORT"                # REPORT : codes TalentSoft sans code ADP ; CHECK : contrôle une saisie ; APPLY : écrit la saisie
p_table = ""                     # table de correspondance (ex. lu_hr_emp_titre) ; vide = 13 tables (REPORT) ou lignes « # lu_hr_... » de p_grid
p_grid = """
"""                              # CHECK/APPLY : grille collée sur la ligne vide ci-dessus (Excel ou résultat SQL, en-têtes compris) ; cellule vide = inchangée
p_assignments = ""               # CHECK/APPLY, à la place de p_grid : JSON {"<code TalentSoft>": {"<colonne>": "<valeur>"}} ; "" efface une valeur
p_change_ref = ""                # APPLY : référence du changement (ticket, validation métier), obligatoire
p_run_id = ""                    # REPORT : run dont l'extraction sert au comptage ; vide = dernière extraction
p_max_lines = "100"              # REPORT : nombre maximal de codes affichés par table
p_test_schema = ""               # tests uniquement : schéma d'isolement ; vide = tables réelles
p_lakehouse_name = ""            # VL_HRIS.lakehouse_name : Lakehouse cible (obligatoire, aucun Lakehouse par défaut)
p_log_cell_success = "true"      # true : trace aussi les cellules réussies ; false : échecs seulement

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CONFIGURATION MINIMALE =============
cell_log = None
try:
    runtime_context = notebookutils.runtime.context
    tables = tables_for_notebook(runtime_context, p_test_schema, p_lakehouse_name)   # 4 parties : aucun Lakehouse par défaut requis
    cell_log = CellLog(runtime_context, lakehouse_name=tables.lakehouse, log_success=as_bool(p_log_cell_success),
                       writer=spark_writer(spark, tables))
    cell_log.start("CONFIG")
    ensure_session_settings(spark)
    mode = str(p_mode).strip().upper()
    if mode not in ("REPORT", "CHECK", "APPLY"):
        raise HrisError("PARAM_INVALID", "p_mode doit valoir REPORT, CHECK ou APPLY")
    if p_table and p_table not in reference_api.CATALOGUE:
        raise HrisError("PARAM_INVALID", "p_table inconnue : " + p_table)
    use_grid = bool(str(p_grid).strip())
    if mode != "REPORT" and use_grid == bool(str(p_assignments).strip()):
        raise HrisError("PARAM_INVALID", "renseigner p_grid ou p_assignments (un seul des deux)")
    if mode != "REPORT" and not use_grid and not p_table:
        raise HrisError("PARAM_MISSING", "p_table (obligatoire avec p_assignments)")
    if mode == "APPLY" and not p_change_ref.strip():
        raise HrisError("PARAM_MISSING", "p_change_ref (obligatoire pour écrire)")
    max_lines = int(str(p_max_lines).strip())
    admin_id = "REFADMIN-" + uuid.uuid4().hex[:12]                       # identifiant de la trace dans ctl.run_step
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "admin_id": admin_id, "mode": mode,
                      "table": p_table or ("tables de la grille" if mode != "REPORT" else "13 tables utilisées"),
                      "input": "REPORT" if mode == "REPORT" else ("p_grid" if use_grid else "p_assignments")},
                     ensure_ascii=False))
    cell_log.success()
except Exception as error:
    if cell_log is None:
        report_unlogged_failure("CONFIG", error)
    else:
        cell_log.failure(error)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= ADMINISTRATION DES CODES ADP (aucun fichier) =============
cell_log.start("REFERENCE_ADMIN")
try:
    store = reference_api.SparkReferenceStore(spark, tables)
    result = {"admin_id": admin_id, "mode": mode}
    if mode == "REPORT":
        run_id = p_run_id or store.latest_run()
        result["run_id"] = run_id
        summary, grid = reference_api.adp_grid_report(store, [p_table] if p_table else USED_LOOKUP_TABLES,
                                                      lambda table: store.usage(run_id, table) if run_id else {},
                                                      max_lines)
        result["tables"] = summary
        print(json.dumps(result, ensure_ascii=False, default=str))
        print("\n".join(grid))
    else:
        technical = {"_loaded_at_utc": utc_now(), "_loaded_by": cell_log.base["notebook_name"] + ":" + admin_id,
                     "_change_ref": p_change_ref.strip() or None}
        errors, plans = [], []                                # plans : (table, lignes complètes, changements)
        if use_grid:
            grids, errors = reference_api.parse_grid(p_grid, p_table)
            requested = {table: grid.assignments for table, grid in grids.items()}
            for table, grid in grids.items():
                print(f"# {table} : {grid.rows} ligne(s) lue(s), {len(grid.assignments)} code(s) avec valeur, "
                      f"{grid.rows_without_value} sans valeur ; colonnes : {', '.join(grid.columns) or 'aucune'}"
                      + (f" ; ignorées : {', '.join(grid.ignored_columns)}" if grid.ignored_columns else ""))
        else:
            try:
                requested = {p_table: json.loads(p_assignments)}
            except ValueError:
                raise HrisError("PARAM_INVALID", "p_assignments n'est pas un JSON valide") from None
        for table, assignments in requested.items():
            state = store.read(table)
            if not state.exists or not state.schema_ok:
                errors.append(f"{table} : table indisponible, {state.schema_detail[:200]}")
                continue
            if use_grid:
                resolution = reference_api.resolve_grid(table, state.rows, assignments)
                if resolution.unknown:
                    print(f"# {table} : {len(resolution.unknown)} code(s) absent(s) de la table, ignoré(s) : "
                          + ", ".join(resolution.unknown[:max_lines]))
                if resolution.case_matched:
                    print(f"# {table} : rapprochés à la casse près : "
                          + ", ".join(f"{a} -> {b}" for a, b in resolution.case_matched[:max_lines]))
                errors += [f"{table} : {message}" for message in resolution.errors]
                assignments = resolution.assignments
            rows, outcome = reference_api.apply_assignments(table, state.rows, assignments, technical)
            for code, column, old, new in outcome.changes:
                print(f"   {table:<34} {code:<30} {column:<36} {old!r} -> {new!r}")
            errors += [f"{table} : {message}" for message in outcome.errors]
            remaining = len(reference_api.missing_adp(table, rows, {}))
            print(f"# {table} : {len(outcome.changes)} changement(s) ; codes TalentSoft encore sans code ADP : {remaining}")
            plans.append((table, rows, outcome.changes))
        for message in errors:
            print("   ERREUR :", message)
        if errors:
            raise HrisError("ASSIGNMENTS_INVALID", f"{len(errors)} erreur(s) : rien n'est écrit")
        written = []
        if mode == "APPLY":
            for table, rows, changes in plans:                # une table après l'autre ; relancer APPLY est sans effet
                if changes:
                    store.write(table, rows)
                    written.append(table)
                    print(f"# {table} : écrite")
        counts = {table: len(changes) for table, _, changes in plans}
        spark_io.StepLogger(spark, tables, admin_id, "REFERENCE_ADMIN").succeeded(
            rows_out=sum(counts[t] for t in written), counters_only=True,
            message=json.dumps({"mode": mode, "input": "p_grid" if use_grid else "p_assignments", "changes": counts,
                                "written": written, "change_ref": p_change_ref.strip()}, ensure_ascii=False)[:7800])
        result.update(tables=counts, changes=sum(counts.values()), written=written)
        print(json.dumps(result, ensure_ascii=False, default=str))
    cell_log.success()
except Exception as error:
    cell_log.failure(error)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= CLÔTURE =============
cell_log.flush()
notebookutils.notebook.exit(json_exit(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
