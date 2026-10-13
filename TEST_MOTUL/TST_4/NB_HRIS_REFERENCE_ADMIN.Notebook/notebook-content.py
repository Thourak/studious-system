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
# # TEST_MOTUL/TST_4/NB_HRIS_REFERENCE_ADMIN.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Les tables de correspondance cfg.lu_hr_* sont la seule source des codes ADP : aucun fichier n'est utilisé. Leur partie TalentSoft, c'est-à-dire les codes et les libellés, est mise à jour automatiquement à chaque run par le notebook d'acquisition, à partir des référentiels du Core HR et de l'extraction. Leur partie ADP n'existe pas dans TalentSoft, ce qui a été vérifié sur le tenant de production : elle est saisie ici. Le notebook liste d'abord, table par table, les codes TalentSoft qui n'ont encore aucun code ADP, les plus employés en premier, avec un modèle de saisie prêt à compléter. Il contrôle ensuite une saisie sans rien écrire, puis l'écrit. Une saisie n'est écrite que si elle est entièrement valide : code TalentSoft présent dans la table, colonne modifiable par le métier, valeur textuelle. Les colonnes TalentSoft et les colonnes techniques ne sont jamais modifiables ici.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB et utilise son module hris.reference_api, ainsi que l'écriture Delta, le modèle de données et le journal des cellules. Il lit les tables cfg.lu_hr_* et l'extraction stg.ts_employee_raw, écrit les tables cfg.lu_hr_* en mode APPLY, consigne les saisies dans ctl.run_step et ajoute ses traces dans log.cell_execution. Aucun secret n'est lu et aucun appel réseau n'est fait.
# # **Fonctionnement et logique de traitement**
# # La configuration lit le contexte réel du notebook, résout les noms complets des tables, ouvre le journal des cellules et valide les paramètres. En mode REPORT, la cellule d'administration retient le run demandé ou, à défaut, la dernière extraction, puis affiche pour chaque table le nombre de codes TalentSoft, ceux qui n'ont aucun code ADP, le nombre d'occurrences de chacun dans l'extraction, les colonnes à renseigner et un modèle de saisie au format JSON. En mode CHECK, elle applique la saisie en mémoire et affiche chaque changement prévu, ancienne et nouvelle valeur, sans écrire. En mode APPLY, elle fait la même chose puis remplace le contenu de la table, ce qui crée une nouvelle version Delta. Si la saisie contient une erreur, toutes les erreurs sont affichées et rien n'est écrit. La clôture écrit les traces du journal puis renvoie le résultat.
# # **Paramètres**
# # Le paramètre p_mode vaut REPORT par défaut ; CHECK contrôle une saisie sans l'écrire et APPLY l'écrit. Le paramètre p_table désigne la table, par exemple lu_hr_emp_titre ; il est obligatoire en CHECK et en APPLY, et vide en REPORT pour parcourir les treize tables utilisées par la transformation. Le paramètre p_assignments contient la saisie au format JSON : pour chaque code TalentSoft, les colonnes à renseigner et leurs valeurs ; une chaîne vide efface une valeur. Le paramètre p_change_ref, obligatoire en APPLY, donne la référence du changement, par exemple un ticket ou une validation métier ; il est écrit dans la colonne _change_ref des lignes modifiées. Le paramètre p_run_id choisit le run dont l'extraction sert à compter les occurrences en REPORT ; vide, la dernière extraction est retenue. Le paramètre p_max_lines vaut 100 et limite le nombre de codes affichés par table. Les paramètres p_test_schema, p_lakehouse_name et p_log_cell_success ont le même rôle que dans les autres notebooks.
# # **Sorties produites**
# # En mode APPLY, le notebook remplace le contenu de la table désignée, avec _loaded_at_utc, _loaded_by (notebook et identifiant REFADMIN) et _change_ref sur les lignes modifiées. Il consigne chaque contrôle ou saisie dans ctl.run_step, étape REFERENCE_ADMIN, avec le nombre de changements et la référence du changement, sans aucune valeur. Sa valeur de sortie est un texte structuré qui résume le mode, la table, le nombre de changements et l'écriture éventuelle.
# # **Limitations connues et points d'attention**
# # Les codes ADP ne peuvent venir que du métier : TalentSoft ne les stocke pas, et l'ancien flux les lisait dans des fichiers déposés à la main sur le SFTP, qui ne sont plus utilisés. Tant qu'une table utilisée n'a aucun code ADP, la transformation bloque sur MAPPING_TABLE_EMPTY ; tant qu'un code TalentSoft employé n'a pas de code ADP, les salariés concernés sont rejetés avec le motif Référence_Rejet. Une saisie se fait une table à la fois. Le notebook affiche des codes et des libellés de référentiel, qui ne sont pas des données personnelles, mais aucune donnée de salarié.
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
p_table = ""                     # table de correspondance (ex. lu_hr_emp_titre) ; obligatoire en CHECK/APPLY ; vide en REPORT = 13 tables
p_assignments = ""               # CHECK/APPLY : JSON {"<code TalentSoft>": {"<colonne>": "<valeur>"}} ; "" efface une valeur
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
    if mode != "REPORT" and not p_table:
        raise HrisError("PARAM_MISSING", "p_table")
    if mode == "APPLY" and not p_change_ref.strip():
        raise HrisError("PARAM_MISSING", "p_change_ref (obligatoire pour écrire)")
    max_lines = int(str(p_max_lines).strip())
    admin_id = "REFADMIN-" + uuid.uuid4().hex[:12]                       # identifiant de la trace dans ctl.run_step
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "notebook_name": cell_log.base["notebook_name"], "admin_id": admin_id, "mode": mode,
                      "table": p_table or "13 tables utilisées"}, ensure_ascii=False))
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
        summary = {}
        for table in ([p_table] if p_table else USED_LOOKUP_TABLES):
            state = store.read(table)
            if not state.schema_ok:
                summary[table] = state.schema_detail[:200]
                print(f"== {table} : table non conforme, {state.schema_detail[:200]}")
                continue
            usage = store.usage(run_id, table) if run_id else {}
            missing = reference_api.missing_adp(table, state.rows, usage)
            concerned = sum(item["employees"] for item in missing)
            summary[table] = {"codes": len(state.rows), "without_adp": len(missing), "occurrences_without_adp": concerned}
            print(f"== {table} : {len(state.rows)} codes TalentSoft, {len(missing)} sans code ADP "
                  f"({concerned} occurrence(s) dans l'extraction)")
            if missing:
                print("   colonnes modifiables :", ", ".join(reference_api.business_columns(table)))
                for item in missing[:max_lines]:
                    print(f"   {item['code']:<40} {item['employees']:>6}  {item['label'] or ''}")
                template = {item["code"]: {column: "" for column in reference_api.adp_columns(table)}
                            for item in missing[:max_lines]}
                print(f"   modèle de saisie : p_table = \"{table}\", p_assignments =")
                print("   " + json.dumps(template, ensure_ascii=False))
        result["tables"] = summary
    else:
        try:
            assignments = json.loads(p_assignments or "{}")
        except ValueError:
            raise HrisError("PARAM_INVALID", "p_assignments n'est pas un JSON valide") from None
        state = store.read(p_table)
        if not state.exists or not state.schema_ok:
            raise HrisError("TABLE_UNAVAILABLE", f"{p_table} : {state.schema_detail[:200]}")
        rows, outcome = reference_api.apply_assignments(p_table, state.rows, assignments, {
            "_loaded_at_utc": utc_now(), "_loaded_by": cell_log.base["notebook_name"] + ":" + admin_id,
            "_change_ref": p_change_ref.strip() or None})
        for code, column, old, new in outcome.changes:
            print(f"   {code:<40} {column:<36} {old!r} -> {new!r}")
        for message in outcome.errors:
            print("   ERREUR :", message)
        if outcome.errors:
            raise HrisError("ASSIGNMENTS_INVALID", f"{len(outcome.errors)} erreur(s) : rien n'est écrit")
        written = mode == "APPLY" and bool(outcome.changes)
        if written:
            store.write(p_table, rows)
        spark_io.StepLogger(spark, tables, admin_id, "REFERENCE_ADMIN").succeeded(
            rows_out=len(outcome.changes) if written else 0, counters_only=True,
            message=json.dumps({"mode": mode, "table": p_table, "changes": len(outcome.changes), "written": written,
                                "change_ref": p_change_ref.strip()}, ensure_ascii=False))
        result.update(table=p_table, changes=len(outcome.changes), written=written)
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
