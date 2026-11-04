"""Tests locaux de T6_NB_HRIS_REFERENCE_IMPORT (lecture des fichiers de l'existant et plan d'import, sans Spark).

Ce fichier n'est PAS un item Fabric : il se lance sur le poste, sans Spark ni réseau, avec la seule bibliothèque
standard :  python -I -B LOCAL_TESTS/test_reference_import.py

Principe : le code testé est celui du notebook (cellules de constantes et de fonctions extraites de
notebook-content.py) ; les modules hris.common, hris.model et hris.reference_api sont ceux de la bibliothèque
T6_NB_HRIS_LIB (source embarquée). hris.spark_io est remplacé par un module vide : il n'est utilisé que par le
stockage Spark, que ces tests ne touchent pas. La lecture réelle de la zone Files (Spark) n'est pas testée ici.
"""
from __future__ import annotations

import re
import sys
import types
import unittest
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "QUOTIDIEN" / "NB_HRIS_LIB.Notebook" / "notebook-content.py"
IMPORT_NB = ROOT / "ONESHOT" / "NB_HRIS_REFERENCE_IMPORT.Notebook" / "notebook-content.py"
SYNAPSE_INIT = Path(r"C:\Users\thibault.delalande\VS_CODE\SYN-ADO-MGE-AZR-CORP-HRIS-CORE\pipeline\Pipeline INIT.json")

CELL_SPLIT = re.compile(r"^# (?:CELL|PARAMETERS CELL) \*+\s*$", re.M)
META_SPLIT = re.compile(r"^# (?:METADATA|MARKDOWN) \*+\s*$", re.M)
MODULE_CALL = re.compile(r"register_hris_module\('([^']+)',\s*'[0-9a-f]{64}',\s*r'''(.*?)'''\)", re.S)
WANTED_MODULES = ("hris.common", "hris.parity", "hris.model", "hris.reference_api")


def notebook_cells(path: Path) -> List[str]:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    return [META_SPLIT.split(cell)[0].strip("\n") for cell in CELL_SPLIT.split(text)[1:]]


def load_hris() -> types.ModuleType:
    package = types.ModuleType("hris")
    package.__path__ = []  # type: ignore[attr-defined]
    sys.modules["hris"] = package
    sys.modules["hris.spark_io"] = types.ModuleType("hris.spark_io")
    package.spark_io = sys.modules["hris.spark_io"]  # type: ignore[attr-defined]
    sources = dict(MODULE_CALL.findall(LIB.read_text(encoding="utf-8").replace("\r\n", "\n")))
    for name in WANTED_MODULES:
        module = types.ModuleType(name)
        sys.modules[name] = module
        exec(compile(sources[name], name, "exec"), module.__dict__)
        setattr(package, name.split(".", 1)[1], module)
    return package


class FakeLogger:
    def __getattr__(self, name: str):
        return lambda *args, **kwargs: None


def load_import_functions(hris: types.ModuleType) -> Dict[str, Any]:
    """Exécute les cellules d'imports, de constantes et de fonctions du notebook (pas la lecture Spark)."""
    namespace: Dict[str, Any] = {"hris": hris, "logger": FakeLogger()}
    for cell in notebook_cells(IMPORT_NB):
        first = cell.lstrip().splitlines()[0] if cell.strip() else ""
        wanted = (first.startswith("# ============= IMPORTS")
                  or first.startswith("# ============= CONSTANTES")
                  or first.startswith("def "))
        if wanted:
            exec(compile(cell, "NB_HRIS_REFERENCE_IMPORT", "exec"), namespace)
    return namespace


HRIS = load_hris()
NS = load_import_functions(HRIS)
TECH = {"_loaded_at_utc": "2026-11-04T00:00:00Z", "_loaded_by": "test", "_change_ref": "TEST"}


def csv_text(table: str, rows: List[List[str]], delimiter: str = ",", quoted: bool = True) -> str:
    columns = HRIS.model.LOOKUP_TABLES[table]

    def cell(value: str) -> str:
        return '"' + value.replace('"', '""') + '"' if quoted else value
    lines = [delimiter.join(cell(c) for c in columns)]
    lines += [delimiter.join(cell(v) for v in row) for row in rows]
    return "\n".join(lines) + "\n"


class FilesAndTables(unittest.TestCase):
    def test_association_matches_library(self):
        self.assertEqual(sorted(NS["FILE_TO_TABLE"].values()), sorted(HRIS.model.LOOKUP_TABLES))
        self.assertEqual(len(NS["FILE_TO_TABLE"]), 17)

    @unittest.skipUnless(SYNAPSE_INIT.exists(), "dépôt Synapse absent du poste")
    def test_association_matches_synapse_external_tables(self):
        import json
        init = json.loads(SYNAPSE_INIT.read_text(encoding="utf-8"))
        sql = ""

        def walk(acts):
            for act in acts:
                yield act
                for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
                    yield from walk(act.get("typeProperties", {}).get(key, []))
        for act in walk(init["properties"]["activities"]):
            if act["type"] == "Script":
                for script in act["typeProperties"]["scripts"]:
                    text = script["text"]
                    sql += (text["value"] if isinstance(text, dict) else text) + "\n"
        found = {f: t.lower() for t, f in re.findall(
            r"(?is)create\s+external\s+table\s+\[?dwh\]?\.\[?(lu_hr_\w+)\]?\s*\(.*?\)\s*WITH\s*\(\s*LOCATION\s*=\s*'talentsoft/reference/([\w\.]+)'", sql)}
        self.assertEqual(found, NS["FILE_TO_TABLE"])


class ReadingCsv(unittest.TestCase):
    def test_decode_utf8_bom_and_cp1252(self):
        self.assertEqual(NS["decode_bytes"]("\ufeffcode;libellé".encode("utf-8")), ("code;libellé", "utf-8"))
        self.assertEqual(NS["decode_bytes"]("libellé".encode("cp1252")), ("libellé", "cp1252"))

    def test_delimiters(self):
        for delimiter in (",", ";", "\t"):
            header, rows, found = NS["parse_csv"](f"a{delimiter}b{delimiter}c\n1{delimiter}2{delimiter}3\n")
            self.assertEqual((header, rows, found), (["a", "b", "c"], [["1", "2", "3"]], delimiter))

    def test_quotes_comma_inside_cell_and_blank_lines(self):
        header, rows, delimiter = NS["parse_csv"]('"a","b"\n\n"x, y","say ""hi"""\n   \n')
        self.assertEqual((header, delimiter), (["a", "b"], ","))
        self.assertEqual(rows, [["x, y", 'say "hi"']])

    def test_trailing_empty_header_cells_removed(self):
        header, _, _ = NS["parse_csv"]("a;b;;\n1;2;;\n")
        self.assertEqual(header, ["a", "b"])

    def test_empty_file(self):
        self.assertEqual(NS["parse_csv"](""), ([], [], ","))


class PositionalMapping(unittest.TestCase):
    TABLE = "lu_hr_emp_pays"       # en-têtes du fichier différents du modèle dans l'existant

    def test_positional_with_different_headers(self):
        columns = HRIS.model.LOOKUP_TABLES[self.TABLE]
        header, rows, _ = NS["parse_csv"]("id;code;libelle;id_adp;lib_adp;ordre\n1;FRA;France;FR;France ADP;1\n")
        out, errors, notes = NS["table_rows_from_csv"](self.TABLE, header, rows)
        self.assertEqual(errors, [])
        self.assertTrue(notes)
        self.assertEqual(out[0][columns[1]], "FRA")
        self.assertEqual(out[0][columns[3]], "FR")

    def test_same_headers_no_note(self):
        header = HRIS.model.LOOKUP_TABLES[self.TABLE]
        _, errors, notes = NS["table_rows_from_csv"](self.TABLE, list(header), [])
        self.assertEqual((errors, notes), ([], []))

    def test_wrong_width_is_error(self):
        _, errors, _ = NS["table_rows_from_csv"](self.TABLE, ["a", "b", "c"], [])
        self.assertEqual(len(errors), 1)

    def test_row_too_wide_is_error_but_trailing_empties_are_dropped(self):
        header = list(HRIS.model.LOOKUP_TABLES[self.TABLE])
        out, errors, _ = NS["table_rows_from_csv"](self.TABLE, header, [["1", "FRA", "France", "FR", "France", "1", "", ""]])
        self.assertEqual((len(out), errors), (1, []))
        _, errors, _ = NS["table_rows_from_csv"](self.TABLE, header, [["1", "FRA", "France", "FR", "France", "1", "x"]])
        self.assertEqual(len(errors), 1)

    def test_empty_cell_becomes_none_and_spaces_are_trimmed(self):
        header = list(HRIS.model.LOOKUP_TABLES[self.TABLE])
        out, _, _ = NS["table_rows_from_csv"](self.TABLE, header, [["1", " FRA ", "", "FR", "France", ""]])
        columns = header
        self.assertEqual((out[0][columns[1]], out[0][columns[2]], out[0][columns[5]]), ("FRA", None, None))


class Duplicates(unittest.TestCase):
    TABLE = "lu_hr_emp_sexe"

    def rows(self, *items):
        columns = HRIS.model.LOOKUP_TABLES[self.TABLE]
        return [dict(zip(columns, item)) for item in items]

    def test_identical_duplicate_is_ignored(self):
        rows = self.rows(("M", "Homme", "1", "Masculin", "1"), ("M", "Homme", "1", "Masculin", "1"))
        out, errors, identical, without_code, resolved = NS["dedupe_by_code"](self.TABLE, rows, "ERROR")
        self.assertEqual((len(out), errors, identical, without_code, resolved), (1, [], 1, 0, []))

    def test_conflict_is_error_by_default(self):
        rows = self.rows(("M", "Homme", "1", "Masculin", "1"), ("M", "Homme", "2", "Masculin", "1"))
        out, errors, _, _, _ = NS["dedupe_by_code"](self.TABLE, rows, "ERROR")
        self.assertEqual(len(errors), 1)
        self.assertIn("cd_emp_sexe_adp", errors[0])

    def test_first_and_last(self):
        rows = self.rows(("M", "Homme", "1", "Masculin", "1"), ("M", "Homme", "2", "Masculin", "1"))
        first, _, _, _, resolved = NS["dedupe_by_code"](self.TABLE, rows, "FIRST")
        last, _, _, _, _ = NS["dedupe_by_code"](self.TABLE, rows, "LAST")
        self.assertEqual((first[0]["cd_emp_sexe_adp"], last[0]["cd_emp_sexe_adp"], resolved), ("1", "2", ["M"]))

    def test_row_without_code_is_counted(self):
        rows = self.rows((None, "x", "1", "y", "1"), ("F", "Femme", "2", "Feminin", "2"))
        out, _, _, without_code, _ = NS["dedupe_by_code"](self.TABLE, rows, "ERROR")
        self.assertEqual((len(out), without_code), (1, 1))

    def test_trailing_spaces_are_the_same_code(self):
        rows = self.rows(("M ", "Homme", "1", "Masculin", "1"), ("M", "Homme", "1", "Masculin", "1"))
        out, errors, identical, _, _ = NS["dedupe_by_code"](self.TABLE, rows, "ERROR")
        self.assertEqual(len(out), 1)


class Planning(unittest.TestCase):
    TABLE = "lu_hr_emp_sexe"

    def state(self, *items):
        columns = HRIS.model.LOOKUP_TABLES[self.TABLE] + HRIS.model.LOOKUP_TECH_COLUMNS
        return [dict(zip(columns, item + (None, None, None))) for item in items]

    def file_rows(self, *items):
        columns = HRIS.model.LOOKUP_TABLES[self.TABLE]
        return [dict(zip(columns, item)) for item in items]

    def test_existing_code_gets_adp_values_and_new_code_is_inserted(self):
        state = self.state(("M", "Homme", None, None, None))
        csv_rows = self.file_rows(("M", "Homme", "1", "Masculin", "1"), ("F", "Femme", "2", "Feminin", "2"),
                                  ("X", None, None, None, None))
        rows, counters, errors, changes = NS["plan_table"](self.TABLE, state, csv_rows, False, TECH)
        self.assertEqual(errors, [])
        by_code = {r["cd_emp_sexe_ts"]: r for r in rows}
        self.assertEqual(by_code["M"]["cd_emp_sexe_adp"], "1")
        self.assertEqual(by_code["F"]["cd_emp_sexe_adp"], "2")
        self.assertNotIn("X", by_code)
        self.assertEqual((counters["inserted"], counters["ignored_without_value"]), (1, 1))
        self.assertEqual(by_code["F"]["_change_ref"], "TEST")

    def test_existing_value_is_kept_unless_overwrite(self):
        state = self.state(("M", "Homme", "9", "Saisi", "9"))
        csv_rows = self.file_rows(("M", "Homme", "1", "Masculin", "1"))
        rows, counters, _, changes = NS["plan_table"](self.TABLE, state, csv_rows, False, TECH)
        self.assertEqual(rows[0]["cd_emp_sexe_adp"], "9")
        self.assertEqual((counters["updated_cells"], counters["kept_existing"]), (0, 3))
        rows, counters, _, changes = NS["plan_table"](self.TABLE, state, csv_rows, True, TECH)
        self.assertEqual(rows[0]["cd_emp_sexe_adp"], "1")
        self.assertEqual(counters["updated_cells"], 3)

    def test_empty_file_cell_never_erases_a_value(self):
        state = self.state(("M", "Homme", "9", "Saisi", "9"))
        csv_rows = self.file_rows(("M", "Homme", None, None, None))
        rows, counters, _, _ = NS["plan_table"](self.TABLE, state, csv_rows, True, TECH)
        self.assertEqual((rows[0]["cd_emp_sexe_adp"], counters["updated_cells"]), ("9", 0))

    def test_ts_part_of_existing_row_is_not_touched(self):
        columns = HRIS.model.LOOKUP_TABLES["lu_hr_emp_titre"]
        spec = HRIS.reference_api.CATALOGUE["lu_hr_emp_titre"]
        state = [dict({c: None for c in columns + HRIS.model.LOOKUP_TECH_COLUMNS}, cd_emp_titre_ts="M", ds_emp_titre_ts="Monsieur API")]
        row = {c: None for c in columns}
        row.update(cd_emp_titre_ts="M", ds_emp_titre_ts="Monsieur fichier", cd_emp_titre_adp="MR")
        rows, counters, errors, _ = NS["plan_table"]("lu_hr_emp_titre", state, [row], False, TECH)
        self.assertEqual(errors, [])
        self.assertIn("ds_emp_titre_ts", spec.ts_columns)
        self.assertEqual((rows[0]["ds_emp_titre_ts"], rows[0]["cd_emp_titre_adp"]), ("Monsieur API", "MR"))

    def test_second_import_changes_nothing(self):
        csv_rows = self.file_rows(("M", "Homme", "1", "Masculin", "1"), ("F", "Femme", "2", "Feminin", "2"))
        rows, first, _, _ = NS["plan_table"](self.TABLE, [], csv_rows, False, TECH)
        again, second, errors, _ = NS["plan_table"](self.TABLE, rows, csv_rows, False, TECH)
        self.assertEqual(errors, [])
        self.assertEqual((first["inserted"], second["inserted"], second["updated_cells"]), (2, 0, 0))
        self.assertEqual(len(again), 2)


class WholeModel(unittest.TestCase):
    def test_every_table_imports_and_stays_compatible_with_the_synchronisation(self):
        """Une ligne par table, valeurs ADP renseignées : l'import ajoute la ligne et la synchronisation TalentSoft
        (merge_catalogue) accepte ensuite la table sans la sauter ni toucher aux colonnes ADP."""
        for file_name, table in NS["FILE_TO_TABLE"].items():
            columns = HRIS.model.LOOKUP_TABLES[table]
            spec = HRIS.reference_api.CATALOGUE[table]
            business = HRIS.reference_api.business_columns(table)
            row = [("CODE1" if c == spec.code else ("v-" + c if c in business else "t-" + c)) for c in columns]
            text = csv_text(table, [row], delimiter=";", quoted=False)
            header, raw, _ = NS["parse_csv"](text)
            csv_rows, errors, _ = NS["table_rows_from_csv"](table, header, raw)
            self.assertEqual(errors, [], file_name)
            unique, conflicts, *_ = NS["dedupe_by_code"](table, csv_rows, "ERROR")
            self.assertEqual(conflicts, [], file_name)
            rows, counters, plan_errors, _ = NS["plan_table"](table, [], unique, False, TECH)
            self.assertEqual((plan_errors, counters["inserted"]), ([], 1), file_name)
            merged, sync = HRIS.reference_api.merge_catalogue(spec, rows, {"CODE1": {"label": "Libellé API"}, "CODE2": {}},
                                                              ["CODE3"], TECH)
            self.assertEqual(sync.status, "OK", file_name)
            kept = next(r for r in merged if r[spec.code] == "CODE1")
            for column in business:
                self.assertEqual(kept[column], "v-" + column, f"{file_name} {column}")
            self.assertEqual(sync.inserted, 2, file_name)      # CODE2 (référentiel) et CODE3 (extraction)


if __name__ == "__main__":
    unittest.main(verbosity=2)
