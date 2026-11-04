"""Tests locaux de T6_NB_HRIS_NOTIFY (fonctions de mise en forme du message de fin de run).

Ce fichier n'est PAS un item Fabric : il se lance sur le poste, sans Spark ni réseau, avec la seule bibliothèque
standard :  python -I LOCAL_TESTS/test_notify_html.py

Principe : le code testé est celui du notebook (cellules de constantes et de fonctions extraites du fichier
notebook-content.py) ; le module hris.common est celui de la bibliothèque T6_NB_HRIS_LIB (source embarquée, empreinte
non revérifiée ici : voir le contrôle des empreintes du dépôt). Spark est remplacé par de faux objets.
"""
from __future__ import annotations

import html
import json
import re
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "QUOTIDIEN" / "NB_HRIS_LIB.Notebook" / "notebook-content.py"
NOTIFY = ROOT / "QUOTIDIEN" / "NB_HRIS_NOTIFY.Notebook" / "notebook-content.py"

CELL_SPLIT = re.compile(r"^# (?:CELL|PARAMETERS CELL) \*+\s*$", re.M)
META_SPLIT = re.compile(r"^# (?:METADATA|MARKDOWN) \*+\s*$", re.M)
MODULE_CALL = re.compile(r"register_hris_module\('([^']+)',\s*'[0-9a-f]{64}',\s*r'''(.*?)'''\)", re.S)


def load_notebook_cells(path: Path) -> List[str]:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    return [META_SPLIT.split(cell)[0].strip("\n") for cell in CELL_SPLIT.split(text)[1:]]


def load_library_module(name: str) -> types.ModuleType:
    text = LIB.read_text(encoding="utf-8").replace("\r\n", "\n")
    for module_name, source in MODULE_CALL.findall(text):
        if module_name == name:
            module = types.ModuleType(name)
            sys.modules[name] = module
            exec(compile(source, name, "exec"), module.__dict__)
            return module
    raise KeyError(name)


class FakeColumn:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: Any):  # noqa: D105 - sert seulement à transporter le filtre
        return ("eq", self.name, other)

    def __ge__(self, other: Any):
        return ("ge", self.name, other)

    def desc(self):
        return ("desc", self.name)

    __hash__ = None  # type: ignore


class FakeFunctions:
    @staticmethod
    def col(name: str) -> FakeColumn:
        return FakeColumn(name)

    @staticmethod
    def lit(value: Any) -> Any:
        return value


class FakeRow:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def asDict(self) -> Dict[str, Any]:
        return dict(self._data)


class FakeFrame:
    def __init__(self, rows: List[Dict[str, Any]]):
        self.rows, self.filters, self.columns = rows, [], None

    def where(self, condition: Any) -> "FakeFrame":
        self.filters.append(condition)
        if condition[0] == "ge":                                  # seule comparaison appliquée ; « eq » est seulement notée
            self.rows = [r for r in self.rows if r.get(condition[1]) >= condition[2]]
        return self

    def orderBy(self, condition: Any) -> "FakeFrame":  # noqa: N802 - nom de l'API Spark
        self.rows = sorted(self.rows, key=lambda r: r.get(condition[1]), reverse=condition[0] == "desc")
        return self

    def limit(self, count: int) -> "FakeFrame":
        self.rows = self.rows[:count]
        return self

    def select(self, *columns: str) -> "FakeFrame":
        self.columns = columns
        return self

    def collect(self) -> List[FakeRow]:
        return [FakeRow({c: r.get(c) for c in self.columns}) for r in self.rows]


class FakeSpark:
    def __init__(self, tables: Dict[str, List[Dict[str, Any]]]):
        self.tables, self.opened = tables, []

    def table(self, name: str) -> FakeFrame:
        self.opened.append(name)
        return FakeFrame(self.tables[name])


def build_namespace() -> Dict[str, Any]:
    common = load_library_module("hris.common")
    spark_io = types.SimpleNamespace(check_run_id=common_check_run_id)
    hris = types.SimpleNamespace(common=common, spark_io=spark_io)
    namespace: Dict[str, Any] = {"hris": hris, "F": FakeFunctions, "html": html, "json": json, "re": re,
                                 "timedelta": timedelta, "Any": Any, "Dict": Dict, "List": List, "Tuple": Tuple}
    for cell in load_notebook_cells(NOTIFY):
        first = next((ln for ln in cell.split("\n") if ln.strip()), "")
        if first.startswith("def ") or first.startswith("# ============= CONSTANTES"):
            exec(compile(cell, "NB_HRIS_NOTIFY", "exec"), namespace)
    return namespace


def common_check_run_id(run_id: str) -> str:
    if not run_id or not re.match(r"^[0-9a-fA-F-]{8,64}$", run_id):
        raise ValueError("run_id invalide")
    return run_id


RUN_ID = "1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed"
RUN = {"run_id": RUN_ID, "run_mode": "NORMAL", "business_date": "2026-11-04", "status": "SUCCEEDED", "error_code": None,
       "error_summary": None, "started_at_utc": "2026-11-04 06:00:03", "ended_at_utc": "2026-11-04 06:41:19"}
STEPS = [
    {"step_code": "ACQUIRE", "attempt_no": 1, "status": "SUCCEEDED", "rows_out": 12345, "message": "ok",
     "started_at_utc": "2026-11-04 06:00:10"},
    {"step_code": "TRANSFORM", "attempt_no": 1, "status": "FAILED", "rows_out": None, "message": "erreur 123456789",
     "started_at_utc": "2026-11-04 06:20:00"},
    {"step_code": "TRANSFORM", "attempt_no": 2, "status": "SUCCEEDED", "rows_out": 12000, "message": "",
     "started_at_utc": "2026-11-04 06:25:00"},
    {"step_code": "PUBLISH", "attempt_no": 1, "status": "SKIPPED", "rows_out": None, "message": "désactivé",
     "started_at_utc": "2026-11-04 06:30:00"},
]
GREEN, RED, AMBER = "#d1fadf", "#fee4e2", "#fef0c7"


class NotifyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ns = build_namespace()

    def render(self, status: str, steps=None, anomalies=None, env: str = "DEV", limits=(40, 15)) -> str:
        run = dict(RUN, status=status)
        return self.ns["build_notification_html"](run, STEPS if steps is None else steps, anomalies or [], env, *limits)

    # --- catégories et couleurs -------------------------------------------------------------------------------
    def test_run_status_categories(self) -> None:
        classify = self.ns["classify_run_status"]
        for status in ("SUCCEEDED", "PUBLISHED"):
            self.assertEqual(classify(status), "SUCCESS", status)
        for status in ("PUBLISHED_PARTIAL", "NOT_PUBLISHED_NON_PRD", "NOT_PUBLISHED_DISABLED", "DRY_RUN",
                       "SKIPPED_CONCURRENT"):
            self.assertEqual(classify(status), "WARNING", status)
        for status in ("FAILED", "BLOCKED", "STARTED", "ACQUIRED", "COMPARED", "", None, "INCONNU"):
            self.assertEqual(classify(status), "FAILURE", status)

    def test_banner_colors_follow_status(self) -> None:
        self.assertIn(f'bgcolor="{GREEN}"', self.render("SUCCEEDED"))
        self.assertIn(f'bgcolor="{RED}"', self.render("FAILED"))
        self.assertIn(f'bgcolor="{RED}"', self.render("BLOCKED"))
        self.assertIn(f'bgcolor="{AMBER}"', self.render("DRY_RUN"))
        self.assertNotIn(f'bgcolor="{RED}"', self.render("SUCCEEDED"))

    def test_status_is_never_conveyed_by_color_alone(self) -> None:
        for status, label in (("SUCCEEDED", "SUCCÈS"), ("FAILED", "ÉCHEC"), ("BLOCKED", "BLOQUÉ"),
                              ("DRY_RUN", "LECTURE SEULE")):
            self.assertIn(f"Run HRIS : {label}", self.render(status))

    def test_step_badges_use_latest_attempt(self) -> None:
        body = self.render("SUCCEEDED")
        self.assertEqual(body.count("TRANSFORM"), 1)                 # une seule ligne : dernière tentative
        self.assertIn("SUCCÈS", body)
        self.assertIn("IGNORÉE", body)
        self.assertNotIn("ÉCHEC", body.split("Étapes")[1])           # la tentative 1 en échec est écartée
        failed = self.render("FAILED", steps=[dict(STEPS[1], attempt_no=3)])
        self.assertIn(f"color:#b42318;background-color:{RED}", failed)

    def test_interrupted_step_is_red(self) -> None:
        body = self.render("FAILED", steps=[{"step_code": "ACQUIRE", "attempt_no": 1, "status": "STARTED"}])
        self.assertIn("INTERROMPUE", body)

    # --- sécurité du contenu ------------------------------------------------------------------------------------
    def test_html_injection_is_escaped(self) -> None:
        evil = [{"step_code": "<script>alert(1)</script>", "attempt_no": 1, "status": "FAILED",
                 "message": '<img src=x onerror="steal()"> & "quoted"', "started_at_utc": "2026-11-04 06:00:00"}]
        body = self.render("FAILED", steps=evil)
        self.assertNotIn("<script", body)
        self.assertNotIn("<img", body)
        self.assertIn("&lt;script&gt;", body)
        self.assertIn("&amp;", body)

    def test_free_text_is_masked_but_dates_and_ids_stay_readable(self) -> None:
        body = self.render("FAILED", steps=[dict(STEPS[1], message="matricule 123456789 mail a.b@x.fr https://h/p?q=1")])
        self.assertNotIn("123456789", body)
        self.assertNotIn("a.b@x.fr", body)
        self.assertNotIn("https://h/p", body)
        self.assertIn("2026-11-04", body)                            # date métier lisible
        self.assertIn(RUN_ID, body)                                  # identifiant du run lisible
        self.assertIn("12345", self.render("SUCCEEDED"))             # compteur de lignes lisible (>= 4 chiffres)

    def test_anomalies_listed_only_when_failing(self) -> None:
        anomalies = [{"control_code": "C_BLOCK", "severity": "BLOCKING", "outcome": "FAIL", "message": "m1"},
                     {"control_code": "C_WARN", "severity": "WARNING", "outcome": "FAIL", "message": "m2"},
                     {"control_code": "C_OK", "severity": "BLOCKING", "outcome": "PASS", "message": "m3"}]
        body = self.render("BLOCKED", anomalies=anomalies)
        self.assertIn("C_BLOCK", body)
        self.assertIn("BLOQUANT", body)
        self.assertIn("C_WARN", body)
        self.assertIn("À SURVEILLER", body)
        self.assertNotIn("C_OK", body)

    # --- taille, sujet, destinataires -------------------------------------------------------------------------
    def test_fit_html_reduces_rows_to_stay_under_limit(self) -> None:
        many = [{"step_code": f"STEP_{i:03d}", "attempt_no": 1, "status": "SUCCEEDED", "rows_out": i,
                 "message": "x" * 300, "started_at_utc": f"2026-11-04 06:{i // 60:02d}:{i % 60:02d}"} for i in range(400)]
        default_limit = self.ns["MAX_HTML_BYTES"]
        body, level = self.ns["fit_html"](dict(RUN), many, [], "DEV")
        self.assertLessEqual(len(body.encode("utf-8")), default_limit)
        self.assertEqual(level, 0)                                   # 40 étapes affichées suffisent sous 40 000 octets
        try:
            self.ns["MAX_HTML_BYTES"] = 6000                         # limite resserrée : la réduction doit s'enclencher
            body, level = self.ns["fit_html"](dict(RUN), many, [], "DEV")
            self.assertLessEqual(len(body.encode("utf-8")), 6000)
            self.assertGreaterEqual(level, 1)
            self.assertIn("Run HRIS", body)
            self.ns["MAX_HTML_BYTES"] = 500                          # limite impossible : bandeau seul, sans plantage
            body, level = self.ns["fit_html"](dict(RUN), many, [], "DEV")
            self.assertEqual(level, len(self.ns["ROW_LIMITS"]) - 1)
            self.assertNotIn("STEP_000", body)
            self.assertIn("Run HRIS", body)
        finally:
            self.ns["MAX_HTML_BYTES"] = default_limit

    def test_subject_is_single_line_and_bounded(self) -> None:
        subject = self.ns["build_subject"]("[HRIS]\r\nBcc: x", "PRD", dict(RUN, status="FAILED", business_date="2026-11-04"))
        self.assertNotIn("\n", subject)
        self.assertNotIn("\r", subject)
        self.assertLessEqual(len(subject), 120)
        self.assertIn("ÉCHEC", subject)
        self.assertEqual(self.ns["build_subject"]("[HRIS]", "DEV", dict(RUN, status="WEIRD")).split(" ")[2], "INCOMPLET")

    def test_environment_tag_follows_the_real_workspace_name(self) -> None:
        tag = self.ns["environment_tag"]
        self.assertEqual(tag("DEV", "HRIS-DEV"), "DEV")
        self.assertEqual(tag("UAT", "HRIS-UAT"), "UAT")
        self.assertEqual(tag("DEV", "HRIS-PRD"), "PRD")          # jeu de valeurs non activé : le nom du workspace fait foi
        self.assertEqual(tag("PRD", "HRIS-PRD"), "PRD")
        self.assertEqual(tag("PRD", "HRIS-UAT"), "HORS-PRD")     # jamais présenté comme la production sans PRD dans le nom
        self.assertEqual(tag("PRD", "hris-prd"), "HORS-PRD")     # la garde ADP est sensible à la casse : l'étiquette aussi
        self.assertEqual(tag(None, None), "HORS-PRD")
        self.assertEqual(tag("", "HRIS-DEV"), "HORS-PRD")
        self.assertEqual(tag("int", "HRIS-INT"), "INT")
        self.assertEqual(tag("DEV]\r\nBcc: x", "HRIS-DEV"), "DEVBCCX")   # une seule ligne, aucun caractère de contrôle

    def test_subject_begins_with_the_environment_tag(self) -> None:
        for env in ("DEV", "UAT", "PRD"):
            subject = self.ns["build_subject"]("[HRIS]", env, dict(RUN, status="SUCCEEDED"))
            self.assertTrue(subject.startswith(f"[{env}] [HRIS] "), subject)
            self.assertIn("2026-11-04", subject)
        long_prefix = "[HRIS]" + "x" * 200
        long_subject = self.ns["build_subject"](long_prefix, "PRD", RUN)
        self.assertTrue(long_subject.startswith("[PRD] "))
        self.assertLessEqual(len(long_subject), 120)

    def test_recipients_parsing(self) -> None:
        parse = self.ns["parse_recipients"]
        self.assertEqual(parse("a@x.fr; b@y.com , nope, c@@z.fr"), ["a@x.fr", "b@y.com"])
        self.assertEqual(parse(""), [])
        self.assertEqual(parse(None), [])

    # --- lectures (API DataFrame, avec faux Spark) ------------------------------------------------------------
    def test_reads_use_dataframe_api_and_validate_run_id(self) -> None:
        tables = lambda logical: "`ws`.`lh`." + logical  # noqa: E731
        spark = FakeSpark({"`ws`.`lh`.ctl.run_step": STEPS, "`ws`.`lh`.log.anomaly": [
            {"control_code": "C", "severity": "BLOCKING", "outcome": "FAIL", "message": "m"}]})
        steps = self.ns["read_steps"](spark, tables, RUN_ID)
        self.assertEqual(len(steps), 4)
        self.assertEqual(set(steps[0]), {"step_code", "attempt_no", "status", "rows_out", "message", "started_at_utc",
                                         "ended_at_utc"})
        self.assertEqual(len(self.ns["read_anomalies"](spark, tables, RUN_ID)), 1)
        with self.assertRaises(ValueError):
            self.ns["read_steps"](spark, tables, "x'; DROP TABLE y; --")

    def test_find_latest_run_id_uses_start_time_with_margin(self) -> None:
        tables = lambda logical: "`ws`.`lh`." + logical  # noqa: E731
        runs = [{"run_id": "run-old", "started_at_utc": datetime(2026, 11, 4, 5, 0, 0)},
                {"run_id": "run-hub-a", "started_at_utc": datetime(2026, 11, 4, 5, 58, 0)},
                {"run_id": "run-hub-b", "started_at_utc": datetime(2026, 11, 4, 6, 0, 30)},
                {"run_id": "run-yesterday", "started_at_utc": datetime(2026, 11, 3, 6, 0, 0)}]
        find = self.ns["find_latest_run_id"]
        spark = FakeSpark({"`ws`.`lh`.ctl.run": runs})
        # déclenchement à 06:02 UTC : marge de 5 min => seuil 05:57 ; run-old et run-yesterday sont écartés,
        # le plus récent des deux restants est retenu ; format Fabric à 7 décimales et suffixe Z acceptés
        self.assertEqual(find(spark, tables, "2026-11-04T06:02:00.1234567Z"), "run-hub-b")
        self.assertEqual(spark.opened, ["`ws`.`lh`.ctl.run"])
        with self.assertRaises(self.ns["hris"].common.HrisError):
            find(FakeSpark({"`ws`.`lh`.ctl.run": runs}), tables, "2026-11-05T06:00:00Z")
        with self.assertRaises(self.ns["hris"].common.HrisError):
            find(FakeSpark({"`ws`.`lh`.ctl.run": runs}), tables, "")

    def test_notebook_has_no_embedded_sql_and_no_deprecated_alias(self) -> None:
        source = NOTIFY.read_text(encoding="utf-8")
        self.assertNotIn("mssparkutils", source)
        # seule dérogation du projet : CREATE SCHEMA, dans la fonction partagée de la bibliothèque, pas dans ce notebook
        self.assertNotIn("spark.sql(", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
