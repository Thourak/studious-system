# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# **Nom de l'objet**
# # T3_NB_HRIS_TESTS. C'est le nom affiché dans Microsoft Fabric pour le notebook de tests du hub d'intégration des ressources humaines ; le dossier du dépôt s'appelle NB_HRIS_TESTS.Notebook.
# # **Type d'objet**
# # Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Il n'est pas appelé par le pipeline : on le lance à la main avant une mise en service ou après une modification du code.
# # **Chemin dans le dépôt**
# # TEST_MOTUL/TST_4/NB_HRIS_TESTS.Notebook/notebook-content.py
# # **Description fonctionnelle**
# # Ce notebook vérifie le code du hub sur des données entièrement fictives. Il exécute les tests unitaires des règles de transformation, de l'acquisition TalentSoft contre un faux serveur, de la publication vers l'outil de paie ADP avec des clients simulés et du journal des cellules. Il vérifie aussi que la transformation Spark donne les mêmes résultats que l'implémentation de référence des règles de l'existant. Il peut enfin dérouler des scénarios d'intégration complets dans un schéma isolé du Lakehouse, sans jamais lire ni écrire une table réelle. Il apporte la preuve, dans le contexte réel du workspace, que le code se comporte comme attendu avant de traiter des données de salariés.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB, puis enregistre onze modules propres aux tests : le simulateur de l'outil de paie, le jeu de données fictif et neuf modules de tests. Ces modules restent dans ce notebook pour qu'aucun simulateur ne soit jamais présent dans le code de production. Le Lakehouse désigné par le paramètre p_lakehouse_name reçoit, dans le schéma isolé hris_tst, des copies de test de toutes les tables, recréées à chaque campagne, ainsi que la table de rapport zz_test_report. Le contexte d'exécution de Fabric fournit le nom réel du workspace, indispensable aux tests du garde-fou de publication.
# # **Fonctionnement et logique de traitement**
# # Après le chargement de la bibliothèque et des modules de test, la cellule de configuration relève d'abord les réglages par défaut de la session Spark, avant toute modification, car ils font partie des vérifications de plateforme. Elle résout ensuite les noms de tables du schéma de test, ouvre le journal des cellules, applique les réglages de session requis par la transformation et transmet le contexte aux tests d'intégration. La cellule d'exécution construit la liste des tests, éventuellement filtrée, la lance et affiche le compte rendu, les versions des bibliothèques et les réglages relevés. La cellule de rapport écrit ce compte rendu, sans aucune donnée de salarié, dans la table zz_test_report du schéma de test. La clôture écrit les traces du journal puis renvoie un résumé au format texte structuré.
# # **Paramètres**
# # Le paramètre p_test_schema désigne le schéma isolé ; sa valeur par défaut est hris_tst et il ne doit jamais être vide, sous peine d'écrire dans les tables réelles. Le paramètre p_lakehouse_name donne le nom du Lakehouse cible, en pratique la variable lakehouse_name de la bibliothèque de variables VL_HRIS ; il est obligatoire puisqu'aucun Lakehouse par défaut n'est attaché. Le paramètre p_run_integration vaut true par défaut et active les scénarios d'intégration avec écriture dans le schéma de test ; à false, seuls les tests sans écriture de table sont lancés. Le paramètre p_only, vide par défaut, restreint la campagne aux tests dont l'identifiant contient l'un des fragments donnés, séparés par des virgules. Le paramètre p_log_cell_success vaut true par défaut et active les traces de réussite des cellules ; les échecs sont toujours tracés.
# # **Sorties produites**
# # Le notebook ajoute une ligne de compte rendu dans la table zz_test_report du schéma de test et des traces dans la copie de test du journal des cellules. Sa valeur de sortie est un texte structuré qui contient le nombre de tests exécutés, la liste des échecs, des erreurs et des tests ignorés, les informations de plateforme et, au plus, dix extraits de traces d'échec. Aucune de ces sorties ne contient de donnée réelle de salarié.
# # **Limitations connues et points d'attention**
# # Les tables du schéma de test sont supprimées puis recréées à chaque campagne d'intégration, y compris la copie de test du journal des cellules : ce schéma ne doit contenir aucune donnée à conserver. Le test de cohérence des marqueurs du contrat de publication est ignoré dans Fabric parce que son document de référence n'est pas présent dans le workspace ; il reste exécuté en local. Une campagne complète dure plusieurs minutes, surtout à cause des écritures Delta des scénarios d'intégration.
# # **Responsable et contact**
# # Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance reste à confirmer par l'équipe en charge des ressources humaines.

# CELL ********************

%run T4_NB_HRIS_LIB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE hris.adp_sim =============
register_hris_module('hris.adp_sim', '3698bf9dff53d20f0d9239ce70b866825d7ce1e3a76904e0d70a12ee6fa387d8', r'''"""hris.adp_sim — simulateur ADP ISOLÉ et dépôts en mémoire, réservés aux tests.

Aucun accès réseau : le simulateur répond en mémoire. Il ne peut pas activer le transport réel
(hris.adp_transport.AdpHttpTransport refuse sa construction tant que l'activation n'est pas
approuvée dans le code). Données exclusivement fictives.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from hris.adp_transport import (ACCEPTED, NOT_TRANSMITTED, REJECTED, UNCERTAIN, TransportResult,
                                classify_response, digest)
from hris.adp_ops import OP_OPEN_UNCERTAIN
from hris.adp_port import ReadResponse


class Resp(ReadResponse):
    def __init__(self, status_code: int, body: Any = None, reason: str = ""):
        super().__init__(status_code, body if isinstance(body, str) else json.dumps(body),
                         reason or {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Server Error"}.get(status_code, ""))
        self._body = body

    def json(self):
        if isinstance(self._body, str):
            return json.loads(self._body)
        return copy.deepcopy(self._body)


class FakeAdp:
    """État ADP fictif (workers indexés par id_payroll) ; applique les POST acceptés.
    `post_script` : liste de comportements consommés à chaque POST, ex. 'ok', ('status', 500),
    'timeout_after_send', 'connect_refused', 'send_interrupted', ('redirect', 302), ('2xx_text', 200)."""

    def __init__(self, workers: Optional[Dict[str, dict]] = None, insee: Optional[Dict[str, list]] = None,
                 get_status: Optional[Callable[[str], Optional[int]]] = None):
        self.workers = copy.deepcopy(workers or {})
        self.insee_db = insee or {}
        self.post_script: List[Any] = []
        self.posts: List[Tuple[str, dict]] = []
        self.gets: List[str] = []
        self.insee_calls: List[str] = []
        self.get_status = get_status

    # -- lecture -----------------------------------------------------------------------------
    def get(self, path: str) -> Resp:
        self.gets.append(path)
        if self.get_status:
            forced = self.get_status(path)
            if forced:
                return Resp(forced, {"error": "simulated"})
        m = re.match(r"^hr/v2/workers/([^/?]+)$", path)
        if m:
            w = self.workers.get(m.group(1))
            return Resp(200, {"workers": [w]}) if w else Resp(404, {"workers": []})
        m = re.match(r"^hr/v2/workers\?\$filter=workers/workerID/idValue eq '(.*)'$", path)
        if m:
            w = self.workers.get(m.group(1))
            return Resp(200, {"workers": [w]}) if w else Resp(204, "")
        m = re.match(r"^hr/v2/workers\?\$filter=.*%27(.*)%27$", path)
        if m:
            ssn = m.group(1)
            for w in self.workers.values():
                docs = w.get("person", {}).get("identityDocuments", [])
                if docs and docs[0].get("documentID") == ssn:
                    return Resp(200, {"workers": [w]})
            return Resp(204, "")
        m = re.match(r"^payroll/v2/workers/([^/]+)/pay-distributions$", path)
        if m:
            for w in self.workers.values():
                if w.get("associateOID") == m.group(1):
                    return Resp(200, {"payDistributions": [w.get("_pay", {"distributionInstructions": [{}]})]})
            return Resp(404, {})
        return Resp(404, {})

    def insee(self, postal_code: str) -> Resp:
        self.insee_calls.append(postal_code)
        return Resp(200, self.insee_db.get(postal_code, []))

    # -- publication -------------------------------------------------------------------------
    def post(self, path: str, body: bytes) -> TransportResult:
        behaviour = self.post_script.pop(0) if self.post_script else "ok"
        payload = json.loads(body.decode("utf-8"))
        if behaviour == "connect_refused":
            return TransportResult(NOT_TRANSMITTED, "CONNECT_FAILED", "CONNECT", error_class="ConnectionRefusedError")
        if behaviour == "send_interrupted":
            return TransportResult(UNCERTAIN, "SEND_INTERRUPTED", "SEND", error_class="BrokenPipeError")
        self.posts.append((path, payload))
        if behaviour == "timeout_after_send":
            self._apply(path, payload)  # le serveur a traité, la réponse est perdue
            return TransportResult(UNCERTAIN, "TIMEOUT_AFTER_SEND", "RECEIVE", error_class="timeout")
        if isinstance(behaviour, tuple) and behaviour[0] == "redirect":
            status, text = behaviour[1], ""
        elif isinstance(behaviour, tuple) and behaviour[0] == "status":
            status, text = behaviour[1], json.dumps({"confirmMessage": {"processMessages": [
                {"developerMessage": {"messageTxt": "simulated"}}]}}) if 400 <= behaviour[1] < 500 else "{}"
        elif isinstance(behaviour, tuple) and behaviour[0] == "2xx_text":
            status, text = behaviour[1], "OK-not-json"
        else:
            status, text = 200, json.dumps({"events": [{"eventStatusCode": {"codeValue": "complete"}}]})
        outcome, reason = classify_response(status, text)
        if outcome == ACCEPTED or behaviour == "apply_then_500":
            self._apply(path, payload)
        return TransportResult(outcome, reason, "DONE", http_status=status, http_reason="SIM",
                               response_text=text, body_digest=digest(text))

    def _apply(self, path: str, payload: dict) -> None:
        ev = (payload.get("events") or [{}])[0].get("data")
        if not isinstance(ev, dict):
            return  # charge utile de test sans structure ADP : rien à appliquer
        if path.endswith("worker.hire"):
            w = copy.deepcopy(ev["transform"]["worker"])
            wid = w["workerID"]["idValue"]
            w["associateOID"] = f"AOID-{wid}"
            w.setdefault("workAssignments", [w.pop("workAssignment", {})])
            self.workers[wid] = w
            return
        ctx = ev.get("eventContext", {})
        aoid = ctx.get("associateOID") or ctx.get("worker", {}).get("associateOID")
        worker = next((w for w in self.workers.values() if w.get("associateOID") == aoid), None)
        if worker is None:
            return
        t = ev["transform"]
        if "workAssignment" in t:
            wa = worker.setdefault("workAssignments", [{}])[0]
            for k, v in t["workAssignment"].items():
                if k == "customFieldGroup":
                    cfg = wa.setdefault("customFieldGroup", {})
                    for group, items in v.items():
                        existing = {i.get("itemID"): i for i in cfg.get(group, [])}
                        for i in items:
                            existing[i.get("itemID")] = i
                        cfg[group] = list(existing.values())
                else:
                    wa[k] = v
        elif "payDistribution" in t:
            worker["_pay"] = t["payDistribution"]
        elif "worker" in t:
            _deep_merge(worker, t["worker"])


def _deep_merge(dst: dict, src: dict) -> None:
    for k, v in src.items():
        if k == "communication" and isinstance(v, dict):
            comm = dst.setdefault("communication", {})
            if "email" in v:
                comm["emails"] = [v["email"]]
            if "mobile" in v:
                comm["mobiles"] = [v["mobile"]]
            continue
        if k == "businessCommunication":
            bc = dst.setdefault("businessCommunication", {})
            if "email" in v:
                bc["emails"] = [v["email"]]
            if "mobile" in v:
                bc["mobiles"] = [v["mobile"]]
            continue
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = copy.deepcopy(v)


# ---------------------------------------------------------------------------
# Dépôts en mémoire (mêmes contrats que les dépôts Spark)
# ---------------------------------------------------------------------------
class MemOps:
    def __init__(self, fail_on_upsert_status: Optional[str] = None):
        self.rows: Dict[str, dict] = {}
        self.fail_on_upsert_status = fail_on_upsert_status
        self.writes: List[str] = []

    def get(self, op_id):
        r = self.rows.get(op_id)
        return dict(r) if r else None

    def open_uncertain_for_employee(self, key):
        return [dict(r) for r in self.rows.values() if r["employee_key"] == key and r["status"] in OP_OPEN_UNCERTAIN]

    def for_item(self, run_id, key):
        return [dict(r) for r in self.rows.values() if r["run_id"] == run_id and r["employee_key"] == key]

    def upsert(self, op):
        if self.fail_on_upsert_status and op["status"] == self.fail_on_upsert_status:
            raise IOError("simulated durable write failure")
        self.rows[op["op_id"]] = dict(op)
        self.writes.append(op["status"])


class MemAttempts:
    def __init__(self):
        self.rows: List[dict] = []

    def append(self, a):
        self.rows.append(dict(a))


class MemLog:
    def __init__(self):
        self.rows: List[dict] = []
        self.decisions: List[tuple] = []

    def append(self, rows):
        self.rows.extend(rows)

    def record(self, run_id, decision):
        self.decisions.append((run_id, decision))


class MemOutbox:
    """Outbox multi-runs ; `run_order` donne l'ordre de promotion des runs (plus récent = plus grand)."""

    def __init__(self, run_order: Optional[Dict[str, int]] = None):
        self.items: Dict[Tuple[str, str], dict] = {}
        self.run_order = run_order or {}

    def add(self, run_id, key, status="PENDING", attempt_count=0):
        self.items[(run_id, key)] = {"run_id": run_id, "employee_key": key, "status": status,
                                     "attempt_count": attempt_count, "change_category": "MODIFIED"}

    def load(self, run_id, statuses, max_attempts):
        return [dict(v) for (r, _), v in sorted(self.items.items()) if r == run_id and v["status"] in statuses
                and int(v.get("attempt_count") or 0) < max_attempts]

    def newer_items(self, run_id, key):
        mine = self.run_order.get(run_id, 0)
        return [dict(v) for (r, k), v in self.items.items() if k == key and self.run_order.get(r, 0) > mine]

    def update(self, run_id, updates):
        for u in updates:
            self.items[(run_id, u["employee_key"])].update({k: v for k, v in u.items() if k != "employee_key"})


def fake_context(name, workspace_id="00000000-0000-0000-0000-000000000000"):
    """Contexte d'exécution SIMULÉ (tests isolés uniquement)."""
    return lambda: {"currentWorkspaceName": name, "currentWorkspaceId": workspace_id, "isForPipeline": True,
                    "currentNotebookName": "NB_HRIS_PUBLISH_ADP"}
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE jdd =============
register_hris_module('jdd', '1afe3dd5706f2e1043bdff70d84c4e6085ca490f53bb021d7207cc1f1b9fc307', r'''"""Jeux de données FICTIFS (JDD-01 à JDD-06, plan de tests 04 § 2). Aucune donnée réelle :
noms, numéros et codes sont inventés et non représentatifs."""
from __future__ import annotations

import copy
from typing import Dict, List

from hris.model import LOOKUP_TABLES, RAW_KEYS
from hris.ts_acquire import initialize_employee_fields, to_raw_row

# --- JDD-03 : correspondances fictives (codes inventés) ---------------------------------------
LOOKUPS: Dict[str, List[dict]] = {
    "lu_hr_emp_titre": [{"cd_emp_titre_ts": "MR", "cd_emp_titre_adp": "1"}, {"cd_emp_titre_ts": "MS", "cd_emp_titre_adp": "2"}],
    "lu_hr_emp_sexe": [{"cd_emp_sexe_ts": "M", "cd_emp_sexe_adp": "1"}, {"cd_emp_sexe_ts": "F", "cd_emp_sexe_adp": "2"}],
    "lu_hr_emp_nationalite": [{"cd_emp_nationalite_ts": "FRA", "cd_emp_nationalite_adp": "FR"}],
    "lu_hr_emp_situation_matrimoniale": [{"cd_emp_situation_matrimoniale_ts": "SINGLE",
                                          "cd_emp_situation_matrimoniale_adp": "C"}],
    "lu_hr_emp_pays": [{"cd_emp_pays_ts": "FRA", "id_emp_pays_adp": "FR"}, {"cd_emp_pays_ts": "FR", "id_emp_pays_adp": "FR"}],
    "lu_hr_ppc_raison_debut_contrat": [{"cd_ppc_raison_debut_contrat_ts": "CRE", "cd_ppc_raison_debut_contrat_adp": "01"}],
    "lu_hr_ppc_contrat2": [{"cd_contract_ts": "CDI", "cd_nature_contract_adp": "00", "cd_type_contract_adp": "00",
                            "cd_type_collaboration_adp": "SAL"},
                           {"cd_contract_ts": "CDD", "cd_nature_contract_adp": "01", "cd_type_contract_adp": "01",
                            "cd_type_collaboration_adp": "SAL"}],
    "lu_hr_ppc_classification": [{"cd_convention_collective_lvl2_ts": "C1", "cd_classification_adp": "IC5",
                                  "cd_coefficient_adp": "100", "cd_ppc_convention_collective_adp": "94"}],
    "lu_hr_ppc_classes": [{"cd_occupational_category_ts": "CAD", "cd_categorie_cotisant_adp": "13",
                           "cd_classe_remuneration_adp": "B"}],
    "lu_hr_geo_entite": [{"cd_geo_entite_ts": "Motul France", "cd_geo_entite_adp": "MF"}],
    "lu_hr_ppc_location_country": [{"cd_ppc_location_country_ts": "SITE1", "cd_ppc_location_country_adp": "02003"}],
    "lu_hr_ppc_cost_center": [{"cd_ppc_cost_center_ts": "CC1", "cd_ppc_cost_center_adp": "S0001"}],
    "lu_hr_organisationalstructure": [{"cd_organisationalstructure_ts": "ORG1", "cd_organisationalstructure_adp": "A1B2"}],
    "lu_hr_emp_diplome": [], "lu_hr_emp_work_accident": [], "lu_hr_ppc_convention_collective": [],
    "lu_hr_ppc_contrat": [],
}


def full_lookup_rows(table: str) -> List[dict]:
    return [{c: r.get(c) for c in LOOKUP_TABLES[table]} for r in LOOKUPS[table]]


def ts_record(n: int, **overrides) -> dict:
    """Enregistrement TalentSoft fictif « valide » au format de la Function (avant JSON_VALUE)."""
    rec = initialize_employee_fields(f"E{n:05d}")
    rec.update({
        "LegacyID": f"{n:08d}", "Matricule_IT": f"IT{n:04d}", "LastName": f"NOM{n}", "FirstName": f"Prenom{n}",
        "BirthName": f"NOM{n}", "Title": "MR", "Gender": "M", "Nationality": "FRA",
        "hubSocialSecurityNumber": f"1{n:012d}", "MaritalStatus": "SINGLE", "MaritalStatus_startDate": "2020-01-01",
        "Date_de_naissance": "1990-05-17", "Ville_de_naissance": "Villefictive", "Pays_de_naissance": "FRA",
        "LegalTownOrCity": "Villefictive", "LegalCountry": "FRA", "LegalPostalCode": "99000",
        "LegalStreet": "Rue Imaginaire", "LegalStreetNumber": str(n), "adresse_countryCode": "FR",
        "BusinessEmail": f"user{n}@example.invalid", "PersonalEmail": f"perso{n}@example.invalid",
        "BusinessMobile": "0000000000", "referenceAdmissionDate": "2019-01-07T00:00:00",
        "GroupStartDate": "2019-01-07", "CompanyStartDate": "2019-01-07", "HiringReason": "CRE",
        "date_debut_contract": "2019-01-07", "date_fin_contract": "1900-01-01", "NatureOfContract": "CDI",
        "OccupationalCategory": "CAD", "Situation_starDate": "2024-01-01", "Coefficient": "C1",
        "LegalStructure": "Motul France", "GeographicOrganisationStructure": "SITE1", "PPC_FTE": "100",
        "OrganisationalStructure": "ORG1", "CostCenter": "CC1", "pourcentage_imputation": "100",
        "AccountHolder": f"NOM{n}", "IBANOrABA": "XX00TEST0000000000", "BicOrSwift": "TESTXXXX",
        "BankName": "BANQUE FICTIVE", "salary_base": 48000, "base_startDate": "2024-01-01",
        "Pourcentage_prime_annuelles": 10, "pourcentage_prime_annuelles_startDate": "2024-01-01",
    })
    rec.update(overrides)
    return rec


def raw_rows(records: List[dict]) -> List[dict]:
    return [to_raw_row(r) for r in records]


def deep(x):
    return copy.deepcopy(x)
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_parity_transform =============
register_hris_module('test_parity_transform', 'e167eada2f5f8e55dcc87e26cce157e96de92dc73e14edf0842fa381e67650c2', r'''"""TU-05 / TU-08 — sémantiques T-SQL émulées et règles R-SQL (implémentation de référence).
Données fictives uniquement."""
import datetime as dt
import decimal
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src"), os.path.join(HERE, "..", "fixtures")]

from hris import parity as P  # noqa: E402
from hris import transform_ref as R  # noqa: E402
from hris.model import ADP_COLUMNS, COMPARE_COLUMNS, LOOKUP_JOINS  # noqa: E402
import jdd  # noqa: E402

D = dt.date(2026, 10, 7)


def staged(records):
    src = [R.parse_source_row(r) for r in jdd.raw_rows(records)]
    lookups = {t: jdd.full_lookup_rows(t) for t in jdd.LOOKUPS}
    return R.enrich_rows(src, lookups)


class JsonValueAndTruncation(unittest.TestCase):
    def test_json_value_repr(self):  # R-SQL-002
        self.assertIsNone(P.json_value_repr(None))
        self.assertEqual(P.json_value_repr(True), "true")
        self.assertEqual(P.json_value_repr(48000), "48000")
        self.assertEqual(P.json_value_repr(48000.5), "48000.5")
        self.assertEqual(P.json_value_repr("abc"), "abc")
        self.assertIsNone(P.json_value_repr({"a": 1}))
        self.assertIsNone(P.json_value_repr("x" * 4001))

    def test_nvarchar_50(self):  # R-SQL-002 / D-PAR-03
        self.assertEqual(len(P.nvarchar("é" * 80)), 50)
        self.assertIsNone(P.nvarchar(None))

    def test_coalesce_null_only_and_replace_none(self):  # R-SQL-003
        src = R.parse_source_row({"BusinessMobile": "", "BusinessPhone": "0102", "date_fin_contract": "None"})
        self.assertEqual(src["lb_telephone_professionnel"], "")
        self.assertEqual(src["dt_ppc_fin_contrat"], "")
        src = R.parse_source_row({"BusinessMobile": None, "BusinessPhone": "0102"})
        self.assertEqual(src["lb_telephone_professionnel"], "0102")


class TsqlEmulation(unittest.TestCase):
    def test_trailing_spaces(self):
        self.assertTrue(P.sql_is_empty("   "))
        self.assertFalse(P.sql_is_empty(None))
        self.assertFalse(P.sql_is_not_empty(None))
        self.assertEqual(P.sql_len("12345678  "), 8)
        self.assertTrue(P.sql_eq("ABC  ", "ABC"))
        self.assertFalse(P.sql_eq(" ABC", "ABC"))

    def test_isdate(self):  # R-SQL-043 / D-PAR-01
        for ok in ["2024-01-31", "2024-1-5", "20240131", "2024-01-31T10:20:30", "2024-01-31T10:20:30.123Z",
                   "2024-01-31 10:20", "01/31/2024", " 2024-01-31 "]:
            self.assertEqual(P.isdate(ok), 1, ok)
        for ko in [None, "", "2024-02-30", "2024-13-01", "1700-01-01", "31/01/2024", "abc",
                   "2024-01-31T10:20:30.1234567"]:
            self.assertEqual(P.isdate(ko), 0, ko)

    def test_convert_date(self):  # R-SQL-052
        self.assertEqual(P.convert_date(""), dt.date(1900, 1, 1))
        self.assertEqual(P.convert_date("  "), dt.date(1900, 1, 1))
        self.assertIsNone(P.convert_date(None))
        self.assertEqual(P.convert_date("2024-01-31T00:00:00"), dt.date(2024, 1, 31))
        with self.assertRaises(ValueError):
            P.convert_date("zz")

    def test_isnumeric_and_cast(self):  # R-SQL-035/036/044, D-PAR-02
        for ok in ["10", "-3.5", "+1", "1e5", "$", ".", ",", "1,000"]:
            self.assertEqual(P.isnumeric(ok), 1, ok)
        for ko in [None, "", "abc", "e5", "1-2"]:
            self.assertEqual(P.isnumeric(ko), 0, ko)
        self.assertEqual(P.cast_numeric_12_2("12.345"), decimal.Decimal("12.35"))
        self.assertEqual(P.cast_numeric_12_2("-12.345"), decimal.Decimal("-12.35"))
        for bad in ["1e5", "$", "", "1,000", "99999999999"]:
            with self.assertRaises(ValueError, msg=bad):
                P.cast_numeric_12_2(bad)

    def test_pct_flag(self):
        self.assertEqual(P.pct_flag("abc", 100), 0)      # non numérique : non rejeté (parité)
        self.assertEqual(P.pct_flag("150", 100), 1)
        self.assertEqual(P.pct_flag("100", 100), 0)
        with self.assertRaises(ValueError):
            P.pct_flag("1e5", 100)                       # ISNUMERIC=1 mais CAST échoue : échec du run

    def test_monthly_salary_scale(self):  # R-SQL-054
        v = P.monthly_salary("5000")
        self.assertEqual(v.as_tuple().exponent, -13)
        self.assertEqual(v, decimal.Decimal("416.6666666666667"))

    def test_utf8_truncate(self):  # R-SQL-066
        self.assertEqual(len(P.utf8_truncate("Référence_Rejet," * 5, 50).encode("utf-8")) <= 50, True)


class Rules(unittest.TestCase):
    def test_valid_employee_goes_through(self):
        rows = staged([jdd.ts_record(1)])
        rej = R.reject_rows(rows, D)
        self.assertEqual(rej, [])
        adp = R.adp_rows(rows, rej, D)
        self.assertEqual(len(adp), 1)
        self.assertEqual(list(adp[0].keys()), ADP_COLUMNS)
        self.assertEqual(adp[0]["dt_ppc_fin_contrat"], dt.date(1900, 1, 1))  # 'None' -> '' -> 1900-01-01
        self.assertEqual(adp[0]["mt_salaire_mensuel"], decimal.Decimal("4000.0000000000000"))

    def test_null_reject(self):  # R-SQL-030/031
        rows = staged([jdd.ts_record(2, Matricule_IT="")])
        rej = [r for r in R.reject_rows(rows, D) if r["cd_type_rejet"] == "Null_Rejet"]
        self.assertEqual([(r["lb_nom_colonne_rejet"], r["ds_rejet"]) for r in rej],
                         [("cd_matricule_it", "cd_matricule_it ne peut pas être NULL")])
        rows = staged([jdd.ts_record(3, Matricule_IT=None)])
        self.assertEqual([r for r in R.reject_rows(rows, D) if r["cd_type_rejet"] == "Null_Rejet"], [])

    def test_format_reject_and_fte_excluded_from_sum(self):  # R-SQL-032..039
        rows = staged([jdd.ts_record(4, PPC_FTE="150")])
        self.assertEqual(R.reject_rows(rows, D), [])          # ETP seul hors bornes : aucun rejet (R-SQL-038)
        rows = staged([jdd.ts_record(5, PPC_FTE="150", Date_de_naissance="2024-02-30")])
        rej = R.reject_rows(rows, D)
        self.assertEqual(sorted(r["lb_nom_colonne_rejet"] for r in rej), ["dt_naissance", "pc_ppc_fte"])
        ds = {r["lb_nom_colonne_rejet"]: r["ds_rejet"] for r in rej}
        self.assertEqual(ds["dt_naissance"], "Format Date non respecté au YYYY-MM-DD pour dt_naissance = 2024-02-30")
        rows = staged([jdd.ts_record(6, LegacyID="123")])
        rej = R.reject_rows(rows, D)
        self.assertEqual([(r["lb_nom_colonne_rejet"], r["ds_rejet"]) for r in rej], [("id_payroll", None)])

    def test_reference_reject(self):  # R-SQL-040/041/042
        rows = staged([jdd.ts_record(7, Title="XX", LegalCountry="ZZZ")])
        rej = R.reject_rows(rows, D)
        self.assertEqual([(r["cd_type_rejet"], r["lb_fichier_mapping"], r["ds_rejet"]) for r in rej],
                         [("Référence_Rejet", "titre.csv", "Mapping non trouvé entre TS et ADP pour cd_emp_titre = XX")])
        adp = R.adp_rows(staged([jdd.ts_record(8, LegalCountry="ZZZ")]), [], D)
        self.assertIsNone(adp[0]["id_fiscal_pays_adp"])   # pays fiscal jamais rejeté (R-SQL-042)

    def test_trailing_space_join(self):  # D-PAR-04
        rows = staged([jdd.ts_record(9, Title="MR   ")])
        self.assertEqual(rows[0]["cd_emp_titre_adp"], "1")

    def test_duplicate_mapping_duplicates_rows(self):  # R-SQL-058
        lookups = {t: jdd.full_lookup_rows(t) for t in jdd.LOOKUPS}
        lookups["lu_hr_emp_sexe"] = lookups["lu_hr_emp_sexe"] + [{"cd_emp_sexe_ts": "M", "cd_emp_sexe_adp": "9"}]
        src = [R.parse_source_row(r) for r in jdd.raw_rows([jdd.ts_record(10)])]
        self.assertEqual(len(R.enrich_rows(src, lookups)), 2)

    def test_salary_conversion_failure_fails_run(self):  # D-PAR-07
        rows = staged([jdd.ts_record(11, salary_base="")])
        with self.assertRaises(ValueError):
            R.adp_rows(rows, R.reject_rows(rows, D), D)

    def test_reject_excludes_employee(self):  # R-SQL-050
        rows = staged([jdd.ts_record(12), jdd.ts_record(13, Gender="X")])
        rej = R.reject_rows(rows, D)
        self.assertEqual([r["id_unique"] for r in R.adp_rows(rows, rej, D)], ["E00012"])

    def test_row_hash(self):  # TU-08
        adp = R.adp_rows(staged([jdd.ts_record(14)]), [], D)[0]
        h = R.row_hash(adp)
        self.assertEqual(h, R.row_hash(dict(adp)))
        self.assertNotEqual(h, R.row_hash(dict(adp, lb_email_personnel="")))
        self.assertNotEqual(R.row_hash(dict(adp, lb_email_personnel=None)), R.row_hash(dict(adp, lb_email_personnel="")))
        self.assertEqual(R.row_hash(dict(adp, dt_transforme=dt.date(2000, 1, 1))), h)  # dt_transforme non comparée
        self.assertEqual(len(COMPARE_COLUMNS), 71)

    def test_summary(self):  # R-SQL-060..066
        rej = R.reject_rows(staged([jdd.ts_record(15, Gender="X"), jdd.ts_record(16, Title="Y")]), D)
        s = R.summary(["E00001", "E00002"], rej, D)
        self.assertEqual((s["mt_lignes_inserts"], s["mt_lignes_rejets"], s["mt_lignes_lues"]), (2, 2, 4))
        self.assertEqual(s["pc_lignes_rejets"], decimal.Decimal("0.5000"))
        self.assertTrue(len(s["ds_type_rejets"].encode("utf-8")) <= 50)

    def test_lookup_join_count(self):
        self.assertEqual(len(LOOKUP_JOINS), 15)


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_ts_acquire =============
register_hris_module('test_ts_acquire', 'ced3888886b11350c33c25065075f4cdf7df8efe1aee5826d847843669c6b75a', r'''"""TU-07 — acquisition TalentSoft (JDD-01) contre un faux serveur HTTP en mémoire. Aucun appel réseau."""
import json
import os
import re
import sys
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src"), os.path.join(HERE, "..", "fixtures")]

from hris.common import TS_PRD_BASE_URL, HrisError, SecretValue  # noqa: E402
from hris import ts_acquire as A  # noqa: E402


class FakeResp:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return json.loads(json.dumps(self._body))


def hub(field, value, start, **kw):
    return dict({"field": field, "value": value, "startDate": start}, **kw)


class FakeClock:
    """Horloge monotone pilotable (renouvellement du jeton selon son âge)."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeTalentSoft:
    """Faux TalentSoft : 3 salariés fictifs, dont 1 hors « Motul France ».

    n_employees : génère n salariés fictifs (tous en position P1) à la place des 3 salariés de base.
    latency : durée simulée de chaque appel GET (mesure de la concurrence réelle).
    fail_after : lève une HrisError fatale au n-ième appel GET.
    employees : annuaire fourni tel quel (remplace les salariés de base).
    entities : (matricule, entité hub) -> corps renvoyé, pour imposer par exemple un contrat terminé.
    situations : matricule -> éléments renvoyés par l'endpoint Situation (dans l'ordre fourni).
    positions : code de poste -> LegalStructure renvoyée, ou {date AAAA-MM-JJ: LegalStructure}, ou None (404).
        Un code absent : « Motul France » sauf P3 (« Motul SA »).
    """

    def __init__(self, total_override=None, scripted=None, n_employees=None, latency=0.0, fail_after=None,
                 employees=None, situations=None, positions=None, entities=None):
        self.situations = situations or {}
        self.positions = positions or {}
        self.entities = entities or {}
        self.calls = []
        self.scripted = scripted or {}   # chemin (regex) -> liste de statuts à renvoyer d'abord
        self.total_override = total_override
        self.closed = False
        self.latency = latency
        self.fail_after = fail_after
        self._lock = threading.Lock()
        self._in_flight = 0
        self.max_in_flight = 0
        if employees is not None:
            self.employees = employees
        elif n_employees:
            self.employees = [{"employeeNumber": f"N{i}", "individual": {}} for i in range(n_employees)]
        else:
            self.employees = [
                {"employeeNumber": "E1", "referenceAdmissionDate": "2019-01-07T00:00:00",
                 "individual": {"extras": {"hubSocialSecurityNumber": 1234567890123}, "user": {"username": "u1"}}},
                {"employeeNumber": "E2", "referenceAdmissionDate": "2020-02-01T00:00:00",
                 "individual": {"extras": {}, "user": {}}},
                {"employeeNumber": "E3", "individual": {}},
            ]

    def close(self):
        self.closed = True

    def post(self, url, headers=None, data=None, timeout=None, allow_redirects=None):
        self.calls.append(("POST", url))
        assert allow_redirects is False
        assert "client_secret" in data and data["grant_type"] == "client_credentials"
        return self._scripted(url) or FakeResp(200, {"access_token": "tok-fictif"})

    def _scripted(self, url):
        for pattern, statuses in self.scripted.items():
            if re.search(pattern, url) and statuses:
                st = statuses.pop(0)
                if isinstance(st, Exception):
                    raise st
                return FakeResp(st, {}, {"Retry-After": "0"})
        return None

    def get(self, url, headers=None, timeout=None, allow_redirects=None):
        self.calls.append(("GET", url))
        assert allow_redirects is False
        assert headers["Authorization"].startswith("Bearer ")
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            raise HrisError("TS_FAKE_FATAL")
        with self._lock:
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
        try:
            if self.latency:
                time.sleep(self.latency)
            return self._answer(url)
        finally:
            with self._lock:
                self._in_flight -= 1

    def _answer(self, url):
        s = self._scripted(url)
        if s:
            return s
        path = url[len(TS_PRD_BASE_URL):]
        if path.startswith("/api/v1/directory/employees?count=1&"):   # comptage ; avec un seul salarié, c'est aussi la liste
            return FakeResp(200, {"totalCount": self.total_override or len(self.employees), "results": self.employees[:1]})
        if path.startswith("/api/v1/directory/employees?count="):
            return FakeResp(200, {"totalCount": len(self.employees), "results": self.employees})
        m = re.match(r"/api/hub/v2/employees/(\w+)/(\w+)$", path)
        if m:
            emp, entity = m.groups()
            return FakeResp(200, self.entity(emp, entity))
        m = re.match(r"/api/hub/v2.0/positions/(\w+)\?date=(.*)$", path)
        if m:
            code, date = m.group(1), m.group(2)[:10]
            legal = self.positions.get(code, "Motul France" if code != "P3" else "Motul SA")
            if isinstance(legal, dict):
                legal = legal.get(date)          # poste valable seulement à certaines dates
            if legal is None:
                return FakeResp(404, {})
            return FakeResp(200, [{"field": "LegalStructure", "value": legal}, {"field": "Level", "value": "80 %"},
                                  {"field": "FullTimeEquivalentPercent", "value": 0},
                                  {"field": "CostCenter", "value": "CC1"}])
        if path.endswith("/postal-addresses"):
            return FakeResp(200, [{"postalCode": "99000", "city": "Villefictive", "countryCode": "be"}])
        return FakeResp(404, {})

    def entity(self, emp, entity):
        if (emp, entity) in self.entities:
            return self.entities[(emp, entity)]
        if entity == "Situation" and emp in self.situations:
            return self.situations[emp]
        if entity == "Situation":
            pos = {"E1": "P1", "E2": "P2", "E3": "P3"}.get(emp, "P1")
            return [hub("WorkingType", "Ingénieur", "2023-01-01"), hub("Position", pos, "2023-01-01"),
                    hub("WorkingType", "Ancien", "2020-01-01")]
        if entity == "Identity":   # ordre de l'API : l'item plus ancien APRÈS le plus récent est ignoré
            return [hub("LastName", "NOM", "2022-01-01"), hub("FirstName", "Prénom", "2022-01-01"),
                    hub("LastName", "ANCIEN", "2021-01-01"), hub("Gender", False, "2022-01-01")]
        if entity == "Compensation":
            return [{"key": "X_ANNUAL_BASE_SALARY", "field": "Amount", "value": 45000.5, "startDate": "2024-01-01"},
                    {"key": "X_ANNUAL_BASE_SALARY", "field": "Amount", "value": 40000, "startDate": "2023-01-01"},
                    {"key": "CONTRACTUAL BONUS", "field": "Amount", "value": 0, "startDate": "2024-01-01"}]
        if entity == "Contract":
            return [hub("NatureOfContract", "CDI", "2019-01-07", endDate=None)]
        if entity == "Disability":
            return [hub("RecognizedAsDisabled", True, "2020-01-01")]
        return []


def make(fake, employee_concurrency=None, **kw):
    stats = A.AcquisitionStats()
    client = A.TsClient(TS_PRD_BASE_URL, SecretValue("id-fictif"), SecretValue("secret-fictif"),
                        session_factory=lambda: fake, stats=stats, sleep=lambda s: None, **kw)
    extractor_kw = {} if employee_concurrency is None else {"employee_concurrency": employee_concurrency}
    return A.TalentSoftExtractor(client, **extractor_kw), stats


def gets(fake, suffix):
    return [url for method, url in fake.calls if method == "GET" and url.endswith(suffix)]


def pos_url(code, start):
    return f"{TS_PRD_BASE_URL}/api/hub/v2.0/positions/{code}?date={start}T00:00:00Z"


def position_urls(fake):
    return [url for method, url in fake.calls if method == "GET" and "/positions/" in url]


def run_one(situation, positions=None, entities=None):
    """Extraction d'un seul salarié (S1) dont l'endpoint Situation renvoie `situation`, dans cet ordre."""
    fake = FakeTalentSoft(employees=[{"employeeNumber": "S1", "individual": {}}], situations={"S1": situation},
                          positions=positions or {}, entities=entities)
    ex, stats = make(fake)
    return ex.run(), stats, fake


class SourceIsPrdOnly(unittest.TestCase):
    def test_refuses_any_other_tenant(self):
        for url in ["https://testmotul.talent-soft.com", "http://motul.talent-soft.com", "", None,
                    "https://motul.talent-soft.com.evil.example"]:
            with self.assertRaises(HrisError, msg=url):
                A.assert_prd_source(url)
        self.assertEqual(A.assert_prd_source(TS_PRD_BASE_URL + "/"), TS_PRD_BASE_URL)


class Extraction(unittest.TestCase):
    def test_full_extraction_rules(self):
        fake = FakeTalentSoft()
        ex, stats = make(fake)
        records = ex.run()
        by = {r["employeeNumber"]: r for r in records}
        self.assertEqual(sorted(by), ["E1", "E2"])                      # E3 hors Motul France (R-TS-010)
        self.assertEqual(by["E1"]["LegalStructure"], "Motul France")
        e1 = by["E1"]
        self.assertEqual(e1["LastName"], "NOM")                         # R-TS-018 : item ancien ignoré
        self.assertEqual(e1["Gender"], "")                              # R-TS-020 : falsy -> ''
        self.assertEqual(e1["WorkingType"], "Ingénieur")
        self.assertEqual(e1["salary_base"], 45000.5)                    # R-TS-019 : valeur brute, max strict par clé
        self.assertEqual(e1["prime_annuelles"], 0)
        self.assertEqual(e1["pourcentage_imputation"], "80")            # R-TS-021 : Level sans %
        self.assertEqual(e1["PPC_FTE"], "")                             # 0 falsy
        self.assertEqual(e1["Hors_France"], "1")                        # R-TS-022 : 'be' != FR
        self.assertEqual(e1["date_fin_contract"], "1900-01-01")         # R-TS-024
        self.assertEqual(e1["hubSocialSecurityNumber"], "1234567890123")
        self.assertEqual(by["E2"]["address_postalCode"], "")            # pas d'username : pas d'appel postal
        raw = A.to_raw_row(e1)
        self.assertNotIn("handicap", raw)                               # données de santé non persistées
        self.assertEqual(raw["salary_base"], "45000.5")
        self.assertEqual(len(raw), 70)
        anomalies = {a["control_code"]: a for a in A.completeness_anomalies(stats.as_dict())}
        self.assertEqual(anomalies["EXTRACTION_INCOMPLETE"]["outcome"], "PASS")
        self.assertEqual(anomalies["TS_ENDPOINT_FAILURE"]["outcome"], "PASS")
        self.assertTrue(fake.calls[0][0] == "POST")

    def test_incomplete_directory_is_blocking(self):  # D-QUA-01
        fake = FakeTalentSoft(total_override=5)
        ex, stats = make(fake)
        ex.run()
        a = {x["control_code"]: x for x in A.completeness_anomalies(stats.as_dict())}
        self.assertEqual((a["EXTRACTION_INCOMPLETE"]["severity"], a["EXTRACTION_INCOMPLETE"]["outcome"]),
                         ("BLOCKING", "FAIL"))

    def test_retries_429_then_success_and_exhausted_503_counted(self):  # R-TS-014/015
        fake = FakeTalentSoft(scripted={r"/E1/Identity$": [429, 429], r"/E2/Contract$": [503, 503, 503]})
        ex, stats = make(fake)
        records = {r["employeeNumber"]: r for r in ex.run()}
        self.assertEqual(records["E1"]["LastName"], "NOM")             # 3e essai réussi
        self.assertEqual(records["E2"]["NatureOfContract"], "")         # parité : valeurs par défaut...
        a = {x["control_code"]: x for x in A.completeness_anomalies(stats.as_dict())}
        self.assertEqual(a["TS_ENDPOINT_FAILURE"]["outcome"], "FAIL")   # ... mais l'échec n'est plus silencieux

    def test_401_refreshes_token_once(self):
        fake = FakeTalentSoft(scripted={r"/E1/KeyDates$": [401]})
        ex, stats = make(fake)
        ex.run()
        self.assertGreaterEqual(stats.counters["token_refresh_on_401"], 1)

    def test_network_errors_and_circuit_breaker(self):  # R-TS-016
        fake = FakeTalentSoft(scripted={r"/hub/v2/employees/E1/": [OSError("x")] * 40})
        ex, stats = make(fake)
        ex.run()
        self.assertGreater(stats.counters["network_error"], 0)
        a = {x["control_code"]: x for x in A.completeness_anomalies(stats.as_dict())}
        self.assertEqual(a["TS_FILTER_TECHNICAL_FAILURE"]["outcome"], "FAIL")

    def test_filter_motul_france_without_duplicate_calls(self):
        fake = FakeTalentSoft()
        ex, stats = make(fake)
        ex.run()
        for emp in ("E1", "E2", "E3"):
            self.assertEqual(len(gets(fake, f"/employees/{emp}/Situation")), 1)   # lue une seule fois, filtre compris
        positions = [u for m, u in fake.calls if "/positions/" in u]
        self.assertEqual(len(positions), 3)                                       # une position par salarié
        e3_calls = [u for m, u in fake.calls if "/employees/E3/" in u]
        self.assertEqual(len(e3_calls), 1)                                        # E3 exclu : aucun détail lu
        self.assertEqual(stats.counters["excluded_not_motul_france"], 1)
        self.assertEqual(stats.counters["retained_phase1"], 2)
        self.assertEqual(stats.counters["records"], 2)

    def test_health_entities_are_not_requested(self):
        fake = FakeTalentSoft()
        ex, _ = make(fake)
        ex.run()
        for entity in ("Disability", "WorkAccident", "MedicalCheckUp"):
            self.assertEqual([u for m, u in fake.calls if u.endswith("/" + entity)], [], entity)

    def test_all_employees_extracted_with_bounded_concurrency(self):
        fake = FakeTalentSoft(n_employees=40, latency=0.005)
        ex, stats = make(fake, employee_concurrency=6, max_concurrency=3)
        records = ex.run()
        self.assertEqual(len(records), 40)
        self.assertEqual(stats.counters["records"], 40)
        self.assertLessEqual(fake.max_in_flight, 3)                     # plafond = limite du client
        self.assertGreater(fake.max_in_flight, 1)                       # et le parallélisme existe bien

    def test_concurrency_does_not_change_the_data(self):
        slow = {r["employeeNumber"]: r for r in make(FakeTalentSoft(), employee_concurrency=1, max_concurrency=1)[0].run()}
        fast = {r["employeeNumber"]: r for r in make(FakeTalentSoft(), employee_concurrency=8, max_concurrency=12)[0].run()}
        self.assertEqual(slow, fast)

    def test_token_is_renewed_by_age_only_once(self):
        clock = FakeClock()
        fake = FakeTalentSoft()
        ex, _ = make(fake, clock=clock)
        client = ex.client
        client.get_token()
        client.get("/api/v1/directory/employees?count=1&offset=0", "directory_count")
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 1)   # jeton encore frais
        clock.now += A.TOKEN_MAX_AGE_SECONDS + 1
        client.get("/api/v1/directory/employees?count=1&offset=0", "directory_count")
        client.get("/api/v1/directory/employees?count=1&offset=0", "directory_count")
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 2)   # un seul renouvellement

    def test_fatal_error_stops_pending_employees(self):
        fake = FakeTalentSoft(n_employees=200, latency=0.002, fail_after=60)
        ex, _ = make(fake, employee_concurrency=2, max_concurrency=2)
        with self.assertRaises(HrisError):
            ex.run()
        self.assertLess(len(fake.calls), 200 * 14 // 4)                 # les salariés non commencés sont annulés

    def test_token_failure_raises(self):
        fake = FakeTalentSoft(scripted={r"/api/token$": [400]})
        ex, _ = make(fake)
        with self.assertRaises(HrisError):
            ex.run()

    def test_stats_contain_no_identifier(self):
        fake = FakeTalentSoft()
        ex, stats = make(fake)
        ex.run()
        text = json.dumps(stats.as_dict())
        for forbidden in ["E1", "E2", "u1", "1234567890123", "talent-soft", "tok-fictif", "secret-fictif"]:
            self.assertNotIn(forbidden, text)
        self.assertNotIn("secret-fictif", repr(ex.client))


def no_position(legal, *extra):
    """Situation sans aucun élément Position, qui porte sa propre structure juridique (cas des sortis, annuaire PRD)."""
    return [hub("WorkingType", "Cadre", "2015-01-01"), hub("LegalStructure", legal, "2019-05-01"),
            hub("GeographicOrganisationStructure", "GEO1", "2019-05-01"), hub("CostCenter", "CC9", "2019-05-01"),
            hub("OrganisationalStructure", "ORG1", "2019-05-01"), *extra]


class SituationLegalStructureFallback(unittest.TestCase):
    """Sans Position exploitable, la structure juridique est lue sur la situation ; avec une Position, rien ne change."""

    def test_situation_without_position_is_retained_on_its_own_legal_structure(self):
        records, stats, fake = run_one(no_position("Motul France"))
        self.assertEqual([r["employeeNumber"] for r in records], ["S1"])
        self.assertEqual(position_urls(fake), [])                                   # aucun poste à interroger
        record = records[0]
        self.assertEqual((record["LegalStructure"], record["GeographicOrganisationStructure"], record["CostCenter"],
                          record["OrganisationalStructure"], record["WorkingType"]),
                         ("Motul France", "GEO1", "CC9", "ORG1", "Cadre"))
        self.assertEqual(record["pourcentage_imputation"], "100")                  # non repris de la situation (sens non démontré)
        c = stats.counters
        self.assertEqual((c["retained_phase1"], c["retained_from_situation_legal_structure"], c["situation_without_position"]),
                         (1, 1, 1))
        self.assertEqual((c["excluded_missing_position_or_date"], c["excluded_not_motul_france"]), (0, 0))

    def test_other_legal_structure_is_excluded_and_counted_both_ways(self):
        records, stats, fake = run_one(no_position("Motul Deutschland"))
        self.assertEqual(records, [])
        c = stats.counters
        self.assertEqual((c["excluded_not_motul_france"], c["excluded_not_motul_france_from_situation"],
                          c["retained_phase1"]), (1, 1, 0))

    def test_the_filter_stays_exact_on_the_fallback(self):
        for legal, kept in (("Motul France", 1), ("Motul France ", 0), ("motul france", 0), ("Motul SA", 0), ("Lescot", 0)):
            records, stats, fake = run_one(no_position(legal))
            self.assertEqual(len(records), kept, repr(legal))

    def test_without_position_and_without_legal_structure_the_original_exclusion_is_kept(self):
        for items in ([hub("WorkingType", "Cadre", "2015-01-01")], no_position("")):
            records, stats, fake = run_one(items)
            self.assertEqual(records, [])
            c = stats.counters
            self.assertEqual((c["excluded_missing_position_or_date"], c["situation_no_legal_structure"]), (1, 1))
            self.assertEqual(position_urls(fake), [])

    def test_a_usable_position_follows_the_original_path_whatever_the_situation_says(self):
        # la situation dit « Motul Deutschland » mais le poste P1 est « Motul France » : le poste fait foi, comme avant
        records, stats, fake = run_one([hub("WorkingType", "Cadre", "2023-01-01"), hub("Position", "P1", "2023-01-01"),
                                        hub("LegalStructure", "Motul Deutschland", "2023-01-01")])
        self.assertEqual([r["LegalStructure"] for r in records], ["Motul France"])
        self.assertEqual(len(position_urls(fake)), 1)
        self.assertEqual((stats.counters["retained_from_situation_legal_structure"], stats.counters["situation_without_position"]),
                         (0, 0))
        # et l'inverse : la situation dit « Motul France » mais le poste P3 est « Motul SA » : exclu, sans repli
        records, stats, fake = run_one([hub("Position", "P3", "2023-01-01"), hub("LegalStructure", "Motul France", "2023-01-01")])
        self.assertEqual(records, [])
        self.assertEqual((stats.counters["excluded_not_motul_france"], stats.counters["excluded_not_motul_france_from_situation"]),
                         (1, 0))

    def test_the_latest_legal_structure_version_wins_whatever_the_order(self):
        old, new = hub("LegalStructure", "Motul SA", "2015-01-01"), hub("LegalStructure", "Motul France", "2021-01-01")
        for items, kept in (([old, new], 1), ([new, old], 1)):
            self.assertEqual(len(run_one([hub("WorkingType", "Cadre", "2015-01-01"), *items])[0]), kept)
        newer_other = hub("LegalStructure", "Motul SA", "2024-01-01")
        for items in ([new, newer_other], [newer_other, new]):
            self.assertEqual(run_one([hub("WorkingType", "Cadre", "2015-01-01"), *items])[0], [])   # la plus récente est « Motul SA »

    def test_former_employee_without_position_is_retained(self):
        records, stats, fake = run_one(no_position("Motul France"), entities={
            ("S1", "Contract"): [hub("NatureOfContract", "CDI", "2001-01-01", endDate="2001-12-31")],
            ("S1", "KeyDates"): [hub("GroupEndDate", "2001-12-31", "2001-12-31")]})
        self.assertEqual(len(records), 1)       # aucun filtre « actifs uniquement »
        self.assertEqual((records[0]["date_fin_contract"], records[0]["GroupEndDate"]), ("2001-12-31", "2001-12-31"))

    def assert_unreliable_and_not_masked(self, stats, cause):
        c = stats.counters
        self.assertEqual((c[f"phase1_situation_{cause}"], c["excluded_situation_unreliable"]), (1, 1))
        for masked in ("excluded_no_situation", "excluded_missing_position_or_date", "excluded_not_motul_france",
                       "phase1_situation_error", "retained_phase1"):
            self.assertEqual(c[masked], 0, masked)          # une erreur de traitement n'est pas une exclusion métier
        control = {a["control_code"]: a for a in A.completeness_anomalies(stats.as_dict())}["TS_FILTER_TECHNICAL_FAILURE"]
        self.assertEqual((control["severity"], control["outcome"]), ("BLOCKING", "FAIL"))

    def test_contradictory_legal_structure_is_never_resolved_by_order(self):
        for first, second in ((hub("LegalStructure", "Motul France", "2019-05-01"), hub("LegalStructure", "Motul SA", "2019-05-01")),
                              (hub("LegalStructure", "Motul SA", "2019-05-01"), hub("LegalStructure", "Motul France", "2019-05-01"))):
            records, stats, fake = run_one([hub("WorkingType", "Cadre", "2015-01-01"), first, second])
            self.assertEqual(records, [])
            self.assert_unreliable_and_not_masked(stats, "conflict")

    def test_invalid_date_keeps_the_original_technical_failure_handling(self):
        # la lecture historique de la situation parse toutes les dates : une date invalide reste une erreur technique
        # comptée (phase1_situation_error) et reprise par le contrôle bloquant, comme avant ; aucun salarié retenu
        records, stats, fake = run_one([hub("WorkingType", "Cadre", "2015-01-01"), hub("LegalStructure", "Motul France", "31/12/2019")])
        self.assertEqual(records, [])
        self.assertEqual((stats.counters["phase1_situation_error"], stats.counters["retained_phase1"]), (1, 0))
        control = {a["control_code"]: a for a in A.completeness_anomalies(stats.as_dict())}["TS_FILTER_TECHNICAL_FAILURE"]
        self.assertEqual((control["severity"], control["outcome"]), ("BLOCKING", "FAIL"))
        with self.assertRaises(A.SituationDataError) as caught:        # et la lecture explicite des attributs refuse aussi
            A.situation_own_attributes([hub("LegalStructure", "Motul France", "31/12/2019")])
        self.assertEqual((caught.exception.code, caught.exception.field), ("invalid_date", "LegalStructure"))

    def test_malformed_items_are_counted_apart(self):
        records, stats, fake = run_one([hub("WorkingType", "Cadre", "2015-01-01"), {"field": "LegalStructure", "startDate": "2019-05-01"}])
        self.assertEqual(records, [])
        self.assert_unreliable_and_not_masked(stats, "malformed")

    def test_unreliable_situation_does_not_stop_the_other_employees(self):
        fake = FakeTalentSoft(employees=[{"employeeNumber": "S1", "individual": {}}, {"employeeNumber": "S2", "individual": {}}],
                              situations={"S1": [hub("LegalStructure", "Motul France", "2019-05-01"),
                                                 hub("LegalStructure", "Motul SA", "2019-05-01")],
                                          "S2": no_position("Motul France")})
        ex, stats = make(fake)
        self.assertEqual([r["employeeNumber"] for r in ex.run()], ["S2"])
        self.assertEqual((stats.counters["excluded_situation_unreliable"], stats.counters["retained_phase1"]), (1, 1))

    def test_the_error_is_not_fatal_and_carries_no_data(self):
        self.assertTrue(issubclass(A.SituationDataError, ValueError))
        self.assertFalse(issubclass(A.SituationDataError, HrisError))        # ne doit pas interrompre l'extraction
        with self.assertRaises(A.SituationDataError) as caught:
            A.situation_own_attributes([hub("LegalStructure", "VALEUR-SECRETE", "2019-05-01"),
                                        hub("LegalStructure", "AUTRE-VALEUR", "2019-05-01")])
        self.assertNotIn("SECRETE", str(caught.exception))
        self.assertNotIn("AUTRE", str(caught.exception))

    def test_counters_carry_no_personal_data(self):
        records, stats, fake = run_one(no_position("Motul France"))
        text = json.dumps(stats.as_dict())
        for forbidden in ("S1", "Cadre", "GEO1", "CC9", "Motul France"):
            self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_adp_publication =============
register_hris_module('test_adp_publication', '6bc67706cfe7e926368c4a048dd995a364f16885b3ea6196fe4745fa1cbbd32f', r'''"""TU-01 à TU-04 + exigences du mandat § 8/§ 10 — publication ADP, uniquement avec clients FICTIFS.
Aucun accès réseau : le simulateur répond en mémoire et la sentinelle bloque tout hôte ADP."""
import datetime as dt
import json
import os
import socket
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src"), os.path.join(HERE, "..", "fixtures")]

from hris import adp_gate as G  # noqa: E402
from hris import adp_ops as O  # noqa: E402
from hris import adp_publish as PUB  # noqa: E402
from hris import adp_transport as TR  # noqa: E402
from hris.adp_sim import FakeAdp, MemAttempts, MemLog, MemOps, MemOutbox, fake_context  # noqa: E402
from hris.common import HrisError  # noqa: E402

NOW = dt.datetime(2026, 10, 7, 3, 0, tzinfo=dt.timezone.utc)


class ForbiddenFactory:
    """Fabrique de transport qui ne doit JAMAIS être appelée (lecture de secrets ADP)."""
    def __init__(self):
        self.called = 0

    def __call__(self):
        self.called += 1
        raise AssertionError("transport ADP construit alors qu'il ne le devait pas")


def simple_planner(variants=("P2_PERSONAL_EMAIL",), targets=None, deps=None):
    """Planificateur fictif : soumet une opération par variante (cibles fictives)."""
    def plan(row, ex):
        for v in variants:
            tgt = (targets or {}).get(v, {"value": row["value"]})
            ex.submit(O.PlannedOp(v[:2], v, "events/hr/v1/fake", {"events": [{"x": tgt}]}, tgt,
                                  {"mode": "UPDATE", "field": v}, depends_on=(deps or {}).get(v, ()),
                                  read_back=lambda rd, t=tgt: rd.read(t)))
    return plan


def run(name, outbox, *, factory, ops=None, mode="NORMAL", source="R1", blockers=lambda: [], planner=None,
        rows=None, max_attempts=3):
    ops = ops if ops is not None else MemOps()
    logs, gate = MemLog(), MemLog()
    ctx = PUB.PublicationContext(publish_run_id="P-" + source, source_run_id=source, mode=mode,
                                 max_attempts=max_attempts, pause_seconds=0)
    summary = PUB.run_publication(ctx, context_provider=fake_context(name) if isinstance(name, (str, type(None))) else name,
                                  outbox=outbox, gate_log=gate, ops=ops, attempts=MemAttempts(),
                                  publication_log=logs, activation_blockers=blockers, transport_factory=factory,
                                  planner=planner or simple_planner(),
                                  rows_by_key=lambda: rows or {k: {"value": f"v-{k}"} for (_, k) in outbox.items},
                                  sleep=lambda s: None, clock=lambda: NOW)
    return summary, ops, gate, logs


class GateRule(unittest.TestCase):
    def test_table(self):  # TU-01 (règle littérale inchangée, D-ENV-02 à valider)
        cases = [("WS-HRIS-PRD", G.ENV_SATISFIED), ("WS_HRIS_PRD", G.ENV_SATISFIED),
                 ("WS-HRIS-DEV", G.DENIED_NON_PRD), ("Sandbox - Fabric BI backend", G.DENIED_NON_PRD),
                 ("ws-hris-prd", G.DENIED_NON_PRD), ("WS-HRIS-Prd", G.DENIED_NON_PRD),
                 ("WS-HRIS-PREPRD", G.ENV_SATISFIED), ("NOTPRD", G.ENV_SATISFIED),
                 ("", G.BLOCKED_UNRESOLVED), ("   ", G.BLOCKED_UNRESOLVED), (None, G.BLOCKED_UNRESOLVED),
                 (123, G.BLOCKED_UNRESOLVED), (b"WS-HRIS-PRD", G.BLOCKED_UNRESOLVED)]
        for name, outcome in cases:
            met, reason = G.decide_adp_push(name)
            self.assertEqual(G.outcome_of(reason), outcome, name)

    def test_context_failure_is_unresolved_not_non_prd(self):  # TU-02
        def boom():
            raise RuntimeError("contexte indisponible")
        d = G.evaluate_gate(boom, clock=lambda: NOW)
        self.assertEqual((d.outcome, d.env_condition_met), (G.BLOCKED_UNRESOLVED, False))
        d = G.evaluate_gate(lambda: {"currentWorkspaceId": "x"}, clock=lambda: NOW)
        self.assertEqual(d.outcome, G.BLOCKED_UNRESOLVED)


class GuardProof(unittest.TestCase):
    """Preuve instrumentée : zéro appel de publication hors PRD et en contexte non résolu ;
    contrôle POSITIF en contexte simulé autorisé (le comptage détecte bien un appel)."""

    def test_non_prd_zero_publication_calls(self):
        for name in ["WS-HRIS-DEV", "Sandbox - Fabric BI backend", "ws-hris-prd"]:
            outbox, factory = MemOutbox(), ForbiddenFactory()
            for k in ("E1", "E2"):
                outbox.add("R1", k)
            s, ops, gate, logs = run(name, outbox, factory=factory)
            self.assertEqual(factory.called, 0, name)
            self.assertEqual(s["status"], PUB.STATUS_NOT_PUBLISHED_NON_PRD)
            self.assertEqual({v["status"] for v in outbox.items.values()}, {"NOT_PUSHED_NON_PRD"})
            self.assertEqual(ops.rows, {})
            self.assertEqual(len(gate.decisions), 1)

    def test_unresolved_zero_calls_and_items_stay_pending(self):
        for ctx in [None, "", lambda: (_ for _ in ()).throw(RuntimeError("x"))]:
            outbox, factory = MemOutbox(), ForbiddenFactory()
            outbox.add("R1", "E1")
            s, ops, gate, _ = run(ctx, outbox, factory=factory)
            self.assertEqual(factory.called, 0)
            self.assertEqual(s["status"], PUB.STATUS_BLOCKED_UNRESOLVED)
            self.assertEqual(outbox.items[("R1", "E1")]["status"], "PENDING")   # réévaluable, pas NOT_PUSHED

    def test_env_satisfied_but_activation_blocked_zero_calls(self):
        outbox, factory = MemOutbox(), ForbiddenFactory()
        outbox.add("R1", "E1")
        s, _, _, _ = run("WS-HRIS-PRD", outbox, factory=factory, blockers=PUB.code_activation_blockers)
        self.assertEqual(factory.called, 0)
        self.assertEqual(s["status"], PUB.STATUS_BLOCKED_ACTIVATION)
        self.assertIn("CONTRAT_ADP_FINAL_ABSENT", s["activation_blockers"])
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "PENDING")

    def test_positive_control_simulated_allowed_context(self):
        adp = FakeAdp()
        rec = TR.RecordingTransport(adp)
        outbox = MemOutbox()
        outbox.add("R1", "E1")
        s, ops, _, logs = run("WS-HRIS-PRD", outbox, factory=lambda: rec)
        self.assertEqual(len(rec.post_calls), 1)                 # l'instrumentation voit bien l'appel
        self.assertEqual(s["status"], PUB.STATUS_PUBLISHED)
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "SENT")

    def test_real_transport_cannot_be_built(self):
        with self.assertRaises(HrisError):
            TR.AdpHttpTransport("c.pem", "k.pem", lambda: "x", 30, _allow_construction=True)
        self.assertFalse(TR.ADP_ACTIVATION_APPROVED)
        self.assertFalse(TR.ADP_READ_POLICY_ESTABLISHED)


class NetworkSentinelTest(unittest.TestCase):
    def test_sentinel_detects_and_blocks_without_network(self):  # contrôle positif de l'observation réseau
        with TR.NetworkSentinel() as s:
            with self.assertRaises(ConnectionRefusedError):
                socket.getaddrinfo("accounts.eu.adp.com", 443)
            with self.assertRaises(ConnectionRefusedError):
                socket.create_connection(("geo.api.gouv.fr", 443), timeout=1)
        self.assertEqual([a["host"] for a in s.attempts], ["accounts.eu.adp.com", "geo.api.gouv.fr"])
        self.assertIs(socket.getaddrinfo.__module__, "socket" if False else socket.getaddrinfo.__module__)

    def test_sentinel_ignores_other_hosts_and_restores(self):
        orig = socket.getaddrinfo
        with TR.NetworkSentinel() as s:
            self.assertFalse(s._watched("motul.talent-soft.com"))
        self.assertIs(socket.getaddrinfo, orig)


class TransportClassification(unittest.TestCase):
    def test_table(self):
        cases = [(200, '{"a":1}', TR.ACCEPTED), (201, '{}', TR.ACCEPTED), (202, "", TR.ACCEPTED),
                 (200, "", TR.UNCERTAIN), (200, "OK", TR.UNCERTAIN), (204, "", TR.UNCERTAIN),
                 (302, "", TR.UNCERTAIN), (401, "{}", TR.REJECTED), (429, "{}", TR.REJECTED),
                 (400, "{}", TR.REJECTED), (408, "", TR.UNCERTAIN), (500, "{}", TR.UNCERTAIN),
                 (503, "", TR.UNCERTAIN)]
        for status, body, outcome in cases:
            self.assertEqual(TR.classify_response(status, body)[0], outcome, (status, body))


class Durability(unittest.TestCase):
    def test_intent_persistence_failure_means_no_send(self):
        adp, outbox = FakeAdp(), MemOutbox()
        rec = TR.RecordingTransport(adp)
        outbox.add("R1", "E1")
        s, ops, _, _ = run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=MemOps(fail_on_upsert_status="INTENT_RECORDED"))
        self.assertEqual(rec.post_calls, [])                    # aucun envoi sans intention durable
        self.assertNotEqual(outbox.items[("R1", "E1")]["status"], "SENT")

    def test_crash_after_send_leaves_intent_uncertain_and_never_resent(self):
        adp, outbox = FakeAdp(), MemOutbox()
        outbox.add("R1", "E1")
        ops = MemOps(fail_on_upsert_status="ACCEPTED")          # issue non persistée (crash après envoi)
        rec = TR.RecordingTransport(adp)
        run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops)
        self.assertEqual(len(rec.post_calls), 1)
        self.assertEqual([r["status"] for r in ops.rows.values()], ["INTENT_RECORDED"])
        ops.fail_on_upsert_status = None
        outbox.items[("R1", "E1")]["status"] = "FAILED"          # même relancé explicitement...
        rec2 = TR.RecordingTransport(FakeAdp())
        s, *_ = run("WS-HRIS-PRD", outbox, factory=lambda: rec2, ops=ops, mode="RETRY_PUBLICATION")
        self.assertEqual(rec2.post_calls, [])                   # ... aucun nouvel envoi automatique
        self.assertEqual(s["blocked_prior_uncertain"], 1)

    def test_crash_before_send(self):
        adp, outbox = FakeAdp(), MemOutbox()
        adp.post_script = ["connect_refused"]
        outbox.add("R1", "E1")
        s, ops, _, _ = run("WS-HRIS-PRD", outbox, factory=lambda: adp)
        self.assertEqual(list(ops.rows.values())[0]["status"], O.OP_NOT_SENT)   # non-transmission prouvée
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "FAILED")
        s, *_ = run("WS-HRIS-PRD", outbox, factory=lambda: adp, ops=ops, mode="RETRY_PUBLICATION")
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "SENT")          # reprise explicite autorisée

    def test_crash_during_send(self):
        adp, outbox = FakeAdp(), MemOutbox()
        adp.post_script = ["send_interrupted"]
        outbox.add("R1", "E1")
        s, ops, _, _ = run("WS-HRIS-PRD", outbox, factory=lambda: adp)
        self.assertEqual(list(ops.rows.values())[0]["status"], O.OP_UNCERTAIN)
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "IN_PROGRESS")
        outbox.items[("R1", "E1")]["status"] = "FAILED"
        rec = TR.RecordingTransport(FakeAdp())
        run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops, mode="RETRY_PUBLICATION")
        self.assertEqual(rec.post_calls, [])


class HttpOutcomes(unittest.TestCase):
    def _one(self, behaviour):
        adp, outbox = FakeAdp(), MemOutbox()
        adp.post_script = [behaviour]
        outbox.add("R1", "E1")
        rec = TR.RecordingTransport(adp)
        s, ops, _, logs = run("WS-HRIS-PRD", outbox, factory=lambda: rec)
        return s, list(ops.rows.values())[0], outbox.items[("R1", "E1")], rec

    def test_2xx_unusable_is_uncertain(self):
        s, op, item, rec = self._one(("2xx_text", 200))
        self.assertEqual((op["status"], item["status"]), (O.OP_UNCERTAIN, "IN_PROGRESS"))
        self.assertEqual(len(rec.post_calls), 1)

    def test_401_rejected_no_resend_and_halt(self):
        adp, outbox = FakeAdp(), MemOutbox()
        adp.post_script = [("status", 401)]
        for k in ("E1", "E2"):
            outbox.add("R1", k)
        rec = TR.RecordingTransport(adp)
        s, ops, _, _ = run("WS-HRIS-PRD", outbox, factory=lambda: rec)
        self.assertEqual(len(rec.post_calls), 1)                 # pas de renvoi, E2 non traité
        self.assertEqual(s["status"], PUB.STATUS_HALTED)
        self.assertEqual(outbox.items[("R1", "E2")]["status"], "PENDING")

    def test_429_5xx_timeout_redirect(self):
        for behaviour, expected in [(("status", 429), O.OP_REJECTED), (("status", 500), O.OP_UNCERTAIN),
                                    ("timeout_after_send", O.OP_UNCERTAIN), (("redirect", 302), O.OP_UNCERTAIN)]:
            s, op, item, rec = self._one(behaviour)
            self.assertEqual(op["status"], expected, behaviour)
            self.assertEqual(len(rec.post_calls), 1, behaviour)    # aucun retry automatique


class IdentityDependenciesSupersession(unittest.TestCase):
    def test_p8_variants_distinct_and_dependency_blocks(self):
        adp, outbox = FakeAdp(), MemOutbox()
        adp.post_script = [("status", 400)]                     # le contrat est rejeté
        outbox.add("R1", "E1")
        plan = simple_planner(("P8_CONTRACT_A", "P8_END_DATE", "P8_SALARY"),
                              deps={"P8_END_DATE": ("P8_CONTRACT_A", "P8_CONTRACT_B")})
        rec = TR.RecordingTransport(adp)
        s, ops, _, _ = run("WS-HRIS-PRD", outbox, factory=lambda: rec, planner=plan)
        st = {r["op_variant"]: r["status"] for r in ops.rows.values()}
        self.assertEqual(st, {"P8_CONTRACT_A": O.OP_REJECTED, "P8_END_DATE": O.OP_BLOCKED_DEPENDENCY,
                              "P8_SALARY": O.OP_ACCEPTED})
        self.assertEqual(len(rec.post_calls), 2)
        self.assertEqual(len({r["op_id"] for r in ops.rows.values()}), 3)

    def test_identity_stable_on_replay_and_distinct_for_new_change(self):
        a = O.op_identity("R1", "E1", "P2_PERSONAL_EMAIL", O.target_hash({"emailUri": "a"}))
        self.assertEqual(a, O.op_identity("R1", "E1", "P2_PERSONAL_EMAIL", O.target_hash({"emailUri": "a"})))
        b = O.op_identity("R2", "E1", "P2_PERSONAL_EMAIL", O.target_hash({"emailUri": "b"}))
        c = O.op_identity("R3", "E1", "P2_PERSONAL_EMAIL", O.target_hash({"emailUri": "a"}))  # retour à A
        self.assertEqual(len({a, b, c}), 3)

    def test_two_successive_legitimate_changes_same_field_both_sent(self):
        adp, outbox, ops = FakeAdp(), MemOutbox({"R1": 1, "R2": 2}), MemOps()
        outbox.add("R1", "E1")
        rec = TR.RecordingTransport(adp)
        run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops, source="R1", rows={"E1": {"value": "A"}})
        outbox.add("R2", "E1")
        run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops, source="R2", rows={"E1": {"value": "B"}})
        outbox.add("R3", "E1")
        outbox.run_order["R3"] = 3
        run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops, source="R3", rows={"E1": {"value": "A"}})
        self.assertEqual(len(rec.post_calls), 3)               # A, puis B, puis retour légitime à A
        self.assertEqual(sum(1 for r in ops.rows.values() if r["status"] == O.OP_ACCEPTED), 3)

    def test_supersession_without_losing_unsent_change(self):
        outbox, ops = MemOutbox({"R1": 1, "R2": 2}), MemOps()
        outbox.add("R1", "E1", status="FAILED")                 # ancien, non envoyé
        outbox.add("R1", "E2", status="FAILED")                 # ancien, sans élément plus récent
        outbox.add("R2", "E1", status="SENT")                   # plus récent, état complet publié
        rec = TR.RecordingTransport(FakeAdp())
        s, *_ = run("WS-HRIS-PRD", outbox, factory=lambda: rec, ops=ops, source="R1", mode="RETRY_PUBLICATION")
        self.assertEqual(outbox.items[("R1", "E1")]["status"], "SUPERSEDED")
        self.assertEqual(outbox.items[("R1", "E2")]["status"], "SENT")   # modification ancienne non perdue
        self.assertEqual(rec.post_calls, ["events/hr/v1/fake"])

    def test_never_supersede_uncertain(self):
        item = {"status": "FAILED"}
        self.assertFalse(O.supersession_candidates(item, [{"status": "SENT"}], [{"status": O.OP_UNCERTAIN}]))
        self.assertFalse(O.supersession_candidates({"status": "IN_PROGRESS"}, [{"status": "SENT"}], []))
        self.assertTrue(O.supersession_candidates(item, [{"status": "PENDING"}], [{"status": O.OP_NOT_SENT}]))

    def test_retry_selects_failed_not_in_progress_and_respects_max_attempts(self):
        outbox = MemOutbox()
        outbox.add("R1", "E1", status="FAILED", attempt_count=1)
        outbox.add("R1", "E2", status="IN_PROGRESS", attempt_count=1)
        outbox.add("R1", "E3", status="FAILED", attempt_count=3)
        rec = TR.RecordingTransport(FakeAdp())
        s, *_ = run("WS-HRIS-PRD", outbox, factory=lambda: rec, mode="RETRY_PUBLICATION")
        self.assertEqual(s["items_selected"], 1)
        self.assertEqual(outbox.items[("R1", "E2")]["status"], "IN_PROGRESS")


class ReconciliationAndDecisions(unittest.TestCase):
    class Reader:
        def __init__(self, observed):
            self.observed = observed

        def read(self, target):
            return self.observed

    def _op(self):
        return O.PlannedOp("P2", "P2_PERSONAL_EMAIL", "x", {}, {"emailUri": "a"}, {},
                           read_back=lambda rd: rd.read(None))

    def test_match_partial_different(self):
        op = self._op()
        self.assertEqual(O.reconcile({}, op, self.Reader({"emailUri": "a"}))["conclusion"], O.OP_APPLIED_OBSERVED)
        self.assertEqual(O.reconcile({}, op, self.Reader({}))["result"], O.RECON_PARTIAL)
        r = O.reconcile({}, op, self.Reader({"emailUri": "zzz"}))
        self.assertEqual(r["conclusion"], "MANUAL_DECISION_REQUIRED")   # un GET différent ne prouve pas le non-traitement

    def test_manual_decision_traced_and_required_fields(self):
        row = {"op_id": "o1", "run_id": "R1", "employee_key": "E1", "status": O.OP_UNCERTAIN}
        with self.assertRaises(HrisError):
            O.apply_manual_decision(row, O.DEC_AUTHORIZE_RESEND, "", "fondement")
        with self.assertRaises(HrisError):
            O.apply_manual_decision(row, O.DEC_AUTHORIZE_RESEND, "exploitant", "  ")
        updated, record = O.apply_manual_decision(row, O.DEC_AUTHORIZE_RESEND, "exploitant", "vérifié avec ADP",
                                                  "TICKET-1", clock=lambda: NOW)
        self.assertEqual(updated["status"], O.OP_NOT_SENT)
        self.assertEqual((record["decided_by"], record["basis"]), ("exploitant", "vérifié avec ADP"))


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_contract_markers =============
register_hris_module('test_contract_markers', 'f5dbc43711cead3e47a932ab9b089cfccf67367977cae041ec2602e6b32442db', r'''"""Cohérence entre les marqueurs `CONTRAT-ADP:Cxx` du code et docs/CONTRAT_ADP_POINTS_D_USAGE.md."""
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
DOC = os.path.join(ROOT, "docs", "CONTRAT_ADP_POINTS_D_USAGE.md")
sys.path[:0] = [os.path.join(ROOT, "tools")]


@unittest.skipUnless(os.path.exists(DOC), "document des points d'usage absent (exécution hors dépôt)")
class ContractMarkers(unittest.TestCase):
    def test_markers_documented_and_located(self):
        import contract_points
        found = contract_points.scan()
        doc = open(DOC, encoding="utf-8").read()
        documented = set(re.findall(r"^\| (C\d\d) \| [^|]+\| [^|]+\| ", doc, flags=re.M))
        self.assertEqual(set(found) - documented, set(), "marqueurs du code non documentés")
        self.assertEqual(documented - set(found), set(), "points documentés sans marqueur dans le code")
        self.assertIn(contract_points.render(found), doc, "localisations à régénérer : python tools/contract_points.py")


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_legacy_logs =============
register_hris_module('test_legacy_logs', '8b9667c33c642e1b061573284f31a479890fcc6fb5dfcaa8e3b4bfd6a28b38c1', r'''"""TU-10 / TI-10 (partie locale) — lecture des logs historiques sur fichiers FICTIFS (JDD-06)."""
import csv
import io
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src")]

from hris import legacy_logs as L  # noqa: E402

COLS = L.EXPECTED_COLUMNS


def write_csv(path, rows, delimiter=";"):
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delimiter, quotechar='"')
    w.writerow(COLS)
    w.writerows(rows)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


class LegacyLogs(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="jdd06_")
        ok = ["2026-01-19 03:00:01.123456", "00000001", "NOM FICTIF", "UPDATE", "salary", "200", "OK",
              '{"events": [1; 2]}', "", "changing salary from 1 to 2"]
        bad_ts = ["19/13/2026 99:99", "00000002", "X", "UPDATE", "bonus", "400", "Bad", "{}", "err", "msg"]
        write_csv(os.path.join(self.root, "ADP", "20260119", "output_log_adp.csv"), [ok, bad_ts, ok[:9]])
        write_csv(os.path.join(self.root, "ADP", "20260120", "output_log_adp.csv"), [ok], delimiter="\t")
        with open(os.path.join(self.root, "ADP", "20260119", "verbose_log_030000.log"), "w") as fh:
            fh.write("verbeux fictif")
        with open(os.path.join(self.root, "ADP", "20260121.csv"), "w") as fh:
            fh.write("a;b;c\n1;2;3\n")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_import_and_replay(self):
        records, rejects, ledger = L.process_folder(self.root, "test", {".csv", ".xlsx"}, set(), "imp-1", "u", "ws")
        status = {e["file_name"] + "@" + e["source_uri"].split("/")[-2]: e["status"] for e in ledger}
        self.assertEqual(sorted(status.values()), ["IMPORTED", "IMPORTED", "REJECTED_SCHEMA", "SKIPPED_UNSUPPORTED"])
        self.assertEqual(len(records), 2)
        self.assertEqual({r["log_date"].isoformat() for r in records}, {"2026-01-19", "2026-01-20"})
        self.assertEqual(records[0]["response_code"], '{"events": [1; 2]}')   # ';' entre guillemets
        self.assertEqual(records[0]["matricule_id"], "00000001")              # zéros de tête conservés
        self.assertEqual(sorted(r["reject_reason"] for r in rejects), ["COLUMN_COUNT_MISMATCH", "TS_ADP_INVALID"])
        for r in rejects:                                                    # aucune valeur de ligne rejetée
            self.assertEqual(set(r), {"import_id", "source_uri", "source_row_number", "reject_reason", "rejected_at_utc"})
        imported = {e["source_uri"] for e in ledger if e["status"] == "IMPORTED"}
        _, _, ledger2 = L.process_folder(self.root, "test", {".csv"}, imported, "imp-2", "u", "ws")
        self.assertEqual(sum(e["status"] == "SKIPPED_ALREADY_IMPORTED" for e in ledger2), 2)
        self.assertTrue(os.path.exists(os.path.join(self.root, "ADP", "20260119", "output_log_adp.csv")))  # rien supprimé


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_tables_naming =============
register_hris_module('test_tables_naming', 'b9f80302efc2a23b7d08a98643b394c89613aab56ee469b05d4c4f4e4be37e03', r'''"""Résolution des noms de tables : 4 parties sans Lakehouse par défaut ; isolement des tests ; refus de deviner."""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src")]

from hris.model import TABLES, Tables, tables_for_notebook  # noqa: E402


class TablesNaming(unittest.TestCase):
    def test_partial_names_without_lakehouse(self):
        self.assertEqual(Tables()("ctl.run"), "ctl.run")
        self.assertEqual(Tables("hris_tst")("ctl.run"), "hris_tst.ctl__run")

    def test_four_part_names(self):
        t = Tables("", "MOTUL_LH_HRIS", "Sandbox - Fabric BI backend")
        self.assertEqual(t("ctl.run"), "`Sandbox - Fabric BI backend`.`MOTUL_LH_HRIS`.ctl.run")
        self.assertEqual(t.schemas()[0], "`Sandbox - Fabric BI backend`.`MOTUL_LH_HRIS`.cfg")
        tt = Tables("hris_tst", "MOTUL_LH_HRIS", "WS")
        self.assertEqual(tt("log.anomaly"), "`WS`.`MOTUL_LH_HRIS`.hris_tst.log__anomaly")
        self.assertEqual(tt.extra("zz_test_report"), "`WS`.`MOTUL_LH_HRIS`.hris_tst.zz_test_report")
        self.assertEqual(len({t(x) for x in TABLES}), len(TABLES))

    def test_real_and_test_tables_never_collide(self):
        real, test = Tables("", "LH", "WS"), Tables("hris_tst", "LH", "WS")
        self.assertFalse({real(x) for x in TABLES} & {test(x) for x in TABLES})

    def test_context_resolution_and_refusal(self):
        ctx = {"currentWorkspaceName": "Sandbox - Fabric BI backend"}
        self.assertIn("`MOTUL_LH_HRIS`", tables_for_notebook(ctx, "", "MOTUL_LH_HRIS")("ctl.run"))
        self.assertIn("`BOUND_LH`", tables_for_notebook(dict(ctx, defaultLakehouseName="BOUND_LH"))("ctl.run"))
        with self.assertRaises(ValueError):
            tables_for_notebook(ctx)                      # ni paramètre ni Lakehouse par défaut : on ne devine pas
        with self.assertRaises(ValueError):
            tables_for_notebook({}, "", "LH")             # workspace non lisible

    def test_invalid_names_refused(self):
        for bad in ("a`b", "a\nb"):
            with self.assertRaises(ValueError):
                Tables("", "LH", bad)
        with self.assertRaises(ValueError):
            Tables("", "LH", "")


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_spark_transform_parity =============
register_hris_module('test_spark_transform_parity', 'b4b9d4fe448e412da8272adb49cfbddbd9de57dced583a1e620169546036ec86', r'''"""NR-02 (portion locale) — parité de l'implémentation Spark (hris.transform) avec l'implémentation de
référence du SQL de production (hris.transform_ref), sur données FICTIVES couvrant les cas limites.
Exécutable en local (pyspark 3.5, sans Delta) et dans Fabric (NB_HRIS_TESTS)."""
import datetime as dt
import decimal
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src"), os.path.join(HERE, "..", "fixtures")]

try:
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover
    SparkSession = None

from hris import transform as S  # noqa: E402
from hris import transform_ref as R  # noqa: E402
from hris.model import ADP_COLUMNS, LOOKUP_TABLES, RAW_KEYS, SOURCE_COLUMN_NAMES, Tables  # noqa: E402
import jdd  # noqa: E402

D = dt.date(2026, 10, 7)
EDGE_RECORDS = [
    jdd.ts_record(1),
    jdd.ts_record(2, PPC_FTE="150"),                                   # ETP seul hors bornes : pas de rejet
    jdd.ts_record(3, PPC_FTE="150", Date_de_naissance="2024-02-30"),   # date invalide + ETP
    jdd.ts_record(4, LegacyID="123", Matricule_IT=""),                 # longueur + Null_Rejet
    jdd.ts_record(5, Title="XX", Gender="Z"),                          # Référence_Rejet
    jdd.ts_record(6, Title="MR   ", LastName="x" * 80),               # espaces de fin, troncature 50
    jdd.ts_record(7, BusinessMobile=None, BusinessPhone="0102"),       # COALESCE
    jdd.ts_record(8, date_fin_contract="2027-01-31", GroupEndDate=""),  # dates
    jdd.ts_record(9, salary_base=50000.555, Pourcentage_prime_annuelles="abc"),  # arrondi, non numérique
    jdd.ts_record(10, referenceAdmissionDate="2019-01-07T00:00:00.000", LegalCountry="ZZZ"),
    jdd.ts_record(11, hubSocialSecurityNumber="   ", Matricule_IT=None),
    jdd.ts_record(12, RecalculatedSeniorityDate="01/31/2020", CompanyStartDate="20200131"),
]


@unittest.skipIf(SparkSession is None, "pyspark absent")
class SparkParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.getActiveSession() or (SparkSession.builder.master("local[1]")
                                                         .config("spark.ui.enabled", "false")
                                                         .config("spark.driver.memory", "2g")
                                                         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        S.ensure_session_settings(cls.spark)
        cls.T = Tables("hris_tst")
        cls.lookups = {}
        for table, cols in LOOKUP_TABLES.items():
            rows = [tuple(r.get(c) for c in cols) for r in jdd.full_lookup_rows(table)]
            cls.lookups[table] = cls.spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))
        cls.loader = staticmethod(lambda table: SparkParity.lookups[table])

    def raw_df(self, records):
        rows = [dict(r, run_id="RUN", business_date=D) for r in jdd.raw_rows(records)]
        schema = "run_id string, business_date date, " + ", ".join(f"`{k}` string" for k in RAW_KEYS)
        return self.spark.createDataFrame([tuple(r.get(k) for k in ["run_id", "business_date"] + RAW_KEYS)
                                           for r in rows], schema)

    def test_full_chain_matches_reference(self):
        src = S.source_df(self.raw_df(EDGE_RECORDS)).localCheckpoint()
        enr = S.enriched_df(self.spark, self.T, src, D, self.loader).localCheckpoint()
        rej = S.reject_df(enr, D).localCheckpoint()
        adp = S.adp_df(enr, rej, D)

        ref_src = [R.parse_source_row(r) for r in jdd.raw_rows(EDGE_RECORDS)]
        ref_enr = R.enrich_rows(ref_src, {t: jdd.full_lookup_rows(t) for t in LOOKUP_TABLES})
        ref_rej = R.reject_rows(ref_enr, D)
        ref_adp = R.adp_rows(ref_enr, ref_rej, D)

        got_src = sorted(tuple(r[c] for c in SOURCE_COLUMN_NAMES) for r in src.collect())
        self.assertEqual(got_src, sorted(tuple(r[c] for c in SOURCE_COLUMN_NAMES) for r in ref_src))

        rej_cols = ["id_unique", "cd_type_rejet", "lb_nom_colonne_rejet", "lb_valeur_rejet", "lb_fichier_mapping",
                    "ds_rejet", "fl_rejet"]
        key = lambda t: tuple("" if v is None else str(v) for v in t)  # noqa: E731
        self.assertEqual(sorted((tuple(r[c] for c in rej_cols) for r in rej.collect()), key=key),
                         sorted((tuple(r[c] for c in rej_cols) for r in ref_rej), key=key))

        got_adp = {r["id_unique"]: r.asDict() for r in adp.collect()}
        ref_by = {r["id_unique"]: r for r in ref_adp}
        self.assertEqual(sorted(got_adp), sorted(ref_by))
        for k, ref in ref_by.items():
            for c in ADP_COLUMNS:
                self.assertEqual(got_adp[k][c], ref[c], f"{k}.{c}")
            self.assertEqual(got_adp[k]["row_hash"], R.row_hash(ref), k)
        self.assertEqual(str(adp.schema["mt_salaire_mensuel"].dataType), "DecimalType(23,13)")

    def test_conversion_error_fails_like_tsql(self):
        src = S.source_df(self.raw_df([jdd.ts_record(20, salary_base="")]))
        enr = S.enriched_df(self.spark, self.T, src, D, self.loader).localCheckpoint()
        with self.assertRaises(Exception):
            S.adp_df(enr, S.reject_df(enr, D), D).collect()
        src = S.source_df(self.raw_df([jdd.ts_record(21, Pourcentage_prime_annuelles="1e5")]))
        enr = S.enriched_df(self.spark, self.T, src, D, self.loader).localCheckpoint()
        with self.assertRaises(Exception):
            S.reject_df(enr, D).collect()

    def test_masking_of_sensitive_rejects(self):
        src = S.source_df(self.raw_df([jdd.ts_record(30, Date_de_naissance="1990-02-30")]))
        enr = S.enriched_df(self.spark, self.T, src, D, self.loader).localCheckpoint()
        masked = S.masked_log_reject(S.reject_df(enr, D), "RUN").collect()
        self.assertTrue(masked)
        for r in masked:
            if r["lb_nom_colonne_rejet"] == "dt_naissance":
                self.assertEqual(r["lb_valeur_rejet"], "***")
                self.assertNotIn("1990", r["ds_rejet"])


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_fabric_integration =============
register_hris_module('test_fabric_integration', 'b4d697b97b8f107a27e319b94bf15b7440c8137e6948d10512e814a06ac7399a', r'''"""Tests d'intégration Fabric (TI, TR, TG, TU-09) — Delta réel, schéma ISOLÉ `hris_tst`, données FICTIVES.

Exécutés par NB_HRIS_TESTS après `configure(spark, notebookutils)`. Aucune table réelle n'est lue ni écrite ;
aucun appel TalentSoft ni ADP : l'acquisition est simulée par des enregistrements fictifs, la publication
évalue le garde-fou dans le contexte RÉEL du notebook avec une fabrique de transport qui échoue si elle est
appelée, sous sentinelle réseau.
"""
import datetime as dt
import threading
import unittest
import uuid

import jdd
from hris import compare as C
from hris import run_control as RC
from hris import spark_io as IO
from hris import steps
from hris.common import PROCESS_CODE
from hris.model import LOOKUP_TABLES, TABLES, Tables, tables_for_notebook
from hris.transform import ensure_session_settings
from hris.ts_acquire import AcquisitionStats

ENV = {"spark": None, "notebookutils": None, "schema": "hris_tst", "lakehouse": ""}


def configure(spark, notebookutils, schema="hris_tst", lakehouse=""):
    ENV.update(spark=spark, notebookutils=notebookutils, schema=schema, lakehouse=lakehouse)


def _ok_stats(n):
    st = AcquisitionStats()
    st.total_count = n
    st.counters.update({"directory_received": n, "records": n})
    return st.as_dict()


class ForbiddenFactory:
    def __init__(self):
        self.called = 0

    def __call__(self):
        self.called += 1
        raise AssertionError("transport ADP construit")


class FabricIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = ENV["spark"]
        if cls.spark is None:
            raise unittest.SkipTest("hors Fabric")
        ensure_session_settings(cls.spark)
        cls.T = tables_for_notebook(ENV["notebookutils"].runtime.context, ENV["schema"], ENV["lakehouse"])
        for schema in cls.T.schemas():
            cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for logical in TABLES:  # tables de TEST uniquement (schéma isolé), recréées à chaque campagne
            cls.spark.sql(f"DROP TABLE IF EXISTS {cls.T(logical)}")
        IO.ensure_schemas_and_tables(cls.spark, cls.T)
        steps.seed(cls.spark, cls.T)
        for table, cols in LOOKUP_TABLES.items():
            rows = [dict(r, _loaded_at_utc=dt.datetime.now(dt.timezone.utc), _loaded_by="NB_HRIS_TESTS",
                         _change_ref="JDD-03-FICTIF") for r in jdd.full_lookup_rows(table)]
            if rows:
                IO.append(IO.rows_df(cls.spark, rows, f"cfg.{table}"), cls.T(f"cfg.{table}"), f"cfg.{table}")

    # -- utilitaires --------------------------------------------------------------------------------------
    def start(self, mode="NORMAL", trigger="2026-10-07T01:00:00Z", target=""):
        run_id = "TST-" + uuid.uuid4().hex[:12]
        res = RC.start(self.spark, self.T, run_id=run_id, process_code=PROCESS_CODE, run_mode=mode, target_run_id=target,
                       trigger_time=trigger, business_timezone="Europe/Paris", lock_lease_minutes=30,
                       business_date_override="", pipeline_name="TEST", workspace_id=None, workspace_name=None)
        self.assertEqual(res["status"], "OK", res)
        return run_id

    def acquire(self, run_id, records):
        run = IO.get_run(self.spark, self.T, run_id)
        step = IO.StepLogger(self.spark, self.T, run_id, "ACQUIRE")
        return steps.store_acquisition(self.spark, self.T, run, records, _ok_stats(len(records)), step,
                                       source=steps.SOURCE_FIXTURE)

    def full_run(self, records, mode="NORMAL", publish=True):
        run_id = self.start(mode)
        self.acquire(run_id, records)
        steps.run_transform(self.spark, self.T, run_id)
        cmp = steps.run_compare(self.spark, self.T, run_id, mode)
        pro = steps.run_promote(self.spark, self.T, run_id, mode)
        pub = None
        if publish:
            pub = self.publish(run_id, mode)
        end = RC.end(self.spark, self.T, run_id, "TEST")
        return run_id, cmp, pro, pub, end

    def publish(self, run_id, mode, factory=None):
        factory = factory or ForbiddenFactory()
        res = steps.run_publish(self.spark, self.T, run_id, mode,
                                context_provider=lambda: ENV["notebookutils"].runtime.context,  # contexte RÉEL
                                transport_factory=factory, planner=lambda row, ex: None, max_attempts=3,
                                pause_seconds=0)
        res["_factory_calls"] = factory.called
        return res

    # -- scénarios ---------------------------------------------------------------------------------------
    def test_01_lock_and_concurrency(self):  # TU-09, TI-07
        a, b = "TST-LOCK-A-" + uuid.uuid4().hex[:6], "TST-LOCK-B-" + uuid.uuid4().hex[:6]
        self.assertTrue(RC.acquire_lock(self.spark, self.T, a, 30))
        self.assertFalse(RC.acquire_lock(self.spark, self.T, b, 30))
        self.assertTrue(RC.release_lock(self.spark, self.T, a))
        results = {}

        def grab(rid):
            results[rid] = RC.acquire_lock(self.spark, self.T, rid, 30)
        threads = [threading.Thread(target=grab, args=(r,)) for r in (a, b)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(sum(results.values()), 1, results)
        holder = [r for r, ok in results.items() if ok][0]
        RC.release_lock(self.spark, self.T, holder)
        # bail expiré : reprise possible
        self.assertTrue(RC.acquire_lock(self.spark, self.T, a, 30, now=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)))
        self.assertTrue(RC.acquire_lock(self.spark, self.T, b, 30))
        RC.release_lock(self.spark, self.T, b)

    def test_02_first_load_blocked_then_init_then_normal_with_outbox_and_gate(self):  # TI-01..03, TG-01, TG-03
        recs = [jdd.ts_record(i) for i in range(1, 6)] + [jdd.ts_record(9, Gender="X")]
        run1, cmp1, pro1, pub1, end1 = self.full_run(recs, "NORMAL")
        self.assertEqual(cmp1["decision"], "BLOCKED")
        self.assertIn("REFERENCE_ABSENTE", cmp1["blocking"])
        self.assertFalse(pro1["promoted"])
        self.assertEqual(end1["status"], "BLOCKED")

        run2, cmp2, pro2, pub2, end2 = self.full_run(recs, "INIT_REFERENCE")
        self.assertEqual(cmp2["decision"], "VALIDATED", cmp2)
        self.assertTrue(pro2["promoted"])
        self.assertEqual(pro2["nb_outbox"], 0)                       # INIT_REFERENCE : aucune outbox
        self.assertEqual(pub2["status"], "SKIPPED_INIT_REFERENCE")   # TG-03
        self.assertEqual(self.spark.table(self.T("ref.ts_employee_adp")).count(), 5)

        recs3 = [jdd.ts_record(1, PersonalEmail="nouveau1@example.invalid")] + \
                [jdd.ts_record(i) for i in range(2, 5)] + [jdd.ts_record(7)]          # E5 absent, E7 nouveau
        run3, cmp3, pro3, pub3, end3 = self.full_run(recs3, "NORMAL")
        self.assertEqual((cmp3["decision"], cmp3["nb_new"], cmp3["nb_modified"], cmp3["nb_absent"]),
                         ("VALIDATED", 1, 1, 1), cmp3)
        self.assertEqual(pro3["nb_outbox"], 2)
        det = {r.business_key: r for r in self.spark.table(self.T("log.comparison_detail"))
               .where(f"run_id = '{run3}'").collect()}
        self.assertEqual(det["E00001"].changed_columns, ["lb_email_personnel"])   # noms seulement
        # TG-01 : contexte réel du workspace DEV (nom sans PRD) -> aucune construction de transport
        self.assertEqual(pub3["proof"]["gate_outcome"], "DENIED_NON_PRD", pub3)
        self.assertEqual(pub3["_factory_calls"], 0)
        self.assertEqual(pub3["proof"]["sentinel_attempts"], 0)
        statuses = {r.status for r in self.spark.table(self.T("pub.adp_outbox")).where(f"run_id = '{run3}'").collect()}
        self.assertEqual(statuses, {"NOT_PUSHED_NON_PRD"})
        self.assertEqual(end3["status"], "NOT_PUBLISHED_NON_PRD")
        gate = self.spark.table(self.T("log.adp_gate_decision")).where(f"run_id = '{run3}'").collect()
        self.assertEqual(len(gate), 1)
        self.assertFalse(gate[0].decision)
        self.assertEqual(gate[0].reason_code, "WORKSPACE_NAME_WITHOUT_PRD")
        self.assertEqual(self.spark.table(self.T("pub.adp_operation")).count(), 0)

        # TR-03 / TR-02 : promotion rejouée sans effet ; comparatif non recalculé après promotion
        before = self.spark.table(self.T("ref.ts_employee_adp")).orderBy("id_unique").collect()
        again = C.promote(self.spark, self.T, run3, "NORMAL")
        self.assertTrue(again.get("already_promoted"))
        self.assertEqual(self.spark.table(self.T("ref.ts_employee_adp")).orderBy("id_unique").collect(), before)
        self.assertFalse(C.compare(self.spark, self.T, run3, "NORMAL")["recomputed"])

        # TI-08 : second run le même jour, sans changement -> delta vide, rang 2+
        run4, cmp4, pro4, pub4, end4 = self.full_run(recs3, "NORMAL")
        self.assertEqual((cmp4["nb_new"], cmp4["nb_modified"]), (0, 0))
        self.assertGreaterEqual(IO.get_run(self.spark, self.T, run4)["run_seq_in_day"], 2)

    def test_03_init_reference_refused_when_reference_not_empty(self):
        if self.spark.table(self.T("ref.ts_employee_adp")).count() == 0:
            self.skipTest("référence vide (ordre des tests)")
        run, cmp, pro, pub, end = self.full_run([jdd.ts_record(1)], "INIT_REFERENCE")
        self.assertEqual(cmp["decision"], "BLOCKED")
        self.assertIn("REFERENCE_NOT_EMPTY", cmp["blocking"])

    def test_04_empty_batch_blocks(self):  # TI-04
        run_id = self.start()
        run = IO.get_run(self.spark, self.T, run_id)
        st = AcquisitionStats()
        st.total_count = 0
        res = steps.store_acquisition(self.spark, self.T, run, [], st.as_dict(),
                                      IO.StepLogger(self.spark, self.T, run_id, "ACQUIRE"), source=steps.SOURCE_FIXTURE)
        self.assertIn("EMPTY_EXTRACTION", res["blocking_failures"])
        steps.run_transform(self.spark, self.T, run_id)
        self.assertEqual(steps.run_compare(self.spark, self.T, run_id, "NORMAL")["decision"], "BLOCKED")
        self.assertFalse(steps.run_promote(self.spark, self.T, run_id, "NORMAL")["promoted"])
        RC.end(self.spark, self.T, run_id, "TEST")

    def test_05_threshold_revalidation(self):  # « revalidation après configuration des seuils »
        if self.spark.table(self.T("ref.ts_employee_adp")).count() == 0:
            self.skipTest("référence vide")
        from delta.tables import DeltaTable
        tbl = DeltaTable.forName(self.spark, self.T("cfg.process"))
        tbl.update(set={"max_modified_pct": "0"})
        try:
            run_id = self.start()
            self.acquire(run_id, [jdd.ts_record(2, PersonalEmail="x2@example.invalid")] + [jdd.ts_record(i) for i in (1, 3, 4, 7)])
            steps.run_transform(self.spark, self.T, run_id)
            cmp = steps.run_compare(self.spark, self.T, run_id, "NORMAL")
            self.assertEqual(cmp["decision"], "BLOCKED")
            self.assertIn("MODIFIED_RATIO", cmp["blocking"])
            tbl.update(set={"max_modified_pct": "100"})
            # lever la configuration ne suffit pas : la promotion relit les contrôles du run (bloqué)
            self.assertFalse(steps.run_promote(self.spark, self.T, run_id, "NORMAL")["promoted"])
            cmp2 = steps.run_compare(self.spark, self.T, run_id, "NORMAL")   # revalidation explicite
            self.assertEqual(cmp2["decision"], "VALIDATED", cmp2)
            self.assertTrue(steps.run_promote(self.spark, self.T, run_id, "NORMAL")["promoted"])
            RC.end(self.spark, self.T, run_id, "TEST")
        finally:
            tbl.update(set={"max_modified_pct": "CAST(NULL AS DECIMAL(9,4))"})

    def test_06_missing_mapping_blocks_only_enrichment(self):  # TI-06
        from delta.tables import DeltaTable
        backup = self.spark.table(self.T("cfg.lu_hr_emp_sexe")).collect()
        DeltaTable.forName(self.spark, self.T("cfg.lu_hr_emp_sexe")).delete()   # table de TEST
        try:
            run_id = self.start()
            self.acquire(run_id, [jdd.ts_record(1)])
            res = steps.run_transform(self.spark, self.T, run_id)
            self.assertEqual(res["status"], "BLOCKED_MAPPINGS")
            self.assertEqual(self.spark.table(self.T("stg.ts_employee_source")).where(f"run_id = '{run_id}'").count(), 1)
            self.assertEqual(steps.run_compare(self.spark, self.T, run_id, "NORMAL")["decision"], "BLOCKED")
            RC.end(self.spark, self.T, run_id, "TEST")
        finally:
            if backup:
                schema = self.spark.table(self.T("cfg.lu_hr_emp_sexe")).schema
                (self.spark.createDataFrame([tuple(r) for r in backup], schema).write.format("delta")
                 .mode("append").saveAsTable(self.T("cfg.lu_hr_emp_sexe")))

    def test_07_run_end_releases_lock_and_summary_has_no_values(self):
        rows = self.spark.table(self.T("ctl.process_lock")).collect()
        self.assertEqual(rows[0].lock_status, "RELEASED")
        summ = self.spark.table(self.T("log.run_summary")).collect()
        self.assertTrue(summ)
        for r in summ:
            self.assertTrue(r.ds_type_rejets is None or len(r.ds_type_rejets.encode("utf-8")) <= 50)


def suite():
    return unittest.defaultTestLoader.loadTestsFromTestCase(FabricIntegration)
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_cell_log =============
register_hris_module('test_cell_log', '416233b1ee5c64b80bc66f58e982cb878328227d9aba62ec415355626af770e6', r'''"""TU-CL — journal des cellules (hris.cell_log) : réussites optionnelles, échecs toujours tracés, une écriture par
flush, écriture en échec sans masquer l'erreur métier, expurgation des messages. Partie Spark : Fabric uniquement."""
import contextlib
import io
import os
import sys
import unittest
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "..", "src")]

from hris.cell_log import CellLog, LOGICAL_TABLE, error_location, redact_error, spark_writer  # noqa: E402
from hris.common import HrisError  # noqa: E402
from hris.model import TABLES  # noqa: E402

CONTEXT = {"activityId": "op-1", "currentNotebookName": "T3_NB_HRIS_TRANSFORM", "currentNotebookId": "nb-1",
           "currentWorkspaceName": "Sandbox - Fabric BI backend", "currentWorkspaceId": "ws-1", "isForPipeline": True}
SENSITIVE = ["Bearer abc.def", "client_secret=xyz", "https://motul.talent-soft.com/api/token",
             "jean.dupont@motul.com", "DUPONT", "123456"]


class Recorder:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def __call__(self, events):
        self.calls.append([dict(e) for e in events])
        if self.fail:
            raise RuntimeError("écriture refusée https://onelake.dfs.fabric.microsoft.com/x token=abc")


def make(writer, log_success=True, run_id="a124f919-88c1-4545-9ef7-ea2b3c8ca031"):
    return CellLog(CONTEXT, lakehouse_name="MOTUL_LH_HRIS", run_id=run_id, log_success=log_success, writer=writer)


def sensitive_error():
    return ValueError("Bearer abc.def client_secret=xyz https://motul.talent-soft.com/api/token "
                      "jean.dupont@motul.com value 'DUPONT' 123456")


class CellLogTests(unittest.TestCase):
    def test_successes_buffered_then_single_write(self):
        rec = Recorder()
        log = make(rec)
        for code in ("CONFIG", "TRANSFORM"):
            log.start(code)
            log.success()
        self.assertEqual(rec.calls, [])                       # rien de persisté avant le flush
        log.flush()
        self.assertEqual(len(rec.calls), 1)
        events = rec.calls[0]
        self.assertEqual([(e["cell_code"], e["cell_seq"], e["status"]) for e in events],
                         [("CONFIG", 1, "SUCCEEDED"), ("TRANSFORM", 2, "SUCCEEDED")])
        e = events[0]
        self.assertEqual((e["run_id"], e["operation_id"], e["notebook_name"], e["workspace_name"], e["lakehouse_name"],
                          e["is_for_pipeline"]), ("a124f919-88c1-4545-9ef7-ea2b3c8ca031", "op-1",
                                                  "T3_NB_HRIS_TRANSFORM", "Sandbox - Fabric BI backend",
                                                  "MOTUL_LH_HRIS", True))
        self.assertIsNone(e["error_type"])
        self.assertGreaterEqual(e["duration_ms"], 0)
        self.assertLessEqual(e["started_at_utc"], e["ended_at_utc"])
        log.flush()
        self.assertEqual(len(rec.calls), 1)                   # flush idempotent : aucune écriture vide

    def test_success_option_disabled_writes_nothing(self):
        rec = Recorder()
        log = make(rec, log_success=False)
        log.start("CONFIG")
        log.success()
        log.flush()
        self.assertEqual(rec.calls, [])

    def test_failure_written_immediately_with_pending_successes(self):
        rec = Recorder()
        log = make(rec)
        log.start("CONFIG")
        log.success()
        log.start("TRANSFORM")
        log.failure(HrisError("TRANSFORM_FAILED", "conversion impossible"))
        self.assertEqual(len(rec.calls), 1)
        self.assertEqual([(e["cell_code"], e["status"]) for e in rec.calls[0]],
                         [("CONFIG", "SUCCEEDED"), ("TRANSFORM", "FAILED")])
        failed = rec.calls[0][1]
        self.assertEqual((failed["error_type"], failed["error_code"]), ("HrisError", "TRANSFORM_FAILED"))
        self.assertTrue(failed["error_message"].startswith("TRANSFORM_FAILED"))
        log.flush()
        self.assertEqual(len(rec.calls), 1)

    def test_failure_always_written_even_if_success_disabled(self):
        rec = Recorder()
        log = make(rec, log_success=False)
        log.start("CONFIG")
        log.success()
        log.start("ACQUIRE")
        log.failure(KeyError("x"))
        self.assertEqual([(e["cell_code"], e["status"]) for e in rec.calls[0]], [("ACQUIRE", "FAILED")])

    def test_write_failure_never_raises_and_is_reported(self):
        rec = Recorder(fail=True)
        log = make(rec)
        log.start("CONFIG")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            log.failure(HrisError("RUN_UNKNOWN"))        # ne lève pas : l'appelant propage l'erreur d'origine
        self.assertTrue(log.write_failed)
        self.assertEqual(log.events_written, 0)
        text = out.getvalue()
        self.assertIn("NON persisté", text)
        self.assertIn("CONFIG=FAILED(HrisError)", text)
        self.assertNotIn("onelake", text)
        self.assertNotIn("abc", text)

    def test_failed_cell_then_original_error_propagates(self):
        rec = Recorder(fail=True)
        log = make(rec)
        with self.assertRaises(ZeroDivisionError), contextlib.redirect_stdout(io.StringIO()):
            log.start("COMPUTE")
            try:
                1 / 0
            except Exception as error:
                log.failure(error)
                raise

    def test_no_event_for_unstarted_cell_success(self):
        rec = Recorder()
        log = make(rec)
        log.success()
        log.flush()
        self.assertEqual(rec.calls, [])

    def test_failure_without_open_cell_is_still_traced(self):
        rec = Recorder()
        log = make(rec)
        log.failure(RuntimeError("boom"))
        self.assertEqual([(e["cell_code"], e["status"]) for e in rec.calls[0]], [("UNKNOWN_CELL", "FAILED")])

    def test_redaction_removes_secrets_and_values(self):
        message = redact_error(sensitive_error())
        self.assertTrue(message.startswith("ValueError: "))
        for fragment in SENSITIVE:
            self.assertNotIn(fragment, message)
        rec = Recorder()
        log = make(rec)
        log.start("ACQUIRE")
        log.failure(sensitive_error())
        dumped = repr(rec.calls)
        for fragment in SENSITIVE:
            self.assertNotIn(fragment, dumped)

    def test_import_error_keeps_module_name(self):
        message = redact_error(ModuleNotFoundError("No module named 'pyspark'", name="pyspark"))
        self.assertEqual(message, "ModuleNotFoundError: No module named '***' (module pyspark)")

    def test_error_location_points_to_library_frame(self):
        namespace = {}
        exec(compile("def boom():\n    raise ValueError('x')\n", "hris/fake_module.py", "exec"), namespace)
        try:
            namespace["boom"]()
        except ValueError as error:
            self.assertEqual(error_location(error), "hris/fake_module.py:boom:2")

    def test_event_columns_match_table_definition(self):
        rec = Recorder()
        log = make(rec)
        log.start("CONFIG")
        log.failure(HrisError("X"))
        self.assertEqual(sorted(rec.calls[0][0]), sorted(name for name, _ in TABLES[LOGICAL_TABLE]))

    def test_missing_writer_reports_limit(self):
        log = make(None)
        log.start("CONFIG")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            log.failure(HrisError("X"))
        self.assertIn("aucune destination", out.getvalue())

    def test_spark_writer_is_lazy(self):
        class Untouchable:
            def __getattr__(self, name):
                raise AssertionError("aucune action Spark avant l'écriture")
        self.assertTrue(callable(spark_writer(Untouchable(), lambda logical: logical)))


class CellLogSpark(unittest.TestCase):
    """Fabric uniquement (NB_HRIS_TESTS) : écriture réelle dans le schéma de test isolé, relue par event_id."""

    @classmethod
    def setUpClass(cls):
        try:
            from test_fabric_integration import ENV
        except ImportError:
            raise unittest.SkipTest("hors Fabric")
        if ENV.get("spark") is None:
            raise unittest.SkipTest("hors Fabric")
        from hris import spark_io as IO
        from hris.model import tables_for_notebook
        cls.spark = ENV["spark"]
        cls.T = tables_for_notebook(ENV["notebookutils"].runtime.context, ENV["schema"], ENV["lakehouse"])
        IO.ensure_schemas_and_tables(cls.spark, cls.T, [LOGICAL_TABLE])

    def test_round_trip_in_test_schema(self):
        run_id = "TST-CL-" + uuid.uuid4().hex[:8]
        log = CellLog(CONTEXT, lakehouse_name=self.T.lakehouse, run_id=run_id,
                      writer=spark_writer(self.spark, self.T))
        log.start("CONFIG")
        log.success()
        log.start("TRANSFORM")
        log.failure(HrisError("X", "test"))
        self.assertFalse(log.write_failed)
        rows = self.spark.table(self.T(LOGICAL_TABLE)).where(f"run_id = '{run_id}'").orderBy("cell_seq").collect()
        self.assertEqual([(r.cell_code, r.status, r.error_code) for r in rows],
                         [("CONFIG", "SUCCEEDED", None), ("TRANSFORM", "FAILED", "X")])
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= IMPORTS =============
# Standard libraries
import datetime as dt
import io
import json
import platform
import unittest
from typing import Dict, Iterator

# Internal HRIS library (chargée par %run T4_NB_HRIS_LIB) and test modules (cellules précédentes)
import test_adp_publication
import test_cell_log
import test_contract_markers
import test_fabric_integration
import test_legacy_logs
import test_parity_transform
import test_spark_transform_parity
import test_tables_naming
import test_ts_acquire
from hris.cell_log import CellLog, report_unlogged_failure, spark_writer
from hris.common import as_bool, json_exit
from hris.model import tables_for_notebook
from hris.transform import ensure_session_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_test_schema = "hris_tst"       # schéma isolé des tests ; ne jamais laisser vide (sinon tables réelles)
p_lakehouse_name = ""            # Lakehouse cible (VL_HRIS.lakehouse_name) ; obligatoire, aucun Lakehouse par défaut
p_run_integration = "true"       # true : scénarios d'intégration avec écriture dans le schéma de test ; false : sans écriture de table
p_only = ""                      # filtre facultatif : fragments d'identifiants de tests séparés par des virgules
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
    default_session_timezone = spark.conf.get("spark.sql.session.timeZone", None)    # NV-02, avant tout réglage
    default_ansi_enabled = spark.conf.get("spark.sql.ansi.enabled", None)            # NV-06
    default_time_parser_policy = spark.conf.get("spark.sql.legacy.timeParserPolicy", None)
    runtime_context = notebookutils.runtime.context
    if not p_test_schema:
        raise ValueError("p_test_schema vide : les tests n'écrivent jamais dans les tables réelles")
    tables = tables_for_notebook(runtime_context, p_test_schema, p_lakehouse_name)   # 4 parties : aucun Lakehouse par défaut requis
    cell_log = CellLog(runtime_context, lakehouse_name=tables.lakehouse, log_success=as_bool(p_log_cell_success),
                       writer=spark_writer(spark, tables))
    cell_log.start("CONFIG")
    ensure_session_settings(spark)
    test_fabric_integration.configure(spark, notebookutils, p_test_schema, p_lakehouse_name)
    print(json.dumps({"workspace_name": cell_log.base["workspace_name"], "lakehouse_name": tables.lakehouse,
                      "test_schema": p_test_schema, "notebook_name": cell_log.base["notebook_name"],
                      "operation_id": cell_log.base["operation_id"], "log_cell_success": cell_log.log_success},
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

def iterate_tests(suite: unittest.TestSuite) -> Iterator[unittest.TestCase]:
    """Parcourt récursivement une suite de tests et renvoie chaque test élémentaire."""
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from iterate_tests(test)
        else:
            yield test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def library_versions() -> Dict[str, str]:
    """Versions des bibliothèques du runtime utilisées par le hub (NV-05), ou ABSENT:<erreur>."""
    versions = {}
    for name in ("requests", "openpyxl", "pandas", "pyarrow", "dateutil", "delta"):
        try:
            module = __import__(name)
            versions[name] = getattr(module, "__version__", "présent")
        except Exception as exc:
            versions[name] = "ABSENT:" + type(exc).__name__
    return versions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= EXÉCUTION DES TESTS =============
cell_log.start("RUN_TESTS")
try:
    test_modules = [test_parity_transform, test_ts_acquire, test_adp_publication, test_contract_markers,
                    test_legacy_logs, test_tables_naming, test_spark_transform_parity, test_cell_log]
    run_integration = as_bool(p_run_integration)
    if run_integration:
        test_modules.append(test_fabric_integration)
    suite = unittest.TestSuite()
    only = [fragment.strip() for fragment in p_only.split(",") if fragment.strip()]
    for test_module in test_modules:
        for test in iterate_tests(unittest.defaultTestLoader.loadTestsFromModule(test_module)):
            if not run_integration and "CellLogSpark" in test.id():
                continue                                # écrit dans le schéma de test : réservé à l'intégration
            if not only or any(fragment in test.id() for fragment in only):
                suite.addTest(test)
    stream = io.StringIO()
    outcome = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    print(stream.getvalue()[-20000:])
    result = {"tests_run": outcome.testsRun, "failures": [t.id() for t, _ in outcome.failures],
              "errors": [t.id() for t, _ in outcome.errors], "skipped": [t.id() for t, _ in outcome.skipped],
              "successful": outcome.wasSuccessful(),
              "platform": {"spark": spark.version, "python": platform.python_version(),
                           "NV-02_session_timezone_default": default_session_timezone,
                           "NV-06_ansi_enabled": default_ansi_enabled,
                           "time_parser_policy_default": default_time_parser_policy,
                           "NV-05_libraries": library_versions(),
                           "workspace_name": runtime_context.get("currentWorkspaceName"),
                           "is_for_pipeline": runtime_context.get("isForPipeline")}}
    details = {t.id(): tb[-1500:] for t, tb in outcome.failures + outcome.errors}
    print(json.dumps(result, ensure_ascii=False, indent=1))
    print(json.dumps(details, ensure_ascii=False, indent=1)[:20000])
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

# ============= RAPPORT DE CAMPAGNE (données fictives, schéma de test) =============
cell_log.start("TEST_REPORT")
try:
    report_table = tables.extra("zz_test_report")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {report_table} (run_at_utc TIMESTAMP, report STRING) USING DELTA")
    report_text = json.dumps(dict(result, details=details), ensure_ascii=False, default=str)
    (spark.createDataFrame([(dt.datetime.utcnow(), report_text)], "run_at_utc timestamp, report string")
     .write.format("delta").mode("append").saveAsTable(report_table))
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
notebookutils.notebook.exit(json_exit(dict(result, details={k: v[-600:] for k, v in list(details.items())[:10]})))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
