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
# # Ce notebook vérifie le code du hub sur des données entièrement fictives. Il exécute les tests unitaires des règles de transformation, de l'acquisition TalentSoft contre un faux serveur, de la publication vers l'outil de paie ADP avec des clients simulés, du journal des cellules, des tables de correspondance alimentées par l'API, du flux de rémunération et du câblage du transport ADP (faux clients, aucun réseau). Il vérifie aussi que la transformation Spark donne les mêmes résultats que l'implémentation de référence des règles de l'existant. Il peut enfin dérouler des scénarios d'intégration complets dans un schéma isolé du Lakehouse, sans jamais lire ni écrire une table réelle. Il apporte la preuve, dans le contexte réel du workspace, que le code se comporte comme attendu avant de traiter des données de salariés.
# # **Dépendances**
# # Le notebook charge la bibliothèque commune par la commande %run T4_NB_HRIS_LIB, puis enregistre quatorze modules propres aux tests : le simulateur de l'outil de paie, le jeu de données fictif et douze modules de tests. Ces modules restent dans ce notebook pour qu'aucun simulateur ne soit jamais présent dans le code de production. Le Lakehouse désigné par le paramètre p_lakehouse_name reçoit, dans le schéma isolé hris_tst, des copies de test de toutes les tables, recréées à chaque campagne, ainsi que la table de rapport zz_test_report. Le contexte d'exécution de Fabric fournit le nom réel du workspace, indispensable aux tests du garde-fou de publication.
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
register_hris_module('test_ts_acquire', '4add74e2f2d4a5a22697e443ceda4c693d6528e7ed2bab0da2c74358a565103e', r'''"""TU-07 — acquisition TalentSoft (JDD-01) contre un faux serveur HTTP en mémoire. Aucun appel réseau."""
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
    """

    def __init__(self, total_override=None, scripted=None, n_employees=None, latency=0.0, fail_after=None):
        self.calls = []
        self.scripted = scripted or {}   # chemin (regex) -> liste de statuts à renvoyer d'abord
        self.total_override = total_override
        self.closed = False
        self.latency = latency
        self.fail_after = fail_after
        self._lock = threading.Lock()
        self._in_flight = 0
        self.max_in_flight = 0
        if n_employees:
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
        if path.startswith("/api/v1/directory/employees?count=1"):
            return FakeResp(200, {"totalCount": self.total_override or len(self.employees), "results": []})
        if path.startswith("/api/v1/directory/employees?count="):
            return FakeResp(200, {"totalCount": len(self.employees), "results": self.employees})
        m = re.match(r"/api/hub/v2/employees/(\w+)/(\w+)$", path)
        if m:
            emp, entity = m.groups()
            return FakeResp(200, self.entity(emp, entity))
        m = re.match(r"/api/hub/v2.0/positions/(\w+)\?date=(.*)$", path)
        if m:
            legal = "Motul France" if m.group(1) != "P3" else "Motul SA"
            return FakeResp(200, [{"field": "LegalStructure", "value": legal}, {"field": "Level", "value": "80 %"},
                                  {"field": "FullTimeEquivalentPercent", "value": 0},
                                  {"field": "CostCenter", "value": "CC1"}])
        if path.endswith("/postal-addresses"):
            return FakeResp(200, [{"postalCode": "99000", "city": "Villefictive", "countryCode": "be"}])
        return FakeResp(404, {})

    def entity(self, emp, entity):
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

# ============= MODULE test_reference_api =============
register_hris_module('test_reference_api', '3e2f31814e12d9a926fb8280ee9b8c1af6da63c8676dd16dfa21ce506e36a4aa', r'''"""TU-REF — correspondances sans fichier : partie TalentSoft lue par l'API (faux client), partie ADP saisie par le métier.
Aucun accès réseau ; valeurs FICTIVES."""
import datetime as dt
import json
import unittest
import uuid

from hris import reference_api as R
from hris.common import HrisError
from hris.model import LOOKUP_JOINS, LOOKUP_TABLES, SOURCE_COLUMNS, USED_LOOKUP_TABLES

NOW = dt.datetime(2026, 10, 13, 8, 0, tzinfo=dt.timezone.utc)
TODAY = dt.date(2026, 10, 13)
TECH = {"_loaded_at_utc": NOW, "_loaded_by": "TST", "_change_ref": "talentsoft-api:TEST"}


def lib_rows(library, nodes):
    """Lignes de référentiel au format de l'API (clientCode / field / culture / value)."""
    rows = []
    for code, attrs in nodes.items():
        for culture, label in attrs.get("labels", {}).items():
            rows.append({"startEffectDate": attrs.get("date", "1900-01-01T00:00:00"), "library": library,
                         "clientCode": code, "culture": culture, "field": "ShortName", "value": label})
        for name, value in (("IsActive", attrs.get("active", "Y")), ("ParentClientCode", attrs.get("parent"))):
            if value is not None:
                rows.append({"startEffectDate": "1900-01-01T00:00:00", "library": library, "clientCode": code,
                             "culture": None, "field": name, "value": value})
        if attrs.get("full"):
            rows.append({"startEffectDate": "1900-01-01T00:00:00", "library": library, "clientCode": code,
                         "culture": "fr-fr", "field": "FullName", "value": attrs["full"]})
    return rows


def default_libraries(tag="lib"):
    libraries = {spec.library for spec in R.CATALOGUE.values() if spec.library}
    return {lib: lib_rows(lib, {f"{lib[:4].upper()}{i}": {"labels": {"fr-fr": f"{tag} {lib} {i} fr",
                                                                        "en-gb": f"{tag} {lib} {i} en"}}
                                for i in (1, 2)}) for lib in libraries}


class FakeClient:
    def __init__(self, libraries, fail=()):
        self.libraries, self.fail, self.paths = libraries, set(fail), []

    def get(self, path, endpoint):
        self.paths.append(path)
        library = path.rsplit("/", 1)[1]
        if library in self.fail:
            return None, 503
        return self.libraries.get(library, []), 200


class FakeStore:
    def __init__(self, tables=None, incompatible=(), observed=None):
        self.tables = {t: [dict(r) for r in rows] for t, rows in (tables or {}).items()}
        self.incompatible, self.writes, self.observed_codes = set(incompatible), [], observed or {}

    def read(self, table):
        if table in self.incompatible:
            return R.TableState(True, False, "INCOMPATIBLE: colonne manquante x")
        if table not in self.tables:
            return R.TableState(False, schema_detail="ABSENTE")
        return R.TableState(True, True, "CONFORME", tuple(dict(r) for r in self.tables[table]))

    def write(self, table, rows):
        self.tables[table] = [dict(r) for r in rows]
        self.writes.append(table)

    def observed(self, keys):
        return sorted({v for k in keys for v in self.observed_codes.get(k, [])})


def row(table, **values):
    out = {c: None for c in LOOKUP_TABLES[table]}
    out.update({"_loaded_at_utc": None, "_loaded_by": "AVANT", "_change_ref": "AVANT"})
    out.update(values)
    return out


def sync(store, client=None, write=True):
    client = client or FakeClient(default_libraries())
    return R.synchronize_catalogue(lambda lib: R.fetch_library(client, lib), store, store.observed,
                                   loaded_by="TST:run", now=NOW, write=write, today=TODAY)


class Catalogue(unittest.TestCase):
    def test_every_table_has_a_source(self):
        self.assertEqual(sorted(R.CATALOGUE), sorted(LOOKUP_TABLES))
        for spec in R.CATALOGUE.values():
            self.assertTrue(spec.library or spec.observed_keys, spec.table)

    def test_observed_keys_feed_the_join_sources(self):
        key_to_column = dict(SOURCE_COLUMNS)
        for table in USED_LOOKUP_TABLES:
            spec = R.CATALOGUE[table]
            self.assertEqual(sorted(key_to_column[k] for k in spec.observed_keys),
                             sorted(j["src"] for j in LOOKUP_JOINS if j["table"] == table))
            self.assertIn(spec.code, {j["key"] for j in LOOKUP_JOINS if j["table"] == table})

    def test_adp_and_business_columns(self):
        self.assertEqual(R.adp_columns("lu_hr_emp_titre"), ["cd_emp_titre_adp"])
        self.assertEqual(R.adp_columns("lu_hr_emp_pays"), ["id_emp_pays_adp"])
        self.assertEqual(R.adp_columns("lu_hr_emp_diplome"), ["cd_emp_diplome_adp", "ds_emp_diplome_adp"])
        self.assertNotIn("or_geo_entite_adp", R.adp_columns("lu_hr_geo_entite"))
        self.assertIn("or_geo_entite_adp", R.business_columns("lu_hr_geo_entite"))
        for spec in R.CATALOGUE.values():
            self.assertFalse(set(spec.ts_columns) & set(R.business_columns(spec.table)))


class Library(unittest.TestCase):
    def test_labels_parent_versions(self):
        rows = lib_rows("LegalStructure", {
            "Motul France": {"labels": {"fr-fr": "Motul France SA", "en-gb": "Motul France Ltd"}, "parent": "GROUP"},
            "X": {"labels": {"en-gb": "Only english"}},
            "Y": {"labels": {"de-de": "Nur Deutsch"}, "full": "Libellé long"}})
        rows.append({"startEffectDate": "2099-01-01", "clientCode": "X", "culture": "en-gb", "field": "ShortName",
                     "value": "futur"})
        rows.append({"startEffectDate": "2020-01-01", "clientCode": "X", "culture": "en-gb", "field": "ShortName",
                     "value": "renamed"})
        nodes = R.library_nodes(rows, TODAY)
        self.assertEqual(nodes["Motul France"]["label"], "Motul France SA")
        self.assertEqual(nodes["Motul France"]["label_en"], "Motul France Ltd")
        self.assertEqual(nodes["Motul France"]["parent"], "GROUP")
        self.assertEqual(nodes["X"]["label"], "renamed")
        self.assertIsNone(nodes["X"]["label_fr"])
        self.assertEqual((nodes["Y"]["label"], nodes["Y"]["full"]), ("Nur Deutsch", "Libellé long"))
        self.assertEqual(nodes["X"]["full"], "renamed")

    def test_fetch_library_quotes_and_fails_loudly(self):
        client = FakeClient({"Title": []}, fail={"CostCenter"})
        self.assertEqual(R.fetch_library(client, "Title"), [])
        self.assertTrue(client.paths[-1].endswith("/libraries/Title"))
        with self.assertRaises(R.LibraryUnavailable):
            R.fetch_library(client, "CostCenter")


class Merge(unittest.TestCase):
    spec = R.CATALOGUE["lu_hr_geo_entite"]

    def test_insert_update_never_touches_adp(self):
        existing = [row("lu_hr_geo_entite", cd_geo_entite_ts="Motul France  ", ds_geo_entite_ts="ancien",
                        cd_geo_entite_adp="MF", or_geo_entite_adp="1"),
                    row("lu_hr_geo_entite", cd_geo_entite_ts="Fermée", cd_geo_entite_adp="FX"),
                    row("lu_hr_geo_entite", cd_geo_entite_ts="Inchangée", ds_geo_entite_ts="même")]
        nodes = {"Motul France": {"label": "nouveau", "parent": "GROUP"}, "Inchangée": {"label": "même"},
                 "Neuve": {"label": "neuve"}}
        rows, s = R.merge_catalogue(self.spec, existing, nodes, ["Motul France", "Observée seulement"], TECH)
        by = {r["cd_geo_entite_ts"].rstrip(): r for r in rows}
        self.assertEqual((by["Motul France"]["ds_geo_entite_ts"], by["Motul France"]["cd_geo_parent_entite_ts"]),
                         ("nouveau", "GROUP"))
        self.assertEqual((by["Motul France"]["cd_geo_entite_adp"], by["Motul France"]["or_geo_entite_adp"]), ("MF", "1"))
        self.assertEqual(by["Motul France"]["_change_ref"], "talentsoft-api:TEST")
        self.assertEqual(by["Inchangée"]["_change_ref"], "AVANT")
        self.assertEqual(by["Fermée"]["cd_geo_entite_adp"], "FX")
        self.assertIsNone(by["Neuve"]["cd_geo_entite_adp"])
        self.assertIn("Observée seulement", by)
        self.assertEqual((s.inserted, s.updated, s.kept_absent, s.observed_not_in_library), (2, 1, 1, 1))
        self.assertEqual((s.without_adp, s.rows_after), (3, 5))

    def test_duplicate_keys_are_left_alone(self):
        existing = [row("lu_hr_geo_entite", cd_geo_entite_ts="A"), row("lu_hr_geo_entite", cd_geo_entite_ts="A ")]
        rows, s = R.merge_catalogue(self.spec, existing, {"B": {"label": "b"}}, [], TECH)
        self.assertEqual((s.status, len(rows)), ("SKIPPED_DUPLICATE_KEYS", 2))

    def test_long_codes_counted(self):
        _, s = R.merge_catalogue(self.spec, [], {"Z" * 51: {"label": "z"}}, [], TECH)
        self.assertEqual(s.codes_over_50, 1)

    def test_observed_from_records(self):
        records = [{"Gender": "M "}, {"Gender": "F"}, {"Gender": ""}, {"Gender": None}, {"Gender": "M"}]
        self.assertEqual(R.observed_from_records(records, ["Gender"]), ["F", "M"])


class Synchronize(unittest.TestCase):
    def test_first_run_fills_all_tables_without_adp(self):
        store = FakeStore(observed={"Gender": ["M", "F"], "Nationality": ["FRA"], "Pays_de_naissance": ["FRA"],
                                    "adresse_countryCode": ["FR"], "LegalCountry": ["FRA"]})
        report = sync(store)
        self.assertEqual(report["status"], "OK")
        self.assertEqual(len(report["tables_written"]), 17)
        self.assertEqual(sorted(r["cd_emp_sexe_ts"] for r in store.tables["lu_hr_emp_sexe"]), ["F", "M"])
        self.assertEqual(sorted(r["cd_emp_pays_ts"] for r in store.tables["lu_hr_emp_pays"]), ["FR", "FRA"])
        self.assertEqual(sorted(report["used_tables_without_any_adp"]), USED_LOOKUP_TABLES)
        titre = store.tables["lu_hr_emp_titre"][0]
        self.assertEqual((titre["_loaded_by"], titre["_change_ref"]), ("TST:run", "talentsoft-api:Title"))
        self.assertEqual(store.tables["lu_hr_emp_sexe"][0]["_change_ref"], "talentsoft-api:extraction:Gender")

    def test_second_run_writes_nothing(self):
        store = FakeStore()
        sync(store)
        store.writes.clear()
        report = sync(store)
        self.assertEqual((store.writes, report["inserted"], report["updated"]), ([], 0, 0))

    def test_library_failure_skips_only_its_tables(self):
        store = FakeStore()
        report = sync(store, FakeClient(default_libraries(), fail={"NatureOfContract"}))
        self.assertEqual(report["status"], "PARTIAL")
        self.assertEqual(report["tables_skipped"], {"lu_hr_ppc_contrat2": "SKIPPED_LIBRARY_UNAVAILABLE",
                                                    "lu_hr_ppc_contrat": "SKIPPED_LIBRARY_UNAVAILABLE"})
        self.assertNotIn("lu_hr_ppc_contrat2", store.writes)
        self.assertEqual(len(store.writes), 15)

    def test_incompatible_table_never_written(self):
        store = FakeStore(incompatible={"lu_hr_ppc_classes"})
        report = sync(store)
        self.assertEqual(report["tables_skipped"], {"lu_hr_ppc_classes": "SKIPPED_SCHEMA"})
        self.assertNotIn("lu_hr_ppc_classes", store.writes)

    def test_check_mode_writes_nothing(self):
        store = FakeStore()
        report = sync(store, write=False)
        self.assertEqual((store.writes, report["writes_enabled"]), ([], False))
        self.assertGreater(report["inserted"], 0)

    def test_adp_codes_survive_label_changes(self):
        store = FakeStore()
        sync(store)
        for r in store.tables["lu_hr_emp_titre"]:
            r["cd_emp_titre_adp"] = "ADP-" + r["cd_emp_titre_ts"]
        sync(store, FakeClient(default_libraries(tag="renamed")))
        self.assertTrue(all(r["cd_emp_titre_adp"] == "ADP-" + r["cd_emp_titre_ts"] for r in store.tables["lu_hr_emp_titre"]))
        self.assertTrue(all(r["lb_emp_titre_ts"].startswith("renamed") for r in store.tables["lu_hr_emp_titre"]))


class Admin(unittest.TestCase):
    def rows(self):
        return [row("lu_hr_emp_titre", cd_emp_titre_ts="MR", lb_emp_titre_ts="Monsieur"),
                row("lu_hr_emp_titre", cd_emp_titre_ts="MS", lb_emp_titre_ts="Madame")]

    def test_assign_adp_codes(self):
        rows, result = R.apply_assignments("lu_hr_emp_titre", self.rows(),
                                           {"MR": {"cd_emp_titre_adp": "1", "ds_emp_titre_adp": "M."}},
                                           {"_loaded_by": "ADMIN", "_change_ref": "CHG-42", "_loaded_at_utc": NOW})
        self.assertEqual(result.errors, [])
        self.assertEqual(result.changes, [("MR", "cd_emp_titre_adp", None, "1"), ("MR", "ds_emp_titre_adp", None, "M.")])
        self.assertEqual((rows[0]["cd_emp_titre_adp"], rows[0]["_change_ref"]), ("1", "CHG-42"))
        self.assertEqual(rows[1]["_change_ref"], "AVANT")

    def test_errors_change_nothing(self):
        bad = {"MR": {"cd_emp_titre_adp": "1"}, "XX": {"cd_emp_titre_adp": "9"}, "MS": {"lb_emp_titre_ts": "x"}}
        rows, result = R.apply_assignments("lu_hr_emp_titre", self.rows(), bad, TECH)
        self.assertEqual(len(result.errors), 2)
        self.assertEqual((rows[0]["cd_emp_titre_adp"], result.changes), (None, []))
        for assignments in ({"MR": {"_change_ref": "x"}}, {"MR": {"cd_emp_titre_ts": "x"}}, {"MR": {}},
                            {"MR": {"cd_emp_titre_adp": True}}, {"MR": {"cd_emp_titre_adp": ["1"]}}):
            self.assertTrue(R.apply_assignments("lu_hr_emp_titre", self.rows(), assignments, TECH)[1].errors)
        self.assertTrue(R.apply_assignments("lu_hr_inconnue", self.rows(), {}, TECH)[1].errors)
        duplicated = self.rows() + [row("lu_hr_emp_titre", cd_emp_titre_ts="MR ")]
        self.assertTrue(R.apply_assignments("lu_hr_emp_titre", duplicated, {"MS": {"cd_emp_titre_adp": "2"}}, TECH)[1].errors)

    def test_empty_string_clears_and_missing_report(self):
        rows, _ = R.apply_assignments("lu_hr_emp_titre", self.rows(), {"MR": {"cd_emp_titre_adp": "1"}}, TECH)
        rows, result = R.apply_assignments("lu_hr_emp_titre", rows, {"MR": {"cd_emp_titre_adp": ""}}, TECH)
        self.assertEqual(result.changes, [("MR", "cd_emp_titre_adp", "1", None)])
        missing = R.missing_adp("lu_hr_emp_titre", rows, {"MS": 7, "MR": 3})
        self.assertEqual([(m["code"], m["label"], m["employees"]) for m in missing],
                         [("MS", "Madame", 7), ("MR", "Monsieur", 3)])


class Reporting(unittest.TestCase):
    def test_anomalies_never_blocking_and_messages_without_values(self):
        store = FakeStore()
        report = sync(store, FakeClient(default_libraries(tag="LIBELLESECRET"), fail={"CostCenter"}))
        anomalies = R.catalogue_anomalies(report) + R.catalogue_anomalies(None, "TS_AUTH_FAILED")
        self.assertTrue(all(a["severity"] != "BLOCKING" for a in anomalies))
        self.assertEqual({a["control_code"] for a in anomalies}, set(R.CATALOGUE_CONTROLS))
        message = R.step_message(report)
        json.loads(message)
        self.assertNotIn("LIBELLESECRET", message + json.dumps(anomalies, default=str))
        self.assertLessEqual(len(message), 7800)


class ReferenceStoreSpark(unittest.TestCase):
    """Fabric uniquement (NB_HRIS_TESTS) : aller-retour Delta dans le schéma de test isolé, table non utilisée."""
    table = "lu_hr_emp_diplome"

    @classmethod
    def setUpClass(cls):
        try:
            from test_fabric_integration import ENV
        except ImportError:
            raise unittest.SkipTest("hors Fabric")
        if ENV.get("spark") is None:
            raise unittest.SkipTest("hors Fabric")
        from hris.model import tables_for_notebook
        cls.spark = ENV["spark"]
        cls.T = tables_for_notebook(ENV["notebookutils"].runtime.context, ENV["schema"], ENV["lakehouse"])
        if not cls.T.test_schema:
            raise unittest.SkipTest("schéma de test absent")
        for schema in cls.T.schemas():
            cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cls.spark.sql("DROP TABLE IF EXISTS " + cls.T("cfg." + cls.table))   # table de TEST (schéma isolé)

    def test_create_merge_assign_round_trip(self):
        store = R.SparkReferenceStore(self.spark, self.T)
        self.assertFalse(store.read(self.table).exists)
        spec = R.CATALOGUE[self.table]
        ref = "TST-" + uuid.uuid4().hex[:8]
        rows, _ = R.merge_catalogue(spec, [], {"D1": {"label": "un"}, "D2": {"label": "deux"}}, [],
                                    {"_loaded_at_utc": NOW, "_loaded_by": "TST", "_change_ref": ref})
        store.write(self.table, rows)
        state = store.read(self.table)
        self.assertEqual(sorted(r["cd_emp_diplome_ts"] for r in state.rows), ["D1", "D2"])
        rows, result = R.apply_assignments(self.table, state.rows, {"D1": {"cd_emp_diplome_adp": "A"}},
                                           {"_loaded_at_utc": NOW, "_loaded_by": "TST", "_change_ref": ref + "-ADP"})
        self.assertEqual(result.errors, [])
        store.write(self.table, rows)
        by = {r["cd_emp_diplome_ts"]: r for r in store.read(self.table).rows}
        self.assertEqual((by["D1"]["cd_emp_diplome_adp"], by["D1"]["_change_ref"]), ("A", ref + "-ADP"))
        self.assertIsNone(by["D2"]["cd_emp_diplome_adp"])


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_remuneration =============
register_hris_module('test_remuneration', 'dbd5b0991d02eb5cdcae699a537ea4469828106125cd24bf6ab22a34c5c1941c', r'''"""TU-REM — flux rémunération (ex-Pipeline_SIRH-SFTP) en tables : règles des vues SQL, instantané, envoi gardé.
Faux client API et faux stockage : aucun réseau, aucune table. Matricules et montants FICTIFS."""
import datetime as dt
import io
import json
import unittest
import zipfile
from decimal import Decimal

from hris import remuneration as R
from hris.common import HrisError

NOW = dt.datetime(2026, 10, 14, 6, 0, tzinfo=dt.timezone.utc)
PRD = lambda: {"currentWorkspaceName": "HRIS PRD", "currentWorkspaceId": "w", "isForPipeline": True}   # noqa: E731
DEV = lambda: {"currentWorkspaceName": "Sandbox DEV", "currentWorkspaceId": "w", "isForPipeline": True}   # noqa: E731


def base(number, amount, date="01/01/2026", currency="EUR"):
    return {"employeenumber": number, "date": date, "type": "ANNUAL_BASE_SALARY", "amount": amount,
            "currency": currency, "periodicity": "annual"}


def bonus(number, value, start="01/01/2026", end=None, kind="CONTRACTUAL_BONUS_PERCENTAGE"):
    return {"employeenumber": number, "type": kind, "start_date": start, "end_date": end, "calculation_method": "PERCENTAGE",
            "value": value, "currency": "XBO", "base_salary_effective_date": "01/01/2026", "periodicity": "annual"}


class BaseSalaryRules(unittest.TestCase):
    def test_amount_change_new_zero_and_date(self):
        current = [base("FR000001", "33000"), base("FR000002", "40000"), base("FR000003", "0"),
                   base("FR000004", "25000", date="01/03/2026"), base("FR000005", None), base("FR000006", "50000"),
                   base("FR000007", None)]
        previous = [base("FR000001", "30000"), base("FR000004", "25000"), base("FR000005", None, date="02/01/2026"),
                    base("FR000006", "50000"), base("FR000007", "28000")]
        rows = {r["employeenumber"]: r for r in R.base_salary_changes(current, previous)}
        self.assertEqual(rows["FR000001"]["amount"], "33000,00")
        self.assertEqual(rows["FR000001"]["extra_augm_n-1"], "10,00")
        self.assertIsNone(rows["FR000002"]["extra_augm_n-1"])          # nouveau : pas de J-1
        self.assertNotIn("FR000003", rows)                               # nouveau à 0 : non retenu (comme SQL)
        self.assertEqual(rows["FR000004"]["extra_augm_n-1"], "0,00")     # date seule changée
        self.assertIsNone(rows["FR000005"]["amount"])                    # date changée, montant NULL
        self.assertNotIn("FR000006", rows)
        self.assertNotIn("FR000007", rows)                               # montant devenu NULL : non retenu (comme SQL)

    def test_rounding_negative_keys_and_duplicates(self):
        self.assertEqual(R.fmt_decimal(R.to_decimal("10.005")), "10,01")
        self.assertEqual(R.fmt_decimal(R.to_decimal("-1.005")), "-1,01")
        self.assertEqual(R.fmt_decimal(R.to_decimal("-0.001")), "0,00")
        rows = R.base_salary_changes([base("FR000001 ", "110")], [base("FR000001", "100"), base("FR000001", "105")])
        self.assertEqual(len(rows), 2)                                    # doublon J-1 : lignes multipliées
        with self.assertRaises(HrisError) as ctx:
            R.base_salary_changes([base("FR000009", "12 000")], [])
        self.assertEqual(ctx.exception.code, "CONVERSION_NUMERIC")
        self.assertNotIn("12", str(ctx.exception))

    def test_increase_pct(self):
        D = Decimal
        self.assertEqual(R.increase_pct(D("110.00"), D("100.00")), "10,00")
        self.assertEqual(R.increase_pct(D("33.33"), D("100.00")), "-66,67")
        self.assertEqual(R.increase_pct(D("100.50"), D("100.00")), "0,50")
        self.assertIsNone(R.increase_pct(D("100.00"), D("0.00")))
        self.assertEqual(R.increase_pct(D("2.00"), D("3.00")), "-33,33")


class BonusRules(unittest.TestCase):
    def test_string_comparison_closures_and_currency(self):
        base_j = [base("FR000001", "1", currency="EUR"), base("FR000002", "1", currency="USD")]
        current = [bonus("FR000001", "10.00"), bonus("FR000002", "5", start="01/03/2026"),
                   bonus("FR000003", None, start=None)]
        previous = [bonus("FR000001", "10.0"), bonus("FR000002", "5", start="01/01/2026"), bonus("FR000003", "7")]
        rows = R.bonus_changes(current, previous, [], [], base_j)
        by_rule = {(r["employeenumber"], r["rule"]): r for r in rows}
        self.assertIn(("FR000001", "R-SFTP-020"), by_rule)                # '10.00' <> '10.0' : comparaison de chaînes
        self.assertEqual(by_rule[("FR000001", "R-SFTP-020")]["value"], "10,00")
        self.assertEqual(by_rule[("FR000001", "R-SFTP-020")]["currency"], "EUR")   # du salaire de base, pas du bonus
        closing = by_rule[("FR000002", "R-SFTP-021")]
        self.assertEqual((closing["start_date"], closing["end_date"]), ("01/01/2026", "28/02/2026 00:00:00"))
        self.assertEqual(closing["currency"], "USD")                      # devise du salaire de base du jour
        self.assertIsNone(by_rule[("FR000003", "R-SFTP-022")]["end_date"])   # anomalie conservée
        self.assertIsNone(by_rule[("FR000003", "R-SFTP-022")]["currency"])   # aucun salaire de base

    def test_new_without_value_or_zero_and_union(self):
        rows = R.bonus_changes([bonus("FR000001", "0"), bonus("FR000002", "3")], [], [], [], [])
        self.assertEqual([r["employeenumber"] for r in rows], ["FR000002"])   # ISNULL(J1, 0) = '0' : '0' non retenu
        twice = R.bonus_changes([bonus("FR000001", "3"), bonus("FR000001", "3")], [], [], [], [])
        self.assertEqual(len(twice), 1)                                    # UNION : doublons exacts retirés
        target = R.bonus_changes([], [], [bonus("FR000004", "1500", kind="CONTRACTUAL BONUS")], [], [])
        self.assertEqual(target[0]["value"], "1500,00")

    def test_closing_date_formats(self):
        self.assertEqual(R.closing_end_date("2026-03-01"), "28/02/2026 00:00:00")
        self.assertEqual(R.closing_end_date("01/01/2026 00:00:00"), "31/12/2025 00:00:00")
        self.assertIsNone(R.closing_end_date(None))


EXPORT = [
    {"EmployeeNumber": "FR000001", "Section": "{{ANNUAL_BASE_SALARY}}", "StartDate": "2026-01-01T00:00:00",
     "EndDate": None, "Amount": 33000.0, "Currency": "eur", "Periodicity": "annual", "Category": "baseSalary"},
    {"EmployeeNumber": "FR000001", "Section": "{{CONTRACTUAL_BONUS_PERCENTAGE}}", "StartDate": "2026-02-01",
     "EndDate": "", "Amount": "10,5", "Currency": "eur", "Periodicity": "annual", "Category": "bonus"},
    {"EmployeeNumber": "FR000002", "Section": "{{CONTRACTUAL BONUS}}", "StartDate": "2026-03-01",
     "Amount": "1500", "Currency": "eur", "Periodicity": "annual"},
    {"EmployeeNumber": "FR000003", "Section": "{{ALLOWANCES}}", "StartDate": "2026-03-01", "Amount": "50"},
]


class Snapshot(unittest.TestCase):
    def test_build_snapshot_default_mapping(self):
        rows = R.parse_export(json.dumps(EXPORT).encode())
        snapshot, stats = R.build_snapshot(rows, R.DEFAULT_MAPPING)
        self.assertEqual(stats["export_rows_ignored"], 1)
        salary = snapshot[R.BASE_SALARY][0]
        self.assertEqual((salary["date"], salary["amount"], salary["currency"], salary["type"]),
                         ("01/01/2026", "33000", "EUR", "ANNUAL_BASE_SALARY"))
        percent = snapshot[R.BONUS_PERCENT][0]
        self.assertEqual((percent["value"], percent["end_date"], percent["base_salary_effective_date"],
                          percent["calculation_method"]), ("10.5", None, "01/01/2026", "PERCENTAGE"))
        target = snapshot[R.BONUS_TARGET_VALUE][0]
        self.assertEqual(target["base_salary_effective_date"], "01/03/2026")   # pas de salaire : date de début

    def test_mapping_overrides(self):
        rules = R.rules_from_rows([dict(r, type_value="Salaire", date_format="%Y-%m-%d", currency_case="raw")
                                   if r["legacy_section"] == R.BASE_SALARY else r
                                   for r in R.mapping_rows(R.DEFAULT_MAPPING)])
        snapshot, _ = R.build_snapshot(R.parse_export(json.dumps(EXPORT).encode()), rules)
        salary = snapshot[R.BASE_SALARY][0]
        self.assertEqual((salary["type"], salary["date"], salary["currency"]), ("Salaire", "2026-01-01", "eur"))
        with self.assertRaises(HrisError):
            R.rules_from_rows([{"section_code": "X", "legacy_section": "AUTRE"}])

    def test_parse_formats(self):
        csv_text = "Employee number;Section;Start date;Amount;Currency;Frequency\r\nFR000001;ANNUAL_BASE_SALARY;01/01/2026;10;eur;annual\r\n"
        rows = R.parse_export(csv_text.encode("utf-8"))
        self.assertEqual((rows[0]["employeenumber"], rows[0]["periodicity"]), ("FR000001", "annual"))
        self.assertEqual(R.parse_export(json.dumps({"data": EXPORT[:1]}).encode())[0]["section"], "{{ANNUAL_BASE_SALARY}}")
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("export.csv", csv_text)
        self.assertEqual(len(R.parse_export(buffer.getvalue())), 1)
        with self.assertRaises(HrisError) as ctx:
            R.parse_export(b"Nom;Montant\r\nFR000001;10\r\n")
        self.assertEqual(ctx.exception.code, "TS_EXPORT_FORMAT")
        self.assertNotIn("FR000001", str(ctx.exception))


class ImportContent(unittest.TestCase):
    def test_legacy_layout(self):
        rows = [{"employeenumber": "FR000001", "date": "01/01/2026", "type": 'A"B', "amount": "1,00", "currency": None,
                 "periodicity": "a\\b", "extra_augm_n-1": None}]
        text = R.import_content(R.IMPORT_SALARY, rows).decode("utf-8")
        lines = text.split("\r\n")
        self.assertEqual(lines[0], '"employeenumber";"date";"type";"amount";"currency";"periodicity";"extra_augm_n-1"')
        self.assertEqual(lines[1], '"FR000001";"01/01/2026";"A\\"B";"1,00";;"a\\\\b";')
        self.assertTrue(text.endswith("\r\n"))
        self.assertEqual(R.import_file_name(R.IMPORT_BONUS, dt.date(2026, 10, 14)),
                         "RemunTargetBonusHistoIE_InsertAndUpdate_fr-FR_2_14102026.csv")


class FakeApi:
    def __init__(self, export_rows, statuses=("Running", "Successful"), import_status="Success"):
        self.export_rows, self.statuses, self.import_status_value = export_rows, list(statuses), import_status
        self.submitted, self.created = [], []

    def create_export(self, payload):
        self.created.append(payload)
        return 42

    def export_status(self, export_id):
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    def export_content(self, export_id):
        return json.dumps(self.export_rows).encode(), "application/json"

    def submit_import(self, import_type, file_name, content):
        self.submitted.append((import_type, file_name, content))
        return 100 + len(self.submitted)

    def import_status(self, import_id):
        return self.import_status_value if not isinstance(self.import_status_value, dict) \
            else self.import_status_value[import_id]


class FakeStore:
    def __init__(self, reference=None):
        self.ref = {s: list(v) for s, v in (reference or {}).items()}
        self.snapshots, self.changes, self.logs, self.advanced = {}, {}, {}, []

    def mapping(self):
        return list(R.DEFAULT_MAPPING)

    def reference(self):
        return {s: list(self.ref.get(s, [])) for s in R.SECTIONS}

    def save_snapshot(self, run_id, business_date, snapshot):
        self.snapshots[run_id] = snapshot

    def save_changes(self, run_id, business_date, changes):
        self.changes[run_id] = changes

    def advance_reference(self, run_id, sections, snapshot):
        self.advanced.append((run_id, tuple(sections)))
        for s in sections:
            self.ref[s] = list(snapshot[s])

    def log_imports(self, run_id, entries):
        self.logs[run_id] = entries


def run(store, api, context=PRD, enabled=True, run_id="RUN-2", follow=None):
    kwargs = {"follow": follow} if follow else {}
    return R.run_remuneration(api, store, context, run_id=run_id, business_date=dt.date(2026, 10, 14),
                              import_enabled=enabled, now=NOW,
                              extract=lambda a, only_active_employees: R.parse_export(json.dumps(a.export_rows).encode()),
                              **kwargs)


def follow_fake(api, import_type, file_name, content):
    return api.submit_import(import_type, file_name, content), api.import_status(100 + len(api.submitted) + 1)


def follow_ok(api, import_type, file_name, content):
    import_id = api.submit_import(import_type, file_name, content)
    return import_id, "Success"


class Orchestration(unittest.TestCase):
    def changed_export(self):
        rows = json.loads(json.dumps(EXPORT))
        rows[0]["Amount"] = 36300.0
        rows[1]["Amount"] = "12"
        return rows

    def initialised(self):
        store = FakeStore()
        run(store, FakeApi(EXPORT), run_id="RUN-1")
        return store

    def test_first_run_initialises_reference_without_sending(self):
        store, api = FakeStore(), FakeApi(EXPORT)
        result = run(store, api, run_id="RUN-1")
        self.assertTrue(result["first_run"])
        self.assertEqual(result["imports"], {R.IMPORT_SALARY: R.INIT_REFERENCE, R.IMPORT_BONUS: R.INIT_REFERENCE})
        self.assertEqual(api.submitted, [])
        self.assertEqual(len(store.ref[R.BASE_SALARY]), 1)

    def test_prd_enabled_sends_legacy_content_and_advances(self):
        store = self.initialised()
        api = FakeApi(self.changed_export())
        result = run(store, api, follow=follow_ok)
        self.assertEqual(result["imports"], {R.IMPORT_SALARY: R.SENT, R.IMPORT_BONUS: R.SENT})
        salary = [s for s in api.submitted if s[0] == R.IMPORT_SALARY][0]
        self.assertEqual(salary[1], "RemunSalaryHistoIE_InsertAndUpdate_fr-FR_1_14102026.csv")
        self.assertIn('"36300,00"', salary[2].decode())
        self.assertIn('"10,00"', salary[2].decode())                      # [extra_augm_n-1]
        self.assertEqual(result["summary"]["baseSalary_nb_inserts"], 1)
        self.assertEqual(store.ref[R.BASE_SALARY][0]["amount"], "36300")
        self.assertTrue(all(e["reference_advanced"] for e in store.logs["RUN-2"]))

    def test_non_prd_computes_without_sending(self):
        store = self.initialised()
        api = FakeApi(self.changed_export())
        result = run(store, api, context=DEV)
        self.assertEqual(set(result["imports"].values()), {R.NOT_PUSHED_NON_PRD})
        self.assertEqual(api.submitted, [])
        self.assertEqual(len(store.changes["RUN-2"][R.IMPORT_SALARY]), 1)

    def test_disabled_keeps_reference_for_next_run(self):
        store = self.initialised()
        result = run(store, FakeApi(self.changed_export()), enabled=False)
        self.assertEqual(set(result["imports"].values()), {R.DISABLED})
        self.assertEqual(store.ref[R.BASE_SALARY][0]["amount"], "33000")   # référence inchangée : rien n'est perdu

    def test_failed_import_is_logged_then_raised(self):
        store = self.initialised()
        api = FakeApi(self.changed_export())

        def follow(a, import_type, file_name, content):
            import_id = a.submit_import(import_type, file_name, content)
            return import_id, ("Failed" if import_type == R.IMPORT_BONUS else "Success")
        with self.assertRaises(R.RemunerationFailed) as ctx:
            run(store, api, follow=follow)
        self.assertEqual(ctx.exception.result["imports"], {R.IMPORT_SALARY: R.SENT, R.IMPORT_BONUS: R.FAILED})
        logs = {e["import_type"]: e for e in store.logs["RUN-2"]}
        self.assertTrue(logs[R.IMPORT_SALARY]["reference_advanced"])
        self.assertFalse(logs[R.IMPORT_BONUS]["reference_advanced"])
        self.assertEqual(store.ref[R.BONUS_PERCENT][0]["value"], "10.5")

    def test_no_change_and_unresolved_workspace(self):
        store = self.initialised()
        result = run(store, FakeApi(EXPORT))
        self.assertEqual(set(result["imports"].values()), {R.EMPTY})
        store = self.initialised()
        result = run(store, FakeApi(self.changed_export()), context=lambda: {})
        self.assertEqual(set(result["imports"].values()), {R.BLOCKED_UNRESOLVED})
        self.assertFalse(any(e["reference_advanced"] for e in store.logs["RUN-2"]))

    def test_logs_and_result_carry_no_values(self):
        store = self.initialised()
        result = run(store, FakeApi(self.changed_export()), follow=follow_ok)
        text = json.dumps([result, store.logs["RUN-2"]], default=str) + R.step_message(result)
        for secret in ("FR000001", "36300", "33000"):
            self.assertNotIn(secret, text)


class Polling(unittest.TestCase):
    def test_export_polling(self):
        sleeps = []
        api = FakeApi(EXPORT, statuses=["Pending", "Running", "Successful"])
        rows = R.extract_compensation(api, sleep=sleeps.append, clock=lambda: 0)
        self.assertEqual(len(rows), 4)
        self.assertEqual(len(sleeps), 2)
        self.assertFalse(api.created[0]["EmployeeFilters"]["ExportOnlyActiveEmployees"])
        self.assertTrue(api.created[0]["RecordFilters"]["ExportOnlyCurrentRecords"])
        with self.assertRaises(HrisError) as ctx:
            R.extract_compensation(FakeApi(EXPORT, statuses=["Failed"]), sleep=lambda s: None)
        self.assertEqual(ctx.exception.code, "TS_EXPORT_FAILED")
        ticks = iter([0, 0, 5000, 5000])
        with self.assertRaises(HrisError) as ctx:
            R.extract_compensation(FakeApi(EXPORT, statuses=["Running"]), sleep=lambda s: None,
                                   clock=lambda: next(ticks), timeout_seconds=10)
        self.assertEqual(ctx.exception.code, "TS_EXPORT_TIMEOUT")

    def test_import_follow(self):
        api = FakeApi(EXPORT, import_status="Success")
        self.assertEqual(R.submit_and_follow(api, R.IMPORT_SALARY, "f.csv", b"x", sleep=lambda s: None)[1], "Success")
        api = FakeApi(EXPORT, import_status="InProgress")
        ticks = iter([0, 0, 99999])
        self.assertEqual(R.submit_and_follow(api, R.IMPORT_SALARY, "f.csv", b"x", sleep=lambda s: None,
                                             clock=lambda: next(ticks), timeout_seconds=10)[1], "InProgress")


class Structure(unittest.TestCase):
    def test_compare_codes_and_shapes_without_values(self):
        produced = R.content_structure(R.import_content(R.IMPORT_SALARY, [
            {"employeenumber": "FR000001", "date": "01/01/2026", "type": "ANNUAL_BASE_SALARY", "amount": "1,00",
             "currency": "EUR", "periodicity": "annual", "extra_augm_n-1": None}]))
        legacy = R.content_structure(
            '"employeenumber";"date";"type";"amount";"currency";"periodicity";"extra_augm_n-1"\r\n'
            '"FR000002";"01/01/2026 00:00:00";"Salaire de base";"2,00";"EUR";"Annual";"0,00"\r\n'.encode())
        gaps = R.compare_structures(produced, legacy)
        self.assertTrue(any(g.startswith("type") for g in gaps))
        self.assertTrue(any(g.startswith("periodicity") for g in gaps))
        self.assertTrue(any(g.startswith("date") for g in gaps))
        self.assertFalse(any(g.startswith("currency") for g in gaps))
        self.assertNotIn("FR00000", json.dumps([produced, legacy, gaps]))


class FakeResponse:
    def __init__(self, status, payload=None, content=b"", headers=None):
        self.status_code, self._payload, self.content, self.headers = status, payload, content, headers or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class FakeClient:
    base_url = "https://motul.talent-soft.com"

    def __init__(self, responses):
        self._session, self.tokens = FakeSession(responses), 0

    def _auth_headers(self):
        return {"Authorization": "Bearer t", "Accept": "application/json"}

    def get_token(self):
        self.tokens += 1


class ApiClient(unittest.TestCase):
    def test_export_creation_retry_on_401_and_import_multipart(self):
        client = FakeClient([FakeResponse(401), FakeResponse(200, 7), FakeResponse(200, 3),
                             FakeResponse(200, {"ImportId": 55}), FakeResponse(200, {"StatusCode": "Success"})])
        api = R.TalentSoftApi(client)
        self.assertEqual(api.create_export(R.export_request(False)), 7)
        self.assertEqual(client.tokens, 1)
        method, url, kwargs = client._session.calls[1]
        self.assertEqual((method, url), ("POST", "https://motul.talent-soft.com/api/hub/v3.0/exports/datasets"))
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(api.export_status(7), "Successful")
        self.assertEqual(api.submit_import(R.IMPORT_SALARY, "f.csv", b"x"), 55)
        _, url, kwargs = client._session.calls[3]
        self.assertTrue(url.endswith("/api/administration/v1.0/imports"))
        self.assertEqual(kwargs["files"]["file"][0], "f.csv")
        self.assertEqual(kwargs["data"]["importTypeCode"], R.IMPORT_SALARY)
        self.assertEqual(api.import_status(55), "Success")
        with self.assertRaises(HrisError):
            R.TalentSoftApi(FakeClient([FakeResponse(500)])).create_export({})


class RemunerationStoreSpark(unittest.TestCase):
    """Fabric uniquement (NB_HRIS_TESTS) : instantané, changements, référence et journal en Delta, schéma de test isolé."""

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
        if not cls.T.test_schema:
            raise unittest.SkipTest("schéma de test absent")
        logicals = ["cfg.ts_remuneration_mapping", "stg.ts_remuneration_snapshot", "ref.ts_remuneration_snapshot",
                    "pub.ts_remuneration_change", "log.ts_remuneration_import"]
        for logical in logicals:
            cls.spark.sql("DROP TABLE IF EXISTS " + cls.T(logical))      # tables de TEST (schéma isolé)
        IO.ensure_schemas_and_tables(cls.spark, cls.T, logicals)

    def test_round_trip(self):
        store = R.SparkRemunerationStore(self.spark, self.T)
        snapshot, _ = R.build_snapshot(R.parse_export(json.dumps(EXPORT).encode()), R.DEFAULT_MAPPING)
        store.save_snapshot("TST-REM-1", dt.date(2026, 10, 14), snapshot)
        store.advance_reference("TST-REM-1", R.SECTIONS, snapshot)
        reference = store.reference()
        self.assertEqual({s: len(reference[s]) for s in R.SECTIONS}, {s: len(snapshot[s]) for s in R.SECTIONS})
        changed = json.loads(json.dumps(EXPORT))
        changed[0]["Amount"] = 36300.0
        snapshot2, _ = R.build_snapshot(R.parse_export(json.dumps(changed).encode()), R.DEFAULT_MAPPING)
        changes = {R.IMPORT_SALARY: R.base_salary_changes(snapshot2[R.BASE_SALARY], reference[R.BASE_SALARY]),
                   R.IMPORT_BONUS: []}
        store.save_changes("TST-REM-2", dt.date(2026, 10, 15), changes)
        store.advance_reference("TST-REM-2", (R.BASE_SALARY,), snapshot2)
        store.log_imports("TST-REM-2", [{"run_id": "TST-REM-2", "import_type": R.IMPORT_SALARY, "rows": 1,
                                         "status": R.SENT, "reference_advanced": True, "created_at_utc": NOW}])
        self.assertEqual(store.reference()[R.BASE_SALARY][0]["amount"], "36300")
        self.assertEqual(len(store.reference()[R.BONUS_PERCENT]), 1)              # bonus non touchés
        saved = self.spark.table(self.T("pub.ts_remuneration_change")).where("run_id = 'TST-REM-2'").collect()
        self.assertEqual((saved[0]["amount"], saved[0]["extra_augm_n_1"]), ("36300,00", "10,00"))


if __name__ == "__main__":
    unittest.main()
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============= MODULE test_adp_wiring =============
register_hris_module('test_adp_wiring', 'bdc098e3285d8579c97c0188f9d592add896431520bb3ebbf847577771a4d06d', r'''"""TU-ADP-W — câblage du transport ADP réel : encodage des chemins (défaut latent corrigé), jeton OAuth, fabrique.
Aucun réseau : connexions factices ; le verrou d'activation n'est levé que le temps d'un test, puis restauré."""
import http.client
import json
import os
import ssl
import tempfile
import unittest

from hris import adp_spark as S
from hris import adp_transport as TR
from hris.common import HrisError, SecretValue

FILTER_PATH = "hr/v2/workers?$filter=workers/workerID/idValue eq 'FR000001'"


class RequestTarget(unittest.TestCase):
    def test_spaces_encoded_reserved_kept(self):
        self.assertEqual(TR.request_target("/", FILTER_PATH),
                         "/hr/v2/workers?$filter=workers/workerID/idValue%20eq%20'FR000001'")
        self.assertEqual(TR.request_target("/", "events/hr/v1/worker.hire"), "/events/hr/v1/worker.hire")

    def test_same_target_as_requests(self):
        try:
            import requests
        except ImportError:
            self.skipTest("requests absent")
        for path in (FILTER_PATH, "payroll/v2/workers/G3ABC/pay-distributions", "hr/v2/workers/FR000001"):
            prepared = requests.Request("GET", "https://accounts.eu.adp.com/" + path).prepare()
            self.assertEqual(TR.request_target("/", path), prepared.path_url)


class CapturingConnection(http.client.HTTPSConnection):
    """Vraie validation http.client (putrequest/putheader), sans réseau : l'envoi est capturé."""

    def __init__(self, response_status=200, response_body=b"{}"):
        super().__init__("accounts.eu.adp.com", 443)
        self.sent, self._status, self._body = [], response_status, response_body

    def connect(self):
        pass

    def send(self, data):
        self.sent.append(data)

    def getresponse(self):
        status, body = self._status, self._body

        class Response:
            reason = "OK"

            def __init__(self):
                self.status = status

            def read(self, n=-1):
                return body

            def getheader(self, name, default=""):
                return "application/json"
        return Response()

    def close(self):
        pass


def bare_transport(conn):
    transport = TR.AdpHttpTransport.__new__(TR.AdpHttpTransport)   # constructeur verrouillé : attributs posés à la main
    transport.host, transport.base_path, transport.timeout = "accounts.eu.adp.com", "/", 30.0
    transport._bearer, transport._ctx = (lambda: "jeton"), None
    transport._connection = lambda: conn
    return transport


class TransportPaths(unittest.TestCase):
    def test_get_with_filter_passes_http_client_validation(self):
        conn = CapturingConnection(200, b'{"workers": []}')
        saved = TR.ADP_READ_POLICY_ESTABLISHED
        TR.ADP_READ_POLICY_ESTABLISHED = True
        try:
            status, _ = bare_transport(conn).get(FILTER_PATH)
        finally:
            TR.ADP_READ_POLICY_ESTABLISHED = saved
        self.assertEqual(status, 200)
        self.assertIn(b"GET /hr/v2/workers?$filter=workers/workerID/idValue%20eq%20'FR000001' HTTP/1.1", conn.sent[0])

    def test_post_uses_encoded_target(self):
        conn = CapturingConnection(200, b'{"ok": true}')
        result = bare_transport(conn).post("events/hr/v1/worker.hire", b"{}")
        self.assertEqual(result.outcome, TR.ACCEPTED)
        self.assertTrue(conn.sent[0].startswith(b"POST /events/hr/v1/worker.hire HTTP/1.1"))

    def test_unencoded_path_was_refused(self):
        with self.assertRaises(http.client.InvalidURL):            # le défaut corrigé
            CapturingConnection().putrequest("GET", "/" + FILTER_PATH)


class Token(unittest.TestCase):
    def make(self, responses, clock):
        conns = []

        def factory():
            status, body = responses.pop(0)
            conns.append(CapturingConnection(status, body))
            return conns[-1]
        token = TR.OAuthToken(None, SecretValue("id&1"), SecretValue("s+2"), 30, clock=clock, connection_factory=factory)
        return token, conns

    def test_fetch_cache_refresh(self):
        now = [0.0]
        token, conns = self.make([(200, json.dumps({"access_token": "A", "expires_in": 3600}).encode()),
                                  (200, json.dumps({"access_token": "B", "expires_in": 3600}).encode())],
                                 lambda: now[0])
        self.assertEqual(token(), "A")
        self.assertEqual(token(), "A")
        self.assertEqual(len(conns), 1)
        raw = b"".join(conns[0].sent)
        self.assertIn(b"POST /auth/oauth/v2/token HTTP/1.1", raw)
        self.assertIn(b"client_id=id%261&client_secret=s%2B2&grant_type=client_credentials", raw)
        now[0] = 3600.0
        self.assertEqual(token(), "B")
        self.assertEqual(repr(token), "OAuthToken(***)")

    def test_refusals(self):
        for status, body in ((401, b"{}"), (200, b"pas du json"), (200, b"{}")):
            token, _ = self.make([(status, body)], lambda: 0.0)
            with self.assertRaises(HrisError) as ctx:
                token()
            self.assertEqual(ctx.exception.code, "ADP_AUTH_FAILED")


class Factory(unittest.TestCase):
    PEM = {"client-id-adp": "id", "client-secret-adp": "secret",
           "MOTUL-SA-cer": "-----BEGIN CERTIFICATE-----AAAA-----END CERTIFICATE-----",
           "motulsa-auth-key": "-----BEGIN RSA PRIVATE KEY-----BBBB-----END RSA PRIVATE KEY-----"}

    def test_refuses_without_reading_secrets_while_locked(self):
        reads = []
        factory = S.RealAdpTransportFactory("https://kv", 30, get_secret=lambda kv, name: reads.append(name))
        with self.assertRaises(HrisError) as ctx:
            factory()
        self.assertEqual((ctx.exception.code, reads), ("ADP_ACTIVATION_NOT_APPROVED", []))

    def test_wiring_when_approved(self):
        events = []

        class Token:
            def __init__(self, context, client_id, client_secret, timeout):
                events.append(("token_init", context, client_id.reveal(), client_secret.reveal()))

            def __call__(self):
                events.append(("token_call",))
                return "T"

        class Transport:
            def __init__(self, context, bearer, timeout, _allow_construction=False):
                events.append(("transport", context, _allow_construction))

        saved = TR.ADP_ACTIVATION_APPROVED
        TR.ADP_ACTIVATION_APPROVED = True
        try:
            reads = []
            factory = S.RealAdpTransportFactory(
                "https://kv", 30, get_secret=lambda kv, name: reads.append((kv, name)) or self.PEM[name],
                context_builder=lambda cert, key: ("ctx", cert.splitlines()[0], key.splitlines()[0]),
                token_class=Token, transport_class=Transport)
            factory()
            self.assertEqual(sorted(n for _, n in reads), sorted(self.PEM))
            self.assertEqual([e[0] for e in events], ["token_init", "token_call", "transport"])
            self.assertEqual(events[0][1], ("ctx", "-----BEGIN CERTIFICATE-----", "-----BEGIN RSA PRIVATE KEY-----"))
            self.assertTrue(events[2][2])
            empty = dict(self.PEM, **{"client-secret-adp": ""})
            with self.assertRaises(HrisError) as ctx:
                S.RealAdpTransportFactory("https://kv", 30, get_secret=lambda kv, name: empty[name],
                                          token_class=Token, transport_class=Transport)()
            self.assertEqual(ctx.exception.code, "ADP_SECRET_EMPTY")
            with self.assertRaises(HrisError) as ctx:
                S.RealAdpTransportFactory("", 30, get_secret=lambda kv, name: "x")()
            self.assertEqual(ctx.exception.code, "PARAM_MISSING")
        finally:
            TR.ADP_ACTIVATION_APPROVED = saved
        self.assertFalse(TR.ADP_ACTIVATION_APPROVED)


class MtlsContext(unittest.TestCase):
    def test_invalid_material_refused_and_nothing_left_on_disk(self):
        root = tempfile.gettempdir()
        before = {n for n in os.listdir(root) if n.startswith("hris-adp-")}
        with self.assertRaises(ssl.SSLError):
            TR.mtls_context("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
                            "-----BEGIN RSA PRIVATE KEY-----\nBBBB\n-----END RSA PRIVATE KEY-----\n")
        after = {n for n in os.listdir(root) if n.startswith("hris-adp-")}
        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
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
import test_adp_wiring
import test_cell_log
import test_contract_markers
import test_fabric_integration
import test_legacy_logs
import test_parity_transform
import test_reference_api
import test_remuneration
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
                    test_legacy_logs, test_tables_naming, test_spark_transform_parity, test_cell_log,
                    test_reference_api, test_remuneration, test_adp_wiring]
    run_integration = as_bool(p_run_integration)
    if run_integration:
        test_modules.append(test_fabric_integration)
    suite = unittest.TestSuite()
    only = [fragment.strip() for fragment in p_only.split(",") if fragment.strip()]
    for test_module in test_modules:
        for test in iterate_tests(unittest.defaultTestLoader.loadTestsFromModule(test_module)):
            if not run_integration and any(name in test.id() for name in (
                    "CellLogSpark", "ReferenceStoreSpark", "RemunerationStoreSpark")):
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
