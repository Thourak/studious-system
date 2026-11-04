"""Contrôles statiques locaux de TST_6 (aucune exécution Fabric, aucun accès réseau).

Usage : python -I verify_tst6.py <TST_6>
Contrôles : empreintes des modules, résidus T5/TST_5, logicalId uniques et références de pipelines résolues,
compilation Python de toutes les cellules et de tous les modules embarqués, alias déprécié mssparkutils,
références à l'item exclu, secrets en clair (motifs simples).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import digest_tools as D  # noqa: E402

root = Path(sys.argv[1])
problems: list[str] = []
report: dict = {}

# 1) empreintes
for nb in root.rglob("notebook-content.py"):
    n, bad = D.check(nb)
    if n:
        report[f"modules:{nb.parent.name}"] = n
    if bad:
        problems.append(f"empreintes fausses dans {nb.parent.name}: {bad}")

def text_files():
    """Fichiers texte du dépôt (hors caches Python)."""
    for f in root.rglob("*"):
        if f.is_file() and "__pycache__" not in f.parts and f.suffix != ".pyc":
            yield f


def exempt(f: Path) -> bool:
    """Le README racine et les outils locaux citent volontairement TST_5 et l'item exclu (historique, décisions)."""
    return f in (root / "README.md", root / "DOC" / "CONFORMITE_TST_6.md") or "LOCAL_TESTS" in f.parts


# 2) résidus de nommage
resid = re.compile(r"(?<![A-Za-z0-9])T5_|TST_5|(?<![A-Za-z0-9])t5_")
for f in text_files():
    if not exempt(f) and resid.search(D.read_text(f)):
        problems.append(f"résidu T5/TST_5 : {f.relative_to(root)}")

# 3) logicalId et références
ids = {}
for plat in root.rglob(".platform"):
    j = json.loads(D.read_text(plat))
    lid, name = j["config"]["logicalId"], j["metadata"]["displayName"]
    if lid in ids:
        problems.append(f"logicalId en double : {lid} ({name} / {ids[lid]})")
    ids[lid] = name
    if not name.startswith("T6_"):
        problems.append(f"displayName hors nomenclature T6_ : {name}")
report["items"] = len(ids)
guid = re.compile(r'"(notebookId|referenceName|pipelineId)":\s*"([0-9a-f-]{36})"')
for pj in root.rglob("pipeline-content.json"):
    for m in guid.finditer(D.read_text(pj)):
        if m.group(2) not in ids and m.group(2) != "00000000-0000-0000-0000-000000000000":
            problems.append(f"{pj.parent.name}: {m.group(1)} {m.group(2)} sans item")

# 4) compilation des cellules et des modules
cell_split = re.compile(r"^# (?:CELL|PARAMETERS CELL) \*+\s*$", re.M)
meta_or_marker = re.compile(r"^# (?:METADATA|MARKDOWN) \*+\s*$", re.M)
n_cells = n_mods = 0
for nb in root.rglob("notebook-content.py"):
    text = D.read_text(nb).replace("\r\n", "\n")
    parts = cell_split.split(text)[1:]
    for i, cell in enumerate(parts, 1):
        cell = meta_or_marker.split(cell)[0]            # coupe à la métadonnée de fin de cellule
        code = "\n".join("pass  # %run" if ln.strip().startswith("%run") else ln for ln in cell.split("\n"))
        try:
            compile(code, f"{nb.parent.name}#cellule{i}", "exec")
            n_cells += 1
        except SyntaxError as e:
            problems.append(f"syntaxe {nb.parent.name} cellule {i}: {e}")
    for m in D.CALL_RE.finditer(text):
        try:
            compile(m.group(3), f"module:{m.group(1)}", "exec")
            n_mods += 1
        except SyntaxError as e:
            problems.append(f"syntaxe module {m.group(1)}: {e}")
report["cellules_compilées"] = n_cells
report["modules_compilés"] = n_mods

# 5) règles de projet
for nb in root.rglob("notebook-content.py"):
    t = D.read_text(nb)
    if "mssparkutils" in t:
        problems.append(f"alias mssparkutils présent : {nb.parent.name}")
for f in text_files():
    if True:
        t = D.read_text(f)
        if not exempt(f) and re.search(r"LEGACY_LOGS_IMPORT", t):
            problems.append(f"référence à l'item exclu : {f.relative_to(root)}")
        for m in re.finditer(r"(?i)(client[_-]?secret|password|api[_-]?key)\s*[:=]\s*['\"]([A-Za-z0-9+/_\-]{16,})['\"]", t):
            if re.fullmatch(r"[a-z]+(-[a-z0-9]+)+", m.group(2)):   # nom de secret Key Vault (kebab-case), pas une valeur
                continue
            problems.append(f"secret possible : {f.relative_to(root)}")

# 6) structure des pipelines : noms uniques, dépendances et références d'activités résolues, Fail complet
def collect_names(acts: list) -> list:
    out = []
    for a in acts:
        out.append(a["name"])
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            out += collect_names(a.get("typeProperties", {}).get(key, []))
    return out


def check_scope(acts: list, pname: str, all_names: list) -> None:
    names = [a["name"] for a in acts]
    for a in acts:
        for dep in a.get("dependsOn", []):
            if dep["activity"] not in names:
                problems.append(f"{pname}: {a['name']} dépend de {dep['activity']} absent de sa portée")
        tp = a.get("typeProperties", {})
        if a["type"] == "Fail" and not (tp.get("message") and tp.get("errorCode")):
            problems.append(f"{pname}: Fail {a['name']} sans message ou errorCode")
        if a["type"] == "IfCondition" and "expression" not in tp:
            problems.append(f"{pname}: IfCondition {a['name']} sans expression")
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            check_scope(tp.get(key, []), pname, all_names)


n_pipelines = 0
for pj in root.rglob("pipeline-content.json"):
    n_pipelines += 1
    content = json.loads(D.read_text(pj))
    acts = content["properties"]["activities"]
    all_names = collect_names(acts)
    if len(all_names) != len(set(all_names)):
        problems.append(f"{pj.parent.name}: noms d'activités en double")
    check_scope(acts, pj.parent.name, all_names)
    for ref in set(re.findall(r"activity\('([^']+)'\)", D.read_text(pj))):
        if ref not in all_names:
            problems.append(f"{pj.parent.name}: expression sur l'activité inconnue {ref}")
report["pipelines_contrôlés"] = n_pipelines

print(json.dumps({"rapport": report, "problèmes": problems}, ensure_ascii=False, indent=2))
sys.exit(1 if problems else 0)
