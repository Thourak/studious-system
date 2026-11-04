"""Outils locaux (hors dépôt) : contrôle et recalcul des empreintes SHA-256 des modules embarqués.

Un notebook de bibliothèque contient des appels
    register_hris_module('nom', '<sha256>', r'''<source>''')
dont l'empreinte doit être celle du texte exact de <source> (UTF-8), comme le fait la fonction
register_hris_module du notebook. Ce module ne lit et n'écrit que les fichiers qu'on lui passe.
"""
from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path
from typing import List, Tuple

CALL_RE = re.compile(r"register_hris_module\('([^']+)',\s*'([0-9a-f]{64})',\s*r'''(.*?)'''\)", re.DOTALL)


def read_text(path: Path) -> str:
    # newline='' : aucune conversion de fin de ligne, pour hacher le texte tel qu'écrit.
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return fh.read()


def write_text(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)


def sha(text: str) -> str:
    # Les empreintes d'origine portent sur le texte normalisé en LF (vérifié sur TST_5 : les
    # fichiers du poste sont en CRLF, le dépôt Git les stocke en LF).
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def check(path: Path) -> Tuple[int, List[str]]:
    """Retourne (nombre de modules, liste des modules dont l'empreinte est fausse)."""
    text = read_text(path)
    bad: List[str] = []
    n = 0
    for m in CALL_RE.finditer(text):
        n += 1
        if sha(m.group(3)) != m.group(2):
            bad.append(m.group(1))
    declared = len(re.findall(r"register_hris_module\('", text))
    if declared != n:
        bad.append(f"<{declared} appels déclarés, {n} analysés>")
    return n, bad


def refresh(path: Path) -> int:
    """Recalcule les empreintes d'un fichier ; retourne le nombre d'empreintes modifiées."""
    text = read_text(path)
    changed = 0

    def repl(m: "re.Match[str]") -> str:
        nonlocal changed
        new = sha(m.group(3))
        if new != m.group(2):
            changed += 1
        return f"register_hris_module('{m.group(1)}', '{new}', r'''{m.group(3)}''')"

    out = CALL_RE.sub(repl, text)
    if out != text:
        write_text(path, out)
    return changed


if __name__ == "__main__":
    rc = 0
    for arg in sys.argv[1:]:
        n, bad = check(Path(arg))
        print(f"{arg}: {n} modules, empreintes fausses = {bad if bad else 'aucune'}")
        rc |= 1 if bad else 0
    sys.exit(rc)
