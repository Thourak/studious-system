"""Génère trois aperçus HTML du message de fin de run (données FICTIVES) dans LOCAL_TESTS/apercu/.

Usage : python -I LOCAL_TESTS/render_preview.py   puis ouvrir les fichiers dans un navigateur.
Un aperçu navigateur ne remplace PAS le test dans Outlook (web, poste, mobile), dont le rendu diffère.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("test_notify_html", HERE / "test_notify_html.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)  # type: ignore[union-attr]

ns = module.build_namespace()
anomalies = [
    {"control_code": "MAPPING_EMPTY", "severity": "BLOCKING", "outcome": "FAIL", "message": "table de correspondance vide"},
    {"control_code": "REJECT_RATE", "severity": "WARNING", "outcome": "FAIL", "message": "taux de rejet au-dessus du seuil"},
]
cases = {
    "succes": ("SUCCEEDED", [dict(s, status="SUCCEEDED") for s in module.STEPS if s["step_code"] != "TRANSFORM"], []),
    "echec": ("BLOCKED", module.STEPS, anomalies),
    "attention": ("DRY_RUN", [dict(s, status="SUCCEEDED") for s in module.STEPS[:1]] + [module.STEPS[3]], []),
}
out_dir = HERE / "apercu"
out_dir.mkdir(exist_ok=True)
for name, (status, steps, found) in cases.items():
    run = dict(module.RUN, status=status, error_code="HRIS_RUN_BLOCKED" if status == "BLOCKED" else None,
               error_summary="Run bloqué par un contrôle (fictif)" if status == "BLOCKED" else None)
    body = ns["build_notification_html"](run, steps, found, "DEV", 40, 15)
    subject = ns["build_subject"]("[HRIS]", "DEV", run)
    (out_dir / f"apercu_{name}.html").write_text(
        f'<!-- Objet : {subject} -->\n<meta charset="utf-8">\n{body}\n', encoding="utf-8")
    print(f"apercu_{name}.html : {subject} ({len(body.encode('utf-8'))} octets)")
