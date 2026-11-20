"""Compara dos reportes JSON de Trivy y falla si el primero tiene vulnerabilidades críticas que el segundo no tiene.

Las vulnerabilidades que ya estaban en la rama destino se informan sin bloquear: se corrigen en su propia tarea.
"""
import json
import os
import sys
from pathlib import Path


def findings(path: Path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    report = json.loads(path.read_text(encoding="utf-8"))
    found = {}
    for result in report.get("Results") or []:
        for vuln in result.get("Vulnerabilities") or []:
            found[(vuln["VulnerabilityID"], vuln["PkgName"])] = vuln
    return found


def main() -> int:
    if not Path(sys.argv[1]).exists():
        print(f"No existe el reporte {sys.argv[1]}")
        return 1
    head = findings(Path(sys.argv[1]))
    base = findings(Path(sys.argv[2])) if len(sys.argv) > 2 else {}
    compare = len(sys.argv) > 2 and Path(sys.argv[2]).exists()
    new_critical = sorted(k for k, v in head.items() if v["Severity"] == "CRITICAL" and compare and k not in base)

    lines = ["## Vulnerabilidades de la imagen", "", "| Severidad | ID | Paquete | Instalada | Corregida | Nueva |",
             "|---|---|---|---|---|---|"]
    for key, vuln in sorted(head.items(), key=lambda item: (item[1]["Severity"] != "CRITICAL", item[0])):
        is_new = "Sí" if compare and key not in base else "No"
        lines.append(f"| {vuln['Severity']} | {key[0]} | {key[1]} | {vuln.get('InstalledVersion', '')} "
                     f"| {vuln.get('FixedVersion', '')} | {is_new} |")
    if not head:
        lines.append("| - | Sin vulnerabilidades altas ni críticas con corrección disponible | | | | |")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write("\n".join(lines) + "\n")
    print("\n".join(lines))

    if new_critical:
        print(f"\nEl cambio agrega {len(new_critical)} vulnerabilidad(es) crítica(s): "
              + ", ".join(f"{vid} en {pkg}" for vid, pkg in new_critical))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
