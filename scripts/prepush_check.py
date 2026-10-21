"""Read-only pre-push checks for project hygiene and documentation assets."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check() -> list[str]:
    errors: list[str] = []
    tracked = (
        subprocess.run(
            ["git", "ls-files", "-z", "--cached"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8", errors="replace")
        .split("\0")
    )
    risky = [
        p
        for p in tracked
        if p
        and (
            p == ".env"
            or p.startswith((".venv/", ".vscode/"))
            or p.startswith("data/perf_")
            or p == "data/issues.jsonl"
            or p.endswith("runtime_validation.json")
        )
    ]
    if risky:
        errors.append(
            "Tracked local/sensitive files (untrack first): " + ", ".join(risky)
        )

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    if re.search(r"лабораторн|учебн", readme, re.I):
        errors.append(
            "README contains terminology not intended for product documentation"
        )

    for link in re.findall(r"!?(?:\[[^\]]*\])\(([^)]+)\)", readme):
        if link.startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = link.split("#", 1)[0]
        if path and not (ROOT / path).exists():
            errors.append(f"Broken local README link: {link}")
    return errors


def main() -> None:
    issues = check()
    if issues:
        print("PRE-PUSH CHECK FAILED")
        for issue in issues:
            print(" -", issue)
        sys.exit(1)
    print("PRE-PUSH CHECK PASSED: no tracked local credentials / broken README links")


if __name__ == "__main__":
    main()
