#!/usr/bin/env python3
"""Mirror the canonical DJ skills into the OSS plugin directory.

Canonical source: datajunction-clients/python/datajunction/skills/<name>/SKILL.md
Mirror target:    plugins/datajunction/skills/<name>/SKILL.md

Run from anywhere; paths are resolved relative to the repo root (this file's
grandparent). Use --check to verify the mirror is up to date without writing.
"""
import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "datajunction-clients" / "python" / "datajunction" / "skills"
DST = REPO_ROOT / "plugins" / "datajunction" / "skills"


def _skill_files(root: Path) -> dict[str, str]:
    return {
        p.parent.name: p.read_text(encoding="utf-8")
        for p in sorted(root.glob("*/SKILL.md"))
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="verify only; exit 1 if stale")
    args = ap.parse_args()

    src = _skill_files(SRC)
    if args.check:
        dst = _skill_files(DST)
        if src != dst:
            print("Plugin skills mirror is stale. Run: python scripts/sync_plugin_skills.py", file=sys.stderr)
            return 1
        print("Plugin skills mirror is up to date.")
        return 0

    if DST.exists():
        shutil.rmtree(DST)
    for name, text in src.items():
        (DST / name).mkdir(parents=True, exist_ok=True)
        (DST / name / "SKILL.md").write_text(text, encoding="utf-8")
    print(f"Mirrored {len(src)} skills to {DST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
