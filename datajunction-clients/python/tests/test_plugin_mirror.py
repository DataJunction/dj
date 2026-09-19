"""Guards that the OSS plugin mirror and manifests stay valid and in sync."""

import json
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
PKG_SKILLS = REPO_ROOT / "datajunction-clients" / "python" / "datajunction" / "skills"
PLUGIN = REPO_ROOT / "plugins" / "datajunction"
PLUGIN_SKILLS = PLUGIN / "skills"


def _skills(root: Path) -> dict[str, str]:
    return {p.parent.name: p.read_text() for p in sorted(root.glob("*/SKILL.md"))}


def test_plugin_skills_mirror_matches_package():
    assert _skills(PLUGIN_SKILLS) == _skills(PKG_SKILLS), (
        "plugins/datajunction/skills is out of sync with the package skills. "
        "Run: python scripts/sync_plugin_skills.py"
    )


def test_plugin_manifest_valid():
    manifest = json.loads((PLUGIN / ".claude-plugin" / "plugin.json").read_text())
    assert manifest["name"] == "datajunction"
    assert manifest["skills"] == "./skills/"


def test_marketplace_manifest_lists_plugin():
    mkt = json.loads((REPO_ROOT / ".claude-plugin" / "marketplace.json").read_text())
    sources = [p["source"] for p in mkt["plugins"]]
    assert "./plugins/datajunction" in sources


def test_agent_skills_match_package_skills():
    text = (PLUGIN / "agents" / "dj.md").read_text()
    _, fm_text, _ = text.split("---\n", 2)
    agent_skills = set(yaml.safe_load(fm_text)["skills"])
    assert agent_skills == set(_skills(PKG_SKILLS))
