"""
Structural and consistency tests for the bundled DJ Claude skills.

Catches:
- Frontmatter parse errors / missing required fields
- ``name:`` not matching filename
- Cross-references to sibling skills that no longer exist (e.g. after rename)
- CLI installer / subagent skill list drifting from the actual bundled files

Skipped on purpose:
- Content assertions (string-matching for specific guidance). Brittle, no payoff.
- Behavioral / eval tests (does Claude follow the skill?). Different discipline.
"""

import re
from pathlib import Path

import pytest
import yaml

SKILLS_DIR = Path(__file__).parent.parent / "datajunction" / "skills"
CLI_PATH = Path(__file__).parent.parent / "datajunction" / "cli.py"


def _parse_skill(path: Path) -> tuple[dict, str]:
    """Return (frontmatter_dict, body_text) for a skill file."""
    text = path.read_text()
    assert text.startswith("---\n"), (
        f"{path.name}: must start with YAML frontmatter delimiter (---)"
    )
    _, fm_text, body = text.split("---\n", 2)
    return yaml.safe_load(fm_text), body


def _all_skills() -> dict[str, tuple[Path, dict, str]]:
    """Map skill name → (path, frontmatter, body)."""
    skills: dict[str, tuple[Path, dict, str]] = {}
    for path in sorted(SKILLS_DIR.glob("*.md")):
        frontmatter, body = _parse_skill(path)
        skills[path.stem] = (path, frontmatter, body)
    return skills


# -- Frontmatter structure ------------------------------------------------


def test_skill_dir_has_files():
    """Sanity check that we're looking at the right directory."""
    skill_files = list(SKILLS_DIR.glob("*.md"))
    assert skill_files, f"No .md files found under {SKILLS_DIR}"


@pytest.mark.parametrize("skill_name", sorted(_all_skills().keys()))
def test_frontmatter_required_fields(skill_name: str):
    """Every skill must have ``name`` and ``description`` in its frontmatter."""
    _, frontmatter, _ = _all_skills()[skill_name]
    assert "name" in frontmatter, f"{skill_name}: frontmatter missing ``name``"
    assert "description" in frontmatter, (
        f"{skill_name}: frontmatter missing ``description``"
    )
    assert isinstance(frontmatter["description"], str), (
        f"{skill_name}: ``description`` must be a string"
    )
    assert frontmatter["description"].strip(), (
        f"{skill_name}: ``description`` must not be empty"
    )


@pytest.mark.parametrize("skill_name", sorted(_all_skills().keys()))
def test_name_matches_filename(skill_name: str):
    """``name`` in frontmatter must match the filename stem (modulo .md)."""
    path, frontmatter, _ = _all_skills()[skill_name]
    assert frontmatter["name"] == path.stem, (
        f"{path.name}: frontmatter name '{frontmatter['name']}' "
        f"does not match filename stem '{path.stem}'"
    )


# -- Cross-references between skills --------------------------------------


_BACKTICK_REF = re.compile(r"`(datajunction(?:-[a-z-]+)?)`")


@pytest.mark.parametrize("skill_name", sorted(_all_skills().keys()))
def test_cross_references_resolve(skill_name: str):
    """
    Every backtick-quoted ``datajunction*`` reference in a skill must point
    to a skill that actually exists. Catches stale references after a rename
    or removal. Scans the *whole* file — including the frontmatter
    description, where most cross-references live.
    """
    skills = _all_skills()
    valid_names = set(skills.keys())
    path, _, _ = skills[skill_name]

    refs = set(_BACKTICK_REF.findall(path.read_text()))
    # Self-references are fine but uninformative; ignore them.
    refs.discard(skill_name)

    unknown = sorted(refs - valid_names)
    assert not unknown, (
        f"{path.name} references unknown skill(s): {unknown}. "
        f"Known skills: {sorted(valid_names)}"
    )


# -- CLI installer consistency --------------------------------------------


def _cli_bundled_skill_filenames() -> list[str]:
    """Parse ``bundled_skills`` in cli.py for the list of skill filenames."""
    source = CLI_PATH.read_text()
    # Look for ``"filename": "datajunction*.md"`` entries inside the
    # ``bundled_skills`` block.
    # Tolerate an optional ``: list[...]`` type annotation between the name
    # and the ``=``.
    match = re.search(
        r"bundled_skills(?:\s*:\s*[^\n=]+)?\s*=\s*\[(.*?)\]\s*\n\s+missing",
        source,
        re.DOTALL,
    )
    assert match, "Could not locate bundled_skills list in cli.py"
    block = match.group(1)
    return re.findall(r'"filename":\s*"([^"]+)"', block)


def test_cli_bundled_skills_matches_filesystem():
    """
    The ``bundled_skills`` list in cli.py must include every .md file in
    ``datajunction/skills/`` exactly once and reference nothing else.
    A new skill file with no installer entry won't ship; an installer
    entry pointing at a deleted file will crash setup-claude.
    """
    on_disk = sorted(p.name for p in SKILLS_DIR.glob("*.md"))
    in_cli = sorted(_cli_bundled_skill_filenames())
    assert in_cli == on_disk, (
        f"bundled_skills in cli.py is out of sync with skills/ directory.\n"
        f"  In cli.py:  {in_cli}\n"
        f"  On disk:    {on_disk}"
    )


def test_cli_bundled_skill_names_match_frontmatter():
    """
    Each ``bundled_skills`` entry's ``name`` must match the frontmatter
    ``name`` of the file it references.
    """
    source = CLI_PATH.read_text()
    # Grab name + filename pairs from the block, preserving order.
    entries = re.findall(
        r'"name":\s*"(datajunction[-\w]*)"\s*,\s*"filename":\s*"([^"]+)"',
        source,
    )
    assert entries, "Could not parse name/filename pairs from bundled_skills"

    skills = _all_skills()
    for cli_name, filename in entries:
        stem = filename.removesuffix(".md")
        assert stem in skills, f"cli.py refers to {filename!r} but no such skill"
        fm_name = skills[stem][1]["name"]
        assert cli_name == fm_name, (
            f"cli.py bundled_skills entry name '{cli_name}' does not match "
            f"frontmatter name '{fm_name}' in {filename}"
        )


# -- Subagent consistency -------------------------------------------------


def test_subagent_skills_list_matches_bundled():
    """
    The ``skills:`` list in the subagent definition (in cli.py) must
    reference exactly the bundled skill names. Otherwise the dj subagent
    will fail to load skills or silently skip ones the package ships.
    """
    source = CLI_PATH.read_text()
    # The subagent definition is a triple-quoted string with a yaml block.
    match = re.search(
        r"subagent_content\s*=\s*\"\"\"\\?\n(---.*?---)",
        source,
        re.DOTALL,
    )
    assert match, "Could not locate subagent frontmatter in cli.py"
    fm = yaml.safe_load(match.group(1).strip("-").strip())
    subagent_skills = set(fm.get("skills", []))

    bundled = {p.stem for p in SKILLS_DIR.glob("*.md")}
    assert subagent_skills == bundled, (
        f"Subagent skills: list out of sync with bundled skills/ directory.\n"
        f"  In subagent: {sorted(subagent_skills)}\n"
        f"  On disk:     {sorted(bundled)}"
    )
