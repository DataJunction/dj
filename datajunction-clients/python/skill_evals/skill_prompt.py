"""promptfoo prompt function: load the skill(s) into context, then the request.

This simulates how Claude uses a skill — the SKILL.md is injected as the system
message and the test case's request is the user turn. The model's response is what
the assertions in promptfooconfig.yaml grade.

`vars.skill` is a comma-separated string of skill names — multiple compose them (in
real use a skill like datajunction-semantic-model is loaded alongside the skills it
defers to, e.g. datajunction-repo for the actual YAML authoring). It's a string, not
a YAML list, because promptfoo expands a list var into separate test cases.
"""

from pathlib import Path

SKILLS_DIR = Path(__file__).resolve().parent.parent / "datajunction" / "skills"


def build_prompt(context):
    variables = context["vars"]
    names = [n.strip() for n in variables["skill"].split(",") if n.strip()]
    skill_docs = [(SKILLS_DIR / f"{name}.md").read_text() for name in names]
    return [
        {"role": "system", "content": "\n\n---\n\n".join(skill_docs)},
        {"role": "user", "content": variables["request"]},
    ]
