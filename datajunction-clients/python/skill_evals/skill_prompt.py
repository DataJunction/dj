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

# System prompt for the baseline arm, where the provider drops the skill (see
# provider.py's ``skill_mode``). It names the domain and the output format but gives
# no modeling guidance, so the with-skill minus baseline delta measures what the
# skill's *advice* contributes — not whether the model can guess that we wanted DJ
# YAML. A bare baseline would fail every case for the wrong reason.
CONTROL_SYSTEM = (
    "You are helping a user model data in DataJunction (DJ), an open-source semantic "
    "layer. Answer the request directly. When asked for node definitions, output them "
    "as YAML in a fenced ```yaml block."
)


def build_prompt(context):
    variables = context["vars"]
    names = [n.strip() for n in variables["skill"].split(",") if n.strip()]
    skill_docs = [(SKILLS_DIR / name / "SKILL.md").read_text() for name in names]
    return [
        {"role": "system", "content": "\n\n---\n\n".join(skill_docs)},
        {"role": "user", "content": variables["request"]},
    ]
