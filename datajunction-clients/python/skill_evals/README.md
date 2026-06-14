# Skill evals (behavioral)

Behavioral evals for the bundled DJ Claude skills: load a `SKILL.md` into a model's
context, give it a realistic request, and grade what it produces.

**This is a local / on-demand tool, not CI.** It makes real LLM calls, so it isn't
wired into GitHub Actions (public runners have no provider key). The free, every-PR
safety net is the *programmatic* tier —
`tests/test_skill_examples.py`, which validates the example YAML in the skills against
the deployment schema with no model. This dir is the deliberate, occasional quality
check on top of that.

## What it checks

Cases fall in two groups. **Authoring** cases (single turn) ask for a node and grade the
YAML. **Decomposition** cases (two turns) test the semantic-model skill's "propose, don't
produce" workflow: turn 1 should return a *structured decomposition proposal* (no YAML),
turn 2 produces the deployment YAML after a follow-up. `provider.py` drives both turns in
one test row so there's no fragile cross-row ordering.

The semantic-modeling traps under test: ratio → named base metrics + a derived metric
(not one inlined blob); filters via `CASE WHEN`, not a `WHERE`; slice-by-dimension via a
dimension link, not a baked-in `JOIN`; the reusability rule holding even when the user
asks for "just one metric"; mixed grains split into separate transforms; business-
meaningful naming.

Each case asserts with some mix of:

- **programmatic, no LLM** — `assert_node.py` (single node) / `assert_deployment.py`
  (multi-node): nodes parse, use `node_type` (not legacy `type:`), have required fields,
  and satisfy case-specific shape (min nodes per type, a derived ratio metric exists, a
  dimension link exists, no JOIN baked into a query, a query matches/avoids a pattern).
  Rules are shared in `node_rules.py` so the two asserts can't drift;
- **llm-rubric** (LLM judge) — the modeling judgment: did it decompose correctly, resist
  the shortcut, name things meaningfully.

## Running it

The provider endpoint comes from env — nothing about it is hardcoded. Copy
`.env.example` to `.env` (gitignored) and fill it in, then run `./run.sh`.

**Bring your own key (default — public OpenAI API):**

```bash
export OPENAI_API_KEY="sk-..."                  # your OpenAI key
./run.sh                                        # defaults to gpt-4o
npx promptfoo@latest view                       # browse results
```

**Any OpenAI-compatible endpoint (e.g. a gateway/proxy):** point the base URL at it and
pick a model it serves. `run.sh` generates a gitignored `promptfooconfig.local.yaml`
with `SKILL_EVAL_MODEL` (the committed config stays `gpt-4o`, so BYO-key runs and
gateway runs don't fight over one line):

```bash
export OPENAI_BASE_URL="http://your-endpoint/v1"
export OPENAI_API_KEY="..."                     # whatever the endpoint expects
SKILL_EVAL_MODEL=claude-sonnet-4-6 ./run.sh     # any model the endpoint serves
```

## Files

- `promptfooconfig.yaml` — providers, prompts, golden cases + assertions.
- `skill_prompt.py` — prompt function: injects the SKILL.md(s) as system + the request.
  `vars.skill` is a comma-separated string (multiple skills compose).
- `provider.py` — two-turn OpenAI-compatible client; a `followup` var triggers turn 2.
- `node_rules.py` — shared node-spec rules mirroring the server deployment schema.
- `assert_node.py` — structural check on a single produced node.
- `assert_deployment.py` — structural check on a multi-node decomposition / deployment.
