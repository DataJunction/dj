#!/usr/bin/env bash
# Run the skill evals with config from a local .env (gitignored). See .env.example.
#
#   .env supplies: OPENAI_BASE_URL, OPENAI_API_KEY, SKILL_EVAL_MODEL
#   Extra args pass through to promptfoo.
#
# By default every case runs twice — once with the skill, once without (the baseline
# arm) — so the report shows what the skill contributed. `--no-baseline` runs only the
# with-skill arm, for half the tokens.
#
# We source .env and export it here so it's authoritative for this run — that avoids
# a stale OPENAI_BASE_URL in your shell silently overriding it. promptfoo doesn't
# template the provider id from env, so the model is applied by generating a
# gitignored promptfooconfig.local.yaml.
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

ARGS=()
ARMS="with-skill + baseline"
for arg in "$@"; do
  if [ "$arg" = "--no-baseline" ]; then
    ARGS+=(--filter-providers with-skill)
    ARMS="with-skill only"
  else
    ARGS+=("$arg")
  fi
done

MODEL="${SKILL_EVAL_MODEL:-gpt-4o}"
sed "s|openai:chat:gpt-4o|openai:chat:${MODEL}|g" promptfooconfig.yaml > promptfooconfig.local.yaml

echo "model: ${MODEL}  |  base: ${OPENAI_BASE_URL:-<unset>}  |  arms: ${ARMS}"
npx promptfoo@latest eval -c promptfooconfig.local.yaml ${ARGS[@]+"${ARGS[@]}"}
