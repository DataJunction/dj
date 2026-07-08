"""promptfoo custom provider: a thin OpenAI-compatible chat client that can run a
case as one turn or two.

Why a custom provider instead of the built-in ``openai:chat``: the semantic-model
skill's decomposition workflow says *propose, don't produce* — on a raw-query request
the model should first return a structured decomposition, and only emit node YAML when
asked. To eval that faithfully we need the model's turn-1 reply fed back as context for
a turn-2 "now produce the YAML" follow-up. A single combined prompt can't test the
"no YAML on the first pass" discipline; chaining across promptfoo rows is order/
concurrency-fragile. So one test row drives both turns here.

Behaviour:
  - turn 1: call the model with the rendered prompt (system = skill(s), user = request);
  - if the case sets a ``followup`` var, turn 2: replay [system, user, assistant=turn1,
    user=followup] and append the reply after ``node_rules.TURN2_MARKER``.

Config is all env (nothing about the provider is hardcoded): ``OPENAI_BASE_URL``
(default the public OpenAI API), ``OPENAI_API_KEY``, and ``SKILL_EVAL_MODEL`` (default
gpt-4o).
"""

import json
import os
import urllib.error
import urllib.request

import node_rules

DEFAULT_BASE_URL = "https://api.openai.com/v1"


def _messages_from_prompt(prompt):
    """promptfoo hands the rendered prompt as a string; our prompt function returns a
    chat message list, so it arrives JSON-encoded. Fall back to a single user turn."""
    try:
        parsed = json.loads(prompt)
    except (TypeError, json.JSONDecodeError):
        return [{"role": "user", "content": str(prompt)}]
    if isinstance(parsed, list):
        return parsed
    return [{"role": "user", "content": str(prompt)}]


def _chat(messages, model, temperature, base_url, api_key):
    """One chat-completions call. Returns (content, usage_dict)."""
    body = json.dumps(
        {"model": model, "messages": messages, "temperature": temperature},
    ).encode()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.loads(response.read())
    content = payload["choices"][0]["message"]["content"]
    return content, payload.get("usage", {})


def call_api(prompt, options, context):
    config = options.get("config", {}) if isinstance(options, dict) else {}
    temperature = config.get("temperature", 0)
    model = os.environ.get("SKILL_EVAL_MODEL", "gpt-4o")
    base_url = os.environ.get("OPENAI_BASE_URL", DEFAULT_BASE_URL)
    api_key = os.environ.get("OPENAI_API_KEY", "")

    messages = _messages_from_prompt(prompt)
    variables = (context or {}).get("vars", {})
    followup = variables.get("followup")

    try:
        turn1, usage1 = _chat(messages, model, temperature, base_url, api_key)
        if not followup:
            return {"output": turn1, "tokenUsage": _usage(usage1)}

        messages = messages + [
            {"role": "assistant", "content": turn1},
            {"role": "user", "content": followup},
        ]
        turn2, usage2 = _chat(messages, model, temperature, base_url, api_key)
        combined = f"{turn1}\n\n{node_rules.TURN2_MARKER}\n\n{turn2}"
        return {"output": combined, "tokenUsage": _usage(usage1, usage2)}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        return {"error": f"HTTP {exc.code} from {base_url}: {detail}"}
    except Exception as exc:  # noqa: BLE001 - surface any provider failure to promptfoo
        return {"error": f"{type(exc).__name__}: {exc}"}


def _usage(*usages):
    total = {"prompt": 0, "completion": 0, "total": 0}
    for usage in usages:
        total["prompt"] += usage.get("prompt_tokens", 0)
        total["completion"] += usage.get("completion_tokens", 0)
        total["total"] += usage.get("total_tokens", 0)
    return total
