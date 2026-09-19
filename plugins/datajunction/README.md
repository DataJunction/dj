# DataJunction plugin for Claude Code

This plugin gives Claude Code everything it needs to work with a
[DataJunction](https://github.com/DataJunction/dj) semantic layer: the DJ
skills, the MCP server configuration, and the DJ subagent.

## Install

Add the DataJunction marketplace and install the plugin:

```
/plugin marketplace add DataJunction/dj
/plugin install datajunction@datajunction
```

The skills and subagent work immediately. The MCP tools run through the
`dj-mcp` command, which ships with the Python client, so install that too if you
want Claude to query a live instance:

```bash
pip install datajunction[mcp]
```

The MCP server reads your DJ instance URL from the `DJ_API_URL` environment
variable (default `http://localhost:8000`).

## What's inside

- **`skills/`** — the DataJunction skills: core concepts, querying, semantic
  modeling, repo-backed authoring, and the REST API. Claude loads these
  contextually based on what you're doing.
- **`agents/dj.md`** — the DJ subagent, with the skills pre-loaded so DJ
  expertise is available without invoking it by hand.
- **`.mcp.json`** — the DJ MCP server (`dj-mcp`), which connects Claude to your
  running DJ instance for search, SQL generation, lineage, and queries.

## Maintainers

The skills under `skills/` are a **generated mirror** of the canonical copies in
the `datajunction` Python client
(`datajunction-clients/python/datajunction/skills/`). Do not edit them here —
edit the canonical files and regenerate:

```bash
python scripts/sync_plugin_skills.py
```

`python scripts/sync_plugin_skills.py --check` fails if the mirror has drifted;
`tests/test_plugin_mirror.py` guards the same in CI.
