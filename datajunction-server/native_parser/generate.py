#!/usr/bin/env python3
"""
Generate the C++ lexer and parser, and the lookup tables module.cpp includes,
from DJ's grammar.

    python generate.py --antlr-jar build/antlr.jar --out build/gen

The only target-specific code in the grammar is three helper methods in the
lexer's `@members` block, which are ported to C++ here.
"""

import argparse
import re
import subprocess
from pathlib import Path

GRAMMAR = (
    Path(__file__).resolve().parents[1]
    / "datajunction_server/sql/parsing/backends/grammar"
)

CPP_MEMBERS = """@members {
public:
  bool has_unclosed_bracketed_comment = false;
  bool isValidDecimal() {
    size_t nextChar = _input->LA(1);
    return !((nextChar >= 'A' && nextChar <= 'Z') || (nextChar >= '0' && nextChar <= '9') || nextChar == '_');
  }
  bool isHint() { return _input->LA(1) == '+'; }
  void markUnclosedComment() { has_unclosed_bracketed_comment = true; }
}"""


def port_lexer_members(source: str) -> str:
    start = source.index("@members {")
    depth = 0
    for position in range(start + len("@members "), len(source)):
        if source[position] == "{":
            depth += 1
        elif source[position] == "}":
            depth -= 1
            if depth == 0:
                return source[:start] + CPP_MEMBERS + source[position + 1 :]
    raise ValueError("unterminated @members block")


def parser_tables(header: str, out: Path) -> None:
    # Rules that can be called without arguments.
    rules = []
    for _, name in re.findall(r"^\s+(\w+Context)\*\s+(\w+)\(\);", header, re.M):
        if name not in rules:
            rules.append(name)
    (out / "rules_table.inc").write_text(
        "\n".join(
            f'  {{"{name}", [](SqlBaseParser& p) -> ParserRuleContext* {{ return p.{name}(); }}}},'
            for name in rules
        ),
    )

    # Labelled elements (`name=rule`, `op=TOKEN`, and `+=` lists), per context class.
    labels: dict[str, list[tuple[str, bool]]] = {}
    for cls, body in re.findall(
        r"class\s+(\w+Context)\s*:\s*public\s+[\w:]+\s*\{(.*?)\n  \};",
        header,
        re.S,
    ):
        for pattern, is_list in (
            (r"^\s+(?:SqlBaseParser::)?\w+Context\s*\*\s*(\w+)\s*=\s*nullptr;", False),
            (r"^\s+antlr4::Token\s*\*\s*(\w+)\s*=\s*nullptr;", False),
            (
                r"^\s+std::vector<(?:antlr4::Token\s*\*|(?:SqlBaseParser::)?\w+Context\s*\*)>\s+(\w+);",
                True,
            ),
        ):
            for match in re.finditer(pattern, body, re.M):
                labels.setdefault(cls, []).append((match.group(1), is_list))

    names = sorted({entry for entries in labels.values() for entry in entries})
    ids = {entry: index for index, entry in enumerate(names)}
    entries = []
    for cls, members in labels.items():
        calls = " ".join(
            (
                f"for (auto* x : c->{name}) emit(i, {ids[(name, True)]}, x);"
                if is_list
                else f"if (c->{name}) emit(i, {ids[(name, False)]}, c->{name});"
            )
            for name, is_list in members
        )
        entries.append(
            f"  {{&typeid(SqlBaseParser::{cls}), [](ParserRuleContext* ctx, int32_t i, const Emit& emit) "
            f"{{ auto* c = static_cast<SqlBaseParser::{cls}*>(ctx); {calls} }}}},",
        )
    (out / "labels_table.inc").write_text("\n".join(entries))

    # ANTLR appends "_" to labels that are C++ keywords (`operator` -> `operator_`);
    # the Python parser keeps the grammar's name.
    (out / "label_names.inc").write_text(
        ",".join(
            f'{{"{name[:-1] if name.endswith("_") else name}", {"true" if is_list else "false"}}}'
            for name, is_list in names
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--antlr-jar", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    (out / "SqlBaseLexer.g4").write_text(
        port_lexer_members((GRAMMAR / "SqlBaseLexer.g4").read_text()),
    )
    (out / "SqlBaseParser.g4").write_text((GRAMMAR / "SqlBaseParser.g4").read_text())
    subprocess.run(
        [
            "java",
            "-jar",
            args.antlr_jar,
            "-Dlanguage=Cpp",
            "-no-visitor",
            "-no-listener",
            "SqlBaseLexer.g4",
            "SqlBaseParser.g4",
        ],
        cwd=out,
        check=True,
    )
    parser_tables((out / "SqlBaseParser.h").read_text(), out)


if __name__ == "__main__":
    main()
