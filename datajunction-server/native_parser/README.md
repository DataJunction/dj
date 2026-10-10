# Native parser prototype

Parses SQL with the ANTLR **C++** runtime instead of the pure-Python one, then
rebuilds the same `SqlBaseParser.*Context` objects so `visit()` runs unchanged.
It is an experiment. DJ uses it only when the extension is built and
`DJ_NATIVE_PARSER=1` is set; otherwise nothing changes.

## Build

Needs Java 11+, CMake, a C++17 compiler, and `pybind11` in the Python you build for.

    cd datajunction-server/native_parser
    PYTHON=$(which python) ./build.sh

This downloads ANTLR 4.13.2 and its C++ runtime, generates the C++ lexer and
parser from `datajunction_server/sql/parsing/backends/grammar`, and builds
`build/dj_native_parse*.so`. To use it, put `native_parser/build` on `PYTHONPATH` and set
`DJ_NATIVE_PARSER=1`. `tests/sql/parsing/backends/native_parser_test.py` checks it against the
Python parser.

## How it works

- `module.cpp` runs the C++ lexer and parser (SLL first, then LL, the same as the
  Python path) and returns the tree as flat arrays: node kinds, parents, token
  positions, and the rule and token labels the visitor reads (`ctx.name`, ...).
- `datajunction_server/sql/parsing/backends/native.py` rebuilds the Python context objects
  from those arrays, and `parse_sql_with_sll_fallback` tries it first when enabled.
- `generate.py` ports the three helper methods in the lexer's `@members` block to
  C++ and generates the rule and label tables from the generated parser header.

## Results

On 1,233 real node queries (warm, one machine):

| | Python | Native |
|---|---|---|
| ANTLR parse | about 1.1s | about 0.06s (C++), 0.35s with the rebuild |
| parse + `visit` | 1.70s | 0.94s |

The DJ AST matched the Python parser's on every query that parses, including the
TPC-DS queries in this repo's tests, by SQL text and by `serialize_ast`.
After the native parse, `visit` is most of the remaining time.

## Not done

- Non-ASCII input: the Python stream upper-cases every character, this one only ASCII.
- Parse error messages differ; the error types match.
- Wheels (manylinux, macOS) and an optional install extra with fallback to the
  Python parser.
- Keeping the generated parser in step when the grammar changes.
