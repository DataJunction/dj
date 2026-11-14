#!/usr/bin/env bash
# Build the native parser prototype: generate the C++ lexer/parser from DJ's
# grammar, build the ANTLR C++ runtime, and compile the Python extension.
#
# Needs: java (11+), cmake, a C++17 compiler, python with pybind11 installed.
#   PYTHON=/path/to/python ./build.sh
set -euo pipefail

ANTLR_VERSION=4.13.2
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="$HERE/build"
PYTHON="${PYTHON:-python3}"
CXX="${CXX:-c++}"
mkdir -p "$BUILD"
RUNTIME_LIB="$BUILD/runtime/runtime/libantlr4-runtime.a"  # CMake puts the library under runtime/runtime

if [ ! -f "$BUILD/antlr.jar" ]; then
  curl -sfL -o "$BUILD/antlr.jar" \
    "https://repo1.maven.org/maven2/org/antlr/antlr4/$ANTLR_VERSION/antlr4-$ANTLR_VERSION-complete.jar"
fi
if [ ! -d "$BUILD/runtime-src" ]; then
  curl -sfL -o "$BUILD/runtime.zip" \
    "https://www.antlr.org/download/antlr4-cpp-runtime-$ANTLR_VERSION-source.zip"
  unzip -q "$BUILD/runtime.zip" -d "$BUILD/runtime-src"
fi
if [ ! -f "$RUNTIME_LIB" ]; then
  cmake -S "$BUILD/runtime-src" -B "$BUILD/runtime" -DCMAKE_BUILD_TYPE=Release \
    -DANTLR_BUILD_CPP_TESTS=OFF -DANTLR_BUILD_SHARED=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_POLICY_VERSION_MINIMUM=3.5
  cmake --build "$BUILD/runtime" --target antlr4_static -j "$(getconf _NPROCESSORS_ONLN)"
fi

"$PYTHON" "$HERE/generate.py" --antlr-jar "$BUILD/antlr.jar" --out "$BUILD/gen"

EXT="$("$PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')"
EXTRA=()
if [ "$(uname)" = "Darwin" ]; then EXTRA=(-undefined dynamic_lookup); fi
# shellcheck disable=SC2046
"$CXX" -std=c++17 -O2 -shared -fPIC "${EXTRA[@]}" -DANTLR4CPP_STATIC \
  $("$PYTHON" -m pybind11 --includes) \
  -I"$BUILD/runtime-src/runtime/src" -I"$BUILD/gen" \
  "$HERE/module.cpp" "$BUILD/gen/SqlBaseLexer.cpp" "$BUILD/gen/SqlBaseParser.cpp" \
  "$RUNTIME_LIB" -o "$BUILD/dj_native_parse$EXT"
echo "built $BUILD/dj_native_parse$EXT"
