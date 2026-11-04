// Prototype: parse SQL with the ANTLR C++ runtime and hand back the parse tree as flat arrays.
// The lookup tables included below (rules_table.inc, labels_table.inc, label_names.inc)
// are generated from the grammar by generate.py.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <cxxabi.h>
#include <cctype>
#include <functional>
#include <memory>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <vector>
#include "antlr4-runtime.h"
#include "SqlBaseLexer.h"
#include "SqlBaseParser.h"

namespace py = pybind11;
using namespace antlr4;

namespace {
// Upper-cased lookahead, original text (same idea as DJ's UpperCaseInputStream).
class UpperStream : public ANTLRInputStream {
 public:
  using ANTLRInputStream::ANTLRInputStream;
  size_t LA(ssize_t i) override {
    size_t c = ANTLRInputStream::LA(i);
    // ASCII only. The Python stream upper-cases every character; non-ASCII input
    // needs the same treatment before this is used for real.
    return (c < 128 && std::islower((int)c)) ? (size_t)std::toupper((int)c) : c;
  }
};
class ThrowListener : public BaseErrorListener {
  void syntaxError(Recognizer*, Token*, size_t line, size_t col, const std::string& msg, std::exception_ptr) override {
    throw std::runtime_error("Parse error " + std::to_string(line) + ":" + std::to_string(col) + ": " + msg);
  }
};

using RuleFn = std::function<ParserRuleContext*(SqlBaseParser&)>;
const std::unordered_map<std::string, RuleFn>& rules() {
  static const std::unordered_map<std::string, RuleFn> table = {
#include "rules_table.inc"
  };
  return table;
}

struct Pending { int32_t owner, id; tree::ParseTree* node; int32_t tok; };
struct Emit {
  std::vector<Pending>* out;
  void operator()(int32_t owner, int32_t id, tree::ParseTree* n) const { out->push_back({owner, id, n, 0}); }
  void operator()(int32_t owner, int32_t id, Token* t) const { out->push_back({owner, id, nullptr, (int32_t)t->getTokenIndex()}); }
};
using LabelFn = std::function<void(ParserRuleContext*, int32_t, const Emit&)>;
const std::unordered_map<const std::type_info*, LabelFn>& label_fns() {
  static const std::unordered_map<const std::type_info*, LabelFn> table = {
#include "labels_table.inc"
  };
  return table;
}
const std::vector<std::pair<std::string, bool>>& label_names() {
  static const std::vector<std::pair<std::string, bool>> names = {
#include "label_names.inc"
  };
  return names;
}

struct Result {
  std::vector<std::string> names;
  std::vector<int32_t> kind, parent, tok, startTok, stopTok;
  std::vector<int32_t> tType, tStart, tStop, tLine, tCol;
  std::vector<int32_t> lblOwner, lblId, lblTarget;
};

std::string demangled(const std::type_info& ti) {
  int status = 0;
  char* d = abi::__cxa_demangle(ti.name(), nullptr, nullptr, &status);
  std::string s = d ? d : ti.name();
  free(d);
  auto pos = s.rfind("::");
  return pos == std::string::npos ? s : s.substr(pos + 2);
}

Result run(const std::string& sql, const std::string& rule, bool sll, bool bail) {
  auto it = rules().find(rule);
  if (it == rules().end()) throw std::runtime_error("unknown rule " + rule);
  ThrowListener tl;
  UpperStream stream(sql);
  SqlBaseLexer lexer(&stream);
  lexer.removeErrorListeners(); lexer.addErrorListener(&tl);
  CommonTokenStream tokens(&lexer);
  SqlBaseParser parser(&tokens);
  parser.removeErrorListeners(); parser.addErrorListener(&tl);
  if (bail) parser.setErrorHandler(std::make_shared<BailErrorStrategy>());
  if (sll) parser.getInterpreter<atn::ParserATNSimulator>()->setPredictionMode(atn::PredictionMode::SLL);
  ParserRuleContext* root = it->second(parser);

  Result r;
  std::vector<Pending> pending;
  Emit emit{&pending};
  std::unordered_map<tree::ParseTree*, int32_t> index;
  std::unordered_map<const std::type_info*, int32_t> kinds;
  std::vector<std::pair<tree::ParseTree*, int32_t>> stack{{root, -1}};
  while (!stack.empty()) {
    auto [node, par] = stack.back(); stack.pop_back();
    int32_t i = (int32_t)r.kind.size();
    index[node] = i;
    r.parent.push_back(par);
    if (auto* term = dynamic_cast<tree::TerminalNode*>(node)) {
      r.kind.push_back(-1); r.tok.push_back((int32_t)term->getSymbol()->getTokenIndex());
      r.startTok.push_back(-1); r.stopTok.push_back(-1);
    } else {
      auto* ctx = static_cast<ParserRuleContext*>(node);
      const std::type_info* ti = &typeid(*ctx);
      auto k = kinds.find(ti);
      if (k == kinds.end()) { k = kinds.emplace(ti, (int32_t)r.names.size()).first; r.names.push_back(demangled(*ti)); }
      r.kind.push_back(k->second); r.tok.push_back(-1);
      r.startTok.push_back(ctx->getStart() ? (int32_t)ctx->getStart()->getTokenIndex() : -1);
      r.stopTok.push_back(ctx->getStop() ? (int32_t)ctx->getStop()->getTokenIndex() : -1);
      auto lf = label_fns().find(ti);
      if (lf != label_fns().end()) lf->second(ctx, i, emit);
      for (size_t c = ctx->children.size(); c-- > 0;) stack.push_back({ctx->children[c], i});
    }
  }
  for (Token* t : tokens.getTokens()) {
    r.tType.push_back((int32_t)t->getType()); r.tStart.push_back((int32_t)t->getStartIndex());
    r.tStop.push_back((int32_t)t->getStopIndex()); r.tLine.push_back((int32_t)t->getLine());
    r.tCol.push_back((int32_t)t->getCharPositionInLine());
  }
  for (auto& p : pending) {
    int32_t target;
    if (p.node) { auto f = index.find(p.node); if (f == index.end()) continue; target = f->second; }
    else target = -(p.tok + 1);
    r.lblOwner.push_back(p.owner); r.lblId.push_back(p.id); r.lblTarget.push_back(target);
  }
  return r;
}

template <class T> py::bytes bytes_of(const std::vector<T>& v) { return py::bytes(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(T)); }
}  // namespace

py::tuple parse(const std::string& sql, const std::string& rule) {
  Result r;
  {
    py::gil_scoped_release release;  // only the C++ parse runs without the GIL
    try {
      r = run(sql, rule, /*sll=*/true, /*bail=*/true);
    } catch (std::exception&) {
      r = run(sql, rule, /*sll=*/false, /*bail=*/false);  // same SLL -> LL fallback as the Python path
    }
  }
  return py::make_tuple(r.names, bytes_of(r.kind), bytes_of(r.parent), bytes_of(r.tok), bytes_of(r.startTok), bytes_of(r.stopTok),
                        bytes_of(r.tType), bytes_of(r.tStart), bytes_of(r.tStop), bytes_of(r.tLine), bytes_of(r.tCol),
                        bytes_of(r.lblOwner), bytes_of(r.lblId), bytes_of(r.lblTarget), label_names());
}

PYBIND11_MODULE(dj_native_parse, m) { m.def("parse", &parse); }
