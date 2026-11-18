"""
Optional parser backend: parse with the ANTLR C++ runtime, then rebuild the same
`SqlBaseParser.*Context` objects the Python parser would have produced, so
`visit()` runs unchanged.

Used only when the `dj_native_parse` extension is built (see native_parser/) and
`DJ_NATIVE_PARSER` is set to 1/true/yes. Anything that goes wrong here makes the
caller fall back to the Python parser.
"""

from array import array
import os

try:
    import dj_native_parse
except ImportError:  # the extension is optional
    dj_native_parse = None
from antlr4 import ParserRuleContext
from antlr4.Token import CommonToken
from antlr4.tree.Tree import TerminalNodeImpl
from datajunction_server.sql.parsing.backends.grammar.generated.SqlBaseParser import (
    SqlBaseParser,
)

ENABLED = dj_native_parse is not None and os.environ.get(
    "DJ_NATIVE_PARSER",
    "",
).lower() in (
    "1",
    "true",
    "yes",
)

_CLASSES: dict[str, tuple[type, bool]] = {}  # name -> (class, takes (parser, ctx))


def _cls(name):
    c = _CLASSES.get(name)
    if c is None:
        k = getattr(SqlBaseParser, name)
        try:
            k(None, None, 0)
            labeled = False
        except TypeError:
            labeled = True
        c = _CLASSES[name] = (k, labeled)
    return c


def _ints(b):
    a = array("i")
    a.frombytes(b)
    return a


def native_tree(sql: str, rule: str):
    try:
        (
            names,
            kind,
            parent,
            tok,
            stt,
            stp,
            tType,
            tStart,
            tStop,
            tLine,
            tCol,
            lOwner,
            lId,
            lTarget,
            lNames,
        ) = dj_native_parse.parse(sql, rule)
    except RuntimeError as exc:
        from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError

        raise SqlSyntaxError(str(exc)) from exc
    kind, parent, tok, stt, stp = map(_ints, (kind, parent, tok, stt, stp))
    tType, tStart, tStop, tLine, tCol = map(_ints, (tType, tStart, tStop, tLine, tCol))
    lOwner, lId, lTarget = map(_ints, (lOwner, lId, lTarget))
    classes = [_cls(n) for n in names]
    tokens: dict[int, CommonToken] = {}

    def token(i):
        t = tokens.get(i)
        if t is None:
            t = CommonToken(
                source=(None, None),
                type=tType[i],
                start=tStart[i],
                stop=tStop[i],
            )
            t.line = tLine[i]
            t.column = tCol[i]
            t.tokenIndex = i
            t._text = "<EOF>" if tType[i] == -1 else sql[tStart[i] : tStop[i] + 1]
            tokens[i] = t
        return t

    nodes = [None] * len(kind)
    for i in range(len(kind)):
        p = nodes[parent[i]] if parent[i] >= 0 else None
        k = kind[i]
        if k < 0:
            n = TerminalNodeImpl(token(tok[i]))
            n.parentCtx = p
        else:
            cls, labeled = classes[k]
            n = cls(None, ParserRuleContext(p, 0)) if labeled else cls(None, p, 0)
            n.parentCtx = p
            if stt[i] >= 0:
                n.start = token(stt[i])
            if stp[i] >= 0:
                n.stop = token(stp[i])
        nodes[i] = n
        if p is not None:
            if p.children is None:
                p.children = [n]
            else:
                p.children.append(n)
    for o, lid, tgt in zip(lOwner, lId, lTarget):
        name, is_list = lNames[lid]
        value = nodes[tgt] if tgt >= 0 else token(-tgt - 1)
        if is_list:
            cur = getattr(nodes[o], name, None)
            if cur is None:
                setattr(nodes[o], name, [value])
            else:
                cur.append(value)
        else:
            setattr(nodes[o], name, value)
    return nodes[0]
