"""A SQL fragment that may or may not need Jinja evaluation.

Generated models are a mix of two things: plain SQL that the warehouse reads
directly (`count(...)`, `min(...)`) and calls into the dialect-dispatch macros
(`fs_collect_set(...)`). Those compose -- an exact distinct roll-up is
`fs_array_size(fs_array_union_agg(...))` -- and the naive approach of pasting
`{{ ... }}` blocks into each other produces invalid Jinja.

`Expr` tracks which mode a fragment is in and promotes plain SQL into a Jinja
concatenation only when it actually has to wrap a macro call.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_PLACEHOLDER = re.compile(r"\{(\d+)\}")


def jinja_str(text: str) -> str:
    """Render `text` as a Jinja string literal."""
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


@dataclass(frozen=True)
class Expr:
    text: str
    is_jinja: bool = False

    @staticmethod
    def sql(text: str) -> Expr:
        return Expr(text, False)

    @staticmethod
    def jinja(text: str) -> Expr:
        return Expr(text, True)

    def as_arg(self) -> str:
        """This fragment as an argument inside a Jinja macro call."""
        return self.text if self.is_jinja else jinja_str(self.text)

    def render(self) -> str:
        """This fragment as it appears in a generated .sql file."""
        return "{{ " + self.text + " }}" if self.is_jinja else self.text


def macro(name: str, *args: Expr | str) -> Expr:
    """A call to a dialect-dispatch macro."""
    rendered = [a.as_arg() if isinstance(a, Expr) else str(a) for a in args]
    return Expr.jinja(f"{name}({', '.join(rendered)})")


def compose(template: str, *children: Expr) -> Expr:
    """Splice children into a SQL template using {0}, {1}, ... placeholders.

    Stays in plain-SQL mode while every child is plain SQL, and promotes to a
    Jinja concatenation the moment any child is a macro call. Substitution is
    done by hand rather than via str.format so that braces occurring naturally
    in SQL are never misread as placeholders.
    """
    if not any(c.is_jinja for c in children):
        out, pos = [], 0
        for m in _PLACEHOLDER.finditer(template):
            out.append(template[pos : m.start()])
            out.append(children[int(m.group(1))].text)
            pos = m.end()
        out.append(template[pos:])
        return Expr.sql("".join(out))

    parts, pos = [], 0
    for m in _PLACEHOLDER.finditer(template):
        literal = template[pos : m.start()]
        if literal:
            parts.append(jinja_str(literal))
        parts.append("(" + children[int(m.group(1))].as_arg() + ")")
        pos = m.end()
    tail = template[pos:]
    if tail:
        parts.append(jinja_str(tail))
    return Expr.jinja(" ~ ".join(parts))
