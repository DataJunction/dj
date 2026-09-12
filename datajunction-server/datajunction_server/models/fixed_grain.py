"""
Semantics of a metric's declared fixed grain.

`fixed_grain` is tri-state and every consumer has to honour the same three
rules, so they live here rather than being restated at each boundary:

- `None` means the query grain (no declaration).
- `[]` means the global grain, and must stay distinct from `None`.
- Dimension order and duplicates are irrelevant: the declaration becomes a
  `PARTITION BY` set.
"""


def fixed_grain_identity(grain: list[str] | None) -> frozenset[str] | None:
    """
    The value to compare when deciding whether two declarations differ.
    """
    return None if grain is None else frozenset(grain)
