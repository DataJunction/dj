"""Low-level helpers for parsing semantic dimension references."""


def split_dimension_ref(ref: str) -> tuple[str, str | None]:
    """Split a dimension reference into its base reference and optional role."""
    if "[" not in ref:
        return ref, None
    base, role = ref.rsplit("[", 1)
    return base, role.rstrip("]")
