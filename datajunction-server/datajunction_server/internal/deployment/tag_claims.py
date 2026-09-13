"""
Namespace claims over tag types.

Tags are global — ``Tag.name`` is unique with no owning namespace — while
deployments are not, so every namespace that declares the same tag file is a
writer of the same rows. A claim names the one namespace allowed to write the
tags of a given tag type; every other deployment may reference those tags but
leaves them untouched.
"""

from sqlalchemy.ext.asyncio import AsyncSession


async def claimed_tag_type_namespaces(session: AsyncSession) -> dict[str, str]:
    """
    Map each claimed tag type to the namespace that claims it.

    This is the seam where the claim store plugs in — it is the only place that
    knows where claims live. Until one exists it returns an empty mapping, which
    leaves tag deployment behaving exactly as it did before claims.
    """
    return {}


def claim_owner_blocking_write(
    claims: dict[str, str],
    tag_type: str,
    namespace: str,
) -> str | None:
    """
    The namespace claiming ``tag_type`` when ``namespace`` is not it, else None.

    Identity of the deploying namespace is the whole test: a branch deploy of
    ``root`` targets ``root.<branch>``, so it is not the claim owner and skips.
    """
    owner = claims.get(tag_type)
    return owner if owner is not None and owner != namespace else None
