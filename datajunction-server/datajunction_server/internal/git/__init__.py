"""Git integration services for DJ."""

__all__ = ["GitHubService"]


def __getattr__(name: str):
    """Lazy import to avoid circular import issues."""
    if name == "GitHubService":
        from datajunction_server.internal.git.github_service import GitHubService

        return GitHubService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
