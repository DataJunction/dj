"""Tests for yaml_export helpers (fetch_existing_yaml_map)."""

import io
import tarfile
from unittest.mock import AsyncMock, patch

import pytest

from datajunction_server.internal.git.yaml_export import fetch_existing_yaml_map


def _make_tarball(files: dict[str, str], top_dir: str = "repo-abc123") -> bytes:
    """Build a gzipped tarball that mimics what GitHub's archive endpoint returns:
    a single top-level directory containing the repo files."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=f"{top_dir}/{path}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _make_empty_tarball() -> bytes:
    """Tarball with no entries — covers the `not extracted_dirs` early return."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz"):
        pass
    return buf.getvalue()


@pytest.mark.asyncio
async def test_fetch_existing_yaml_map_with_git_path():
    """When git_path is set, only YAMLs under that subdirectory are returned,
    keyed by their full repo-relative path."""
    archive = _make_tarball(
        {
            "definitions/foo.yaml": "name: foo\n",
            "definitions/bar/baz.yaml": "name: bar.baz\n",
            "outside/skip.yaml": "name: skip\n",  # outside git_path, must be excluded
        },
    )
    with patch(
        "datajunction_server.internal.git.yaml_export.GitHubService",
    ) as mock_cls:
        mock_cls.return_value.download_archive = AsyncMock(return_value=archive)
        result = await fetch_existing_yaml_map(
            github_repo_path="org/repo",
            git_branch="main",
            git_path="definitions",
        )

    assert result == {
        "definitions/foo.yaml": "name: foo\n",
        "definitions/bar/baz.yaml": "name: bar.baz\n",
    }


@pytest.mark.asyncio
async def test_fetch_existing_yaml_map_empty_archive():
    """An archive with no top-level directory returns an empty map."""
    with patch(
        "datajunction_server.internal.git.yaml_export.GitHubService",
    ) as mock_cls:
        mock_cls.return_value.download_archive = AsyncMock(
            return_value=_make_empty_tarball(),
        )
        result = await fetch_existing_yaml_map(
            github_repo_path="org/repo",
            git_branch="main",
            git_path=None,
        )
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_existing_yaml_map_missing_git_path():
    """If git_path is set but doesn't exist in the archive, return empty map."""
    archive = _make_tarball({"README.md": "hi\n"})
    with patch(
        "datajunction_server.internal.git.yaml_export.GitHubService",
    ) as mock_cls:
        mock_cls.return_value.download_archive = AsyncMock(return_value=archive)
        result = await fetch_existing_yaml_map(
            github_repo_path="org/repo",
            git_branch="main",
            git_path="definitions",  # not present in archive
        )
    assert result == {}
