"""
Shared helper for generating namespace YAML exports.

Both the CLI export endpoint (`POST /namespaces/{ns}/export/yaml`) and the
UI sync-to-git endpoint (`POST /namespaces/{ns}/sync-to-git`) use this so
they produce identical YAML for the same node state.

Callers are responsible for supplying `existing_files_map` — either fetched
from GitHub via `fetch_existing_yaml_map` (sync-to-git) or unpacked from
the client's local directory (dj pull).
"""

import io
import logging
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.git.github_service import (
    GitHubService,
    GitHubServiceError,
)
from datajunction_server.internal.namespaces import (
    get_node_specs_for_export,
    node_spec_to_yaml,
)

_logger = logging.getLogger(__name__)


async def fetch_existing_yaml_map(
    github_repo_path: str,
    git_branch: str,
    git_path: Optional[str],
) -> Dict[str, str]:
    """
    Download the configured git branch as a tarball and return a mapping of
    `<git_path>/<file>.yaml` -> file contents for every YAML in the repo
    (or under git_path if configured).

    Returns an empty dict if the archive cannot be fetched or extracted.
    """
    existing_files_map: Dict[str, str] = {}

    try:
        github = GitHubService()
        archive_bytes = await github.download_archive(
            repo_path=github_repo_path,
            branch=git_branch,
            format="tarball",
        )
    except GitHubServiceError as e:
        _logger.warning(
            "Failed to download git archive for namespace export merge: %s",
            e,
        )
        return existing_files_map

    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
            tar.extractall(tmpdir)

        extracted_dirs = list(Path(tmpdir).iterdir())
        if not extracted_dirs:
            return existing_files_map
        repo_dir = extracted_dirs[0]

        if git_path:
            files_dir = repo_dir / git_path.strip("/")
        else:
            files_dir = repo_dir

        if not files_dir.exists():
            return existing_files_map

        for yaml_file in files_dir.rglob("*.yaml"):
            rel_path = yaml_file.relative_to(repo_dir)
            try:
                existing_files_map[str(rel_path)] = yaml_file.read_text(
                    encoding="utf-8",
                )
            except Exception:  # pragma: no cover
                pass

    return existing_files_map


def _node_spec_to_file_path(spec_name: str, git_path: Optional[str]) -> str:
    """
    Convert a node spec name (with `${prefix}` injected) into the YAML
    file path within the git repository.

    e.g. `${prefix}orders` with `git_path="nodes"` -> `"nodes/orders.yaml"`.
    """
    if spec_name.startswith("${prefix}"):
        short_name = spec_name[len("${prefix}") :]
    else:  # pragma: no cover
        short_name = spec_name

    parts = short_name.split(".")
    file_path = "/".join(parts) + ".yaml"
    if git_path:
        file_path = f"{git_path.strip('/')}/{file_path}"
    return file_path


async def generate_namespace_yaml_files(
    session: AsyncSession,
    namespace: str,
    existing_files_map: Dict[str, str],
    git_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Generate YAML files for every node in a namespace.

    `existing_files_map` is a `{file_path: yaml_content}` dict that the caller
    has already fetched or assembled. Paths must use the same convention as
    `_node_spec_to_file_path` — i.e. relative to the repo root, including any
    git_path prefix. Pass `{}` for a fresh dump with no merge.

    Returns a list of dicts with keys:
        - path: relative file path (including git_path prefix if set)
        - content: serialized YAML
        - node_name: spec name with `${prefix}` injected
        - existing_yaml: original content from existing_files_map (or None)
        - node_spec: the NodeSpec object
    """
    node_specs = await get_node_specs_for_export(session, namespace)

    files: List[Dict[str, Any]] = []
    for node_spec in node_specs:
        file_path = _node_spec_to_file_path(node_spec.name, git_path)
        existing_yaml = existing_files_map.get(file_path)
        yaml_content = node_spec_to_yaml(node_spec, existing_yaml=existing_yaml)
        files.append(
            {
                "path": file_path,
                "content": yaml_content,
                "node_name": node_spec.name,
                "existing_yaml": existing_yaml,
                "node_spec": node_spec,
            },
        )

    return files
