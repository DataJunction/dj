"""Rendering helpers for deployment results and downstream impacts."""

from rich.console import Console, Group
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text

from datajunction.models import DeploymentInfo, DeploymentResult, DownstreamImpact


class TerminalColor:
    """ANSI-safe terminal color names for rich output."""

    RED = "red"
    GREEN = "green"
    YELLOW = "yellow"
    CYAN = "cyan"
    WHITE = "white"
    BRIGHT_BLACK = "bright_black"  # dark grey


class TextStyle:
    """Rich text style attribute names."""

    BOLD = "bold"
    DIM = "dim"
    ITALIC = "italic"


# Keyed by the string values of ResultStatus - rendering is a display layer and
# these values are stable API constants that won't change without a major version.
_RESULT_ICONS: dict[str, str] = {
    "success": "✓",
    "failed": "✗",
    "invalid": "✗",
    "skipped": "–",
    "noop": "·",
}

_RESULT_COLORS: dict[str, str] = {
    "success": TerminalColor.GREEN,
    "failed": TerminalColor.RED,
    "invalid": TerminalColor.RED,
    "skipped": TerminalColor.BRIGHT_BLACK,
    "noop": TextStyle.DIM,
}

_ERROR_STATUSES = frozenset({"failed", "invalid"})


def _strip_summary_lines(message: str) -> str:
    """Remove server-generated success-summary lines from an error message.

    The server bundles lines like "Updated transform (v2.0)" and
    "└─ Updated query, dimension_links" into the same message string as the
    actual error.  Those restate what the result row already shows, so we drop
    any line that starts with "Updated " or with a tree connector ("└─", "├─").
    """
    lines = [
        line
        for line in message.split("\n")
        if not line.startswith("Updated ")
        and not line.startswith("└─")
        and not line.startswith("├─")
    ]
    return "\n".join(line for line in lines if line.strip())


def _short_name(name: str, namespace: str) -> str:
    prefix = namespace + "."
    return name[len(prefix) :] if name.startswith(prefix) else name


def _render_error_bullets(
    message: str,
    color: str = TerminalColor.RED,
    indent: str = "     ",
) -> Text:
    """Render a semicolon-separated error message as indented bullet points."""
    bullets = [m.strip() for m in message.split(";") if m.strip()]
    continuation = " " * (len(indent) + 2)  # aligns text after "• "

    def _append_text(body: Text, text: str) -> None:
        """Append text with backtick highlighting, indenting embedded newlines."""
        for li, line in enumerate(text.split("\n")):
            if li > 0:
                body.append(f"\n{continuation}")
            for j, part in enumerate(line.split("`")):
                body.append(
                    part,
                    style=f"{TextStyle.BOLD} {TerminalColor.BRIGHT_BLACK}"
                    if j % 2 == 1
                    else color,
                )

    body = Text()
    for i, bullet in enumerate(bullets):
        if i > 0:
            body.append("\n")
        body.append(f"{indent}• ", style=f"{TextStyle.BOLD} {color}")
        head, _, tail = bullet.partition(": ")
        _append_text(body, head)
        if tail:
            body.append(f"\n{continuation}")
            _append_text(body, tail)
    return body


def _count_by_status(results: list[DeploymentResult]) -> dict[str, int]:
    """Count results grouped by status."""
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts


def _group_dim_links(
    results: list[DeploymentResult],
) -> tuple[list[DeploymentResult], dict[str, list[DeploymentResult]]]:
    """Split results into regular nodes and dimension link children.

    Dimension link results have names of the form 'parent_node -> dim_node'.
    They are grouped by parent so they can be rendered nested under their parent row.
    """
    regular: list[DeploymentResult] = []
    dim_link_children: dict[str, list[DeploymentResult]] = {}
    for r in results:
        if " -> " in r.name:
            parent = r.name.split(" -> ")[0]
            dim_link_children.setdefault(parent, []).append(r)
        else:
            regular.append(r)
    return regular, dim_link_children


def _render_dim_link_children(
    node_name: str,
    dim_link_children: dict[str, list[DeploymentResult]],
    verbose: bool,
) -> Text:
    """Return a Text block of dimension link children for a given parent node."""
    visible = [
        c
        for c in dim_link_children.get(node_name, [])
        if verbose or c.operation != "noop"
    ]
    out = Text()
    if not visible:
        return out
    out.append("\n     ", style="")
    out.append("dimension links", style=f"{TextStyle.DIM} {TextStyle.ITALIC}")
    for ci, child in enumerate(visible):
        connector = "└" if ci == len(visible) - 1 else "├"
        color = _RESULT_COLORS.get(child.status, TerminalColor.WHITE)
        icon = _RESULT_ICONS.get(child.status, " ")
        child_is_error = child.status in _ERROR_STATUSES
        dim_name = child.name.split(" -> ", 1)[-1]
        weight = f"{TextStyle.BOLD} " if child_is_error else ""
        out.append(f"\n     {connector}─ ")
        out.append(f"{icon}  ", style=f"{weight}{color}")
        out.append(f"{child.operation:<8}  ", style=TextStyle.DIM)
        out.append(f"→ {dim_name}", style=f"{weight}{color}")
        if child.changed_fields:
            out.append(f"  [{', '.join(child.changed_fields)}]", style=TextStyle.DIM)
        if child_is_error and child.message:
            out.append("\n")
            out.append_text(_render_error_bullets(child.message, indent="        "))
    return out


def _render_result_rows(
    regular_results: list[DeploymentResult],
    dim_link_children: dict[str, list[DeploymentResult]],
    verbose: bool,
) -> Text:
    """Build the Text block showing each result row and its dim link children."""
    rows = Text()
    for result in regular_results:
        color = _RESULT_COLORS.get(result.status, TerminalColor.WHITE)
        icon = _RESULT_ICONS.get(result.status, " ")
        if not verbose and result.operation == "noop":
            continue
        if rows:
            rows.append("\n")
        is_error = result.status in _ERROR_STATUSES
        weight = f"{TextStyle.BOLD} " if is_error else ""
        rows.append(f"  {icon}  ", style=f"{weight}{color}")
        rows.append(f"{result.operation:<8}  ", style=TextStyle.DIM)
        rows.append(result.name, style=f"{weight}{color}")
        if result.changed_fields:
            rows.append(f"  [{', '.join(result.changed_fields)}]", style=TextStyle.DIM)
        if is_error and result.message:
            error_msg = _strip_summary_lines(result.message)
            if error_msg:
                rows.append("\n")
                rows.append_text(_render_error_bullets(error_msg))
        dim_text = _render_dim_link_children(result.name, dim_link_children, verbose)
        if dim_text:
            rows.append_text(dim_text)
    return rows


def _impact_annotation(
    impact: DownstreamImpact,
    namespace: str,
    current_parent: str = "",
) -> str:
    if impact.node_type == "cube":
        via = [_short_name(c, namespace) for c in impact.caused_by]
        return (
            f"  [{TextStyle.DIM}](via: {', '.join(via)})[/{TextStyle.DIM}]"
            if via
            else ""
        )
    others = [
        _short_name(c, namespace) for c in impact.caused_by if c != current_parent
    ]
    return (
        f"  [{TextStyle.DIM}](also via: {', '.join(others)})[/{TextStyle.DIM}]"
        if others
        else ""
    )


def _build_impacts_by_cause(
    impacts: list[DownstreamImpact],
) -> dict[str, list[DownstreamImpact]]:
    index: dict[str, list[DownstreamImpact]] = {}
    for impact in impacts:
        for cause in impact.caused_by:
            index.setdefault(cause, []).append(impact)
    return index


def _collect_transitive_cubes(
    node_name: str,
    impacts_by_cause: dict[str, list[DownstreamImpact]],
) -> list[DownstreamImpact]:
    cubes: dict[str, DownstreamImpact] = {}
    visited: set[str] = set()
    queue = [node_name]
    while queue:
        current = queue.pop()
        for impact in impacts_by_cause.get(current, []):
            if impact.name in visited:
                continue
            visited.add(impact.name)
            if impact.node_type == "cube":
                cubes[impact.name] = impact
            else:
                queue.append(impact.name)
    return list(cubes.values())


def _collect_impact_lines(
    items: list[DownstreamImpact],
    indent: str,
    namespace: str,
    impacts_by_cause: dict[str, list[DownstreamImpact]],
    rendered: set[str],
    current_parent: str,
) -> list[str]:
    """Recursively build downstream impact tree as markup strings."""
    lines: list[str] = []
    for i, impact in enumerate(items):
        branch = indent + ("└" if i == len(items) - 1 else "├")
        continuation = indent + (" " if i == len(items) - 1 else "│")
        name = _short_name(impact.name, namespace)
        annotation = _impact_annotation(impact, namespace, current_parent)
        lines.append(
            f"{branch} [{TerminalColor.RED}]✗[/{TerminalColor.RED}]"
            f" [{TextStyle.DIM}]{impact.node_type}[/{TextStyle.DIM}]"
            f" [{TextStyle.BOLD} {TerminalColor.RED}]{name}[/{TextStyle.BOLD} {TerminalColor.RED}]"
            f"[{TextStyle.DIM}]  → invalid[/{TextStyle.DIM}]{annotation}",
        )
        children = [
            child
            for child in impacts_by_cause.get(impact.name, [])
            if child.name not in rendered and child.node_type != "cube"
        ]
        rendered.update(child.name for child in children)
        if children:
            lines.extend(
                _collect_impact_lines(
                    children,
                    continuation + "  ",
                    namespace,
                    impacts_by_cause,
                    rendered,
                    impact.name,
                ),
            )
    return lines


def _build_downstream_text(
    downstream_impacts: list[DownstreamImpact],
    namespace: str,
) -> Text | None:
    """
    Build the downstream impact tree as a single Text renderable (for panel embedding).
    """
    # Only surface nodes that will actually be invalidated. This uses predicted_status
    # since both dry-run and wet-run go through the same SAVEPOINT-based execution path.
    invalidated = [d for d in downstream_impacts if d.predicted_status == "invalid"]
    if not invalidated:
        return None

    impacts_by_cause = _build_impacts_by_cause(invalidated)
    roots = list(
        dict.fromkeys(cause for impact in invalidated for cause in impact.caused_by),
    )

    lines: list[str] = []
    rendered: set[str] = set()
    for root in roots:
        non_cube = [
            impact
            for impact in impacts_by_cause.get(root, [])
            if impact.node_type != "cube" and impact.name not in rendered
        ]
        cubes = [
            impact
            for impact in _collect_transitive_cubes(root, impacts_by_cause)
            if impact.name not in rendered
        ]
        items = non_cube + cubes
        if not items:
            continue
        lines.append(
            f"  [{TextStyle.DIM}]from[/{TextStyle.DIM}]"
            f" [{TextStyle.BOLD}]{_short_name(root, namespace)}[/{TextStyle.BOLD}]",
        )
        rendered.update(impact.name for impact in items)
        lines.extend(
            _collect_impact_lines(
                items,
                "  ",
                namespace,
                impacts_by_cause,
                rendered,
                root,
            ),
        )

    if not lines:
        return None

    return Text.from_markup("\n".join(lines))


def _build_summary(
    counts: dict[str, int],
    error_count: int,
    downstream_impacts: list[DownstreamImpact],
    verbose: bool,
) -> list[Text]:
    """
    Build the summary line segments shown at the bottom of the panel.
    """
    invalidated_downstream = [
        d for d in downstream_impacts if d.predicted_status == "invalid"
    ]
    parts: list[Text] = []
    if counts.get("success"):
        parts.append(
            Text(f"✓ {counts['success']} succeeded", style=TerminalColor.GREEN),
        )
    if counts.get("skipped"):
        parts.append(
            Text(f"– {counts['skipped']} skipped", style=TerminalColor.BRIGHT_BLACK),
        )
    if not verbose and counts.get("noop"):
        parts.append(Text(f"{counts['noop']} noop", style=TextStyle.DIM))
    downstream_count = len(invalidated_downstream)
    total_invalid = error_count + downstream_count
    if total_invalid:
        t = Text(
            f"✗ {total_invalid} invalid",
            style=f"{TextStyle.BOLD} {TerminalColor.RED}",
        )
        if error_count and downstream_count:
            t.append(
                f"  ({error_count} direct, {downstream_count} downstream)",
                style=f"{TextStyle.DIM} {TerminalColor.RED}",
            )
        elif downstream_count:
            t.append("  (downstream)", style=f"{TextStyle.DIM} {TerminalColor.RED}")
        parts.append(t)
    return parts


def print_results(
    deployment_uuid: str,
    deployment: DeploymentInfo,
    console: Console,
    verbose: bool = False,
) -> None:
    """
    Render deployment results (and potential downstream impacts) in a single panel.
    """
    counts = _count_by_status(deployment.results)
    error_count = counts.get("failed", 0) + counts.get("invalid", 0)
    border_color = (
        TerminalColor.RED
        if deployment.status == "failed" or error_count > 0
        else TerminalColor.GREEN
    )

    regular, dim_link_children = _group_dim_links(deployment.results)
    rows = _render_result_rows(regular, dim_link_children, verbose)
    summary_parts = _build_summary(
        counts,
        error_count,
        deployment.downstream_impacts,
        verbose,
    )
    summary = Text("  ").join(summary_parts) if summary_parts else Text()
    impacts_text = _build_downstream_text(
        deployment.downstream_impacts,
        deployment.namespace,
    )

    panel_parts: list = [rows]
    if impacts_text is not None:
        panel_parts += [
            Rule(style=TextStyle.DIM),
            Text("  Downstream Impacts", style=TextStyle.BOLD),
            impacts_text,
        ]
    if summary_parts:
        panel_parts += [Rule(style=TextStyle.DIM), summary]
    content = Group(*panel_parts) if len(panel_parts) > 1 else rows

    console.print()
    console.print(
        Panel(
            content,
            title=f"[{TextStyle.DIM}]{deployment_uuid}  ·  {deployment.namespace}[/{TextStyle.DIM}]",
            title_align="left",
            border_style=border_color,
            padding=(0, 1),
        ),
    )
