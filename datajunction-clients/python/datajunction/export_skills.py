"""Export DataJunction skills for Claude Code."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import httpx
import yaml
from rich.console import Console

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
console = Console()


def export_skills(
    api_url: str,
    token: Optional[str] = None,
    output_dir: Path = Path.home() / ".claude" / "skills",
    force: bool = False,
) -> int:
    """Export DJ skills from server.

    Args:
        api_url: DJ API URL (e.g., http://localhost:8000)
        token: Optional API authentication token
        output_dir: Directory to write skill files
        force: Force re-export even if files exist

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Set up HTTP client
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    console.print(f"\n[bold blue]📚 Exporting DJ skills from {api_url}[/bold blue]\n")

    try:
        with httpx.Client(headers=headers, timeout=30.0) as client:
            # Fetch core skills
            skill_names = ["dj-core", "dj-builder", "dj-consumer"]

            for skill_name in skill_names:
                output_file = output_dir / f"{skill_name}.yaml"

                # Skip if exists and not force
                if output_file.exists() and not force:
                    console.print(f"⚠️  Skipped {skill_name}.yaml (already exists, use --force to overwrite)")
                    continue

                console.print(f"Fetching [cyan]{skill_name}[/cyan]...")

                # Make request
                response = client.get(f"{api_url}/skills/{skill_name}")

                if response.status_code != 200:
                    console.print(f"[red]✗ Failed to fetch {skill_name}: {response.status_code}[/red]")
                    console.print(f"[dim]{response.text}[/dim]")
                    continue

                # Parse and write
                skill_data = response.json()
                with open(output_file, "w") as f:
                    yaml.dump(skill_data, f, sort_keys=False, allow_unicode=True)

                console.print(f"[green]✓ Exported {output_file}[/green]")
                console.print(f"  [dim]Version: {skill_data.get('version', 'unknown')}[/dim]")
                console.print(f"  [dim]Keywords: {len(skill_data.get('keywords', []))}[/dim]\n")

            console.print(f"\n[bold green]✓ Skills exported to {output_dir}[/bold green]")
            console.print("\n[dim]Skills are now available in Claude Code.[/dim]")

            return 0

    except httpx.ConnectError:
        console.print(f"\n[red]✗ Failed to connect to DJ server at {api_url}[/red]")
        console.print("[dim]Make sure the DJ server is running and the URL is correct.[/dim]")
        return 1
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
        logger.exception("Failed to export skills")
        return 1


def main():
    """CLI entry point for dj-export-skills command."""
    parser = argparse.ArgumentParser(
        description="Export DataJunction skills for Claude Code",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export from local server
  dj-export-skills

  # Export from remote server
  dj-export-skills --api-url https://dj.company.com

  # Export with authentication
  dj-export-skills --token YOUR_API_TOKEN

  # Force re-export (overwrite existing)
  dj-export-skills --force

  # Custom output directory
  dj-export-skills --output ~/my-skills
        """,
    )

    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="DJ API URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--token",
        help="API authentication token",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.home() / ".claude" / "skills",
        help="Output directory for skill files (default: ~/.claude/skills)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-export, overwriting existing files",
    )

    args = parser.parse_args()

    exit_code = export_skills(
        api_url=args.api_url,
        token=args.token,
        output_dir=args.output,
        force=args.force,
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
