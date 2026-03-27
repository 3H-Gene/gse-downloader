"""CLI commands for GSE Downloader.

This module provides the command-line interface using Typer.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from gse_downloader import __version__
from gse_downloader.archive.profile import ArchiveGenerator, ArchiveProfile
from gse_downloader.core.downloader import GSEDownloader
from gse_downloader.core.state_manager import DownloadState, StateManager
from gse_downloader.parser.geo_query import GEOQuery
from gse_downloader.utils.config import Config, load_config
from gse_downloader.utils.logger import get_logger, setup_logger


def _ensure_utf8_streams() -> None:
    """Re-wrap stdout/stderr as UTF-8 on Windows (called at runtime, not import time).

    This avoids breaking pytest's stdout/stderr capture which happens at import time.
    """
    if sys.platform != "win32":
        return
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name)
        # Only re-wrap if it has a buffer and isn't already UTF-8
        if hasattr(stream, "buffer"):
            enc = getattr(stream, "encoding", "").lower().replace("-", "")
            if enc != "utf8":
                wrapped = io.TextIOWrapper(
                    stream.buffer,
                    encoding="utf-8",
                    errors="replace",
                    line_buffering=True,
                )
                setattr(sys, name, wrapped)


app = typer.Typer(
    name="gse-downloader",
    help="Enterprise-grade GEO data downloader with resume, checksum and statistics",
    add_completion=False,
)
# Console bound to current stdout (will be refreshed in callback)
console = Console(highlight=False)

logger = get_logger("cli")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(False, "--version", "-v", help="Show version"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
    log_level: str = typer.Option("INFO", "--log-level", help="Log level"),
    log_dir: Optional[Path] = typer.Option(None, "--log-dir", help="Log directory"),
):
    """Main entry point."""
    # Apply UTF-8 fix at runtime (avoids breaking pytest capture at import time)
    _ensure_utf8_streams()
    global console
    console = Console(file=sys.stdout, highlight=False)

    if version:
        console.print(f"[bold green]GSE Downloader[/bold green] v{__version__}")
        raise typer.Exit(0)

    # Setup logging
    setup_logger(log_dir=log_dir, log_level=log_level)

    # Load config
    if config and config.exists():
        cfg = load_config(config)
        typer.context = {"config": cfg}


@app.command()
def download(
    gse_id: str = typer.Argument(..., help="GSE identifier (e.g., GSE123456)"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Output directory"),
    files: Optional[str] = typer.Option(None, "--files", "-f", help="Comma-separated file list"),
    show_progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress"),
    force: bool = typer.Option(False, "--force", help="Force re-download even if already completed"),
):
    """Download GSE dataset.

    Automatically detects download status and resumes if incomplete.
    Use --force to re-download even if already completed.
    """
    gse_id = gse_id.upper().strip()

    # Load config
    if config and config.exists():
        cfg = load_config(config)
    else:
        cfg = Config()

    if output_dir:
        cfg.download.output_dir = output_dir

    console.print(f"[bold cyan]GSE Downloader[/bold cyan] - Downloading {gse_id}")

    # Validate GSE ID format and existence
    from gse_downloader.parser.geo_query import GEOQuery

    geo = GEOQuery()
    is_valid, error_msg = geo.validate_gse_id(gse_id)

    if not is_valid:
        console.print(f"[bold red]Error:[/bold red] {error_msg}")
        console.print("\n[yellow]Tips:[/yellow]")
        console.print("  * GSE ID should be in format: GSE123456")
        console.print("  * You can search for valid GSE IDs at: https://www.ncbi.nlm.nih.gov/geo/")
        raise typer.Exit(1)

    console.print(f"[green]OK[/green] GSE ID validated: {gse_id}")

    # Initialize downloader
    with GSEDownloader(
        output_dir=cfg.download.output_dir,
        max_workers=cfg.download.max_workers,
        timeout=cfg.download.timeout,
        verify_ssl=cfg.download.verify_ssl,
        retry_times=cfg.download.retry_times,
        auto_resume=cfg.download.auto_resume,
        checksum_algorithm=cfg.checksum.algorithm,
        show_progress=show_progress,
    ) as downloader:
        # Check current status
        state_manager = StateManager(cfg.download.output_dir / gse_id)
        status = state_manager.get_status()

        status_colors = {
            DownloadState.NOT_STARTED: "yellow",
            DownloadState.INCOMPLETE: "yellow",
            DownloadState.COMPLETED: "green",
            DownloadState.INVALID: "red",
        }

        console.print(f"Status: [{status_colors[status]}]{status.value}[/{status_colors[status]}]")

        if status == DownloadState.COMPLETED and not force:
            console.print("[green]Download already completed! Use --force to re-download.[/green]")
            raise typer.Exit(0)

        if status == DownloadState.COMPLETED and force:
            console.print("[yellow]Force re-download requested...[/yellow]")
            # Reset state so downloader treats files as not verified
            info = state_manager.load_state()
            for fs in info.files.values():
                fs.verified = False
            state_manager.save_state(info)

        if status == DownloadState.INCOMPLETE and cfg.download.auto_resume:
            console.print("[yellow]Resuming incomplete download...[/yellow]")

        # Get files to download
        if files:
            # User specified specific files, convert to dict format
            file_list = [{"filename": f.strip(), "url": None} for f in files.split(",")]
        else:
            console.print("Fetching file list...")
            file_list = downloader.get_gse_files(gse_id)
            if not file_list:
                console.print("[red]No files found for this GSE[/red]")
                raise typer.Exit(1)

        # Download
        console.print(f"Downloading {len(file_list)} files...")
        results = downloader.download_gse(gse_id, file_list)

        # Show results
        success_count = sum(1 for r in results.values() if r.success)
        fail_count = len(results) - success_count

        table = Table(title="Download Results")
        table.add_column("Filename", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Size", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Avg Speed", justify="right")
        table.add_column("Error", style="red")

        for filename, result in results.items():
            status_style = "green" if result.success else "red"
            status_mark = "[green]OK[/green]" if result.success else "[red]FAIL[/red]"

            # Size: show in human-readable form
            if result.size >= 1024 ** 3:
                size_str = f"{result.size / 1024**3:.2f} GB"
            elif result.size >= 1024 ** 2:
                size_str = f"{result.size / 1024**2:.1f} MB"
            elif result.size > 0:
                size_str = f"{result.size / 1024:.1f} KB"
            else:
                size_str = "-"

            # Duration
            dur = result.duration or 0
            if dur >= 60:
                duration_str = f"{int(dur // 60)}m {int(dur % 60)}s"
            else:
                duration_str = f"{dur:.1f}s"

            # Avg speed
            spd = getattr(result, "avg_speed", 0) or 0
            if spd >= 1024 ** 2:
                speed_str = f"{spd / 1024**2:.1f} MB/s"
            elif spd >= 1024:
                speed_str = f"{spd / 1024:.1f} KB/s"
            elif spd > 0:
                speed_str = f"{spd:.0f} B/s"
            else:
                speed_str = "-"

            error_str = (result.error or "")[:40] if not result.success else ""

            table.add_row(
                filename[:48],
                status_mark,
                size_str,
                duration_str,
                speed_str,
                error_str,
            )

        console.print(table)
        console.print(f"\n[green]Success: {success_count}[/green] | [red]Failed: {fail_count}[/red]")


@app.command()
def status(
    gse_id: str = typer.Argument(..., help="GSE identifier"),
):
    """Check download status of a GSE dataset."""
    gse_id = gse_id.upper().strip()

    cfg = Config()
    state_manager = StateManager(cfg.download.output_dir / gse_id)
    info = state_manager.load_state()

    table = Table(title=f"Status for {gse_id}")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("GSE ID", info.gse_id)
    table.add_row("Status", info.status.value)

    if info.started_at:
        table.add_row("Started", info.started_at.strftime("%Y-%m-%d %H:%M:%S"))

    if info.completed_at:
        table.add_row("Completed", info.completed_at.strftime("%Y-%m-%d %H:%M:%S"))

    table.add_row("Progress", f"{info.progress_percentage:.1f}%")
    table.add_row("Files", f"{info.completed_files}/{info.total_files}")
    table.add_row("Retries", str(info.retry_count))

    if info.last_error:
        table.add_row("[red]Last Error[/red]", info.last_error)

    console.print(table)


@app.command()
def archive(
    gse_id: str = typer.Argument(..., help="GSE identifier"),
    format: str = typer.Option("table", "--format", "-f", help="Output format (table/json)"),
):
    """View data archive for a GSE dataset."""
    gse_id = gse_id.upper().strip()

    cfg = Config()
    generator = ArchiveGenerator(cfg.download.output_dir)
    profile = generator.load(gse_id)

    if not profile:
        console.print(f"[red]Archive not found for {gse_id}[/red]")
        raise typer.Exit(1)

    if format == "json":
        console.print_json(profile.to_json())
    else:
        _print_archive_table(profile)


def _print_archive_table(profile: ArchiveProfile):
    """Print archive as table."""
    schema = profile.schema

    table = Table(title=f"Archive for {schema.gse_id}", show_header=False)
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("GSE ID", schema.gse_id)
    table.add_row("Title", schema.title[:80] + "..." if len(schema.title) > 80 else schema.title)
    table.add_row("Omics Type", schema.omics_type.value if hasattr(schema.omics_type, 'value') else str(schema.omics_type))
    table.add_row("Sample Count", str(schema.sample_count))
    table.add_row("Organisms", ", ".join(o.name for o in schema.organisms) if schema.organisms else "-")
    table.add_row("Tissues", ", ".join(schema.tissues[:3]) + ("..." if len(schema.tissues) > 3 else "") if schema.tissues else "-")
    table.add_row("Series Type", schema.series_type[:60] + "..." if len(schema.series_type) > 60 else schema.series_type)
    table.add_row("Status", schema.status.value)

    console.print(table)


@app.command()
def stats(
    output_dir: Optional[Path] = typer.Option(None, "--output-dir", "-o", help="Data directory"),
    by: Optional[str] = typer.Option(None, "--by", "-b", help="Group by (organism/omics_type)"),
):
    """Show statistics for all downloaded datasets."""
    cfg = Config()
    data_dir = output_dir or cfg.download.output_dir

    if not data_dir.exists():
        console.print(f"[red]Directory not found: {data_dir}[/red]")
        raise typer.Exit(1)

    # Scan for archives
    archives = []
    for gse_dir in data_dir.iterdir():
        if gse_dir.is_dir():
            archive_file = gse_dir / "archive.json"
            if archive_file.exists():
                try:
                    profile = ArchiveProfile.from_json(archive_file)
                    archives.append(profile)
                except Exception as e:
                    logger.warning(f"Failed to load {archive_file}: {e}")

    if not archives:
        console.print("[yellow]No archives found[/yellow]")
        return

    console.print(f"[bold]Found {len(archives)} datasets[/bold]\n")

    # Summary
    total_samples = sum(p.schema.sample_count for p in archives)
    console.print(f"Total Datasets: [cyan]{len(archives)}[/cyan]")
    console.print(f"Total Samples: [cyan]{total_samples}[/cyan]")

    # Group by
    if by == "organism" or by is None:
        console.print("\n[bold]By Organism:[/bold]")
        by_organism: dict = {}
        for p in archives:
            for org in p.schema.organisms:
                name = org.name or "Unknown"
                if name not in by_organism:
                    by_organism[name] = {"datasets": 0, "samples": 0}
                by_organism[name]["datasets"] += 1
                by_organism[name]["samples"] += p.schema.sample_count

        table = Table()
        table.add_column("Organism", style="cyan")
        table.add_column("Datasets", justify="right")
        table.add_column("Samples", justify="right")

        for name, counts in sorted(by_organism.items(), key=lambda x: x[1]["datasets"], reverse=True):
            table.add_row(name, str(counts["datasets"]), str(counts["samples"]))

        console.print(table)

    if by == "omics_type" or by is None:
        console.print("\n[bold]By Omics Type:[/bold]")
        by_omics: dict = {}
        for p in archives:
            omics = str(p.schema.omics_type.value if hasattr(p.schema.omics_type, 'value') else p.schema.omics_type)
            if omics not in by_omics:
                by_omics[omics] = {"datasets": 0, "samples": 0}
            by_omics[omics]["datasets"] += 1
            by_omics[omics]["samples"] += p.schema.sample_count

        table = Table()
        table.add_column("Omics Type", style="cyan")
        table.add_column("Datasets", justify="right")
        table.add_column("Samples", justify="right")

        for name, counts in sorted(by_omics.items(), key=lambda x: x[1]["datasets"], reverse=True):
            table.add_row(name, str(counts["datasets"]), str(counts["samples"]))

        console.print(table)


@app.command()
def verify(
    gse_id: Optional[str] = typer.Argument(None, help="GSE identifier (omit with --all)"),
    all_datasets: bool = typer.Option(False, "--all", "-a", help="Verify all downloaded datasets"),
    output_dir: Optional[Path] = typer.Option(None, "--output-dir", "-o", help="Data directory"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
):
    """Verify checksum of downloaded files.

    Examples::

        gse-downloader verify GSE123456
        gse-downloader verify --all
        gse-downloader verify --all --output-dir ./my_data
    """
    if config and config.exists():
        cfg = load_config(config)
    else:
        cfg = Config()

    if output_dir:
        cfg.download.output_dir = output_dir

    downloader = GSEDownloader(
        output_dir=cfg.download.output_dir,
        checksum_algorithm=cfg.checksum.algorithm,
        show_progress=False,
    )

    def _verify_one(gid: str) -> list[tuple[str, bool, str]]:
        """Verify a single GSE dataset. Returns list of (filename, ok, msg)."""
        sm = StateManager(cfg.download.output_dir / gid)
        if not sm.state_file.exists():
            return [(gid, False, "No state file")]
        info = sm.load_state()
        rows = []
        for filename, file_info in info.files.items():
            filepath = cfg.download.output_dir / gid / filename
            if not filepath.exists():
                rows.append((filename, False, "File not found"))
                continue
            if file_info.md5:
                ok = downloader.verify_file(filepath, file_info.md5, "md5")
                rows.append((filename, ok, "OK" if ok else "Checksum mismatch"))
            else:
                rows.append((filename, True, "No checksum (skipped)"))
        return rows

    if all_datasets:
        # Scan all GSE directories
        data_dir = cfg.download.output_dir
        if not data_dir.exists():
            console.print(f"[red]Directory not found: {data_dir}[/red]")
            raise typer.Exit(1)

        gse_dirs = [d for d in data_dir.iterdir() if d.is_dir() and d.name.upper().startswith("GSE")]
        if not gse_dirs:
            console.print("[yellow]No GSE directories found[/yellow]")
            raise typer.Exit(0)

        console.print(f"[bold]Verifying {len(gse_dirs)} datasets...[/bold]\n")
        overall_ok = 0
        overall_fail = 0

        for gse_dir in sorted(gse_dirs):
            gid = gse_dir.name.upper()
            rows = _verify_one(gid)
            if not rows:
                continue
            ok_count = sum(1 for _, ok, _ in rows if ok)
            fail_count = len(rows) - ok_count

            if fail_count == 0:
                console.print(f"[green][OK] {gid}  ({ok_count} files)[/green]")
                overall_ok += 1
            else:
                console.print(f"[red][FAIL] {gid}  ({fail_count}/{len(rows)} files failed)[/red]")
                for fname, ok, msg in rows:
                    if not ok:
                        console.print(f"       [red]  {fname}: {msg}[/red]")
                overall_fail += 1

        console.print(f"\n[bold]Verification complete:[/bold]")
        console.print(f"[green]Passed: {overall_ok}[/green] | [red]Failed: {overall_fail}[/red]")

    else:
        if not gse_id:
            console.print("[red]Provide a GSE ID or use --all[/red]")
            raise typer.Exit(1)

        gse_id = gse_id.upper().strip()
        sm = StateManager(cfg.download.output_dir / gse_id)
        if not sm.state_file.exists():
            console.print(f"[red]No download state found for {gse_id}[/red]")
            raise typer.Exit(1)

        console.print(f"[bold]Verifying {gse_id}...[/bold]")
        rows = _verify_one(gse_id)

        table = Table(title=f"Verification: {gse_id}")
        table.add_column("Filename", style="cyan")
        table.add_column("Status")
        table.add_column("Details")

        for filename, ok, msg in rows:
            st = "[green]OK[/green]" if ok else "[red]FAIL[/red]"
            table.add_row(filename[:55], st, msg)

        console.print(table)

    downloader.close()


@app.command()
def batch(
    input_file: Path = typer.Argument(..., help="File containing GSE IDs (one per line)"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Output directory"),
    retry_failed: int = typer.Option(1, "--retry", "-r", help="Retry failed downloads N times"),
    report: Optional[Path] = typer.Option(None, "--report", help="Save batch report to file"),
):
    """Batch download multiple GSE datasets with retry and report."""
    if not input_file.exists():
        console.print(f"[red]Input file not found: {input_file}[/red]")
        raise typer.Exit(1)

    # Load GSE IDs
    with open(input_file, "r") as f:
        gse_ids = [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]

    if not gse_ids:
        console.print("[yellow]No GSE IDs found in file[/yellow]")
        raise typer.Exit(0)

    console.print(f"[bold]Batch download: {len(gse_ids)} datasets[/bold]")

    # Load config
    if config and config.exists():
        cfg = load_config(config)
    else:
        cfg = Config()

    if output_dir:
        cfg.download.output_dir = output_dir

    import time as _time
    batch_results: dict[str, dict] = {}

    with GSEDownloader(
        output_dir=cfg.download.output_dir,
        max_workers=cfg.download.max_workers,
        timeout=cfg.download.timeout,
        verify_ssl=cfg.download.verify_ssl,
        retry_times=cfg.download.retry_times,
        auto_resume=cfg.download.auto_resume,
        checksum_algorithm=cfg.checksum.algorithm,
        show_progress=True,
    ) as downloader:
        for gse_id in gse_ids:
            gse_id = gse_id.strip()
            console.print(f"\n[cyan]>>> {gse_id}[/cyan]")

            attempt = 0
            success = False
            last_error = ""
            t0 = _time.time()

            # Retry loop
            while attempt <= retry_failed:
                attempt += 1
                try:
                    files = downloader.get_gse_files(gse_id)
                    if not files:
                        last_error = "No files found"
                        break

                    results = downloader.download_gse(gse_id, files)
                    success = all(r.success for r in results.values())
                    if success:
                        break
                    else:
                        failed_files = [fn for fn, r in results.items() if not r.success]
                        last_error = f"{len(failed_files)} file(s) failed"
                        if attempt <= retry_failed:
                            console.print(f"  [yellow]Retry {attempt}/{retry_failed}: {last_error}[/yellow]")

                except Exception as exc:
                    last_error = str(exc)
                    if attempt <= retry_failed:
                        console.print(f"  [yellow]Retry {attempt}/{retry_failed}: {last_error}[/yellow]")

            duration = _time.time() - t0
            batch_results[gse_id] = {
                "success": success,
                "attempts": attempt,
                "duration_s": round(duration, 1),
                "error": last_error if not success else "",
            }

            if success:
                console.print(f"[green][OK] {gse_id} completed in {duration:.1f}s[/green]")
            else:
                console.print(f"[red][FAIL] {gse_id}: {last_error}[/red]")

    # Summary table
    success_count = sum(1 for v in batch_results.values() if v["success"])
    fail_count = len(batch_results) - success_count

    console.print(f"\n[bold]Batch Complete:[/bold]")
    console.print(f"[green]Success: {success_count}[/green] | [red]Failed: {fail_count}[/red]")

    table = Table(title="Batch Summary")
    table.add_column("GSE ID", style="cyan")
    table.add_column("Status")
    table.add_column("Attempts", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Notes")

    for gse_id, info in batch_results.items():
        st = "[green]OK[/green]" if info["success"] else "[red]FAIL[/red]"
        table.add_row(
            gse_id,
            st,
            str(info["attempts"]),
            f"{info['duration_s']}s",
            info["error"] or "-",
        )

    console.print(table)

    # Save report
    if report:
        import json as _json
        report.write_text(
            _json.dumps(
                {
                    "total": len(batch_results),
                    "success": success_count,
                    "failed": fail_count,
                    "datasets": batch_results,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        console.print(f"[green]Report saved to: {report}[/green]")


@app.command("format")
def format_data(
    gse_id: str = typer.Argument(..., help="GSE identifier"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Data directory"),
):
    """Normalize downloaded data into standardized directory structure.

    Creates raw/, processed/, metadata/ sub-directories and writes
    expression_matrix.csv and metadata.csv.
    """
    gse_id = gse_id.upper().strip()

    if config and config.exists():
        cfg = load_config(config)
    else:
        cfg = Config()

    if output_dir:
        cfg.download.output_dir = output_dir

    gse_dir = cfg.download.output_dir / gse_id
    if not gse_dir.exists():
        console.print(f"[red]Directory not found: {gse_dir}[/red]")
        raise typer.Exit(1)

    # Detect omics type from archive.json
    from gse_downloader.archive.profile import ArchiveGenerator
    from gse_downloader.formatter.factory import FormatterFactory

    generator = ArchiveGenerator(cfg.download.output_dir)
    profile = generator.load(gse_id)

    if profile:
        omics_type = profile.schema.omics_type
        console.print(f"Detected omics type: [cyan]{omics_type.value}[/cyan]")
    else:
        from gse_downloader.parser.omics_detector import OmicsType
        omics_type = OmicsType.OTHER
        console.print("[yellow]No archive found, using generic formatter[/yellow]")

    formatter = FormatterFactory.get(omics_type)
    console.print(f"[bold]Formatting {gse_id} with {formatter.__class__.__name__}...[/bold]")

    result = formatter.format(gse_dir)

    if result.success:
        console.print(f"[green][OK] Format completed[/green]")
    else:
        console.print(f"[yellow]Format completed with warnings[/yellow]")

    # Show result table
    table = Table(title=f"Format Result: {gse_id}", show_header=False)
    table.add_column("Item", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Omics Type", result.omics_type)
    table.add_row("Status", "[green]OK[/green]" if result.success else "[yellow]Partial[/yellow]")
    if result.raw_dir and result.raw_dir.exists():
        raw_count = len(list(result.raw_dir.glob("*")))
        table.add_row("raw/ files", str(raw_count))
    if result.processed_dir and result.processed_dir.exists():
        proc_count = len(list(result.processed_dir.glob("*")))
        table.add_row("processed/ files", str(proc_count))
    if result.metadata_file:
        table.add_row("metadata.csv", str(result.metadata_file.name))
    if result.expression_matrix:
        table.add_row("expression_matrix.csv", str(result.expression_matrix.name))
    if result.moved_files:
        table.add_row("Files moved", str(len(result.moved_files)))
    if result.errors:
        for e in result.errors:
            table.add_row("[red]Error[/red]", e)

    console.print(table)


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query (e.g., 'lung cancer RNA-seq')"),
    retmax: int = typer.Option(10, "--limit", "-n", help="Maximum results to return (default 10, max 100)"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table or json"),
):
    """Search GEO database for datasets matching a query.

    Examples::

        gse-downloader search "breast cancer RNA-seq"
        gse-downloader search "Alzheimer scRNA-seq" --limit 20
        gse-downloader search "ATAC-seq mouse" --format json
    """
    from gse_downloader.parser.geo_query import GEOQuery

    console.print(f"[bold]Searching GEO for:[/bold] {query}")

    geo = GEOQuery()
    results = geo.search_series_detailed(query, retmax=retmax)

    if not results:
        console.print("[yellow]No results found.[/yellow]")
        raise typer.Exit(0)

    console.print(f"[green]Found {len(results)} datasets[/green]\n")

    if format == "json":
        import json as _json
        console.print_json(_json.dumps(results, indent=2, ensure_ascii=False))
    else:
        table = Table(title=f"GEO Search: {query[:50]}")
        table.add_column("GSE ID", style="bold cyan", min_width=12)
        table.add_column("Title", max_width=45)
        table.add_column("Organisms", style="green", max_width=20)
        table.add_column("Samples", justify="right")
        table.add_column("Type", max_width=18)
        table.add_column("Date")

        for r in results:
            orgs = ", ".join(r["organisms"][:2]) or "-"
            title = r["title"][:43] + "..." if len(r["title"]) > 43 else r["title"]
            stype = r["series_type"][:18]
            date = r["submission_date"][:10] if r["submission_date"] else "-"
            table.add_row(
                r["gse_id"],
                title,
                orgs,
                str(r["sample_count"]),
                stype,
                date,
            )

        console.print(table)
        console.print("\n[dim]Use 'gse-downloader download <GSE_ID>' to download a dataset.[/dim]")
        console.print("[dim]Use 'gse-downloader info <GSE_ID>' to view full metadata.[/dim]")


@app.command()
def info(
    gse_id: str = typer.Argument(..., help="GSE identifier (e.g., GSE123456)"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table or json"),
    local: bool = typer.Option(False, "--local", "-l", help="Show local archive only (no network)"),
):
    """Show detailed metadata for a GSE dataset.

    Fetches from local archive (if downloaded) or queries NCBI online.

    Examples::

        gse-downloader info GSE134520
        gse-downloader info GSE134520 --local
        gse-downloader info GSE134520 --format json
    """
    gse_id = gse_id.upper().strip()
    cfg = Config()

    # Try local archive first
    from gse_downloader.archive.profile import ArchiveGenerator

    generator = ArchiveGenerator(cfg.download.output_dir)
    profile = generator.load(gse_id)

    if profile:
        schema = profile.schema
        if format == "json":
            console.print_json(profile.to_json())
        else:
            _print_info_table(gse_id, schema)
        return

    if local:
        console.print(f"[yellow]No local archive found for {gse_id}. Use without --local to fetch online.[/yellow]")
        raise typer.Exit(1)

    # Query online
    console.print(f"[dim]No local archive. Querying NCBI...[/dim]")
    from gse_downloader.parser.geo_query import GEOQuery

    geo = GEOQuery()
    is_valid, err = geo.validate_gse_id(gse_id)
    if not is_valid:
        console.print(f"[red]Error: {err}[/red]")
        raise typer.Exit(1)

    series = geo.get_series_info(gse_id)

    if format == "json":
        import json as _json
        data = {
            "gse_id": series.gse_id,
            "title": series.title,
            "summary": series.summary,
            "overall_design": series.overall_design,
            "series_type": series.series_type,
            "organisms": series.organism,
            "platforms": series.platforms,
            "sample_count": series.sample_count,
            "submission_date": series.submission_date,
            "last_update_date": series.last_update_date,
            "pubmed_ids": series.pubmed_ids,
            "keywords": series.keywords,
        }
        console.print_json(_json.dumps(data, indent=2, ensure_ascii=False))
    else:
        table = Table(title=f"Info: {gse_id}", show_header=False)
        table.add_column("Field", style="cyan", min_width=18)
        table.add_column("Value", style="white")

        def _trunc(s: str, n: int = 100) -> str:
            return s[:n] + "..." if len(s) > n else s

        table.add_row("GSE ID", series.gse_id)
        table.add_row("Title", _trunc(series.title, 80))
        table.add_row("Series Type", series.series_type or "-")
        table.add_row("Organisms", ", ".join(series.organism) or "-")
        table.add_row("Platforms", ", ".join(series.platforms[:3]) or "-")
        table.add_row("Samples", str(series.sample_count) if series.sample_count else str(len(series.samples)))
        table.add_row("Submitted", series.submission_date or "-")
        table.add_row("Updated", series.last_update_date or "-")
        table.add_row("PubMed IDs", ", ".join(series.pubmed_ids) or "-")
        if series.keywords:
            table.add_row("Keywords", ", ".join(series.keywords[:8]))
        table.add_row("Summary", _trunc(series.summary, 200))

        console.print(table)


def _print_info_table(gse_id: str, schema) -> None:
    """Print archive schema as info table."""
    table = Table(title=f"Info: {gse_id}", show_header=False)
    table.add_column("Field", style="cyan", min_width=18)
    table.add_column("Value", style="white")

    def _trunc(s: str, n: int = 100) -> str:
        return s[:n] + "..." if s and len(s) > n else (s or "-")

    table.add_row("GSE ID", schema.gse_id)
    table.add_row("Title", _trunc(schema.title, 80))
    table.add_row("Omics Type", schema.omics_type.value if hasattr(schema.omics_type, "value") else str(schema.omics_type))
    table.add_row("Series Type", _trunc(schema.series_type, 60))
    table.add_row("Organisms", ", ".join(o.name for o in schema.organisms) if schema.organisms else "-")
    table.add_row("Tissues", ", ".join(schema.tissues[:5]) if schema.tissues else "-")
    table.add_row("Diseases", ", ".join(schema.diseases[:5]) if schema.diseases else "-")
    table.add_row("Sample Count", str(schema.sample_count))
    table.add_row("Submitted", schema.submission_date or "-")
    table.add_row("Updated", schema.last_update_date or "-")
    if schema.references and schema.references.pubmed_ids:
        table.add_row("PubMed IDs", ", ".join(schema.references.pubmed_ids))
    table.add_row("Status", schema.status.value if hasattr(schema.status, "value") else str(schema.status))
    table.add_row("Summary", _trunc(schema.summary, 200))

    console.print(table)

    # Print sample list if available (up to 10)
    if schema.samples:
        console.print(f"\n[bold]Samples ({len(schema.samples)} total, showing first 10):[/bold]")
        sample_table = Table(show_header=True, header_style="bold magenta")
        sample_table.add_column("GSM ID", style="cyan")
        sample_table.add_column("Title", max_width=35)
        sample_table.add_column("Organism", max_width=20)
        sample_table.add_column("Source", max_width=25)

        for sample in schema.samples[:10]:
            sample_table.add_row(
                sample.gsm_id,
                (sample.title[:33] + "...") if len(sample.title) > 33 else sample.title,
                sample.organism or "-",
                (sample.source_name[:23] + "...") if sample.source_name and len(sample.source_name) > 23 else (sample.source_name or "-"),
            )
        console.print(sample_table)


@app.command("init")
def init_config(
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Default output directory"),
    config_path: Path = typer.Option(Path("config.toml"), "--config", "-c", help="Config file to create"),
):
    """Initialize configuration file with interactive prompts.

    Creates a config.toml with sensible defaults that you can customize.

    Examples::

        gse-downloader init
        gse-downloader init --output /data/geo --config my_config.toml
    """
    console.print("[bold cyan]GSE Downloader[/bold cyan] Configuration Wizard\n")

    # Gather settings interactively only if not provided as options
    if output_dir is None:
        default_out = Path.home() / "gse_data"
        user_input = typer.prompt(
            "Default output directory",
            default=str(default_out),
        )
        output_dir = Path(user_input)

    max_workers = typer.prompt("Max parallel downloads", default=4, type=int)
    timeout = typer.prompt("Request timeout (seconds)", default=300, type=int)
    auto_resume = typer.confirm("Enable auto-resume on incomplete downloads?", default=True)
    algorithm = typer.prompt(
        "Checksum algorithm (md5 / sha256)",
        default="md5",
    )
    if algorithm not in ("md5", "sha256"):
        console.print("[yellow]Unknown algorithm, defaulting to md5[/yellow]")
        algorithm = "md5"

    rate_limit = typer.prompt(
        "Max HTTP requests/sec for NCBI (recommended ≤ 3, 0 = unlimited)",
        default=2.0,
        type=float,
    )

    ncbi_email = typer.prompt(
        "NCBI email address (recommended by NCBI policy)",
        default="anonymous@example.com",
    )

    api_key_input = typer.prompt(
        "NCBI API key (optional, press Enter to skip)",
        default="",
    )

    # Build TOML content
    toml_content = f"""# GSE Downloader configuration
# Generated by: gse-downloader init

[download]
output_dir = "{str(output_dir).replace(chr(92), '/')}"
max_workers = {max_workers}
timeout = {timeout}
verify_ssl = true
retry_times = 3
auto_resume = {"true" if auto_resume else "false"}
rate_limit = {rate_limit}

[checksum]
enabled = true
algorithm = "{algorithm}"

[ncbi]
email = "{ncbi_email}"
{"api_key = \"" + api_key_input + "\"" if api_key_input else "# api_key = \"your_key_here\""}

[logging]
level = "INFO"
# log_dir = "./logs"
"""

    config_path.write_text(toml_content, encoding="utf-8")
    console.print(f"\n[green]Config saved to: {config_path.resolve()}[/green]")
    console.print("\nYou can now use:")
    console.print(f"  [cyan]gse-downloader download GSE123456 --config {config_path}[/cyan]")
    console.print(f"  [cyan]gse-downloader batch gse_list.txt --config {config_path}[/cyan]")


if __name__ == "__main__":
    app()
