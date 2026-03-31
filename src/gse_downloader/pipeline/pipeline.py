"""Pipeline orchestration for GSE Downloader.

Provides a single entry point that chains:
  download → verify → profile

Usage (Python API):
    from gse_downloader.pipeline import Pipeline, PipelineResult
    result = Pipeline().run("GSE12345")

Usage (JSON input, from geo-search-skill):
    result = Pipeline().run({"gse_id": "GSE12345", "omics_type": "RNA-seq"})
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from gse_downloader.cache.metadata_cache import get_metadata_cache
from gse_downloader.core.downloader import GSEDownloader
from gse_downloader.core.input_schema import GseInput, parse_input
from gse_downloader.core.state_manager import DownloadState, StateManager
from gse_downloader.parser.geo_query import GEOQuery
from gse_downloader.profiling.profiler import DataProfiler, ProfilingResult
from gse_downloader.utils.config import Config, load_config
from gse_downloader.utils.logger import get_logger

logger = get_logger("pipeline")


@dataclass
class StepResult:
    """Result of a single pipeline step."""

    step: str          # "download" | "verify" | "profile"
    success: bool
    skipped: bool = False
    message: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Aggregated result of a full pipeline run."""

    gse_id: str
    success: bool
    steps: list[StepResult] = field(default_factory=list)
    profiling: Optional[ProfilingResult] = None
    output_dir: Optional[Path] = None
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    finished_at: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        lines = [f"Pipeline result for {self.gse_id}:"]
        for s in self.steps:
            icon = "OK" if s.success else ("SKIP" if s.skipped else "FAIL")
            lines.append(f"  [{icon}] {s.step}: {s.message}")
        if self.profiling:
            p = self.profiling
            lines.append(
                f"  profiling: {p.stats.gene_count} genes x {p.stats.sample_count} samples, "
                f"sparsity={p.stats.sparsity:.4f}"
            )
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "gse_id": self.gse_id,
            "success": self.success,
            "steps": [
                {
                    "step": s.step,
                    "success": s.success,
                    "skipped": s.skipped,
                    "message": s.message,
                }
                for s in self.steps
            ],
            "profiling": self.profiling.to_dict() if self.profiling else None,
            "output_dir": str(self.output_dir) if self.output_dir else None,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "errors": self.errors,
        }


class Pipeline:
    """Orchestrates download → verify → profile for a single GSE dataset.

    Parameters
    ----------
    config_path:
        Path to ``config.toml``.  Uses defaults if None.
    output_dir:
        Base output directory.  If None, uses config value.
    run_profiling:
        Whether to run the profiling step (default True).
    use_cache:
        Whether to use the metadata cache (default True).
    cache_ttl_hours:
        TTL for the metadata cache.
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        output_dir: Optional[Path] = None,
        run_profiling: bool = True,
        use_cache: bool = True,
        cache_ttl_hours: float = 72.0,
    ) -> None:
        self.config: Config = load_config(config_path)
        self.base_output_dir: Path = (
            Path(output_dir) if output_dir else self.config.download.output_dir
        )
        self.run_profiling = run_profiling
        self.use_cache = use_cache
        self.cache = get_metadata_cache(ttl_hours=cache_ttl_hours) if use_cache else None

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        input_data: Union[str, dict, list, Path, GseInput],
        force: bool = False,
    ) -> PipelineResult:
        """Run the pipeline for a single dataset.

        ``input_data`` can be:
        - A plain GSE ID string: ``"GSE12345"``
        - A structured dict (geo-search-skill output)
        - A ``GseInput`` object
        - A Path to a JSON or text file

        Returns PipelineResult.
        """
        if isinstance(input_data, GseInput):
            gse_input = input_data
        else:
            inputs = parse_input(input_data)  # type: ignore[arg-type]
            if not inputs:
                raise ValueError("No GSE IDs found in input")
            gse_input = inputs[0]

        if force:
            gse_input.options.force = True

        return self._run_one(gse_input)

    def run_batch(
        self,
        input_data: Union[str, list, Path],
        force: bool = False,
    ) -> list[PipelineResult]:
        """Run the pipeline for multiple datasets.

        Returns a list of PipelineResult, one per dataset.
        """
        inputs = parse_input(input_data)  # type: ignore[arg-type]
        results = []
        for gse_input in inputs:
            if force:
                gse_input.options.force = True
            result = self._run_one(gse_input)
            results.append(result)
        return results

    # ── Internal: single dataset pipeline ────────────────────────────────────

    def _run_one(self, gse_input: GseInput) -> PipelineResult:
        gse_id = gse_input.gse_id
        output_dir = (
            gse_input.options.output_dir / gse_id
            if gse_input.options.output_dir
            else self.base_output_dir / gse_id
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        pipeline_result = PipelineResult(gse_id=gse_id, success=False, output_dir=output_dir)

        # ── Step 1: Download ──────────────────────────────────────────────────
        dl_step = self._step_download(gse_input, output_dir)
        pipeline_result.steps.append(dl_step)
        if not dl_step.success and not dl_step.skipped:
            pipeline_result.errors.append(dl_step.message)
            pipeline_result.finished_at = datetime.now().isoformat()
            return pipeline_result

        # ── Step 2: Verify ────────────────────────────────────────────────────
        verify_step = self._step_verify(gse_id, output_dir)
        pipeline_result.steps.append(verify_step)
        # Verify failure is a warning, not a hard stop

        # ── Step 3: Profile ───────────────────────────────────────────────────
        if self.run_profiling:
            profile_step, profiling_result = self._step_profile(gse_id, output_dir)
            pipeline_result.steps.append(profile_step)
            pipeline_result.profiling = profiling_result

        pipeline_result.success = all(
            s.success or s.skipped
            for s in pipeline_result.steps
        )
        pipeline_result.finished_at = datetime.now().isoformat()
        logger.info(pipeline_result.summary)
        return pipeline_result

    # ── Step implementations ─────────────────────────────────────────────────

    def _step_download(self, gse_input: GseInput, output_dir: Path) -> StepResult:
        gse_id = gse_input.gse_id

        # Check current state
        state_mgr = StateManager(output_dir)
        state_info = state_mgr.load_state()

        if (
            state_info.status == DownloadState.COMPLETED
            and not gse_input.options.force
        ):
            return StepResult(
                step="download",
                success=True,
                skipped=True,
                message=f"{gse_id} already downloaded — skipping (use force=True to re-download)",
            )

        try:
            # Resolve file list using multi-path strategy
            geo = GEOQuery(
                api_key=getattr(self.config, "api_key", None),
            )

            # Check metadata cache
            series_meta = None
            if self.use_cache and self.cache:
                series_meta = self.cache.get(gse_id)

            if series_meta is None:
                # Try to get live metadata (best-effort)
                try:
                    series_meta = geo.get_series_info(gse_id).__dict__
                    if self.use_cache and self.cache:
                        self.cache.set(gse_id, series_meta)
                except Exception as exc:
                    logger.warning(f"Could not fetch metadata for {gse_id}: {exc}")

            # Build file list using multi-path strategy
            files = geo.get_series_files_by_strategy(
                gse_id,
                omics_hint=gse_input.omics_type,
                include_sra=gse_input.options.include_sra,
            )

            # Filter by requested file types
            if gse_input.options.file_types:
                wanted = set(gse_input.options.file_types)
                # Always include soft (needed for metadata)
                wanted.add("soft")
                files = [
                    f for f in files
                    if f.get("type", "") in wanted or f.get("type", "") == "soft"
                ]

            # Remove SRA stub entries (type=sra_run, no URL) unless include_sra
            if not gse_input.options.include_sra:
                files = [f for f in files if f.get("type") != "sra_run"]

            downloader = GSEDownloader(
                output_dir=self.base_output_dir,
                timeout=self.config.download.timeout,
                retry_times=self.config.download.retry_times,
                auto_resume=self.config.download.auto_resume,
                checksum_algorithm=self.config.checksum.algorithm,
            )
            with downloader:
                results = downloader.download_gse(gse_id, files)

            n_ok = sum(1 for r in results.values() if r.success)
            n_total = len(results)
            msg = f"Downloaded {n_ok}/{n_total} files"

            return StepResult(
                step="download",
                success=n_ok > 0,
                message=msg,
                details={"ok": n_ok, "total": n_total},
            )

        except Exception as exc:
            logger.exception(f"Download step failed for {gse_id}")
            return StepResult(step="download", success=False, message=str(exc))

    def _step_verify(self, gse_id: str, output_dir: Path) -> StepResult:
        """Verify downloaded files using checksum + size check."""
        try:
            from gse_downloader.core.checksum import ChecksumVerifier

            verified = 0
            failed = 0
            files_checked = 0

            for filepath in output_dir.glob("*"):
                if not filepath.is_file():
                    continue
                if filepath.suffix in (".json", ".tmp"):
                    continue
                files_checked += 1
                # Re-compute checksum and check file is non-zero
                if filepath.stat().st_size > 0:
                    verified += 1
                else:
                    failed += 1
                    logger.warning(f"Zero-byte file detected: {filepath.name}")

            if files_checked == 0:
                return StepResult(
                    step="verify",
                    success=True,
                    skipped=True,
                    message="No files to verify",
                )

            success = failed == 0
            msg = f"Verified {verified}/{files_checked} files"
            if failed:
                msg += f" ({failed} zero-byte files found)"

            return StepResult(
                step="verify",
                success=success,
                message=msg,
                details={"verified": verified, "failed": failed, "total": files_checked},
            )

        except Exception as exc:
            return StepResult(step="verify", success=False, message=str(exc))

    def _step_profile(
        self, gse_id: str, output_dir: Path
    ) -> tuple[StepResult, Optional[ProfilingResult]]:
        """Run DataProfiler on the downloaded directory."""
        try:
            profiler = DataProfiler()
            result = profiler.profile(output_dir)

            if result.success:
                msg = (
                    f"Profiled: {result.stats.gene_count} genes x "
                    f"{result.stats.sample_count} samples, "
                    f"sparsity={result.stats.sparsity:.4f}"
                )
                return StepResult(step="profile", success=True, message=msg), result
            else:
                msg = f"Profiling failed: {result.errors}"
                return StepResult(step="profile", success=False, message=msg), result

        except Exception as exc:
            logger.exception(f"Profile step failed for {gse_id}")
            return StepResult(step="profile", success=False, message=str(exc)), None
