"""Base formatter class for GSE Downloader.

Defines the abstract interface and shared utilities for all omics formatters.
"""

from __future__ import annotations

import csv
import gzip
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("formatter.base")


@dataclass
class FormatResult:
    """Result of a formatting operation."""

    gse_id: str
    success: bool
    omics_type: str = ""
    raw_dir: Optional[Path] = None
    processed_dir: Optional[Path] = None
    metadata_file: Optional[Path] = None
    expression_matrix: Optional[Path] = None
    moved_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        lines = [f"[{self.omics_type}] Format result for {self.gse_id}:"]
        lines.append(f"  success        : {self.success}")
        if self.raw_dir:
            lines.append(f"  raw/           : {self.raw_dir}")
        if self.processed_dir:
            lines.append(f"  processed/     : {self.processed_dir}")
        if self.metadata_file:
            lines.append(f"  metadata.csv   : {self.metadata_file}")
        if self.expression_matrix:
            lines.append(f"  expression_mat : {self.expression_matrix}")
        if self.moved_files:
            lines.append(f"  moved files    : {len(self.moved_files)}")
        if self.errors:
            lines.append(f"  errors         : {self.errors}")
        return "\n".join(lines)


class BaseFormatter(ABC):
    """Abstract base class for omics-type formatters."""

    #: List of glob patterns that should go into raw/
    RAW_PATTERNS: list[str] = []
    #: List of glob patterns that should go into processed/
    PROCESSED_PATTERNS: list[str] = []

    def __init__(self):
        self.logger = get_logger(f"formatter.{self.__class__.__name__.lower()}")

    @property
    @abstractmethod
    def omics_type(self) -> str:
        """Return the omics type name this formatter handles."""

    def format(self, gse_dir: Path) -> FormatResult:
        """Normalize a GSE directory into standardised sub-directories.

        Steps performed by the base implementation (override as needed):
        1. Create raw/, processed/, metadata/ directories.
        2. Move files matching RAW_PATTERNS → raw/.
        3. Move files matching PROCESSED_PATTERNS → processed/.
        4. Write metadata.csv from archive.json (if present).
        5. Call build_expression_matrix() for omics-specific work.

        Args:
            gse_dir: Path to the GSE data directory (e.g. ./gse_data/GSE1/)

        Returns:
            FormatResult describing what was done.
        """
        gse_id = gse_dir.name
        result = FormatResult(gse_id=gse_id, success=False, omics_type=self.omics_type)

        if not gse_dir.exists():
            result.errors.append(f"Directory not found: {gse_dir}")
            return result

        # Create standard sub-directories
        raw_dir = gse_dir / "raw"
        processed_dir = gse_dir / "processed"
        metadata_dir = gse_dir / "metadata"
        for d in (raw_dir, processed_dir, metadata_dir):
            d.mkdir(exist_ok=True)

        result.raw_dir = raw_dir
        result.processed_dir = processed_dir

        # Move raw files
        for pattern in self.RAW_PATTERNS:
            for fp in gse_dir.glob(pattern):
                if fp.is_file() and fp.parent == gse_dir:
                    dest = raw_dir / fp.name
                    _safe_move(fp, dest)
                    result.moved_files.append(str(dest))

        # Move processed files
        for pattern in self.PROCESSED_PATTERNS:
            for fp in gse_dir.glob(pattern):
                if fp.is_file() and fp.parent == gse_dir:
                    dest = processed_dir / fp.name
                    _safe_move(fp, dest)
                    result.moved_files.append(str(dest))

        # Write metadata.csv from archive.json
        archive_json = gse_dir / "archive.json"
        if archive_json.exists():
            try:
                metadata_file = self._write_metadata_csv(archive_json, metadata_dir)
                result.metadata_file = metadata_file
            except Exception as exc:
                result.errors.append(f"metadata.csv write failed: {exc}")

        # Omics-specific expression matrix
        try:
            expr_matrix = self.build_expression_matrix(gse_dir, processed_dir)
            if expr_matrix:
                result.expression_matrix = expr_matrix
        except Exception as exc:
            result.errors.append(f"expression_matrix build failed: {exc}")

        result.success = len(result.errors) == 0
        self.logger.info(result.summary)
        return result

    @abstractmethod
    def build_expression_matrix(
        self, gse_dir: Path, processed_dir: Path
    ) -> Optional[Path]:
        """Build a unified expression_matrix.csv in processed_dir.

        Args:
            gse_dir: GSE root directory
            processed_dir: processed/ sub-directory

        Returns:
            Path to the written CSV, or None if not applicable.
        """

    # ──────────────────────────────────────────────────────────────────────────
    # Shared helper methods
    # ──────────────────────────────────────────────────────────────────────────

    def _write_metadata_csv(self, archive_json: Path, metadata_dir: Path) -> Path:
        """Write a flat metadata.csv from archive.json sample list.

        Args:
            archive_json: Path to archive.json
            metadata_dir: Directory to write into

        Returns:
            Path to written metadata.csv
        """
        import json

        with open(archive_json, encoding="utf-8") as fh:
            data = json.load(fh)

        samples = data.get("samples", [])
        if not samples:
            # Fall back: write a minimal one-row summary
            rows = [{
                "gse_id": data.get("gse_id", ""),
                "title": data.get("metadata", {}).get("title", ""),
                "omics_type": data.get("omics_type", ""),
                "sample_count": data.get("sample_count", 0),
                "organisms": "; ".join(
                    o.get("name", "") for o in data.get("organisms", [])
                ),
            }]
            fieldnames = list(rows[0].keys())
        else:
            # Flatten each sample into a row
            rows = []
            for s in samples:
                chars = s.get("characteristics", {})
                row = {
                    "gsm_id": s.get("gsm_id", ""),
                    "title": s.get("title", ""),
                    "source_name": s.get("source_name", ""),
                    "organism": s.get("organism", ""),
                    "extraction_molecule": s.get("extraction_molecule", ""),
                    "library_strategy": s.get("library_strategy", ""),
                    "library_layout": s.get("library_layout", ""),
                    "instrument_model": s.get("instrument_model", ""),
                }
                row.update({f"char_{k}": v for k, v in chars.items()})
                rows.append(row)

            # Collect all fieldnames
            fieldnames = []
            seen = set()
            for row in rows:
                for k in row:
                    if k not in seen:
                        fieldnames.append(k)
                        seen.add(k)

        out_path = metadata_dir / "metadata.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        self.logger.info(f"Wrote metadata.csv with {len(rows)} rows → {out_path}")
        return out_path

    @staticmethod
    def _read_gzipped_lines(filepath: Path) -> list[str]:
        """Read all lines from a plain or gzip-compressed text file."""
        if filepath.suffix == ".gz":
            with gzip.open(filepath, "rt", encoding="utf-8", errors="replace") as fh:
                return fh.readlines()
        else:
            with open(filepath, encoding="utf-8", errors="replace") as fh:
                return fh.readlines()


# ──────────────────────────────────────────────────────────────────────────────
# Module-level helpers
# ──────────────────────────────────────────────────────────────────────────────

def _safe_move(src: Path, dest: Path) -> None:
    """Move src to dest, skipping if dest already exists."""
    if dest.exists():
        logger.debug(f"Skip move (dest exists): {dest}")
        return
    shutil.move(str(src), str(dest))
    logger.debug(f"Moved {src.name} → {dest}")
