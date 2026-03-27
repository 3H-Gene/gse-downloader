"""Series Matrix formatter for GSE Downloader.

Parses the *_series_matrix.txt.gz file and produces:
  processed/expression_matrix.csv   – genes × samples expression table
  metadata/metadata.csv             – sample metadata
"""

from __future__ import annotations

import csv
import gzip
import re
from pathlib import Path
from typing import Optional

from gse_downloader.formatter.base import BaseFormatter
from gse_downloader.utils.logger import get_logger

logger = get_logger("formatter.series_matrix")


class SeriesMatrixFormatter(BaseFormatter):
    """Formatter that extracts data from GEO series matrix files.

    Works for both Microarray and RNA-seq when only the series matrix
    file is available (no raw counts).
    """

    RAW_PATTERNS = ["*.soft.gz"]
    PROCESSED_PATTERNS = ["*_series_matrix.txt.gz", "*_series_matrix.txt"]

    @property
    def omics_type(self) -> str:
        return "SeriesMatrix"

    def build_expression_matrix(
        self, gse_dir: Path, processed_dir: Path
    ) -> Optional[Path]:
        """Parse series matrix file and produce expression_matrix.csv.

        The series matrix text format looks like::

            !Series_geo_accession  "GSE1"
            ...
            !Sample_geo_accession  "GSM1"  "GSM2" ...
            !Sample_title          "ctrl"  "trt"  ...
            "ID_REF"   "GSM1"   "GSM2"
            "gene1"    1.23     4.56
            ...
            !series_matrix_table_end

        Args:
            gse_dir: GSE root directory
            processed_dir: processed/ sub-directory

        Returns:
            Path to expression_matrix.csv or None if no matrix file found.
        """
        # Look for series matrix in current dir and processed/
        matrix_files = (
            list(gse_dir.glob("*_series_matrix.txt.gz"))
            + list(gse_dir.glob("*_series_matrix.txt"))
            + list(processed_dir.glob("*_series_matrix.txt.gz"))
            + list(processed_dir.glob("*_series_matrix.txt"))
        )

        if not matrix_files:
            logger.warning(f"No series matrix file found in {gse_dir}")
            return None

        matrix_file = matrix_files[0]
        logger.info(f"Parsing series matrix: {matrix_file.name}")

        try:
            return self._parse_series_matrix(matrix_file, processed_dir)
        except Exception as exc:
            logger.error(f"Failed to parse series matrix {matrix_file}: {exc}")
            return None

    # ──────────────────────────────────────────────────────────────────────────

    def _parse_series_matrix(self, matrix_file: Path, processed_dir: Path) -> Path:
        """Parse a GEO series matrix file into expression_matrix.csv.

        Args:
            matrix_file: Path to the series matrix file (.txt or .txt.gz)
            processed_dir: Directory to write output files into

        Returns:
            Path to the written expression_matrix.csv
        """
        sample_meta: dict[str, dict] = {}  # gsm_id → {title, source, ...}
        in_table = False
        header: list[str] = []
        data_rows: list[list[str]] = []

        # Metadata key → list of values per sample
        meta_keys = [
            "!Sample_geo_accession",
            "!Sample_title",
            "!Sample_source_name_ch1",
            "!Sample_organism_ch1",
            "!Sample_characteristics_ch1",
        ]
        meta_buffer: dict[str, list[str]] = {k: [] for k in meta_keys}

        lines = self._read_gzipped_lines(matrix_file)

        for line in lines:
            line = line.rstrip("\n")

            # ── Table section ─────────────────────────────────────────────
            if line.startswith('"ID_REF"') or line.startswith("ID_REF"):
                in_table = True
                raw_header = _split_matrix_line(line)
                header = raw_header  # first column is ID_REF
                continue

            if line.strip() == "!series_matrix_table_end":
                in_table = False
                continue

            if in_table:
                parts = _split_matrix_line(line)
                if parts:
                    data_rows.append(parts)
                continue

            # ── Metadata section ──────────────────────────────────────────
            for key in meta_keys:
                if line.startswith(key):
                    vals = _split_matrix_line(line)
                    # vals[0] is the key name, rest are per-sample values
                    meta_buffer[key] = vals[1:]
                    break

        # Build expression matrix CSV
        out_path = processed_dir / "expression_matrix.csv"
        if header and data_rows:
            with open(out_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(header)
                writer.writerows(data_rows)
            logger.info(
                f"Wrote expression_matrix.csv: {len(data_rows)} features × "
                f"{len(header) - 1} samples"
            )
        else:
            logger.warning("No data table found in series matrix file")
            # Write an empty placeholder so callers know we tried
            out_path.write_text("ID_REF\n", encoding="utf-8")

        # Build sample metadata CSV from meta_buffer
        gsm_ids = meta_buffer.get("!Sample_geo_accession", [])
        if gsm_ids:
            meta_out = processed_dir.parent / "metadata" / "metadata.csv"
            meta_out.parent.mkdir(exist_ok=True)
            n = len(gsm_ids)

            def _get(key: str) -> list[str]:
                vals = meta_buffer.get(key, [])
                # Pad or trim to n
                return (vals + [""] * n)[:n]

            rows = []
            for i, gsm_id in enumerate(gsm_ids):
                rows.append({
                    "gsm_id": gsm_id,
                    "title": _get("!Sample_title")[i],
                    "source_name": _get("!Sample_source_name_ch1")[i],
                    "organism": _get("!Sample_organism_ch1")[i],
                    "characteristics": _get("!Sample_characteristics_ch1")[i],
                })

            fieldnames = ["gsm_id", "title", "source_name", "organism", "characteristics"]
            with open(meta_out, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logger.info(f"Wrote sample metadata: {len(rows)} samples → {meta_out}")

        return out_path


# ──────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

def _split_matrix_line(line: str) -> list[str]:
    """Split a tab-separated GEO matrix line, stripping surrounding quotes."""
    parts = line.split("\t")
    result = []
    for p in parts:
        p = p.strip()
        if p.startswith('"') and p.endswith('"'):
            p = p[1:-1]
        result.append(p)
    return result
