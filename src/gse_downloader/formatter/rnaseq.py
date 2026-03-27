"""RNA-seq formatter for GSE Downloader.

Handles normalization of RNA-seq data (FASTQ, BAM, count matrices).

Directory layout after formatting::

    GSE123456/
    ├── raw/
    │   ├── *.fastq.gz      # Raw reads
    │   └── *.bam           # Alignments
    ├── processed/
    │   ├── *_counts.txt    # Feature counts (moved here)
    │   ├── *_fpkm*.txt     # FPKM files
    │   ├── *_tpm*.txt      # TPM files
    │   └── expression_matrix.csv  # Unified counts / TPM matrix
    ├── metadata/
    │   └── metadata.csv
    └── archive.json
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional

from gse_downloader.formatter.base import BaseFormatter
from gse_downloader.formatter.series_matrix import SeriesMatrixFormatter
from gse_downloader.utils.logger import get_logger

logger = get_logger("formatter.rnaseq")


class RNASeqFormatter(BaseFormatter):
    """Formatter for bulk RNA-seq data."""

    RAW_PATTERNS = [
        "*.fastq",
        "*.fastq.gz",
        "*.fq",
        "*.fq.gz",
        "*.bam",
        "*.bam.bai",
        "*.cram",
    ]
    PROCESSED_PATTERNS = [
        "*_counts.txt",
        "*_counts.csv",
        "*counts*.txt",
        "*counts*.txt.gz",
        "*counts*.csv",
        "*_fpkm*.txt",
        "*_fpkm*.csv",
        "*_tpm*.txt",
        "*_tpm*.csv",
        "*_RPKM*.txt",
        "*_expression*.txt",
        "*_expression*.csv",
        "*matrix*.txt",
        "*matrix*.csv",
        "*_series_matrix.txt.gz",
        "*_series_matrix.txt",
    ]

    @property
    def omics_type(self) -> str:
        return "RNA-seq"

    def build_expression_matrix(
        self, gse_dir: Path, processed_dir: Path
    ) -> Optional[Path]:
        """Build expression_matrix.csv.

        Strategy (in order):
        1. If a count/TPM matrix is present in processed/, use _merge_count_files().
        2. Otherwise fall back to SeriesMatrixFormatter (series matrix → matrix CSV).

        Args:
            gse_dir: GSE root directory
            processed_dir: processed/ sub-directory

        Returns:
            Path to expression_matrix.csv, or None if nothing found.
        """
        # Check for count / expression files in processed/ or gse_dir
        count_candidates = (
            list(processed_dir.glob("*_counts*"))
            + list(processed_dir.glob("*counts*"))
            + list(processed_dir.glob("*_tpm*"))
            + list(processed_dir.glob("*_fpkm*"))
            + list(processed_dir.glob("*_expression*"))
            + list(processed_dir.glob("*matrix*.txt"))
            + list(processed_dir.glob("*matrix*.csv"))
            + list(gse_dir.glob("*_counts*"))
            + list(gse_dir.glob("*counts*"))
            + list(gse_dir.glob("*_tpm*"))
        )
        # Remove duplicates and series_matrix files
        seen: set[Path] = set()
        deduped = []
        for f in count_candidates:
            if f.is_file() and f not in seen and "series_matrix" not in f.name:
                seen.add(f)
                deduped.append(f)
        count_candidates = deduped

        if count_candidates:
            try:
                return self._merge_count_files(count_candidates, processed_dir)
            except Exception as exc:
                logger.warning(f"Merge count files failed: {exc}, falling back to series matrix")

        # Fall back to series matrix
        sm = SeriesMatrixFormatter()
        return sm.build_expression_matrix(gse_dir, processed_dir)

    # ──────────────────────────────────────────────────────────────────────────

    def _merge_count_files(
        self, count_files: list[Path], processed_dir: Path
    ) -> Optional[Path]:
        """Merge per-sample count files into a single gene × sample matrix.

        Handles two formats:
        - Multi-sample matrix (one file with columns per sample)
        - Per-sample files (one column per file, filename becomes column name)

        Args:
            count_files: List of count/TPM/FPKM files
            processed_dir: Directory to write output

        Returns:
            Path to expression_matrix.csv
        """
        if len(count_files) == 1:
            # Single file - likely already a matrix
            return self._convert_single_matrix(count_files[0], processed_dir)
        else:
            # Multiple files - merge them
            return self._merge_multiple_files(count_files, processed_dir)

    def _convert_single_matrix(
        self, count_file: Path, processed_dir: Path
    ) -> Path:
        """Convert a single count matrix to the standard CSV format.

        Args:
            count_file: Path to the count matrix file
            processed_dir: Output directory

        Returns:
            Path to written expression_matrix.csv
        """
        lines = self._read_gzipped_lines(count_file)
        out_path = processed_dir / "expression_matrix.csv"

        rows = []
        for line in lines:
            line = line.rstrip("\n")
            if line.startswith("#") or not line.strip():
                continue
            # Auto-detect separator
            if "\t" in line:
                parts = line.split("\t")
            else:
                parts = re.split(r"\s+", line)
            rows.append(parts)

        if not rows:
            logger.warning("Empty count file, writing placeholder")
            out_path.write_text("gene_id\n", encoding="utf-8")
            return out_path

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerows(rows)

        logger.info(
            f"Converted count matrix: {len(rows) - 1} genes × "
            f"{len(rows[0]) - 1} samples"
        )
        return out_path

    def _merge_multiple_files(
        self, count_files: list[Path], processed_dir: Path
    ) -> Path:
        """Merge multiple per-sample count files into one matrix.

        Assumes each file has two columns: gene_id and count.

        Args:
            count_files: List of per-sample count files
            processed_dir: Output directory

        Returns:
            Path to written expression_matrix.csv
        """
        from collections import defaultdict

        # gene_id → {sample_name: count}
        matrix: dict[str, dict[str, str]] = defaultdict(dict)
        sample_names: list[str] = []

        for fp in sorted(count_files):
            # Use filename (strip extensions) as sample name
            sample_name = re.sub(r"\.(txt|csv|tsv)(\.gz)?$", "", fp.name, flags=re.I)
            sample_names.append(sample_name)

            lines = self._read_gzipped_lines(fp)
            first_data_line = True
            for line in lines:
                line = line.rstrip("\n")
                if line.startswith("#") or not line.strip():
                    continue
                if "\t" in line:
                    parts = line.split("\t")
                else:
                    parts = re.split(r"\s+", line)
                if len(parts) >= 2:
                    gene_id = parts[0]
                    count_val = parts[1]
                    # Skip header lines: if the count column is not numeric
                    if first_data_line:
                        first_data_line = False
                        try:
                            float(count_val)
                        except ValueError:
                            # This is a header row, skip it
                            continue
                    matrix[gene_id][sample_name] = count_val

        out_path = processed_dir / "expression_matrix.csv"
        fieldnames = ["gene_id"] + sample_names

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
            )
            writer.writeheader()
            for gene_id, counts in matrix.items():
                row = {"gene_id": gene_id}
                row.update(counts)
                writer.writerow(row)

        logger.info(
            f"Merged {len(count_files)} count files: {len(matrix)} genes × "
            f"{len(sample_names)} samples → {out_path}"
        )
        return out_path
