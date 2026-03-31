"""Core profiling logic for GEO datasets.

DataProfiler reads downloaded GEO files, constructs a 2-D expression matrix,
and computes basic structural statistics — without modifying expression values.
"""

from __future__ import annotations

import csv
import gzip
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("profiling.profiler")


# ─────────────────────────────────────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MatrixStats:
    """Basic statistics computed on the expression matrix.

    All statistics are STRUCTURAL — no value transformation is performed.
    """

    sample_count: int = 0          # number of columns (samples)
    gene_count: int = 0            # number of rows (features/genes)
    total_cells: int = 0           # sample_count × gene_count
    missing_count: int = 0         # cells with NaN / empty value
    zero_count: int = 0            # cells with value == 0.0
    missing_rate: float = 0.0      # missing_count / total_cells
    zero_rate: float = 0.0         # zero_count / total_cells
    sparsity: float = 0.0          # (missing + zero) / total_cells
    duplicate_genes_removed: int = 0
    empty_genes_removed: int = 0
    value_type: str = "unknown"    # "integer", "float", "mixed", "unknown"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProfilingResult:
    """Result returned by DataProfiler.profile()."""

    gse_id: str
    success: bool
    omics_type: str = ""
    matrix_file: Optional[Path] = None      # path to written expression_matrix.csv
    metadata_file: Optional[Path] = None    # path to written metadata.csv
    summary_file: Optional[Path] = None     # path to profiling_summary.json
    stats: MatrixStats = field(default_factory=MatrixStats)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    profiled_at: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def summary(self) -> str:
        lines = [f"Profiling result for {self.gse_id} [{self.omics_type}]:"]
        lines.append(f"  success       : {self.success}")
        lines.append(f"  samples       : {self.stats.sample_count}")
        lines.append(f"  genes/features: {self.stats.gene_count}")
        lines.append(f"  missing_rate  : {self.stats.missing_rate:.4f}")
        lines.append(f"  zero_rate     : {self.stats.zero_rate:.4f}")
        lines.append(f"  sparsity      : {self.stats.sparsity:.4f}")
        if self.warnings:
            lines.append(f"  warnings      : {len(self.warnings)}")
        if self.errors:
            lines.append(f"  errors        : {self.errors}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "gse_id": self.gse_id,
            "success": self.success,
            "omics_type": self.omics_type,
            "matrix_file": str(self.matrix_file) if self.matrix_file else None,
            "metadata_file": str(self.metadata_file) if self.metadata_file else None,
            "summary_file": str(self.summary_file) if self.summary_file else None,
            "stats": self.stats.to_dict(),
            "warnings": self.warnings,
            "errors": self.errors,
            "profiled_at": self.profiled_at,
        }


# ─────────────────────────────────────────────────────────────────────────────
# DataProfiler
# ─────────────────────────────────────────────────────────────────────────────

class DataProfiler:
    """Profile a downloaded GSE dataset.

    Workflow
    --------
    1. Detect the best available expression source (priority):
       a. processed/ directory → look for *count*, *matrix*, *expr* files
       b. Series Matrix file   → ``*_series_matrix.txt.gz``
       c. raw/ CSV/TSV files
    2. Read data → 2-D gene × sample in-memory table (list-of-rows)
    3. Structural clean:
       - Align column names (samples)
       - Remove duplicate gene rows (keep first occurrence)
       - Remove fully-empty gene rows
    4. Compute MatrixStats
    5. Write:
       - ``processed/expression_matrix.csv``
       - ``metadata/metadata.csv`` (from archive.json if available)
       - ``profiling_summary.json``
    """

    def __init__(self, max_rows: int = 200_000):
        """
        Args:
            max_rows: Safety limit — refuse to load matrices larger than this.
                      Prevents accidental OOM on huge single-cell datasets.
                      Set to 0 to disable.
        """
        self.max_rows = max_rows

    # ── Public API ────────────────────────────────────────────────────────────

    def profile(self, gse_dir: Path) -> ProfilingResult:
        """Run the full profiling pipeline on a GSE directory.

        Args:
            gse_dir: Path to the GSE data directory (e.g. ./gse_data/GSE12345/)

        Returns:
            ProfilingResult
        """
        gse_id = gse_dir.name
        result = ProfilingResult(gse_id=gse_id, success=False)

        if not gse_dir.exists():
            result.errors.append(f"Directory not found: {gse_dir}")
            return result

        # Ensure standard sub-dirs exist
        processed_dir = gse_dir / "processed"
        metadata_dir = gse_dir / "metadata"
        processed_dir.mkdir(exist_ok=True)
        metadata_dir.mkdir(exist_ok=True)

        # Detect omics type from archive.json
        result.omics_type = self._detect_omics_type(gse_dir)

        # Read expression data
        try:
            gene_col, sample_cols, rows = self._read_expression(gse_dir, result)
        except Exception as exc:
            result.errors.append(f"Failed to read expression data: {exc}")
            logger.exception(f"Expression read error for {gse_id}")
            return result

        if not rows:
            result.warnings.append("No expression data found — skipping matrix stats")
            # Still try to write metadata
            self._write_metadata_csv(gse_dir, metadata_dir, result)
            result.success = len(result.errors) == 0
            return result

        # Structural cleaning
        rows, stats = self._clean_and_stats(gene_col, sample_cols, rows, result)

        result.stats = stats

        # Write expression matrix
        matrix_path = processed_dir / "expression_matrix.csv"
        try:
            self._write_matrix(gene_col, sample_cols, rows, matrix_path)
            result.matrix_file = matrix_path
            logger.info(f"Wrote expression_matrix.csv ({stats.gene_count} genes × {stats.sample_count} samples)")
        except Exception as exc:
            result.errors.append(f"Failed to write expression_matrix.csv: {exc}")

        # Write metadata.csv
        self._write_metadata_csv(gse_dir, metadata_dir, result)

        # Write profiling_summary.json
        summary_path = gse_dir / "profiling_summary.json"
        try:
            with open(summary_path, "w", encoding="utf-8") as fh:
                json.dump(result.to_dict(), fh, indent=2, ensure_ascii=False)
            result.summary_file = summary_path
        except Exception as exc:
            result.warnings.append(f"Failed to write profiling_summary.json: {exc}")

        result.success = len(result.errors) == 0
        logger.info(result.summary)
        return result

    # ── Private: expression source detection & reading ───────────────────────

    def _detect_omics_type(self, gse_dir: Path) -> str:
        archive = gse_dir / "archive.json"
        if archive.exists():
            try:
                data = json.loads(archive.read_text(encoding="utf-8"))
                return data.get("omics_type", "")
            except Exception:
                pass
        return ""

    def _read_expression(
        self,
        gse_dir: Path,
        result: ProfilingResult,
    ) -> tuple[str, list[str], list[dict]]:
        """Return (gene_col_name, [sample_cols], [{gene: ..., s1: ..., ...}]).

        Priority:
          1. processed/ count/matrix/expr files
          2. raw/ count/matrix files
          3. Series Matrix (*_series_matrix.txt.gz)
          4. Any .csv / .tsv in the GSE dir
        """
        # 1. processed/
        for candidate in self._find_expression_files(gse_dir / "processed"):
            try:
                g, s, rows = self._read_tabular(candidate, result)
                if rows:
                    logger.info(f"Expression source: {candidate.name} (processed/)")
                    return g, s, rows
            except Exception as exc:
                result.warnings.append(f"Skipped {candidate.name}: {exc}")

        # 2. raw/
        for candidate in self._find_expression_files(gse_dir / "raw"):
            try:
                g, s, rows = self._read_tabular(candidate, result)
                if rows:
                    logger.info(f"Expression source: {candidate.name} (raw/)")
                    return g, s, rows
            except Exception as exc:
                result.warnings.append(f"Skipped {candidate.name}: {exc}")

        # 3. Series Matrix
        for sm in sorted(gse_dir.glob("*_series_matrix.txt.gz")):
            try:
                g, s, rows = self._read_series_matrix(sm, result)
                if rows:
                    logger.info(f"Expression source: {sm.name} (series_matrix)")
                    return g, s, rows
            except Exception as exc:
                result.warnings.append(f"Skipped {sm.name}: {exc}")

        # 4. Fallback: any tabular file at root
        for candidate in self._find_expression_files(gse_dir):
            try:
                g, s, rows = self._read_tabular(candidate, result)
                if rows:
                    logger.info(f"Expression source: {candidate.name} (root)")
                    return g, s, rows
            except Exception as exc:
                result.warnings.append(f"Skipped {candidate.name}: {exc}")

        return "gene_id", [], []

    def _find_expression_files(self, directory: Path) -> list[Path]:
        """Find candidate expression files in a directory, highest priority first."""
        if not directory.exists():
            return []

        priority_keywords = ("count", "matrix", "expr", "tpm", "fpkm", "rpkm", "raw")
        candidates: list[tuple[int, Path]] = []

        for ext in ("*.csv.gz", "*.tsv.gz", "*.txt.gz", "*.csv", "*.tsv", "*.txt"):
            for fp in directory.glob(ext):
                name_lower = fp.name.lower()
                # Skip known non-expression files
                if any(skip in name_lower for skip in ("readme", "manifest", "checksums", "md5")):
                    continue
                prio = next(
                    (i for i, kw in enumerate(priority_keywords) if kw in name_lower),
                    len(priority_keywords),
                )
                candidates.append((prio, fp))

        candidates.sort(key=lambda x: x[0])
        return [fp for _, fp in candidates]

    def _read_tabular(
        self,
        filepath: Path,
        result: ProfilingResult,
    ) -> tuple[str, list[str], list[dict]]:
        """Read a tabular CSV/TSV file (plain or gzip-compressed).

        Returns (gene_col, sample_cols, rows).
        """
        lines = self._read_lines(filepath)
        if not lines:
            return "gene_id", [], []

        # Detect delimiter
        first_line = lines[0]
        delimiter = "\t" if first_line.count("\t") > first_line.count(",") else ","

        reader = csv.DictReader(lines, delimiter=delimiter)
        fieldnames = reader.fieldnames
        if not fieldnames:
            return "gene_id", [], []

        # First column is gene/feature identifier
        gene_col = fieldnames[0]
        sample_cols = list(fieldnames[1:])

        rows = []
        for i, row in enumerate(reader):
            if self.max_rows and i >= self.max_rows:
                result.warnings.append(
                    f"Truncated {filepath.name} at {self.max_rows} rows (max_rows limit)"
                )
                break
            rows.append(dict(row))

        return gene_col, sample_cols, rows

    def _read_series_matrix(
        self,
        filepath: Path,
        result: ProfilingResult,
    ) -> tuple[str, list[str], list[dict]]:
        """Parse a *_series_matrix.txt.gz file.

        Returns (gene_col, sample_cols, rows).
        The series matrix format:
          - Header lines start with "!"
          - "!Sample_geo_accession" row contains sample IDs
          - "!series_matrix_table_begin" marks the start of the data table
          - Data table: first col = gene/probe ID, rest = expression values
        """
        lines = self._read_lines(filepath)

        sample_ids: list[str] = []
        in_table = False
        header: list[str] = []
        rows: list[dict] = []

        for line in lines:
            line = line.rstrip("\n\r")

            if line.startswith("!Sample_geo_accession"):
                parts = line.split("\t")
                sample_ids = [p.strip().strip('"') for p in parts[1:] if p.strip()]

            elif line.startswith("!series_matrix_table_begin"):
                in_table = True

            elif line.startswith("!series_matrix_table_end"):
                break

            elif in_table:
                parts = line.split("\t")
                if not header:
                    # First row in table is the column header
                    header = [p.strip().strip('"') for p in parts]
                    continue
                if len(parts) < 2:
                    continue

                gene_id = parts[0].strip().strip('"')
                values = [p.strip().strip('"') for p in parts[1:]]

                # Pad or trim to match sample count
                n = len(sample_ids)
                values = (values + [""] * n)[:n]

                row = {"gene_id": gene_id}
                for sid, val in zip(sample_ids, values):
                    row[sid] = val
                rows.append(row)

                if self.max_rows and len(rows) >= self.max_rows:
                    result.warnings.append(
                        f"Truncated {filepath.name} at {self.max_rows} rows"
                    )
                    break

        gene_col = "gene_id"
        return gene_col, sample_ids, rows

    # ── Private: structural cleaning & statistics ─────────────────────────────

    def _clean_and_stats(
        self,
        gene_col: str,
        sample_cols: list[str],
        rows: list[dict],
        result: ProfilingResult,
    ) -> tuple[list[dict], MatrixStats]:
        """Remove duplicates and empty rows; compute statistics.

        No value transformation is performed.
        """
        stats = MatrixStats()
        stats.sample_count = len(sample_cols)

        # Remove duplicate gene IDs (keep first occurrence)
        seen_genes: set[str] = set()
        deduped: list[dict] = []
        for row in rows:
            gid = str(row.get(gene_col, "")).strip()
            if gid in seen_genes:
                stats.duplicate_genes_removed += 1
            else:
                seen_genes.add(gid)
                deduped.append(row)

        if stats.duplicate_genes_removed:
            result.warnings.append(
                f"Removed {stats.duplicate_genes_removed} duplicate gene rows"
            )

        # Remove fully-empty gene rows
        cleaned: list[dict] = []
        for row in deduped:
            vals = [row.get(s, "") for s in sample_cols]
            if all(v == "" or v is None for v in vals):
                stats.empty_genes_removed += 1
            else:
                cleaned.append(row)

        if stats.empty_genes_removed:
            result.warnings.append(
                f"Removed {stats.empty_genes_removed} fully-empty gene rows"
            )

        stats.gene_count = len(cleaned)
        stats.total_cells = stats.sample_count * stats.gene_count

        # Compute missing / zero counts
        missing_count = 0
        zero_count = 0
        value_types: set[str] = set()

        for row in cleaned:
            for s in sample_cols:
                v = str(row.get(s, "")).strip()
                if v == "" or v.lower() in ("na", "nan", "null", "n/a", "."):
                    missing_count += 1
                else:
                    try:
                        fv = float(v)
                        if fv == 0.0:
                            zero_count += 1
                        value_types.add("integer" if "." not in v else "float")
                    except ValueError:
                        value_types.add("string")

        stats.missing_count = missing_count
        stats.zero_count = zero_count

        if stats.total_cells > 0:
            stats.missing_rate = round(missing_count / stats.total_cells, 6)
            stats.zero_rate = round(zero_count / stats.total_cells, 6)
            stats.sparsity = round((missing_count + zero_count) / stats.total_cells, 6)

        if len(value_types) == 1:
            stats.value_type = value_types.pop()
        elif value_types:
            stats.value_type = "mixed"

        return cleaned, stats

    # ── Private: I/O helpers ─────────────────────────────────────────────────

    @staticmethod
    def _read_lines(filepath: Path) -> list[str]:
        """Read all lines from a plain or gzip-compressed text file."""
        if filepath.suffix == ".gz" or str(filepath).endswith(".gz"):
            with gzip.open(filepath, "rt", encoding="utf-8", errors="replace") as fh:
                return fh.readlines()
        else:
            with open(filepath, encoding="utf-8", errors="replace") as fh:
                return fh.readlines()

    @staticmethod
    def _write_matrix(
        gene_col: str,
        sample_cols: list[str],
        rows: list[dict],
        out_path: Path,
    ) -> None:
        """Write the cleaned matrix to a CSV file."""
        fieldnames = [gene_col] + sample_cols
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)

    def _write_metadata_csv(
        self,
        gse_dir: Path,
        metadata_dir: Path,
        result: ProfilingResult,
    ) -> None:
        """Write metadata.csv from archive.json if available."""
        archive_path = gse_dir / "archive.json"
        if not archive_path.exists():
            return
        try:
            data = json.loads(archive_path.read_text(encoding="utf-8"))
            samples = data.get("samples", [])

            if samples:
                fieldnames: list[str] = []
                seen: set[str] = set()
                rows: list[dict] = []
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
                    for k in row:
                        if k not in seen:
                            fieldnames.append(k)
                            seen.add(k)
            else:
                rows = [{
                    "gse_id": data.get("gse_id", ""),
                    "title": data.get("metadata", {}).get("title", ""),
                    "omics_type": data.get("omics_type", ""),
                    "sample_count": data.get("sample_count", 0),
                }]
                fieldnames = list(rows[0].keys())

            meta_path = metadata_dir / "metadata.csv"
            with open(meta_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

            result.metadata_file = meta_path
            logger.debug(f"Wrote metadata.csv → {meta_path} ({len(rows)} rows)")

        except Exception as exc:
            result.warnings.append(f"Failed to write metadata.csv: {exc}")
