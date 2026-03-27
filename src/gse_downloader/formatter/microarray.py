"""Microarray formatter for GSE Downloader.

Handles normalization of microarray data (Affymetrix CEL, Agilent TXT, etc.)
and series matrix expression data for array-based studies.
"""

from __future__ import annotations

import csv
import gzip
import re
from pathlib import Path
from typing import Optional

from gse_downloader.formatter.base import BaseFormatter, _safe_move
from gse_downloader.formatter.series_matrix import SeriesMatrixFormatter
from gse_downloader.utils.logger import get_logger

logger = get_logger("formatter.microarray")


class MicroarrayFormatter(BaseFormatter):
    """Formatter for microarray (Affymetrix, Agilent, Illumina bead array) data.

    Directory layout after formatting::

        GSE123456/
        ├── raw/
        │   ├── *.CEL.gz        # Affymetrix raw CEL files
        │   ├── *.txt.gz        # Agilent raw files
        │   └── *.idat.gz       # Illumina IDAT files
        ├── processed/
        │   ├── *_series_matrix.txt.gz   # GEO matrix file (moved here)
        │   └── expression_matrix.csv   # Unified long-format expression table
        ├── metadata/
        │   └── metadata.csv
        └── archive.json
    """

    # Raw data patterns
    RAW_PATTERNS = [
        "*.CEL",
        "*.CEL.gz",
        "*.cel",
        "*.cel.gz",
        "*.idat",
        "*.idat.gz",
    ]
    # Processed / matrix patterns
    PROCESSED_PATTERNS = [
        "*_series_matrix.txt.gz",
        "*_series_matrix.txt",
        "*_processed*.txt",
        "*_processed*.csv",
        "*_normalized*.txt",
        "*_normalized*.csv",
    ]

    @property
    def omics_type(self) -> str:
        return "Microarray"

    def build_expression_matrix(
        self, gse_dir: Path, processed_dir: Path
    ) -> Optional[Path]:
        """Build expression_matrix.csv by parsing the series matrix file.

        For microarray data the series matrix file already contains the
        (normalized) expression values, so we delegate to
        SeriesMatrixFormatter.

        Args:
            gse_dir: GSE root directory
            processed_dir: processed/ sub-directory

        Returns:
            Path to expression_matrix.csv or None.
        """
        sm = SeriesMatrixFormatter()
        return sm.build_expression_matrix(gse_dir, processed_dir)
