"""Formatter module for GSE Downloader.

Provides data normalization and standardization for different omics types.
Each formatter converts raw GEO data into a unified directory structure:

    GSE123456/
    ├── raw/                   # Raw data files (.fastq.gz, .cel, etc.)
    ├── processed/             # Processed files
    │   └── expression_matrix.csv   # Unified expression matrix
    ├── metadata/
    │   └── metadata.csv       # Sample metadata table
    └── archive.json           # Archive manifest

Usage:
    from gse_downloader.formatter import FormatterFactory
    from gse_downloader.parser.omics_detector import OmicsType

    formatter = FormatterFactory.get(OmicsType.RNA_SEQ)
    formatter.format(gse_dir)
"""

from gse_downloader.formatter.base import BaseFormatter, FormatResult
from gse_downloader.formatter.factory import FormatterFactory
from gse_downloader.formatter.microarray import MicroarrayFormatter
from gse_downloader.formatter.rnaseq import RNASeqFormatter
from gse_downloader.formatter.series_matrix import SeriesMatrixFormatter

__all__ = [
    "BaseFormatter",
    "FormatResult",
    "FormatterFactory",
    "MicroarrayFormatter",
    "RNASeqFormatter",
    "SeriesMatrixFormatter",
]
