"""Formatter factory for GSE Downloader.

Returns the appropriate formatter for a given omics type.
"""

from __future__ import annotations

from gse_downloader.formatter.base import BaseFormatter
from gse_downloader.parser.omics_detector import OmicsType


class FormatterFactory:
    """Factory class that returns the correct formatter for an omics type."""

    @staticmethod
    def get(omics_type: OmicsType | str) -> BaseFormatter:
        """Return a formatter instance for the given omics type.

        Args:
            omics_type: OmicsType enum or string value (e.g. "RNA-seq")

        Returns:
            Appropriate BaseFormatter subclass instance.
        """
        # Lazy imports to avoid circular deps
        from gse_downloader.formatter.microarray import MicroarrayFormatter
        from gse_downloader.formatter.rnaseq import RNASeqFormatter
        from gse_downloader.formatter.series_matrix import SeriesMatrixFormatter

        if isinstance(omics_type, str):
            try:
                omics_type = OmicsType(omics_type)
            except ValueError:
                omics_type = OmicsType.OTHER

        _map: dict[OmicsType, type[BaseFormatter]] = {
            OmicsType.MICROARRAY: MicroarrayFormatter,
            OmicsType.RNA_SEQ: RNASeqFormatter,
            OmicsType.MIRNA_SEQ: RNASeqFormatter,
            OmicsType.SMALL_RNA_SEQ: RNASeqFormatter,
            OmicsType.ATAC_SEQ: RNASeqFormatter,      # ATAC-seq shares same raw structure
            OmicsType.CHIP_SEQ: RNASeqFormatter,      # ChIP-seq shares same raw structure
            OmicsType.METHYLATION_SEQ: RNASeqFormatter,
            OmicsType.METHYLATION_ARRAY: MicroarrayFormatter,
            OmicsType.SINGLE_CELL_RNA_SEQ: RNASeqFormatter,
            OmicsType.SINGLE_CELL_ATAC_SEQ: RNASeqFormatter,
            OmicsType.WGS: RNASeqFormatter,
            OmicsType.WES: RNASeqFormatter,
        }

        formatter_cls = _map.get(omics_type, SeriesMatrixFormatter)
        return formatter_cls()

    @staticmethod
    def get_all_types() -> list[OmicsType]:
        """Return all supported omics types."""
        return [
            OmicsType.MICROARRAY,
            OmicsType.RNA_SEQ,
            OmicsType.MIRNA_SEQ,
            OmicsType.SMALL_RNA_SEQ,
            OmicsType.ATAC_SEQ,
            OmicsType.CHIP_SEQ,
            OmicsType.METHYLATION_SEQ,
            OmicsType.METHYLATION_ARRAY,
            OmicsType.SINGLE_CELL_RNA_SEQ,
            OmicsType.SINGLE_CELL_ATAC_SEQ,
            OmicsType.WGS,
            OmicsType.WES,
            OmicsType.PROTEOMICS,
            OmicsType.OTHER,
        ]
