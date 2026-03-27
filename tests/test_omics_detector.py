"""Tests for omics_detector module."""

import pytest

from gse_downloader.parser.omics_detector import (
    DetectionRule,
    OmicsDetector,
    OmicsType,
)


class TestOmicsDetector:
    """Test OmicsDetector class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.detector = OmicsDetector()

    def test_detect_rna_seq(self):
        """Test RNA-seq detection."""
        omics_type = self.detector.detect(
            series_type="Expression profiling by high throughput sequencing",
            summary="RNA-seq of tumor samples",
        )

        assert omics_type == OmicsType.RNA_SEQ

    def test_detect_microarray(self):
        """Test microarray detection."""
        omics_type = self.detector.detect(
            series_type="Expression profiling by array",
            summary="Gene expression profiling using Affymetrix arrays",
        )

        assert omics_type == OmicsType.MICROARRAY

    def test_detect_single_cell(self):
        """Test single-cell RNA-seq detection."""
        omics_type = self.detector.detect(
            summary="Single-cell RNA sequencing of PBMCs using 10x Genomics",
        )

        assert omics_type == OmicsType.SINGLE_CELL_RNA_SEQ

    def test_detect_atac_seq(self):
        """Test ATAC-seq detection."""
        omics_type = self.detector.detect(
            summary="ATAC-seq analysis of chromatin accessibility",
        )

        assert omics_type == OmicsType.ATAC_SEQ

    def test_detect_chip_seq(self):
        """Test ChIP-seq detection."""
        omics_type = self.detector.detect(
            library_strategy="ChIP-Seq",
        )

        assert omics_type == OmicsType.CHIP_SEQ

    def test_detect_methylation_array(self):
        """Test methylation array detection."""
        omics_type = self.detector.detect(
            platform_title="Illumina HumanMethylation450 BeadChip",
        )

        assert omics_type == OmicsType.METHYLATION_ARRAY

    def test_detect_unknown(self):
        """Test unknown type defaults to Other."""
        omics_type = self.detector.detect(
            series_type="Unknown Type",
            summary="Some unknown experiment",
        )

        assert omics_type == OmicsType.OTHER

    def test_priority_single_cell_over_rna_seq(self):
        """Test that single-cell takes priority over RNA-seq."""
        omics_type = self.detector.detect(
            summary="Single-cell RNA-seq using 10x Genomics",
        )

        assert omics_type == OmicsType.SINGLE_CELL_RNA_SEQ

    def test_series_type_mapping(self):
        """Test series type to omics type mapping."""
        mapping = OmicsDetector.get_series_type_mapping()

        assert mapping["Expression profiling by array"] == OmicsType.MICROARRAY
        assert mapping["Expression profiling by high throughput sequencing"] == OmicsType.RNA_SEQ
        assert mapping["Single cell RNA sequencing"] == OmicsType.SINGLE_CELL_RNA_SEQ
        assert mapping["ATAC-seq"] == OmicsType.ATAC_SEQ


class TestOmicsType:
    """Test OmicsType enum."""

    def test_omics_types(self):
        """Test all omics types exist."""
        expected_types = [
            "RNA-seq",
            "miRNA-seq",
            "Small RNA-seq",
            "ATAC-seq",
            "ChIP-seq",
            "Methylation sequencing",
            "Methylation array",
            "Single-cell RNA-seq",
            "Single-cell ATAC-seq",
            "Microarray",
            "WGS",
            "WES",
            "Proteomics",
            "Other",
        ]

        for type_name in expected_types:
            omics_type = OmicsType(type_name)
            assert omics_type.value == type_name

    def test_omics_type_values(self):
        """Test omics type values."""
        assert OmicsType.RNA_SEQ.value == "RNA-seq"
        assert OmicsType.MICROARRAY.value == "Microarray"
        assert OmicsType.ATAC_SEQ.value == "ATAC-seq"
