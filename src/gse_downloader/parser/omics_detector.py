"""Omics type detection module for GSE Downloader.

This module handles automatic detection of omics types from GEO metadata.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("omics_detector")


class OmicsType(str, Enum):
    """Enumeration of supported omics types."""

    RNA_SEQ = "RNA-seq"
    """High-throughput RNA sequencing"""

    MIRNA_SEQ = "miRNA-seq"
    """MicroRNA sequencing"""

    SMALL_RNA_SEQ = "Small RNA-seq"
    """Small RNA sequencing"""

    ATAC_SEQ = "ATAC-seq"
    """Assay for Transposase-Accessible Chromatin sequencing"""

    CHIP_SEQ = "ChIP-seq"
    """Chromatin Immunoprecipitation sequencing"""

    METHYLATION_SEQ = "Methylation sequencing"
    """Methylation profiling by sequencing"""

    METHYLATION_ARRAY = "Methylation array"
    """Methylation profiling by array"""

    SINGLE_CELL_RNA_SEQ = "Single-cell RNA-seq"
    """Single-cell RNA sequencing"""

    SINGLE_CELL_ATAC_SEQ = "Single-cell ATAC-seq"
    """Single-cell ATAC sequencing"""

    MICROARRAY = "Microarray"
    """Expression profiling by array"""

    WGS = "WGS"
    """Whole Genome Sequencing"""

    WES = "WES"
    """Whole Exome Sequencing"""

    PROTEOMICS = "Proteomics"
    """Proteomic profiling by mass spectrometry"""

    OTHER = "Other"
    """Other or unknown omics type"""


@dataclass
class DetectionRule:
    """Rule for omics type detection."""

    omics_type: OmicsType
    patterns: list[str]
    priority: int = 0


class OmicsDetector:
    """Detects omics type from GEO metadata."""

    # Detection rules (order matters - first match wins)
    RULES: list[DetectionRule] = [
        # Highest priority rules
        DetectionRule(
            omics_type=OmicsType.SINGLE_CELL_RNA_SEQ,
            patterns=[
                r"single.cell.*rna",
                r"single-cell",
                r"scrna-seq",
                r"10x genomics",
                r"10x chromium",
                r"drop-seq",
                r"smart-seq",
                r"single cell transcriptomics",
            ],
            priority=100,
        ),
        DetectionRule(
            omics_type=OmicsType.SINGLE_CELL_ATAC_SEQ,
            patterns=[
                r"single.cell.*atac",
                r"single-cell atac",
                r"scatac-seq",
            ],
            priority=100,
        ),
        DetectionRule(
            omics_type=OmicsType.ATAC_SEQ,
            patterns=[
                r"\batac\b",
                r"assay for transposase-accessible chromatin",
                r"chromatin accessibility",
            ],
            priority=90,
        ),
        DetectionRule(
            omics_type=OmicsType.CHIP_SEQ,
            patterns=[
                r"\bchip[- ]?seq\b",
                r"chip[- ]?seq",
                r"chromatin immunoprecipitation",
                r"genome binding",
                r"occupancy profiling",
            ],
            priority=80,
        ),
        DetectionRule(
            omics_type=OmicsType.METHYLATION_SEQ,
            patterns=[
                r"methylation profiling.*high throughput sequencing",
                r"methyl-seq",
                r"bisulfite.*seq",
                r"wgbs",
                r"rrbs",
            ],
            priority=80,
        ),
        DetectionRule(
            omics_type=OmicsType.METHYLATION_ARRAY,
            patterns=[
                r"methylation profiling.*array",
                r"epic array",
                r"850k array",
                r"450k array",
                r"humanmethylation",
            ],
            priority=70,
        ),
        DetectionRule(
            omics_type=OmicsType.MIRNA_SEQ,
            patterns=[
                r"\bmirna[- ]?seq\b",
                r"mirna profiling",
                r"microrna sequencing",
            ],
            priority=70,
        ),
        DetectionRule(
            omics_type=OmicsType.SMALL_RNA_SEQ,
            patterns=[
                r"\bsmall rna[- ]?seq\b",
                r"small rna profiling",
            ],
            priority=70,
        ),
        DetectionRule(
            omics_type=OmicsType.WGS,
            patterns=[
                r"\bwgs\b",
                r"whole genome sequencing",
                r"genome variation profiling",
            ],
            priority=60,
        ),
        DetectionRule(
            omics_type=OmicsType.WES,
            patterns=[
                r"\bwes\b",
                r"whole exome sequencing",
                r"exome sequencing",
            ],
            priority=60,
        ),
        DetectionRule(
            omics_type=OmicsType.RNA_SEQ,
            patterns=[
                r"expression profiling.*high throughput sequencing",
                r"rna[- ]?seq",
                r"transcriptome sequencing",
                r"mrna[- ]?seq",
            ],
            priority=50,
        ),
        DetectionRule(
            omics_type=OmicsType.PROTEOMICS,
            patterns=[
                r"proteomic",
                r"mass spectrometry",
                r"lc-ms",
                r"label-free quantitation",
            ],
            priority=40,
        ),
        DetectionRule(
            omics_type=OmicsType.MICROARRAY,
            patterns=[
                r"expression profiling.*array",
                r"gene expression",
                r"microarray",
                r"hg-u133",
                r"affymetrix",
                r"agilent",
                r"illumina.*array",
            ],
            priority=30,
        ),
    ]

    # Platform-based detection
    PLATFORM_PATTERNS: dict[str, OmicsType] = {
        # Illumina sequencing platforms -> RNA-seq
        r"hiseq": OmicsType.RNA_SEQ,
        r"novaseq": OmicsType.RNA_SEQ,
        r"nextseq": OmicsType.RNA_SEQ,
        r"miseq": OmicsType.RNA_SEQ,
        # 10x platforms -> Single-cell
        r"10x": OmicsType.SINGLE_CELL_RNA_SEQ,
        # Methylation arrays
        r"850k": OmicsType.METHYLATION_ARRAY,
        r"450k": OmicsType.METHYLATION_ARRAY,
        r"humanmethylation": OmicsType.METHYLATION_ARRAY,
        r"epic": OmicsType.METHYLATION_ARRAY,
    }

    def __init__(self):
        """Initialize omics detector."""
        # Sort rules by priority (highest first)
        self.rules = sorted(self.RULES, key=lambda r: r.priority, reverse=True)
        logger.debug("OmicsDetector initialized")

    def detect(
        self,
        series_type: Optional[str] = None,
        summary: Optional[str] = None,
        overall_design: Optional[str] = None,
        platform_title: Optional[str] = None,
        library_strategy: Optional[str] = None,
        filenames: Optional[list[str]] = None,
    ) -> OmicsType:
        """Detect omics type from metadata.

        Args:
            series_type: Series type from GEO
            summary: Series summary text
            overall_design: Overall design description
            platform_title: Platform title
            library_strategy: Library strategy
            filenames: List of file names

        Returns:
            Detected OmicsType
        """
        # Combine text fields for matching
        text = " ".join(
            filter(
                None,
                [
                    series_type or "",
                    summary or "",
                    overall_design or "",
                    platform_title or "",
                    library_strategy or "",
                ],
            )
        ).lower()

        # Check filenames
        if filenames:
            filename_text = " ".join(filenames).lower()
            text = f"{text} {filename_text}"

        logger.debug(f"Detecting omics type from text: {text[:200]}...")

        # Apply detection rules
        for rule in self.rules:
            for pattern in rule.patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    logger.info(f"Detected omics type: {rule.omics_type.value} (matched: {pattern})")
                    return rule.omics_type

        # Try platform-based detection
        if platform_title:
            platform_lower = platform_title.lower()
            for pattern, omics_type in self.PLATFORM_PATTERNS.items():
                if re.search(pattern, platform_lower, re.IGNORECASE):
                    logger.info(f"Detected omics type from platform: {omics_type.value}")
                    return omics_type

        # Try library strategy
        if library_strategy:
            lib_lower = library_strategy.lower()
            if "rna" in lib_lower and "seq" in lib_lower:
                return OmicsType.RNA_SEQ
            if "chip" in lib_lower:
                return OmicsType.CHIP_SEQ
            if "atac" in lib_lower:
                return OmicsType.ATAC_SEQ

        logger.warning("Could not detect omics type, defaulting to 'Other'")
        return OmicsType.OTHER

    def detect_from_metadata(self, metadata: dict) -> OmicsType:
        """Detect omics type from metadata dictionary.

        Args:
            metadata: Metadata dictionary with keys like 'series_type', 'summary', etc.

        Returns:
            Detected OmicsType
        """
        return self.detect(
            series_type=metadata.get("series_type"),
            summary=metadata.get("summary"),
            overall_design=metadata.get("overall_design"),
            platform_title=metadata.get("platform_title"),
            library_strategy=metadata.get("library_strategy"),
            filenames=metadata.get("filenames"),
        )

    @staticmethod
    def get_series_type_mapping() -> dict[str, OmicsType]:
        """Get mapping from GEO series types to omics types.

        Returns:
            Dictionary mapping series type to omics type
        """
        return {
            "Expression profiling by array": OmicsType.MICROARRAY,
            "Expression profiling by high throughput sequencing": OmicsType.RNA_SEQ,
            "Non-coding RNA profiling by high throughput sequencing": OmicsType.MIRNA_SEQ,
            "Genome binding/occupancy profiling by high throughput sequencing": OmicsType.CHIP_SEQ,
            "Genome variation profiling by high throughput sequencing": OmicsType.WGS,
            "Methylation profiling by high throughput sequencing": OmicsType.METHYLATION_SEQ,
            "Methylation profiling by array": OmicsType.METHYLATION_ARRAY,
            "ATAC-seq": OmicsType.ATAC_SEQ,
            "Single cell RNA sequencing": OmicsType.SINGLE_CELL_RNA_SEQ,
            "Proteomic profiling by mass spectrometry": OmicsType.PROTEOMICS,
        }
