"""Metadata parsing module for GSE Downloader.

This module handles parsing metadata from GEO records in SOFT format.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("metadata")


@dataclass
class GSEMetadata:
    """Metadata for a GEO Series (GSE)."""

    gse_id: str
    title: str = ""
    summary: str = ""
    overall_design: str = ""
    series_type: str = ""
    contributor: str = ""
    contributor_email: str = ""
    contributor_institution: str = ""
    submission_date: Optional[datetime] = None
    last_update_date: Optional[datetime] = None
    pubmed_ids: list[str] = field(default_factory=list)
    bioproject_id: Optional[str] = None
    sra_id: Optional[str] = None
    keywords: list[str] = field(default_factory=list)
    funding: list[str] = field(default_factory=list)
    organism: list[str] = field(default_factory=list)
    platforms: list["GPLMetadata"] = field(default_factory=list)
    samples: list["GSMMetadata"] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "gse_id": self.gse_id,
            "title": self.title,
            "summary": self.summary,
            "overall_design": self.overall_design,
            "series_type": self.series_type,
            "contributor": {
                "name": self.contributor,
                "email": self.contributor_email,
                "institution": self.contributor_institution,
            },
            "submission_date": self.submission_date.isoformat() if self.submission_date else None,
            "last_update_date": self.last_update_date.isoformat() if self.last_update_date else None,
            "pubmed_ids": self.pubmed_ids,
            "bioproject_id": self.bioproject_id,
            "sra_id": self.sra_id,
            "keywords": self.keywords,
            "funding": self.funding,
            "platforms": [p.to_dict() for p in self.platforms],
            "sample_count": len(self.samples),
        }


@dataclass
class GSMMetadata:
    """Metadata for a GEO Sample (GSM)."""

    gsm_id: str
    title: str = ""
    status: str = "Public"
    source_name: str = ""
    organism: str = ""
    organism_taxid: Optional[int] = None
    extraction_molecule: str = ""
    extraction_protocol: str = ""
    library_strategy: str = ""
    library_source: str = ""
    library_selection: str = ""
    library_layout: str = ""
    instrument_model: str = ""
    sequencing_platform: str = ""
    data_processing: str = ""
    reference_genome: str = ""
    data_processing_pipeline: str = ""
    characteristics: dict[str, str] = field(default_factory=dict)

    @property
    def tissue(self) -> Optional[str]:
        """Get tissue from characteristics."""
        return self.characteristics.get("tissue")

    @property
    def disease(self) -> Optional[str]:
        """Get disease from characteristics."""
        return self.characteristics.get("disease")

    @property
    def treatment(self) -> Optional[str]:
        """Get treatment from characteristics."""
        return self.characteristics.get("treatment")

    @property
    def cell_type(self) -> Optional[str]:
        """Get cell type from characteristics."""
        return self.characteristics.get("cell type")

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "gsm_id": self.gsm_id,
            "title": self.title,
            "status": self.status,
            "source_name": self.source_name,
            "organism": self.organism,
            "organism_taxid": self.organism_taxid,
            "extraction_molecule": self.extraction_molecule,
            "library_strategy": self.library_strategy,
            "library_layout": self.library_layout,
            "instrument_model": self.instrument_model,
            "characteristics": self.characteristics,
        }


@dataclass
class GPLMetadata:
    """Metadata for a GEO Platform (GPL)."""

    gpl_id: str
    title: str = ""
    technology: str = ""
    organism: str = ""
    manufacturer: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "gpl_id": self.gpl_id,
            "title": self.title,
            "technology": self.technology,
            "organism": self.organism,
            "manufacturer": self.manufacturer,
        }


class MetadataParser:
    """Parser for GEO metadata files."""

    def __init__(self):
        """Initialize metadata parser."""
        logger.debug("MetadataParser initialized")

    def parse_soft_file(self, filepath: Path) -> tuple[Optional[GSEMetadata], list[GSMMetadata], list[GPLMetadata]]:
        """Parse a SOFT format file (supports plain text and gzip compressed).

        Args:
            filepath: Path to SOFT file (.soft or .soft.gz)

        Returns:
            Tuple of (GSE metadata, list of GSM metadata, list of GPL metadata)
        """
        import gzip

        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return None, [], []

        try:
            filename = str(filepath).lower()
            if filename.endswith(".gz"):
                with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            else:
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

            return self.parse_soft_content(content)

        except Exception as e:
            logger.error(f"Failed to parse SOFT file {filepath}: {e}")
            return None, [], []

    def parse_soft_content(
        self, content: str
    ) -> tuple[Optional[GSEMetadata], list[GSMMetadata], list[GPLMetadata]]:
        """Parse SOFT format content.

        Args:
            content: SOFT format text

        Returns:
            Tuple of (GSE metadata, list of GSM metadata, list of GPL metadata)
        """
        gse_metadata: Optional[GSEMetadata] = None
        gsm_list: list[GSMMetadata] = []
        gpl_list: list[GPLMetadata] = []

        lines = content.split("\n")
        current_entity = None
        current_gsm: Optional[GSMMetadata] = None
        current_gpl: Optional[GPLMetadata] = None

        # Multi-line field accumulator
        _summary_parts: list[str] = []
        _overall_design_parts: list[str] = []

        for line in lines:
            line = line.rstrip()

            # Detect entity starts
            if line.startswith("^SERIES"):
                # Flush accumulated multi-line fields
                if gse_metadata and _summary_parts:
                    gse_metadata.summary = " ".join(_summary_parts)
                    _summary_parts.clear()
                if gse_metadata and _overall_design_parts:
                    gse_metadata.overall_design = " ".join(_overall_design_parts)
                    _overall_design_parts.clear()

                gse_id = self._extract_accession(line, "GSE")
                if gse_metadata is None:
                    gse_metadata = GSEMetadata(gse_id=gse_id or "")
                current_entity = "series"
                current_gsm = None
                current_gpl = None

            elif line.startswith("^SAMPLE"):
                gsm_id = self._extract_accession(line, "GSM")
                current_gsm = GSMMetadata(gsm_id=gsm_id or "")
                gsm_list.append(current_gsm)
                current_entity = "sample"
                current_gpl = None

            elif line.startswith("^PLATFORM"):
                gpl_id = self._extract_accession(line, "GPL")
                current_gpl = GPLMetadata(gpl_id=gpl_id or "")
                gpl_list.append(current_gpl)
                current_entity = "platform"
                current_gsm = None

            # Parse attributes
            elif line.startswith("!") and " = " in line:
                key, value = line[1:].split(" = ", 1)
                value = value.strip().strip('"')
                key = key.strip()

                if current_entity == "series" and gse_metadata:
                    # Handle multi-line summary/overall_design
                    if key == "Series_summary":
                        _summary_parts.append(value)
                    elif key == "Series_overall_design":
                        _overall_design_parts.append(value)
                    else:
                        self._parse_series_attr(gse_metadata, key, value)

                elif current_entity == "sample" and current_gsm:
                    self._parse_sample_attr(current_gsm.__dict__, key, value)

                elif current_entity == "platform" and current_gpl:
                    self._parse_platform_attr(current_gpl.__dict__, key, value)

        # Flush final multi-line fields
        if gse_metadata:
            if _summary_parts:
                gse_metadata.summary = " ".join(_summary_parts)
            if _overall_design_parts:
                gse_metadata.overall_design = " ".join(_overall_design_parts)

            # Collect organism from samples (if not already populated from series)
            if not gse_metadata.organism:
                organisms: list[str] = []
                for gsm in gsm_list:
                    if gsm.organism and gsm.organism not in organisms:
                        organisms.append(gsm.organism)
                gse_metadata.organism = organisms

        return gse_metadata, gsm_list, gpl_list

    def _extract_accession(self, line: str, prefix: str) -> str:
        """Extract accession number from entity line.

        Args:
            line: Entity line
            prefix: Prefix (GSE, GSM, GPL)

        Returns:
            Accession number
        """
        match = re.search(rf"{prefix}(\d+)", line)
        if match:
            return f"{prefix}{match.group(1)}"
        return ""

    def _parse_series_attr(self, gse: Optional[GSEMetadata], key: str, value: str) -> None:
        """Parse series attribute.

        Args:
            gse: GSEMetadata instance
            key: Attribute key
            value: Attribute value
        """
        if gse is None:
            return

        if key == "Series_title":
            gse.title = value

        elif key == "Series_summary":
            gse.summary = value

        elif key == "Series_overall_design":
            gse.overall_design = value

        elif key == "Series_type":
            gse.series_type = value

        elif key == "Series_contributor":
            gse.contributor = value

        elif key == "Series_contact_email":
            gse.contributor_email = value

        elif key == "Series_contact_institute":
            gse.contributor_institution = value

        elif key == "Series_pubmed_id":
            if value:
                gse.pubmed_ids.append(value)

        elif key == "Series_keywords":
            if value:
                gse.keywords = [k.strip() for k in value.split(";")]

        elif key == "Series_submission_date":
            gse.submission_date = self._parse_date(value)

        elif key == "Series_last_update_date":
            gse.last_update_date = self._parse_date(value)

        elif key == "Series_relation":
            # e.g. "BioProject: PRJNA123" or "SRA: SRP123"
            # or "BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA555477"
            if value.startswith("BioProject:"):
                raw = value.split(":", 1)[1].strip()
                # Extract just the accession (PRJNA...)
                import re as _re
                m = _re.search(r"(PRJNA\d+)", raw)
                gse.bioproject_id = m.group(1) if m else raw
            elif value.startswith("SRA:"):
                raw = value.split(":", 1)[1].strip()
                import re as _re
                m = _re.search(r"(SRP\d+|SRR\d+|ERP\d+|DRP\d+)", raw)
                gse.sra_id = m.group(1) if m else raw

        elif key == "Series_platform_id":
            # This is a reference, not full platform data
            pass

        elif key == "Series_sample_id":
            # This is a reference, not full sample data
            pass

    def _parse_sample_attr(self, sample_dict: dict, key: str, value: str) -> None:
        """Parse sample attribute.

        Args:
            sample_dict: Sample dictionary
            key: Attribute key
            value: Attribute value
        """
        if key == "Sample_title":
            sample_dict["title"] = value

        # source_name: support _ch1 and plain form
        elif key in ("Sample_source_name_ch1", "Sample_source_name"):
            if not sample_dict.get("source_name"):
                sample_dict["source_name"] = value

        # organism: support _ch1 and plain form
        elif key in ("Sample_organism_ch1", "Sample_organism"):
            if not sample_dict.get("organism"):
                sample_dict["organism"] = value

        # taxid: support _ch1 and plain form
        elif key in ("Sample_taxid_ch1", "Sample_taxid"):
            if not sample_dict.get("organism_taxid"):
                try:
                    sample_dict["organism_taxid"] = int(value)
                except (ValueError, TypeError):
                    pass

        # molecule: support _ch1 and plain form
        elif key in ("Sample_molecule_ch1", "Sample_molecule"):
            if not sample_dict.get("extraction_molecule"):
                sample_dict["extraction_molecule"] = value

        elif key == "Sample_library_strategy":
            sample_dict["library_strategy"] = value

        elif key == "Sample_library_source":
            sample_dict["library_source"] = value

        elif key == "Sample_library_selection":
            sample_dict["library_selection"] = value

        elif key == "Sample_library_layout":
            sample_dict["library_layout"] = value

        elif key == "Sample_instrument_model":
            sample_dict["instrument_model"] = value

        elif key == "Sample_data_processing":
            sample_dict["data_processing"] = value

        elif key.startswith("Sample_characteristics_ch1"):
            # Parse characteristics like "tissue: liver" or "tissue = liver"
            char_value = value.strip()
            if ": " in char_value:
                parts = char_value.split(": ", 1)
                char_key = parts[0].strip().lower()
                char_val = parts[1].strip()
                sample_dict["characteristics"][char_key] = char_val
            elif " = " in char_value:
                parts = char_value.split(" = ", 1)
                char_key = parts[0].strip().lower()
                char_val = parts[1].strip()
                sample_dict["characteristics"][char_key] = char_val

    def _parse_platform_attr(self, platform_dict: dict, key: str, value: str) -> None:
        """Parse platform attribute.

        Args:
            platform_dict: Platform dictionary
            key: Attribute key
            value: Attribute value
        """
        if key == "Platform_title":
            platform_dict["title"] = value

        elif key == "Platform_technology":
            platform_dict["technology"] = value

        elif key == "Platform_organism":
            platform_dict["organism"] = value

        elif key == "Platform_manufacturer":
            platform_dict["manufacturer"] = value

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string.

        Args:
            date_str: Date string

        Returns:
            datetime object or None
        """
        try:
            # Try common formats
            for fmt in ["%Y-%m-%d", "%b %d, %Y", "%Y%m%d"]:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None
