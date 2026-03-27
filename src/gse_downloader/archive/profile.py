"""Archive profile generation module for GSE Downloader.

This module handles generating complete data archives for GSE datasets.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from gse_downloader.archive.schema import (
    ArchiveSchema,
    ArchiveVersion,
    Contributor,
    DownloadInfo,
    DownloadStatus,
    FileInfo,
    FileType,
    Organism,
    Platform,
    Reference,
    SampleCharacteristics,
    SampleInfo,
)
from gse_downloader.parser.metadata import GSEMetadata, GSMMetadata
from gse_downloader.parser.geo_query import GSESeries, GEOFile
from gse_downloader.parser.omics_detector import OmicsDetector, OmicsType
from gse_downloader.utils.logger import get_logger

logger = get_logger("archive")


class ArchiveProfile:
    """Represents a complete data archive for a GSE dataset."""

    def __init__(self, schema: ArchiveSchema):
        """Initialize archive profile.

        Args:
            schema: ArchiveSchema instance
        """
        self.schema = schema

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return self.schema.to_dict()

    def to_json(self, filepath: Optional[Path] = None, indent: int = 2) -> str:
        """Convert to JSON string.

        Args:
            filepath: Optional path to save JSON file
            indent: JSON indentation

        Returns:
            JSON string
        """
        data = self.to_dict()
        json_str = json.dumps(data, indent=indent, ensure_ascii=False)

        if filepath:
            filepath = Path(filepath)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(json_str)
            logger.info(f"Saved archive to {filepath}")

        return json_str

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveProfile":
        """Create from dictionary.

        Args:
            data: Dictionary data

        Returns:
            ArchiveProfile instance
        """
        schema = ArchiveSchema.from_dict(data)
        return cls(schema)

    @classmethod
    def from_json(cls, filepath: Path | str) -> "ArchiveProfile":
        """Load from JSON file.

        Args:
            filepath: Path to JSON file

        Returns:
            ArchiveProfile instance
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Archive file not found: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"Loaded archive from {filepath}")
        return cls.from_dict(data)


class ArchiveGenerator:
    """Generates data archives for GSE datasets."""

    ARCHIVE_FILENAME = "archive.json"

    def __init__(self, output_dir: Path):
        """Initialize archive generator.

        Args:
            output_dir: Base output directory
        """
        self.output_dir = Path(output_dir)
        self.omics_detector = OmicsDetector()
        logger.debug(f"ArchiveGenerator initialized for {output_dir}")

    def generate(
        self,
        gse_id: str,
        metadata: Optional[GSEMetadata] = None,
        series_info: Optional[GSESeries] = None,
        samples: Optional[list[GSMMetadata]] = None,
        files: Optional[list[GEOFile]] = None,
        status: DownloadStatus = DownloadStatus.NOT_STARTED,
    ) -> ArchiveProfile:
        """Generate a complete archive for a GSE dataset.

        Args:
            gse_id: GSE identifier
            metadata: GSEMetadata instance
            series_info: GSESeries instance
            samples: List of GSMMetadata instances
            files: List of GEOFile instances
            status: Download status

        Returns:
            ArchiveProfile instance
        """
        schema = ArchiveSchema(
            gse_id=gse_id,
            status=status,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Parse metadata/series info
        if metadata:
            self._parse_gse_metadata(schema, metadata)
        elif series_info:
            self._parse_series_info(schema, series_info)

        # Parse samples
        if samples:
            self._parse_samples(schema, samples)

        # Detect omics type
        schema.omics_type = self._detect_omics_type(schema, samples)

        # Parse files
        if files:
            self._parse_files(schema, files)

        return ArchiveProfile(schema)

    def _parse_gse_metadata(self, schema: ArchiveSchema, metadata: GSEMetadata) -> None:
        """Parse GSE metadata into schema.

        Args:
            schema: ArchiveSchema to populate
            metadata: GSEMetadata instance
        """
        schema.title = metadata.title
        schema.summary = metadata.summary
        schema.overall_design = metadata.overall_design
        schema.series_type = metadata.series_type
        schema.keywords = metadata.keywords
        schema.funding = metadata.funding

        if metadata.submission_date:
            schema.submission_date = metadata.submission_date.strftime("%Y-%m-%d")

        if metadata.last_update_date:
            schema.last_update_date = metadata.last_update_date.strftime("%Y-%m-%d")

        # Contributor
        schema.contributor = Contributor(
            name=metadata.contributor,
            email=metadata.contributor_email,
            institution=metadata.contributor_institution,
        )

        # References
        schema.references = Reference(pubmed_ids=metadata.pubmed_ids)

        # Organisms
        schema.organisms = [
            Organism(name=org) for org in metadata.organism
        ] if metadata.organism else []

    def _parse_series_info(self, schema: ArchiveSchema, series_info: GSESeries) -> None:
        """Parse GSESeries into schema.

        Args:
            schema: ArchiveSchema to populate
            series_info: GSESeries instance
        """
        schema.title = series_info.title
        schema.summary = series_info.summary
        schema.overall_design = series_info.overall_design
        schema.series_type = series_info.series_type
        schema.keywords = series_info.keywords

        schema.submission_date = series_info.submission_date
        schema.last_update_date = series_info.last_update_date

        # Contributor
        schema.contributor = Contributor(name=series_info.contributor)

        # References
        schema.references = Reference(
            pubmed_ids=series_info.pubmed_ids,
            bioproject_id=series_info.bioproject_id,
            sra_id=series_info.sra_id,
        )

        # Organisms
        schema.organisms = [
            Organism(name=org) for org in series_info.organism
        ] if series_info.organism else []

        # Platform
        if series_info.platforms:
            schema.platform = Platform(gpl_id=series_info.platforms[0])

        # Sample count
        schema.sample_count = len(series_info.samples)

    def _parse_samples(self, schema: ArchiveSchema, samples: list[GSMMetadata]) -> None:
        """Parse sample metadata into schema.

        Args:
            schema: ArchiveSchema to populate
            samples: List of GSMMetadata instances
        """
        schema.sample_count = len(samples)
        schema.samples = []

        tissues = set()
        diseases = set()

        for sample in samples:
            characteristics = SampleCharacteristics(
                tissue=sample.characteristics.get("tissue"),
                cell_type=sample.characteristics.get("cell type"),
                disease=sample.characteristics.get("disease"),
                treatment=sample.characteristics.get("treatment"),
                genotype=sample.characteristics.get("genotype"),
                sex=sample.characteristics.get("sex"),
                age=sample.characteristics.get("age"),
                stage=sample.characteristics.get("stage"),
            )

            sample_info = SampleInfo(
                gsm_id=sample.gsm_id,
                title=sample.title,
                source_name=sample.source_name,
                organism=sample.organism,
                organism_taxid=sample.organism_taxid,
                extraction_molecule=sample.extraction_molecule,
                library_strategy=sample.library_strategy,
                library_layout=sample.library_layout,
                instrument_model=sample.instrument_model,
                characteristics=characteristics,
            )

            schema.samples.append(sample_info)

            # Collect tissues and diseases
            # Try characteristics.tissue first, then source_name as fallback
            tissue_val = characteristics.tissue or (sample.source_name if sample.source_name else None)
            if tissue_val:
                tissues.add(tissue_val)
            if characteristics.disease:
                diseases.add(characteristics.disease)

        schema.tissues = sorted(list(tissues))
        schema.diseases = sorted(list(diseases))

    def _parse_files(self, schema: ArchiveSchema, files: list[GEOFile]) -> None:
        """Parse file info into schema.

        Args:
            schema: ArchiveSchema to populate
            files: List of GEOFile instances
        """
        schema.files = [
            FileInfo(
                filename=f.filename,
                type=FileType(f.type) if f.type in [e.value for e in FileType] else FileType.RAW,
                size_bytes=f.size,
                download_url=f.url,
            )
            for f in files
        ]

        # Update checksum status
        schema.checksum_status.total_files = len(files)

    def _detect_omics_type(
        self,
        schema: ArchiveSchema,
        samples: Optional[list[GSMMetadata]] = None,
    ) -> OmicsType:
        """Detect omics type from schema and samples.

        Args:
            schema: ArchiveSchema
            samples: Optional list of GSMMetadata

        Returns:
            Detected OmicsType
        """
        # Try series type first
        series_type_mapping = OmicsDetector.get_series_type_mapping()
        if schema.series_type in series_type_mapping:
            return series_type_mapping[schema.series_type]

        # Collect all text for detection
        texts = [schema.series_type, schema.summary, schema.overall_design]

        library_strategies = []
        if samples:
            for sample in samples:
                if sample.library_strategy:
                    library_strategies.append(sample.library_strategy)

        return self.omics_detector.detect(
            series_type=schema.series_type,
            summary=schema.summary,
            overall_design=schema.overall_design,
            library_strategy=" ".join(library_strategies),
        )

    def save(self, profile: ArchiveProfile, gse_id: str) -> Path:
        """Save archive to file.

        Args:
            profile: ArchiveProfile to save
            gse_id: GSE identifier

        Returns:
            Path to saved file
        """
        gse_dir = self.output_dir / gse_id
        filepath = gse_dir / self.ARCHIVE_FILENAME

        profile.to_json(filepath)
        return filepath

    def load(self, gse_id: str) -> Optional[ArchiveProfile]:
        """Load archive from file.

        Args:
            gse_id: GSE identifier

        Returns:
            ArchiveProfile or None if not found
        """
        filepath = self.output_dir / gse_id / self.ARCHIVE_FILENAME

        if not filepath.exists():
            return None

        return ArchiveProfile.from_json(filepath)

    def exists(self, gse_id: str) -> bool:
        """Check if archive exists.

        Args:
            gse_id: GSE identifier

        Returns:
            True if archive exists
        """
        filepath = self.output_dir / gse_id / self.ARCHIVE_FILENAME
        return filepath.exists()
