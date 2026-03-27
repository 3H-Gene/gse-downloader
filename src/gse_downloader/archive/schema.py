"""Archive schema definition for GSE Downloader.

This module defines the JSON schema for the archive.json file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from gse_downloader.parser.omics_detector import OmicsType


class ArchiveVersion(str, Enum):
    """Archive version."""

    V1 = "1.0.0"
    V2 = "2.0.0"


class DownloadStatus(str, Enum):
    """Download status."""

    NOT_STARTED = "not_started"
    INCOMPLETE = "incomplete"
    COMPLETED = "completed"
    INVALID = "invalid"


class FileType(str, Enum):
    """File type."""

    RAW = "raw"
    PROCESSED = "processed"
    SUPPLEMENTARY = "supplementary"


class ChecksumStatus(str, Enum):
    """Checksum verification status."""

    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"


@dataclass
class Contributor:
    """Contributor information."""

    name: str = ""
    email: str = ""
    institution: str = ""
    address: str = ""


@dataclass
class Reference:
    """Reference information."""

    pubmed_ids: list[str] = field(default_factory=list)
    bioproject_id: Optional[str] = None
    sra_id: Optional[str] = None


@dataclass
class Organism:
    """Organism information."""

    name: str = ""
    taxid: Optional[int] = None


@dataclass
class Platform:
    """Platform information."""

    gpl_id: str = ""
    title: str = ""
    technology: str = ""
    manufacturer: str = ""


@dataclass
class SampleCharacteristics:
    """Sample characteristics."""

    tissue: Optional[str] = None
    cell_type: Optional[str] = None
    disease: Optional[str] = None
    treatment: Optional[str] = None
    genotype: Optional[str] = None
    sex: Optional[str] = None
    age: Optional[str] = None
    stage: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class SampleInfo:
    """Sample information."""

    gsm_id: str
    title: str = ""
    source_name: str = ""
    organism: str = ""
    organism_taxid: Optional[int] = None
    extraction_molecule: str = ""
    library_strategy: str = ""
    library_layout: str = ""
    instrument_model: str = ""
    characteristics: SampleCharacteristics = field(default_factory=SampleCharacteristics)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "gsm_id": self.gsm_id,
            "title": self.title,
            "source_name": self.source_name,
            "organism": self.organism,
            "organism_taxid": self.organism_taxid,
            "extraction_molecule": self.extraction_molecule,
            "library_strategy": self.library_strategy,
            "library_layout": self.library_layout,
            "instrument_model": self.instrument_model,
            "characteristics": self.characteristics.to_dict(),
        }


@dataclass
class FileInfo:
    """File information."""

    filename: str
    type: FileType = FileType.RAW
    size_bytes: int = 0
    md5: Optional[str] = None
    sha256: Optional[str] = None
    verified: bool = False
    download_url: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "filename": self.filename,
            "type": self.type.value,
            "size_bytes": self.size_bytes,
            "md5": self.md5,
            "sha256": self.sha256,
            "verified": self.verified,
            "download_url": self.download_url,
        }


@dataclass
class DownloadInfo:
    """Download information."""

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: int = 0
    retries: int = 0
    downloaded_bytes: int = 0
    total_bytes: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "retries": self.retries,
            "downloaded_bytes": self.downloaded_bytes,
            "total_bytes": self.total_bytes,
        }


@dataclass
class ChecksumStatusInfo:
    """Checksum status information."""

    overall: ChecksumStatus = ChecksumStatus.PENDING
    algorithm: str = "md5"
    verified_files: int = 0
    total_files: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "overall": self.overall.value,
            "algorithm": self.algorithm,
            "verified_files": self.verified_files,
            "total_files": self.total_files,
        }


@dataclass
class ArchiveSchema:
    """Complete archive schema."""

    gse_id: str
    archive_version: str = ArchiveVersion.V2.value
    status: DownloadStatus = DownloadStatus.NOT_STARTED

    # Metadata
    title: str = ""
    summary: str = ""
    overall_design: str = ""
    series_type: str = ""
    keywords: list[str] = field(default_factory=list)
    funding: list[str] = field(default_factory=list)
    submission_date: Optional[str] = None
    last_update_date: Optional[str] = None

    # Contributor
    contributor: Contributor = field(default_factory=Contributor)

    # References
    references: Reference = field(default_factory=Reference)

    # Organisms
    organisms: list[Organism] = field(default_factory=list)

    # Omics type
    omics_type: OmicsType = OmicsType.OTHER

    # Platform
    platform: Optional[Platform] = None

    # Samples
    sample_count: int = 0
    samples: list[SampleInfo] = field(default_factory=list)

    # Extracted info
    tissues: list[str] = field(default_factory=list)
    diseases: list[str] = field(default_factory=list)

    # Files
    files: list[FileInfo] = field(default_factory=list)

    # Download info
    download_info: DownloadInfo = field(default_factory=DownloadInfo)

    # Checksum status
    checksum_status: ChecksumStatusInfo = field(default_factory=ChecksumStatusInfo)

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "gse_id": self.gse_id,
            "archive_version": self.archive_version,
            "status": self.status.value,
            "metadata": {
                "title": self.title,
                "summary": self.summary,
                "overall_design": self.overall_design,
                "series_type": self.series_type,
                "keywords": self.keywords,
                "funding": self.funding,
                "submission_date": self.submission_date,
                "last_update_date": self.last_update_date,
            },
            "contributor": {
                "name": self.contributor.name,
                "email": self.contributor.email,
                "institution": self.contributor.institution,
                "address": self.contributor.address,
            },
            "references": {
                "pubmed_ids": self.references.pubmed_ids,
                "bioproject_id": self.references.bioproject_id,
                "sra_id": self.references.sra_id,
            },
            "organisms": [
                {"name": o.name, "taxid": o.taxid} for o in self.organisms
            ],
            "omics_type": self.omics_type.value if isinstance(self.omics_type, OmicsType) else self.omics_type,
            "platform": self.platform.__dict__ if self.platform else None,
            "sample_count": self.sample_count,
            "samples": [s.to_dict() for s in self.samples],
            "tissues": self.tissues,
            "diseases": self.diseases,
            "files": [f.to_dict() for f in self.files],
            "download_info": self.download_info.to_dict(),
            "checksum_status": self.checksum_status.to_dict(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveSchema":
        """Create from dictionary."""
        # Parse contributor
        contributor_data = data.get("contributor", {})
        contributor = Contributor(
            name=contributor_data.get("name", ""),
            email=contributor_data.get("email", ""),
            institution=contributor_data.get("institution", ""),
            address=contributor_data.get("address", ""),
        )

        # Parse references
        ref_data = data.get("references", {})
        references = Reference(
            pubmed_ids=ref_data.get("pubmed_ids", []),
            bioproject_id=ref_data.get("bioproject_id"),
            sra_id=ref_data.get("sra_id"),
        )

        # Parse organisms
        organisms = [
            Organism(name=o.get("name", ""), taxid=o.get("taxid"))
            for o in data.get("organisms", [])
        ]

        # Parse platform
        platform_data = data.get("platform")
        platform = Platform(**platform_data) if platform_data else None

        # Parse samples
        samples = [SampleInfo(**s) for s in data.get("samples", [])]

        # Parse files
        files = [FileInfo(**f) for f in data.get("files", [])]

        # Parse download info
        download_info_data = data.get("download_info", {})
        download_info = DownloadInfo(
            retries=download_info_data.get("retries", 0),
            downloaded_bytes=download_info_data.get("downloaded_bytes", 0),
            total_bytes=download_info_data.get("total_bytes", 0),
        )

        # Parse checksum status
        checksum_data = data.get("checksum_status", {})
        checksum_status = ChecksumStatusInfo(
            algorithm=checksum_data.get("algorithm", "md5"),
            verified_files=checksum_data.get("verified_files", 0),
            total_files=checksum_data.get("total_files", 0),
        )

        # Parse timestamps
        created_at = None
        if data.get("created_at"):
            created_at = datetime.fromisoformat(data["created_at"])

        updated_at = None
        if data.get("updated_at"):
            updated_at = datetime.fromisoformat(data["updated_at"])

        # Parse omics type
        omics_type = data.get("omics_type", "Other")
        if isinstance(omics_type, str):
            try:
                omics_type = OmicsType(omics_type)
            except ValueError:
                omics_type = OmicsType.OTHER

        metadata = data.get("metadata", {})

        return cls(
            gse_id=data.get("gse_id", ""),
            archive_version=data.get("archive_version", ArchiveVersion.V2.value),
            status=DownloadStatus(data.get("status", "not_started")),
            title=metadata.get("title", ""),
            summary=metadata.get("summary", ""),
            overall_design=metadata.get("overall_design", ""),
            series_type=metadata.get("series_type", ""),
            keywords=metadata.get("keywords", []),
            funding=metadata.get("funding", []),
            submission_date=metadata.get("submission_date"),
            last_update_date=metadata.get("last_update_date"),
            contributor=contributor,
            references=references,
            organisms=organisms,
            omics_type=omics_type,
            platform=platform,
            sample_count=data.get("sample_count", 0),
            samples=samples,
            tissues=data.get("tissues", []),
            diseases=data.get("diseases", []),
            files=files,
            download_info=download_info,
            checksum_status=checksum_status,
            created_at=created_at,
            updated_at=updated_at,
        )
