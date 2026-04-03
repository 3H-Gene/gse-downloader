"""Statistics module for GSE Downloader.

This module handles generating statistics for downloaded datasets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from gse_downloader.archive.profile import ArchiveProfile
from gse_downloader.utils.logger import get_logger

logger = get_logger("stats")


@dataclass
class DatasetStats:
    """Statistics for a single dataset."""

    gse_id: str
    title: str = ""
    omics_type: str = ""
    sample_count: int = 0
    organisms: list[str] = field(default_factory=list)
    tissues: list[str] = field(default_factory=list)
    status: str = ""


@dataclass
class SummaryStats:
    """Summary statistics for all datasets."""

    total_datasets: int = 0
    total_samples: int = 0
    by_organism: dict[str, int] = field(default_factory=dict)
    by_omics_type: dict[str, int] = field(default_factory=dict)
    # tissue → {"datasets": N, "samples": M}
    by_tissue: dict[str, dict] = field(default_factory=dict)
    by_status: dict[str, int] = field(default_factory=dict)


class Statistics:
    """Handles statistics generation for downloaded datasets."""

    def __init__(self, data_dir: Path):
        """Initialize statistics.

        Args:
            data_dir: Directory containing downloaded datasets
        """
        self.data_dir = Path(data_dir)
        logger.debug(f"Statistics initialized for {data_dir}")

    def scan_archives(self) -> list[ArchiveProfile]:
        """Scan data directory for archives.

        Returns:
            List of ArchiveProfile instances
        """
        archives = []

        if not self.data_dir.exists():
            logger.warning(f"Data directory not found: {self.data_dir}")
            return archives

        for gse_dir in self.data_dir.iterdir():
            if gse_dir.is_dir():
                archive_file = gse_dir / "archive.json"
                if archive_file.exists():
                    try:
                        profile = ArchiveProfile.from_json(archive_file)
                        archives.append(profile)
                    except Exception as e:
                        logger.warning(f"Failed to load {archive_file}: {e}")

        logger.info(f"Found {len(archives)} archives")
        return archives

    def get_summary(self) -> SummaryStats:
        """Get summary statistics.

        Returns:
            SummaryStats instance
        """
        archives = self.scan_archives()

        summary = SummaryStats(
            total_datasets=len(archives),
            total_samples=sum(p.schema.sample_count for p in archives),
        )

        for profile in archives:
            schema = profile.schema

            # By organism
            for org in schema.organisms:
                name = org.name or "Unknown"
                summary.by_organism[name] = summary.by_organism.get(name, 0) + 1

            # By omics type
            omics = str(schema.omics_type.value if hasattr(schema.omics_type, 'value') else schema.omics_type)
            summary.by_omics_type[omics] = summary.by_omics_type.get(omics, 0) + 1

            # By tissue / organ
            # Collect from sample characteristics first, fall back to schema.tissues
            seen: set[str] = set()
            tissue_labels: list[str] = []
            for s in schema.samples:
                t = (s.characteristics.tissue if s.characteristics else None) or ""
                t = t.strip()
                if t and t.lower() not in seen:
                    seen.add(t.lower())
                    tissue_labels.append(t)
            for t in schema.tissues:
                t = t.strip()
                if t and t.lower() not in seen:
                    seen.add(t.lower())
                    tissue_labels.append(t)
            for tissue in tissue_labels:
                if tissue not in summary.by_tissue:
                    summary.by_tissue[tissue] = {"datasets": 0, "samples": 0}
                summary.by_tissue[tissue]["datasets"] += 1
                summary.by_tissue[tissue]["samples"] += schema.sample_count

            # By status
            status = schema.status.value
            summary.by_status[status] = summary.by_status.get(status, 0) + 1

        return summary

    def get_dataset_stats(self, gse_id: str) -> Optional[DatasetStats]:
        """Get statistics for a specific dataset.

        Args:
            gse_id: GSE identifier

        Returns:
            DatasetStats or None
        """
        archive_file = self.data_dir / gse_id / "archive.json"

        if not archive_file.exists():
            logger.warning(f"Archive not found for {gse_id}")
            return None

        try:
            profile = ArchiveProfile.from_json(archive_file)
            schema = profile.schema

            return DatasetStats(
                gse_id=schema.gse_id,
                title=schema.title,
                omics_type=str(schema.omics_type.value if hasattr(schema.omics_type, 'value') else schema.omics_type),
                sample_count=schema.sample_count,
                organisms=[o.name for o in schema.organisms],
                tissues=schema.tissues,
                status=schema.status.value,
            )

        except Exception as e:
            logger.error(f"Failed to get stats for {gse_id}: {e}")
            return None

    def get_by_organism(self) -> dict[str, list[DatasetStats]]:
        """Group datasets by organism.

        Returns:
            Dictionary mapping organism to list of DatasetStats
        """
        archives = self.scan_archives()
        by_organism: dict[str, list[DatasetStats]] = {}

        for profile in archives:
            schema = profile.schema

            stats = DatasetStats(
                gse_id=schema.gse_id,
                title=schema.title,
                omics_type=str(schema.omics_type.value if hasattr(schema.omics_type, 'value') else schema.omics_type),
                sample_count=schema.sample_count,
                organisms=[o.name for o in schema.organisms],
                tissues=schema.tissues,
                status=schema.status.value,
            )

            for org in schema.organisms:
                name = org.name or "Unknown"
                if name not in by_organism:
                    by_organism[name] = []
                by_organism[name].append(stats)

        return by_organism

    def get_by_omics_type(self) -> dict[str, list[DatasetStats]]:
        """Group datasets by omics type.

        Returns:
            Dictionary mapping omics type to list of DatasetStats
        """
        archives = self.scan_archives()
        by_omics: dict[str, list[DatasetStats]] = {}

        for profile in archives:
            schema = profile.schema

            stats = DatasetStats(
                gse_id=schema.gse_id,
                title=schema.title,
                omics_type=str(schema.omics_type.value if hasattr(schema.omics_type, 'value') else schema.omics_type),
                sample_count=schema.sample_count,
                organisms=[o.name for o in schema.organisms],
                tissues=schema.tissues,
                status=schema.status.value,
            )

            omics = stats.omics_type
            if omics not in by_omics:
                by_omics[omics] = []
            by_omics[omics].append(stats)

        return by_omics

    def to_dict(self) -> dict:
        """Convert statistics to dictionary.

        Returns:
            Dictionary representation
        """
        summary = self.get_summary()

        return {
            "total_datasets": summary.total_datasets,
            "total_samples": summary.total_samples,
            "by_organism": summary.by_organism,
            "by_omics_type": summary.by_omics_type,
            "by_tissue": {k: v for k, v in sorted(
                summary.by_tissue.items(), key=lambda x: x[1]["datasets"], reverse=True
            )[:20]},
            "by_status": summary.by_status,
        }
