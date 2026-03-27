"""GSE Downloader - Enterprise-grade GEO data downloader.

Quick-start example::

    from gse_downloader import GSEDownloader, GEOQuery, ArchiveProfile

    # Download a dataset
    with GSEDownloader(output_dir="./data") as dl:
        files = dl.get_gse_files("GSE134520")
        results = dl.download_gse("GSE134520", files)

    # Search GEO
    geo = GEOQuery()
    hits = geo.search_series_detailed("lung cancer RNA-seq", retmax=5)

    # Load local archive
    profile = ArchiveProfile.from_json("./data/GSE134520/archive.json")
    print(profile.schema.sample_count)
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Core download engine
from gse_downloader.core.downloader import GSEDownloader, DownloadResult
from gse_downloader.core.state_manager import DownloadState, StateManager

# GEO query / metadata
from gse_downloader.parser.geo_query import GEOQuery, GSESeries, GEOFile
from gse_downloader.parser.metadata import MetadataParser, GSEMetadata, GSMMetadata
from gse_downloader.parser.omics_detector import OmicsDetector, OmicsType

# Archive
from gse_downloader.archive.profile import ArchiveProfile, ArchiveGenerator
from gse_downloader.archive.schema import ArchiveSchema, DownloadStatus

# Formatter
from gse_downloader.formatter.factory import FormatterFactory

# Config
from gse_downloader.utils.config import Config, load_config

__all__ = [
    # version
    "__version__",
    # downloader
    "GSEDownloader",
    "DownloadResult",
    # state
    "DownloadState",
    "StateManager",
    # geo query
    "GEOQuery",
    "GSESeries",
    "GEOFile",
    # metadata
    "MetadataParser",
    "GSEMetadata",
    "GSMMetadata",
    # omics
    "OmicsDetector",
    "OmicsType",
    # archive
    "ArchiveProfile",
    "ArchiveGenerator",
    "ArchiveSchema",
    "DownloadStatus",
    # formatter
    "FormatterFactory",
    # config
    "Config",
    "load_config",
]
