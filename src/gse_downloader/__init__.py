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

    # Run full pipeline: download → verify → profile
    from gse_downloader import Pipeline
    result = Pipeline().run("GSE134520")
    print(result.summary)

    # Profile an existing dataset
    from gse_downloader import DataProfiler
    pr = DataProfiler().profile("./data/GSE134520")
    print(pr.stats.sparsity)
"""

__version__ = "1.1.1"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Core download engine
from gse_downloader.core.downloader import GSEDownloader, DownloadResult
from gse_downloader.core.state_manager import DownloadState, StateManager
from gse_downloader.core.input_schema import GseInput, DownloadOptions, parse_input

# GEO query / metadata
from gse_downloader.parser.geo_query import GEOQuery, GSESeries, GEOFile
from gse_downloader.parser.metadata import MetadataParser, GSEMetadata, GSMMetadata
from gse_downloader.parser.omics_detector import OmicsDetector, OmicsType

# Archive
from gse_downloader.archive.profile import ArchiveProfile, ArchiveGenerator
from gse_downloader.archive.schema import ArchiveSchema, DownloadStatus

# Formatter
from gse_downloader.formatter.factory import FormatterFactory

# Profiling
from gse_downloader.profiling.profiler import DataProfiler, ProfilingResult

# Pipeline
from gse_downloader.pipeline.pipeline import Pipeline, PipelineResult

# Cache
from gse_downloader.cache.metadata_cache import MetadataCache, get_metadata_cache

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
    # input schema
    "GseInput",
    "DownloadOptions",
    "parse_input",
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
    # profiling
    "DataProfiler",
    "ProfilingResult",
    # pipeline
    "Pipeline",
    "PipelineResult",
    # cache
    "MetadataCache",
    "get_metadata_cache",
    # config
    "Config",
    "load_config",
]

