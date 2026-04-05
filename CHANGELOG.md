# Changelog

All notable changes to GSE Downloader are documented here.

---

## [1.1.1] - 2026-04-04

### Security
- **C1** `core/downloader.py`: Added safe path filtering before `tarfile.extractall()` to prevent Tar-slip (path traversal) attacks. Python ≥ 3.12 uses `filter="data"`; older versions manually reject members with `..` or absolute paths.

### Fixed
- **C2** `utils/config.py`: `Config.to_file()` now correctly uses `tomli_w.dump()` with a binary-mode file handle instead of the broken `tomli_w.dumps(..., f)` call.
- **H1** `pipeline/pipeline.py`: `_step_verify` now invokes `ChecksumVerifier` for real checksum+size validation, consistent with `verify` CLI semantics.
- **H2** `core/downloader.py`: `download_file()` now calls `_rate_limiter.acquire()` before every HTTP request, preventing accidental NCBI rate-limit breaches.
- **H3** `core/downloader.py`: Added `response.raise_for_status()` check; non-2xx responses now raise an error instead of silently writing an error page to disk.
- **H4** `parser/geo_query.py`: `validate_gse_id` esearch now includes the `email` parameter, consistent with NCBI usage policy and all other E-utilities calls in the module.
- **H5** `cli/commands.py`: Removed invalid `typer.context = {...}` assignment; config is now propagated via `ctx.ensure_object(dict)` / `ctx.obj` as per Typer documentation.
- **L1** `core/downloader.py`, `parser/geo_query.py`: Replaced placeholder `User-Agent` URL (`yourname/gse_downloader`) with the real repository URL `3H-Gene/gse-downloader`.
- **L4** `cli/commands.py`: `batch` command now opens the input file with explicit `encoding="utf-8"`, preventing potential encoding errors on non-UTF-8 Windows locales.

### Improved
- **M1** `core/downloader.py`: `max_workers` is now actually used — parallel downloads are executed via `ThreadPoolExecutor`, matching documented behaviour.
- **M3** `core/downloader.py`: `already_done` byte count is passed into `MultiFileProgress` so resumed downloads show accurate progress from the start.
- **M4** `core/downloader.py`: gzip decompression now uses streaming `iter_content` instead of loading the full response body into memory, preventing OOM on large files.
- **M6** `parser/geo_query.py`: `GEOFile.type` renamed to `GEOFile.file_type` to avoid shadowing the Python built-in `type`.
- **M7** `core/state_manager.py`: `DownloadInfo.files` now uses `field(default_factory=dict)` instead of a mutable default value.
- **M8** `core/checksum.py`: `calculate_batch` return type corrected to `dict[Path, str | None]` to reflect the possibility of `None` for failed paths.
- **M9** `cache/metadata_cache.py`: `get_metadata_cache()` now logs a warning when called with different `cache_dir`/`ttl_hours` after the singleton is already initialised.
- **L2** `core/downloader.py`: Added an explanatory comment to `_get_file_url` clarifying that the `filename` parameter is intentionally unused (extension point for future direct-file URL support).
- **L3** `parser/geo_query.py`: `search_series()` now documents that eSearch returns GDS internal UIDs (not GSE accession numbers) and recommends `search_series_detailed()` for verified accessions.

### Tests
- Added 6 new concurrency and edge-case tests for `ThreadPoolExecutor` download behaviour.
- **142 tests pass** (up from 136 in v1.1.0).

---

## [1.1.0] - 2026-03-31

### Added
- `core/input_schema.py`: Standardised input interface (`GseInput`, `DownloadOptions`, `parse_input`) compatible with geo-search-skill JSON output.
- `profiling/profiler.py`: `DataProfiler` — structured 2-D matrix profiling with basic statistics (missing rate, sparsity).
- `pipeline/pipeline.py`: `Pipeline` orchestration — download → verify → profile in one call.
- `cache/metadata_cache.py`: `MetadataCache` with configurable TTL (default 72 h) and file-backed persistence.
- `parser/geo_query.py`: `get_series_files_by_strategy()` — multi-path file discovery with optional SRA opt-in.
- CLI: `profile` and `pipeline` commands.

### Tests
- 136 tests pass (up from 108 in v1.0.0).

---

## [1.0.0] - 2026-03-30

### Added
- Core download engine with HTTP Range-based resume (`GSEDownloader`).
- GEO metadata parsing: SOFT, MINiML, Series Matrix formats.
- Checksum verification (MD5/SHA256).
- Omics type auto-detection (RNA-seq, scRNA-seq, ATAC-seq, ChIP-seq, Methylation, Microarray).
- Data formatting / normalisation per omics type.
- Archive management (`archive.json`) and dataset statistics (`stats` command).
- Full CLI via Typer: `download`, `status`, `archive`, `format`, `verify`, `batch`, `stats`, `search`, `info`, `init`.
- WorkBuddy Skill packaging (`SKILL.md`, `gse-downloader-skill.zip`).
- 108 tests.
