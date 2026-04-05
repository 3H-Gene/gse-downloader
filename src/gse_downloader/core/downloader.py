"""Download engine for GSE Downloader.

This module handles the core downloading functionality with support for:
- Resume capability (using HTTP Range headers)
- Checksum verification (MD5/SHA256)
- Progress tracking
- Rate limiting
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from gse_downloader.core.state_manager import DownloadInfo, DownloadState, FileState, StateManager
from gse_downloader.utils.logger import get_logger
from gse_downloader.utils.progress import DownloadProgress, MultiFileProgress
from gse_downloader.utils.rate_limiter import NoopRateLimiter, RateLimiter

logger = get_logger("downloader")


@dataclass
class DownloadResult:
    """Result of a download operation."""

    filename: str
    success: bool
    filepath: Optional[Path] = None
    size: int = 0
    duration: float = 0.0
    md5: Optional[str] = None
    error: Optional[str] = None
    avg_speed: float = 0.0  # bytes/s average download speed


class GSEDownloader:
    """Main downloader class for GSE datasets."""

    # GEO FTP base URL
    FTP_BASE_URL = "https://www.ncbi.nlm.nih.gov/geo/download/"
    # Alternative FTP URL
    FTP_ALT_URL = "ftp://ftp.ncbi.nlm.nih.gov/geo/"

    # Default headers
    DEFAULT_HEADERS = {
        "User-Agent": "GSE-Downloader/1.0 (https://github.com/3H-Gene/gse-downloader)",
        "Accept": "*/*",
    }

    def __init__(
        self,
        output_dir: Path | str = "./gse_data",
        max_workers: int = 4,
        timeout: int = 300,
        verify_ssl: bool = True,
        retry_times: int = 3,
        auto_resume: bool = True,
        checksum_algorithm: str = "md5",
        show_progress: bool = True,
        rate_limit: float = 2.0,
    ):
        """Initialize downloader.

        Args:
            output_dir: Base output directory
            max_workers: Maximum parallel file downloads (``download_gse`` uses a thread pool)
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
            retry_times: Number of retry attempts
            auto_resume: Whether to auto-resume incomplete downloads
            checksum_algorithm: Checksum algorithm (md5 or sha256)
            show_progress: Whether to show progress bars
            rate_limit: Max HTTP requests per second (0 = no limit).
                NCBI recommends ≤ 3/s without API key.
        """
        self.output_dir = Path(output_dir)
        self.max_workers = max_workers
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.retry_times = retry_times
        self.auto_resume = auto_resume
        self.checksum_algorithm = checksum_algorithm
        self.show_progress = show_progress

        self._thread_local = threading.local()

        # Rate limiter (token bucket)
        if rate_limit and rate_limit > 0:
            self._rate_limiter = RateLimiter(requests_per_second=rate_limit)
        else:
            self._rate_limiter = NoopRateLimiter()

        # Main-thread HTTP session (worker threads each get their own via ``session`` property)
        self._main_session = self._create_session()

        logger.info(
            f"Initialized GSEDownloader (output_dir={self.output_dir}, "
            f"rate_limit={rate_limit}/s)"
        )

    @property
    def session(self) -> requests.Session:
        """HTTP session for the current thread (``requests.Session`` is not thread-safe)."""
        if threading.current_thread() is threading.main_thread():
            return self._main_session
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = self._create_session()
        return self._thread_local.session

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy.

        Returns:
            Configured requests session
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.retry_times,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.DEFAULT_HEADERS)
        return session

    def _get_file_url(self, gse_id: str, filename: str) -> str:
        """Get download URL for a file.

        Args:
            gse_id: GSE identifier
            filename: Filename (reserved for future per-file URL construction)

        Returns:
            Full download URL
        """
        # NOTE: `filename` is intentionally unused for now.
        # The GEO download endpoint returns a tar bundle for the whole dataset.
        # Individual file URLs would require FTP listing logic; kept here as an
        # extension point for future direct-file download support.
        return f"{self.FTP_BASE_URL}?acc={gse_id}&format=file"

    def download_file(
        self,
        gse_id: str,
        filename: str,
        output_dir: Path,
        resume: bool = True,
    ) -> DownloadResult:
        """Download a single file.

        Args:
            gse_id: GSE identifier
            filename: Filename to download
            output_dir: Output directory
            resume: Whether to resume if file partially exists

        Returns:
            DownloadResult instance
        """
        start_time = time.time()
        filepath = output_dir / filename

        # Build URL - format=file is required for direct file download
        url = f"{self.FTP_BASE_URL}?acc={gse_id}&format=file&filename={filename}"

        logger.debug(f"Downloading {filename} from {url}")

        try:
            # Check if file exists and get its size
            headers = {}
            existing_size = 0

            if resume and filepath.exists():
                existing_size = filepath.stat().st_size
                if existing_size > 0:
                    headers["Range"] = f"bytes={existing_size}-"
                    logger.debug(f"Resuming {filename} from byte {existing_size}")

            # Rate-limit before hitting the server (same as download_file_with_url)
            self._rate_limiter.acquire()

            # Make request
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.timeout,
                stream=True,
                verify=self.verify_ssl,
            )

            # Handle response
            if response.status_code == 404:
                return DownloadResult(
                    filename=filename,
                    success=False,
                    error=f"File not found: {filename}",
                )

            if response.status_code == 416:
                # Range request range not satisfiable, file is complete
                logger.debug(f"File already complete: {filename}")
                return DownloadResult(
                    filename=filename,
                    success=True,
                    filepath=filepath,
                    size=existing_size,
                    duration=time.time() - start_time,
                )

            # Reject other non-success HTTP statuses (avoid silent 5xx/403 bodies)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error downloading {filename}: {e}")
                return DownloadResult(
                    filename=filename,
                    success=False,
                    error=f"HTTP {response.status_code}: {filename}",
                )

            # Check for resume
            content_range = response.headers.get("Content-Range")
            if content_range:
                total_size = int(content_range.split("/")[-1])
            else:
                total_size = int(response.headers.get("Content-Length", 0)) + existing_size

            # Update headers for subsequent chunks
            if resume and existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"
            else:
                existing_size = 0

            # Download file
            mode = "ab" if existing_size > 0 else "wb"
            with open(filepath, mode) as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Calculate final size
            final_size = filepath.stat().st_size

            # Calculate checksum if file is complete
            md5_hash = None
            if final_size == total_size or not content_range:
                md5_hash = self._calculate_checksum(filepath)

            return DownloadResult(
                filename=filename,
                success=True,
                filepath=filepath,
                size=final_size,
                duration=time.time() - start_time,
                md5=md5_hash,
            )

        except requests.exceptions.Timeout:
            return DownloadResult(
                filename=filename,
                success=False,
                error="Request timeout",
            )
        except requests.exceptions.RequestException as e:
            return DownloadResult(
                filename=filename,
                success=False,
                error=str(e),
            )
        except Exception as e:
            return DownloadResult(
                filename=filename,
                success=False,
                error=f"Unexpected error: {e}",
            )

    def download_file_with_url(
        self,
        filename: str,
        url: Optional[str],
        output_dir: Path,
        resume: bool = True,
        needs_gzip: bool = False,
        is_archive: bool = False,
        multi_progress: Optional["MultiFileProgress"] = None,
    ) -> DownloadResult:
        """Download a file using a provided URL.

        Args:
            filename: Filename to save as
            url: Full download URL (if None, constructs from filename)
            output_dir: Output directory
            resume: Whether to resume if file partially exists
            needs_gzip: Wrap plain-text response in gzip
            is_archive: Treat as TAR/TGZ – download to .tmp then extract
            multi_progress: Shared MultiFileProgress context (optional).
                When provided this method reports per-chunk progress to it
                instead of creating a standalone progress bar.

        Returns:
            DownloadResult instance
        """
        import gzip

        CHUNK_SIZE = 65536  # 64 KB – larger chunks = faster I/O + smoother speed readings

        start_time = time.time()
        filepath = output_dir / filename

        # Use provided URL or construct one
        if url is None:
            url = f"{self.FTP_BASE_URL}?acc={filename.split('_')[0]}&format=file&filename={filename}"

        logger.debug(f"Downloading {filename} from {url}")

        try:
            # ── Check local partial file ───────────────────────────────────
            headers: dict = {}
            existing_size = 0

            if resume and filepath.exists():
                existing_size = filepath.stat().st_size
                if existing_size > 0:
                    headers["Range"] = f"bytes={existing_size}-"
                    logger.debug(f"Resuming {filename} from byte {existing_size:,}")

            # ── Rate-limit before hitting the server ───────────────────────
            self._rate_limiter.acquire()

            # ── HTTP request ───────────────────────────────────────────────
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.timeout,
                stream=True,
                verify=self.verify_ssl,
            )

            # Check status
            if response.status_code == 404:
                return DownloadResult(filename=filename, success=False, error=f"File not found: {filename}")
            if response.status_code == 416:
                # Range not satisfiable – file already complete
                logger.debug(f"File already complete: {filename}")
                return DownloadResult(
                    filename=filename, success=True, filepath=filepath,
                    size=existing_size, duration=time.time() - start_time,
                )

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error downloading {filename}: {e}")
                if multi_progress:
                    multi_progress.finish_file(filename, success=False)
                return DownloadResult(
                    filename=filename,
                    success=False,
                    error=f"HTTP {response.status_code}: {filename}",
                )

            content_type = response.headers.get("Content-Type", "")

            # ── Determine total size for progress bar ──────────────────────
            content_range = response.headers.get("Content-Range")
            if content_range:
                # "bytes 123-456/789"  → total = 789
                try:
                    total_size = int(content_range.split("/")[-1])
                except (ValueError, IndexError):
                    total_size = 0
            else:
                cl = response.headers.get("Content-Length", "0")
                try:
                    total_size = int(cl) + existing_size
                except ValueError:
                    total_size = 0

            # ── Helper: stream to file with progress updates ───────────────
            def _stream_to_file(dest: Path, mode: str, append_bytes: int = 0) -> int:
                """Write streamed response to *dest*, returning bytes written."""
                bytes_written = 0
                with open(dest, mode) as fh:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            fh.write(chunk)
                            n = len(chunk)
                            bytes_written += n
                            if multi_progress:
                                multi_progress.advance(n)
                return bytes_written

            # ── Branch: needs_gzip ─────────────────────────────────────────
            if needs_gzip and content_type not in ("application/x-gzip", "application/gzip"):
                # SOFT query API may return plain text – wrap in gzip (streamed, not buffered in memory)
                if multi_progress:
                    multi_progress.start_file(filename, total_size, existing_size)
                with gzip.open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            if multi_progress:
                                multi_progress.advance(len(chunk))
                final_size = filepath.stat().st_size
                md5_hash = self._calculate_checksum(filepath)
                if multi_progress:
                    multi_progress.finish_file(filename, success=True, size=final_size)

            # ── Branch: is_archive (TAR / TGZ) ────────────────────────────
            elif is_archive:
                temp_filepath = output_dir / f"{filename}.tmp"
                if multi_progress:
                    multi_progress.start_file(filename, total_size, existing_size)
                try:
                    mode = "ab" if existing_size > 0 else "wb"
                    _stream_to_file(temp_filepath, mode, existing_size)
                    self._extract_archive(temp_filepath, output_dir)
                    final_size = sum(f.stat().st_size for f in output_dir.glob("*") if f.is_file())
                    md5_hash = None
                    filepath = output_dir
                    if multi_progress:
                        multi_progress.finish_file(filename, success=True, size=final_size)
                finally:
                    temp_filepath.unlink(missing_ok=True)

            # ── Branch: normal binary download ─────────────────────────────
            else:
                if not content_range:
                    existing_size = 0  # server sent full file, reset offset
                if multi_progress:
                    multi_progress.start_file(filename, total_size, existing_size)
                mode = "ab" if existing_size > 0 else "wb"
                bytes_written = _stream_to_file(filepath, mode, existing_size)
                final_size = filepath.stat().st_size
                md5_hash = None
                if final_size >= total_size > 0 or not content_range:
                    md5_hash = self._calculate_checksum(filepath)
                if multi_progress:
                    multi_progress.finish_file(filename, success=True, size=final_size)

            # ── Speed calculation ──────────────────────────────────────────
            elapsed = time.time() - start_time
            avg_speed = final_size / elapsed if elapsed > 0 else 0.0

            return DownloadResult(
                filename=filename,
                success=True,
                filepath=filepath,
                size=final_size,
                duration=elapsed,
                md5=md5_hash,
                avg_speed=avg_speed,
            )

        except requests.exceptions.Timeout:
            if multi_progress:
                multi_progress.finish_file(filename, success=False)
            return DownloadResult(filename=filename, success=False, error="Request timeout")
        except requests.exceptions.RequestException as e:
            if multi_progress:
                multi_progress.finish_file(filename, success=False)
            return DownloadResult(filename=filename, success=False, error=str(e))
        except Exception as e:
            if multi_progress:
                multi_progress.finish_file(filename, success=False)
            return DownloadResult(filename=filename, success=False, error=f"Unexpected error: {e}")

    def _calculate_checksum(self, filepath: Path, algorithm: Optional[str] = None) -> str:
        """Calculate file checksum.

        Args:
            filepath: Path to file
            algorithm: Checksum algorithm (md5 or sha256), uses instance default if None

        Returns:
            Checksum hex string
        """
        if algorithm is None:
            algorithm = self.checksum_algorithm

        hash_func = hashlib.md5() if algorithm == "md5" else hashlib.sha256()

        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_func.update(chunk)

        return hash_func.hexdigest()

    def _extract_archive(self, archive_path: Path, output_dir: Path) -> list[Path]:
        """Extract a TAR/TGZ archive.

        Args:
            archive_path: Path to archive file
            output_dir: Directory to extract to

        Returns:
            List of extracted file paths
        """
        import tarfile

        extracted_files = []
        output_dir = output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        def _is_unsafe_member(member: tarfile.TarInfo) -> bool:
            """Reject path traversal / absolute paths (Python < 3.12 has no filter=)."""
            name = member.name
            if name.startswith(("/", "\\")) or ".." in Path(name).parts:
                return True
            if ".." in name or name.startswith(".."):
                return True
            return False

        def _safe_extractall(tar: tarfile.TarFile) -> None:
            if sys.version_info >= (3, 12):
                tar.extractall(output_dir, filter="data")
            else:
                members = [m for m in tar.getmembers() if not _is_unsafe_member(m)]
                tar.extractall(output_dir, members=members)

        def _listed_file_paths(tar: tarfile.TarFile) -> list[Path]:
            """Paths for regular files whose resolved location stays under output_dir."""
            paths: list[Path] = []
            for m in tar.getmembers():
                if not m.isfile():
                    continue
                dest = (output_dir / m.name).resolve()
                try:
                    dest.relative_to(output_dir)
                except ValueError:
                    logger.warning(f"Skipping member outside output dir: {m.name!r}")
                    continue
                paths.append(dest)
            return paths

        try:
            if archive_path.suffix == ".gz" or str(archive_path).endswith(".tgz"):
                # TGZ file
                with tarfile.open(archive_path, "r:gz") as tar:
                    _safe_extractall(tar)
                    extracted_files = _listed_file_paths(tar)
            else:
                # Regular TAR file
                with tarfile.open(archive_path, "r") as tar:
                    _safe_extractall(tar)
                    extracted_files = _listed_file_paths(tar)

            logger.info(f"Extracted {len(extracted_files)} files from {archive_path.name}")

        except Exception as e:
            logger.error(f"Failed to extract archive {archive_path}: {e}")
            raise

        return extracted_files

    def verify_file(
        self,
        filepath: Path,
        expected_checksum: Optional[str] = None,
        algorithm: Optional[str] = None,
    ) -> bool:
        """Verify file checksum.

        Args:
            filepath: Path to file
            expected_checksum: Expected checksum value
            algorithm: Checksum algorithm

        Returns:
            True if verified, False otherwise
        """
        if not filepath.exists():
            logger.error(f"File not found for verification: {filepath}")
            return False

        if expected_checksum:
            actual = self._calculate_checksum(filepath, algorithm)
            verified = actual.lower() == expected_checksum.lower()
            if verified:
                logger.info(f"Checksum verified: {filepath.name}")
            else:
                logger.error(f"Checksum mismatch: {filepath.name}")
            return verified
        else:
            # Just calculate and return (no verification)
            checksum = self._calculate_checksum(filepath, algorithm)
            logger.debug(f"Checksum for {filepath.name}: {checksum}")
            return True

    def _get_remote_size(self, url: str) -> int:
        """Do a lightweight HEAD request to find the remote file size.

        Returns 0 if the server doesn't respond or doesn't send Content-Length.
        """
        try:
            self._rate_limiter.acquire()
            resp = self.session.head(url, timeout=10, allow_redirects=True, verify=self.verify_ssl)
            cl = resp.headers.get("Content-Length", "0")
            return int(cl)
        except Exception:
            return 0

    def download_gse(
        self,
        gse_id: str,
        files: list[dict],
        config_path: Optional[Path] = None,
    ) -> dict[str, DownloadResult]:
        """Download all files for a GSE dataset.

        Args:
            gse_id: GSE identifier
            files: List of file info dicts with 'filename' and 'url' keys
            config_path: Path to configuration file

        Returns:
            Dictionary mapping filename to DownloadResult
        """
        from gse_downloader.utils.config import load_config

        # Load config
        if config_path:
            config = load_config(config_path)
            output_dir = config.get_output_dir(gse_id)
            self.auto_resume = config.download.auto_resume
        else:
            output_dir = self.output_dir / gse_id

        output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize state manager
        state_manager = StateManager(output_dir)
        info = state_manager.load_state()

        # Update state with file list
        if info.status == DownloadState.NOT_STARTED:
            info.status = DownloadState.INCOMPLETE
            info.started_at = datetime.now()
            info.total_files = len(files)

        # Update file states and build URL map
        file_urls = {}
        for file_info in files:
            filename = file_info["filename"]
            file_urls[filename] = file_info.get("url")
            if filename not in info.files:
                info.files[filename] = FileState(
                    filename=filename,
                    size_bytes=0,
                    downloaded_bytes=0,
                )

        state_manager.save_state(info)

        # ── Pre-fetch remote sizes (HEAD requests) so progress bars show totals ──
        logger.info("Querying remote file sizes...")
        remote_sizes: dict[str, int] = {}
        for file_info in files:
            fname = file_info["filename"]
            furl = file_info.get("url")
            if furl:
                sz = self._get_remote_size(furl)
                remote_sizes[fname] = sz
                if sz:
                    info.files[fname].size_bytes = sz
            else:
                remote_sizes[fname] = 0

        total_remote = sum(remote_sizes.values())

        # Bytes already on disk (resume / partial) — seed overall progress bar
        already_done = sum(
            (Path(output_dir) / fn).stat().st_size
            for fn in remote_sizes
            if (Path(output_dir) / fn).exists()
        )
        total_for_progress = max(total_remote, 1)

        results: dict[str, DownloadResult] = {}

        skipped: list[tuple[dict, Path]] = []
        download_queue: list[dict] = []
        for file_info in files:
            filename = file_info["filename"]
            filepath = output_dir / filename
            if self.auto_resume and filepath.exists():
                file_state = info.files.get(filename)
                if file_state and file_state.verified:
                    skipped.append((file_info, filepath))
                    continue
            download_queue.append(file_info)

        progress_lock = (
            threading.Lock()
            if len(download_queue) > 1 and self.max_workers > 1
            else None
        )

        def _apply_download_result(filename: str, result: DownloadResult) -> None:
            if result.success:
                state_manager.update_file_state(
                    info,
                    filename,
                    result.size,
                    result.md5,
                    verified=True,
                )
            else:
                logger.error(f"Failed to download {filename}: {result.error}")
                info.last_error = result.error
                state_manager.save_state(info)

        with MultiFileProgress(
            len(files),
            total_for_progress,
            self.show_progress,
            initial_completed_bytes=already_done,
            lock=progress_lock,
        ) as mp:
            for file_info, filepath in skipped:
                filename = file_info["filename"]
                logger.info(f"Skipping {filename} (already verified)")
                skip_size = filepath.stat().st_size
                mp.start_file(filename, skip_size, skip_size)
                mp.finish_file(filename, success=True, size=skip_size)
                results[filename] = DownloadResult(
                    filename=filename,
                    success=True,
                    filepath=filepath,
                    size=skip_size,
                )

            def _download_one(fi: dict) -> tuple[str, DownloadResult]:
                fn = fi["filename"]
                u = fi.get("url")
                ng = fi.get("needs_gzip", False)
                ia = fi.get("is_archive", False)
                logger.info(f"Downloading {fn}...")
                res = self.download_file_with_url(
                    fn,
                    u,
                    output_dir,
                    self.auto_resume,
                    needs_gzip=ng,
                    is_archive=ia,
                    multi_progress=mp,
                )
                return fn, res

            if len(download_queue) > 1 and self.max_workers > 1:
                workers = min(self.max_workers, len(download_queue))
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = [ex.submit(_download_one, fi) for fi in download_queue]
                    for fut in concurrent.futures.as_completed(futures):
                        filename, result = fut.result()
                        results[filename] = result
                        _apply_download_result(filename, result)
            else:
                for fi in download_queue:
                    filename, result = _download_one(fi)
                    results[filename] = result
                    _apply_download_result(filename, result)

        # Check if all files are complete
        incomplete_files = state_manager.get_incomplete_files()
        if not incomplete_files:
            state_manager.mark_completed(info)

        # Always try to generate archive.json as long as SOFT file is present
        # (even if some files failed - partial archive is still useful)
        try:
            from gse_downloader.archive.profile import ArchiveGenerator
            from gse_downloader.archive.schema import DownloadStatus
            from gse_downloader.parser.geo_query import GEOFile
            from gse_downloader.parser.metadata import MetadataParser

            # Parse SOFT file for metadata
            soft_file = output_dir / f"{gse_id}_family.soft.gz"
            metadata_parser = MetadataParser()

            gse_metadata = None
            gsm_list: list = []
            gpl_list: list = []

            if soft_file.exists():
                gse_metadata, gsm_list, gpl_list = metadata_parser.parse_soft_file(soft_file)
                logger.info(f"Parsed metadata from SOFT: {len(gsm_list)} samples")

            if gse_metadata is not None:
                # Generate archive with metadata
                generator = ArchiveGenerator(self.output_dir)

                # Build file list from output directory (skip temp and state files)
                geo_files: list[GEOFile] = []
                for f in output_dir.glob("*"):
                    if f.is_file() and not f.name.endswith((".tmp", ".json")):
                        geo_files.append(GEOFile(
                            filename=f.name,
                            url="",
                            size=f.stat().st_size,
                            file_type="supplementary" if "suppl" in f.name.lower()
                                 else ("processed" if "_processed_" in f.name else "metadata"),
                        ))

                dl_status = DownloadStatus.COMPLETED if not incomplete_files else DownloadStatus.INCOMPLETE
                profile = generator.generate(
                    gse_id,
                    metadata=gse_metadata,
                    samples=gsm_list if gsm_list else None,
                    files=geo_files if geo_files else None,
                    status=dl_status,
                )
                archive_path = generator.save(profile, gse_id)
                logger.info(f"Archive generated for {gse_id}: {archive_path}")
        except Exception as e:
            logger.warning(f"Failed to generate archive for {gse_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())

        if incomplete_files:
            state_manager.mark_incomplete(info)

        return results

    def get_gse_files(self, gse_id: str) -> list[dict]:
        """Get list of available files for a GSE dataset.

        Args:
            gse_id: GSE identifier

        Returns:
            List of file info dictionaries
        """
        from gse_downloader.parser.geo_query import GEOQuery

        try:
            geo = GEOQuery()
            files = geo.get_series_files(gse_id)
            return files
        except Exception as e:
            logger.error(f"Failed to get files for {gse_id}: {e}")
            return []

    def close(self) -> None:
        """Close the downloader and cleanup resources."""
        self._main_session.close()
        worker_sess = getattr(self._thread_local, "session", None)
        if worker_sess is not None:
            worker_sess.close()
        logger.info("Downloader closed")

    def __enter__(self) -> "GSEDownloader":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager."""
        self.close()
