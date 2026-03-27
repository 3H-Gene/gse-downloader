"""Logging utilities for GSE Downloader.

This module provides centralized logging configuration using loguru.
"""

from __future__ import annotations

import io
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger


def _get_safe_stderr():
    """Return a UTF-8 safe stderr on Windows to avoid GBK codec errors."""
    if sys.platform == "win32":
        if not isinstance(sys.stderr, io.TextIOWrapper) or sys.stderr.encoding.lower().replace("-", "") != "utf8":
            try:
                return io.TextIOWrapper(
                    sys.stderr.buffer,
                    encoding="utf-8",
                    errors="replace",
                    line_buffering=True,
                )
            except AttributeError:
                pass
    return sys.stderr


def setup_logger(
    log_dir: Optional[Path] = None,
    log_level: str = "INFO",
    rotation: str = "100 MB",
    retention: str = "30 days",
    console: bool = True,
) -> None:
    """Setup logging configuration.

    Args:
        log_dir: Directory for log files (None to disable file logging)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        rotation: Log rotation size (e.g., "100 MB", "1 GB", "1 week")
        retention: Log retention period (e.g., "30 days", "1 year")
        console: Whether to log to console
    """
    # Remove default handler
    logger.remove()

    # Configure console logging
    if console:
        safe_stderr = _get_safe_stderr()
        logger.add(
            safe_stderr,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            colorize=True,
        )

    # Configure file logging
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        # Main log file
        logger.add(
            log_dir / "gse_downloader_{time:YYYY-MM-DD}.log",
            level=log_level,
            rotation=rotation,
            retention=retention,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            compression="zip",
        )

        # Error log file
        logger.add(
            log_dir / "error_{time:YYYY-MM-DD}.log",
            level="ERROR",
            rotation=rotation,
            retention=retention,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            compression="zip",
        )


def get_logger(name: str = "gse_downloader"):
    """Get a logger instance.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logger.bind(name=name)


class ProgressLogger:
    """Logger for progress tracking."""

    def __init__(self, total: int, desc: str = "Processing"):
        """Initialize progress logger.

        Args:
            total: Total number of items
            desc: Description
        """
        self.total = total
        self.desc = desc
        self.current = 0
        self.start_time = datetime.now()

    def update(self, n: int = 1) -> None:
        """Update progress.

        Args:
            n: Number of items completed
        """
        self.current += n
        percentage = (self.current / self.total) * 100
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.current / elapsed if elapsed > 0 else 0

        logger.info(
            f"{self.desc}: {self.current}/{self.total} ({percentage:.1f}%) | "
            f"Rate: {rate:.1f} items/s"
        )

    def finish(self) -> None:
        """Mark progress as finished."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"{self.desc}: Completed {self.current} items in {elapsed:.1f}s")


class DownloadLogger:
    """Logger for download operations."""

    def __init__(self, gse_id: str):
        """Initialize download logger.

        Args:
            gse_id: GSE identifier
        """
        self.gse_id = gse_id
        self.logger = get_logger(f"download.{gse_id}")

    def start(self, total_files: int, total_size: int) -> None:
        """Log download start.

        Args:
            total_files: Total number of files to download
            total_size: Total size in bytes
        """
        size_mb = total_size / (1024 * 1024)
        self.logger.info(f"Starting download: {total_files} files, {size_mb:.2f} MB")

    def progress(self, filename: str, downloaded: int, total: int) -> None:
        """Log download progress.

        Args:
            filename: Current file being downloaded
            downloaded: Bytes downloaded
            total: Total bytes
        """
        percentage = (downloaded / total) * 100 if total > 0 else 0
        self.logger.debug(f"{filename}: {downloaded}/{total} ({percentage:.1f}%)")

    def complete(self, filename: str, size: int, duration: float) -> None:
        """Log file download completion.

        Args:
            filename: Downloaded file name
            size: File size in bytes
            duration: Download duration in seconds
        """
        size_kb = size / 1024
        rate = size / duration / 1024 if duration > 0 else 0
        self.logger.info(f"Downloaded: {filename} ({size_kb:.2f} KB) in {duration:.2f}s ({rate:.2f} KB/s)")

    def error(self, filename: str, error: str) -> None:
        """Log download error.

        Args:
            filename: File that failed
            error: Error message
        """
        self.logger.error(f"Failed to download {filename}: {error}")

    def checksum_verify(self, filename: str, algorithm: str, expected: str, actual: str) -> None:
        """Log checksum verification.

        Args:
            filename: File being verified
            algorithm: Checksum algorithm
            expected: Expected checksum value
            actual: Actual checksum value
        """
        if expected == actual:
            self.logger.info(f"Checksum verified ({algorithm}): {filename}")
        else:
            self.logger.error(
                f"Checksum mismatch ({algorithm}): {filename} "
                f"(expected: {expected[:8]}..., got: {actual[:8]}...)"
            )
