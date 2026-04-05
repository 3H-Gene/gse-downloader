"""Checksum verification module for GSE Downloader.

This module handles file integrity verification using MD5 or SHA256.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("checksum")


class ChecksumVerifier:
    """Handles file checksum verification."""

    SUPPORTED_ALGORITHMS = ["md5", "sha256", "sha1"]

    def __init__(self, algorithm: str = "md5"):
        """Initialize checksum verifier.

        Args:
            algorithm: Checksum algorithm (md5, sha256, or sha1)

        Raises:
            ValueError: If algorithm is not supported
        """
        if algorithm.lower() not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported algorithm: {algorithm}. "
                f"Supported: {', '.join(self.SUPPORTED_ALGORITHMS)}"
            )
        self.algorithm = algorithm.lower()

    def calculate(self, filepath: Path) -> str:
        """Calculate checksum of a file.

        Args:
            filepath: Path to file

        Returns:
            Checksum hex string

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        hash_func = self._get_hash_function()
        file_size = filepath.stat().st_size
        processed = 0

        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_func.update(chunk)
                processed += len(chunk)

        checksum = hash_func.hexdigest()
        logger.debug(f"Calculated {self.algorithm} for {filepath.name}: {checksum}")

        return checksum

    def verify(self, filepath: Path, expected: str) -> bool:
        """Verify file checksum.

        Args:
            filepath: Path to file
            expected: Expected checksum value

        Returns:
            True if verified, False otherwise
        """
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return False

        actual = self.calculate(filepath)
        verified = actual.lower() == expected.lower()

        if verified:
            logger.info(f"Checksum verified ({self.algorithm}): {filepath.name}")
        else:
            logger.error(
                f"Checksum mismatch ({self.algorithm}): {filepath.name}\n"
                f"  Expected: {expected}\n"
                f"  Actual:   {actual}"
            )

        return verified

    def _get_hash_function(self):
        """Get hash function for the configured algorithm.

        Returns:
            Hash function
        """
        if self.algorithm == "md5":
            return hashlib.md5()
        elif self.algorithm == "sha256":
            return hashlib.sha256()
        elif self.algorithm == "sha1":
            return hashlib.sha1()

    @staticmethod
    def get_file_md5(filepath: Path) -> str:
        """Calculate MD5 checksum of a file.

        Args:
            filepath: Path to file

        Returns:
            MD5 hex string
        """
        return ChecksumVerifier("md5").calculate(filepath)

    @staticmethod
    def get_file_sha256(filepath: Path) -> str:
        """Calculate SHA256 checksum of a file.

        Args:
            filepath: Path to file

        Returns:
            SHA256 hex string
        """
        return ChecksumVerifier("sha256").calculate(filepath)


class BatchChecksumVerifier:
    """Handles batch checksum verification."""

    def __init__(self, algorithm: str = "md5", max_workers: int = 4):
        """Initialize batch checksum verifier.

        Args:
            algorithm: Checksum algorithm
            max_workers: Maximum concurrent workers
        """
        self.verifier = ChecksumVerifier(algorithm)
        self.max_workers = max_workers

    def verify_files(self, files: dict[Path, str]) -> dict[Path, bool]:
        """Verify multiple files.

        Args:
            files: Dictionary mapping file paths to expected checksums

        Returns:
            Dictionary mapping file paths to verification results
        """
        results = {}

        for filepath, expected in files.items():
            results[filepath] = self.verifier.verify(filepath, expected)

        return results

    def calculate_batch(self, files: list[Path]) -> dict[Path, str | None]:
        """Calculate checksums for multiple files.

        Args:
            files: List of file paths

        Returns:
            Dictionary mapping file paths to checksums (``None`` if calculation failed)
        """
        results = {}

        for filepath in files:
            try:
                results[filepath] = self.verifier.calculate(filepath)
            except Exception as e:
                logger.error(f"Failed to calculate checksum for {filepath}: {e}")
                results[filepath] = None

        return results
