"""Download state management for GSE Downloader.

This module manages the download state of GSE datasets, tracking completion,
resumable status, and file integrity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("state_manager")


class DownloadState(str, Enum):
    """Download state enumeration."""

    NOT_STARTED = "not_started"
    """No download record exists."""

    INCOMPLETE = "incomplete"
    """Download started but not completed, or checksum failed."""

    COMPLETED = "completed"
    """All files downloaded and checksum verified."""

    INVALID = "invalid"
    """Files exist but checksum verification failed."""


@dataclass
class FileState:
    """State of a single file."""

    filename: str
    size_bytes: int
    downloaded_bytes: int = 0
    md5: Optional[str] = None
    verified: bool = False
    download_url: Optional[str] = None


@dataclass
class DownloadInfo:
    """Download information."""

    gse_id: str
    status: DownloadState = DownloadState.NOT_STARTED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    files: dict[str, FileState] = None
    total_files: int = 0
    total_bytes: int = 0
    downloaded_bytes: int = 0
    retry_count: int = 0
    last_error: Optional[str] = None

    def __post_init__(self):
        """Post initialization."""
        if self.files is None:
            self.files = {}

    @property
    def progress_percentage(self) -> float:
        """Get download progress percentage."""
        if self.total_bytes == 0:
            return 0.0
        return (self.downloaded_bytes / self.total_bytes) * 100

    @property
    def completed_files(self) -> int:
        """Get number of completed files."""
        return sum(1 for f in self.files.values() if f.verified)

    @property
    def is_resumable(self) -> bool:
        """Check if download is resumable."""
        return self.status in (DownloadState.INCOMPLETE, DownloadState.INVALID)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        files_dict = {
            name: asdict(file) | {"download_url": file.download_url}
            for name, file in self.files.items()
        }
        return {
            "gse_id": self.gse_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "files": files_dict,
            "total_files": self.total_files,
            "total_bytes": self.total_bytes,
            "downloaded_bytes": self.downloaded_bytes,
            "retry_count": self.retry_count,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DownloadInfo":
        """Create from dictionary."""
        started_at = None
        if data.get("started_at"):
            started_at = datetime.fromisoformat(data["started_at"])

        completed_at = None
        if data.get("completed_at"):
            completed_at = datetime.fromisoformat(data["completed_at"])

        files = {}
        for filename, file_data in data.get("files", {}).items():
            files[filename] = FileState(**file_data)

        return cls(
            gse_id=data["gse_id"],
            status=DownloadState(data["status"]),
            started_at=started_at,
            completed_at=completed_at,
            files=files,
            total_files=data.get("total_files", 0),
            total_bytes=data.get("total_bytes", 0),
            downloaded_bytes=data.get("downloaded_bytes", 0),
            retry_count=data.get("retry_count", 0),
            last_error=data.get("last_error"),
        )


class StateManager:
    """Manages download state persistence."""

    STATE_FILENAME = "download_state.json"
    ARCHIVE_FILENAME = "archive.json"

    def __init__(self, gse_dir: Path):
        """Initialize state manager.

        Args:
            gse_dir: Directory for GSE dataset
        """
        self.gse_dir = Path(gse_dir)
        self.gse_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.gse_dir / self.STATE_FILENAME

    def load_state(self) -> DownloadInfo:
        """Load download state from file.

        Returns:
            DownloadInfo instance, or new instance if file doesn't exist
        """
        if not self.state_file.exists():
            gse_id = self.gse_dir.name
            logger.debug(f"No state file found for {gse_id}, returning new state")
            return DownloadInfo(gse_id=gse_id)

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug(f"Loaded state from {self.state_file}")
            return DownloadInfo.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load state: {e}, returning new state")
            return DownloadInfo(gse_id=self.gse_dir.name)

    def save_state(self, info: DownloadInfo) -> None:
        """Save download state to file.

        Args:
            info: DownloadInfo instance to save
        """
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(info.to_dict(), f, indent=2)
            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def update_file_state(
        self,
        info: DownloadInfo,
        filename: str,
        downloaded_bytes: int,
        md5: Optional[str] = None,
        verified: bool = False,
    ) -> None:
        """Update state for a single file.

        Args:
            info: DownloadInfo instance
            filename: Filename to update
            downloaded_bytes: Bytes downloaded for this file
            md5: MD5 checksum (optional)
            verified: Whether file is verified
        """
        if filename not in info.files:
            logger.warning(f"File {filename} not in state")
            return

        file_state = info.files[filename]
        file_state.downloaded_bytes = downloaded_bytes
        # Keep size_bytes in sync with actual downloaded size
        if downloaded_bytes > 0 and file_state.size_bytes == 0:
            file_state.size_bytes = downloaded_bytes
        if md5:
            file_state.md5 = md5
        file_state.verified = verified

        # Update totals
        info.downloaded_bytes = sum(f.downloaded_bytes for f in info.files.values())
        info.total_bytes = sum(f.size_bytes for f in info.files.values())

        self.save_state(info)

    def mark_completed(self, info: DownloadInfo) -> None:
        """Mark download as completed.

        Args:
            info: DownloadInfo instance
        """
        info.status = DownloadState.COMPLETED
        info.completed_at = datetime.now()
        info.last_error = None  # Clear error on success
        self.save_state(info)
        logger.info(f"Marked {info.gse_id} as completed")

    def mark_incomplete(self, info: DownloadInfo, error: Optional[str] = None) -> None:
        """Mark download as incomplete.

        Args:
            info: DownloadInfo instance
            error: Error message (optional)
        """
        info.status = DownloadState.INCOMPLETE
        if error:
            info.last_error = error
        self.save_state(info)
        logger.info(f"Marked {info.gse_id} as incomplete")

    def mark_invalid(self, info: DownloadInfo, error: Optional[str] = None) -> None:
        """Mark download as invalid (checksum failed).

        Args:
            info: DownloadInfo instance
            error: Error message (optional)
        """
        info.status = DownloadState.INVALID
        if error:
            info.last_error = error
        self.save_state(info)
        logger.warning(f"Marked {info.gse_id} as invalid")

    def increment_retry(self, info: DownloadInfo) -> None:
        """Increment retry count.

        Args:
            info: DownloadInfo instance
        """
        info.retry_count += 1
        self.save_state(info)

    def get_status(self) -> DownloadState:
        """Get current download status.

        Returns:
            DownloadState
        """
        info = self.load_state()
        return info.status

    def is_resumable(self) -> bool:
        """Check if download is resumable.

        Returns:
            True if resumable, False otherwise
        """
        info = self.load_state()
        return info.is_resumable

    def get_incomplete_files(self) -> list[str]:
        """Get list of incomplete files.

        Returns:
            List of filenames that need to be downloaded
        """
        info = self.load_state()
        return [name for name, file in info.files.items() if not file.verified]

    def delete_state(self) -> None:
        """Delete state file."""
        if self.state_file.exists():
            self.state_file.unlink()
            logger.info(f"Deleted state file: {self.state_file}")
