"""Tests for state_manager module."""

import tempfile
from pathlib import Path

import pytest

from gse_downloader.core.state_manager import (
    DownloadInfo,
    DownloadState,
    FileState,
    StateManager,
)


class TestStateManager:
    """Test StateManager class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.gse_dir = Path(self.temp_dir) / "GSE123456"
        self.state_manager = StateManager(self.gse_dir)

    def test_new_state(self):
        """Test creating a new state."""
        info = self.state_manager.load_state()

        assert info.gse_id == "GSE123456"
        assert info.status == DownloadState.NOT_STARTED
        assert info.files == {}

    def test_save_and_load_state(self):
        """Test saving and loading state."""
        info = DownloadInfo(gse_id="GSE123456")
        info.status = DownloadState.INCOMPLETE
        info.files["test.txt"] = FileState(filename="test.txt", size_bytes=1000)

        self.state_manager.save_state(info)

        loaded = self.state_manager.load_state()

        assert loaded.gse_id == "GSE123456"
        assert loaded.status == DownloadState.INCOMPLETE
        assert "test.txt" in loaded.files

    def test_mark_completed(self):
        """Test marking download as completed."""
        info = self.state_manager.load_state()
        info.files["test.txt"] = FileState(filename="test.txt", size_bytes=1000, verified=True)

        self.state_manager.mark_completed(info)

        loaded = self.state_manager.load_state()
        assert loaded.status == DownloadState.COMPLETED
        assert loaded.completed_at is not None

    def test_mark_incomplete(self):
        """Test marking download as incomplete."""
        info = self.state_manager.load_state()

        self.state_manager.mark_incomplete(info, "Network error")

        loaded = self.state_manager.load_state()
        assert loaded.status == DownloadState.INCOMPLETE
        assert loaded.last_error == "Network error"

    def test_is_resumable(self):
        """Test checking if download is resumable."""
        info = self.state_manager.load_state()

        assert not info.is_resumable

        info.status = DownloadState.INCOMPLETE

        assert info.is_resumable

    def test_get_incomplete_files(self):
        """Test getting incomplete files."""
        info = DownloadInfo(gse_id="GSE123456")
        info.files["file1.txt"] = FileState(filename="file1.txt", size_bytes=100, verified=True)
        info.files["file2.txt"] = FileState(filename="file2.txt", size_bytes=100, verified=False)
        info.files["file3.txt"] = FileState(filename="file3.txt", size_bytes=100, verified=False)

        self.state_manager.save_state(info)

        incomplete = self.state_manager.get_incomplete_files()

        assert len(incomplete) == 2
        assert "file2.txt" in incomplete
        assert "file3.txt" in incomplete


class TestDownloadInfo:
    """Test DownloadInfo class."""

    def test_progress_percentage(self):
        """Test progress percentage calculation."""
        info = DownloadInfo(gse_id="GSE123456")
        info.total_bytes = 1000
        info.downloaded_bytes = 500

        assert info.progress_percentage == 50.0

    def test_progress_percentage_zero(self):
        """Test progress percentage with zero total."""
        info = DownloadInfo(gse_id="GSE123456")

        assert info.progress_percentage == 0.0

    def test_completed_files(self):
        """Test completed files count."""
        info = DownloadInfo(gse_id="GSE123456")
        info.files["file1.txt"] = FileState(filename="file1.txt", size_bytes=100, verified=True)
        info.files["file2.txt"] = FileState(filename="file2.txt", size_bytes=100, verified=True)
        info.files["file3.txt"] = FileState(filename="file3.txt", size_bytes=100, verified=False)

        assert info.completed_files == 2

    def test_to_dict(self):
        """Test converting to dictionary."""
        info = DownloadInfo(gse_id="GSE123456")
        info.status = DownloadState.COMPLETED
        info.files["test.txt"] = FileState(filename="test.txt", size_bytes=1000)

        data = info.to_dict()

        assert data["gse_id"] == "GSE123456"
        assert data["status"] == "completed"
        assert "test.txt" in data["files"]
