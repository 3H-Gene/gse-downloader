"""Tests for checksum module."""

import tempfile
from pathlib import Path

import pytest

from gse_downloader.core.checksum import ChecksumVerifier, BatchChecksumVerifier


class TestChecksumVerifier:
    """Test ChecksumVerifier class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "test.txt"

        # Create test file with known content
        with open(self.test_file, "w") as f:
            f.write("Hello, World!")

    def test_calculate_md5(self):
        """Test MD5 calculation."""
        verifier = ChecksumVerifier("md5")
        checksum = verifier.calculate(self.test_file)

        # MD5 of "Hello, World!"
        expected = "65a8e27d8879283831b664bd8b7f0ad4"
        assert checksum == expected

    def test_calculate_sha256(self):
        """Test SHA256 calculation."""
        verifier = ChecksumVerifier("sha256")
        checksum = verifier.calculate(self.test_file)

        # SHA256 of "Hello, World!"
        expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        assert checksum == expected

    def test_verify_correct(self):
        """Test verification with correct checksum."""
        verifier = ChecksumVerifier("md5")
        expected = "65a8e27d8879283831b664bd8b7f0ad4"

        assert verifier.verify(self.test_file, expected)

    def test_verify_incorrect(self):
        """Test verification with incorrect checksum."""
        verifier = ChecksumVerifier("md5")

        assert not verifier.verify(self.test_file, "wrong_checksum")

    def test_verify_nonexistent_file(self):
        """Test verification with nonexistent file."""
        verifier = ChecksumVerifier("md5")
        nonexistent = Path(self.temp_dir) / "nonexistent.txt"

        assert not verifier.verify(nonexistent, "any_checksum")

    def test_unsupported_algorithm(self):
        """Test unsupported algorithm raises error."""
        with pytest.raises(ValueError):
            ChecksumVerifier("unsupported")

    def test_static_methods(self):
        """Test static convenience methods."""
        md5 = ChecksumVerifier.get_file_md5(self.test_file)
        sha256 = ChecksumVerifier.get_file_sha256(self.test_file)

        assert md5 == "65a8e27d8879283831b664bd8b7f0ad4"
        assert sha256 == "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"


class TestBatchChecksumVerifier:
    """Test BatchChecksumVerifier class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_files = []

        for i in range(3):
            test_file = Path(self.temp_dir) / f"test{i}.txt"
            with open(test_file, "w") as f:
                f.write(f"Content {i}")
            self.test_files.append(test_file)

    def test_calculate_batch(self):
        """Test batch calculation."""
        verifier = BatchChecksumVerifier("md5")
        results = verifier.calculate_batch(self.test_files)

        assert len(results) == 3
        for filepath in self.test_files:
            assert filepath in results
            assert results[filepath] is not None

    def test_verify_files(self):
        """Test batch verification."""
        verifier = BatchChecksumVerifier("md5")

        # Calculate expected checksums
        expected = {f: verifier.verifier.calculate(f) for f in self.test_files}

        results = verifier.verify_files(expected)

        assert all(results.values())
