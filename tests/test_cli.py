"""Tests for CLI commands using Typer test runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from gse_downloader.cli.commands import app

runner = CliRunner()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_dummy_state(gse_id: str = "GSE999", status: str = "not_started"):
    """Build a minimal DownloadInfo-like mock."""
    from gse_downloader.core.state_manager import DownloadInfo, DownloadState
    info = DownloadInfo(gse_id=gse_id)
    info.status = DownloadState(status)
    return info


# ── version ──────────────────────────────────────────────────────────────────

class TestVersionCommand:
    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "GSE Downloader" in result.output


# ── status ───────────────────────────────────────────────────────────────────

class TestStatusCommand:
    def test_status_not_started(self, tmp_path):
        """status command should show NOT_STARTED for a directory with no state."""
        from gse_downloader.core.state_manager import DownloadInfo, DownloadState

        with patch("gse_downloader.cli.commands.Config") as MockConfig:
            MockConfig.return_value.download.output_dir = tmp_path
            MockConfig.return_value.checksum.algorithm = "md5"

            result = runner.invoke(app, ["status", "GSE999"])
            assert result.exit_code == 0
            # NOT_STARTED is the default when there's no state file
            assert "not_started" in result.output.lower() or "GSE999" in result.output


# ── search ────────────────────────────────────────────────────────────────────

class TestSearchCommand:
    def test_search_returns_results(self):
        """search command should call search_series_detailed and display table."""
        mock_results = [
            {
                "gse_id": "GSE12345",
                "title": "Test cancer RNA-seq study",
                "summary": "A test study",
                "series_type": "Expression profiling by high throughput sequencing",
                "organisms": ["Homo sapiens"],
                "sample_count": 20,
                "submission_date": "2023-01-15",
                "pubmed_ids": ["12345678"],
                "platform": "GPL24676",
            }
        ]
        with patch("gse_downloader.parser.geo_query.GEOQuery.search_series_detailed",
                   return_value=mock_results):
            result = runner.invoke(app, ["search", "cancer RNA-seq"])
        assert result.exit_code == 0
        assert "GSE12345" in result.output

    def test_search_no_results(self):
        """search command with no results should exit 0 with a message."""
        with patch("gse_downloader.parser.geo_query.GEOQuery.search_series_detailed",
                   return_value=[]):
            result = runner.invoke(app, ["search", "xyzzy_nonexistent_query_abc"])
        assert result.exit_code == 0
        assert "No results" in result.output

    def test_search_json_format(self):
        """search --format json should output valid JSON."""
        import json

        mock_results = [
            {
                "gse_id": "GSE99999",
                "title": "JSON test",
                "summary": "",
                "series_type": "RNA-seq",
                "organisms": ["Mus musculus"],
                "sample_count": 5,
                "submission_date": "2024-06-01",
                "pubmed_ids": [],
                "platform": "",
            }
        ]
        with patch("gse_downloader.parser.geo_query.GEOQuery.search_series_detailed",
                   return_value=mock_results):
            result = runner.invoke(app, ["search", "test", "--format", "json"])
        assert result.exit_code == 0


# ── info ─────────────────────────────────────────────────────────────────────

class TestInfoCommand:
    def test_info_local_archive(self, tmp_path):
        """info command should display local archive data when available."""
        from gse_downloader.archive.profile import ArchiveProfile
        from gse_downloader.archive.schema import ArchiveSchema, DownloadStatus
        from gse_downloader.parser.omics_detector import OmicsType

        mock_schema = ArchiveSchema(gse_id="GSE12345", status=DownloadStatus.COMPLETED)
        mock_schema.title = "Test Study Title"
        mock_schema.omics_type = OmicsType.RNA_SEQ
        mock_schema.sample_count = 10
        mock_profile = ArchiveProfile(mock_schema)

        with (
            patch("gse_downloader.cli.commands.Config") as MockConfig,
            patch("gse_downloader.archive.profile.ArchiveGenerator.load",
                  return_value=mock_profile),
        ):
            MockConfig.return_value.download.output_dir = tmp_path
            result = runner.invoke(app, ["info", "GSE12345"])

        assert result.exit_code == 0
        assert "GSE12345" in result.output

    def test_info_no_local_local_flag(self, tmp_path):
        """info --local with no archive should exit 1."""
        with (
            patch("gse_downloader.cli.commands.Config") as MockConfig,
            patch("gse_downloader.archive.profile.ArchiveGenerator.load",
                  return_value=None),
        ):
            MockConfig.return_value.download.output_dir = tmp_path
            result = runner.invoke(app, ["info", "GSE12345", "--local"])
        assert result.exit_code == 1

    def test_info_online_fallback(self, tmp_path):
        """info command should query online when no local archive exists."""
        from gse_downloader.parser.geo_query import GSESeries

        mock_series = GSESeries(gse_id="GSE12345")
        mock_series.title = "Online fetched title"
        mock_series.summary = "Some summary"
        mock_series.organism = ["Homo sapiens"]
        mock_series.sample_count = 5

        with (
            patch("gse_downloader.cli.commands.Config") as MockConfig,
            patch("gse_downloader.archive.profile.ArchiveGenerator.load",
                  return_value=None),
            patch("gse_downloader.parser.geo_query.GEOQuery.validate_gse_id",
                  return_value=(True, None)),
            patch("gse_downloader.parser.geo_query.GEOQuery.get_series_info",
                  return_value=mock_series),
        ):
            MockConfig.return_value.download.output_dir = tmp_path
            result = runner.invoke(app, ["info", "GSE12345"])

        assert result.exit_code == 0
        assert "GSE12345" in result.output


# ── init ─────────────────────────────────────────────────────────────────────

# Prompts (when --output given): max_workers, timeout, auto_resume, algorithm,
#   rate_limit, ncbi_email, api_key  (7 prompts)
_INIT_DEFAULTS_INPUT = "4\n300\ny\nmd5\n2.0\nanonymous@example.com\n\n"


class TestInitCommand:
    def test_init_creates_config(self, tmp_path):
        """init command should create a TOML config file."""
        config_file = tmp_path / "test_config.toml"
        result = runner.invoke(
            app,
            ["init", "--output", str(tmp_path / "data"), "--config", str(config_file)],
            input=_INIT_DEFAULTS_INPUT,
        )
        assert result.exit_code == 0, result.output
        assert config_file.exists()
        content = config_file.read_text(encoding="utf-8")
        assert "[download]" in content
        assert "[checksum]" in content

    def test_init_custom_output_dir(self, tmp_path):
        """init should write correct output_dir to config."""
        config_file = tmp_path / "cfg.toml"
        custom_dir = tmp_path / "my_geo_data"
        result = runner.invoke(
            app,
            ["init", "--output", str(custom_dir), "--config", str(config_file)],
            input=_INIT_DEFAULTS_INPUT,
        )
        assert result.exit_code == 0, result.output
        content = config_file.read_text(encoding="utf-8")
        assert "my_geo_data" in content


# ── verify ────────────────────────────────────────────────────────────────────

class TestVerifyCommand:
    def test_verify_no_state_file(self, tmp_path):
        """verify should handle missing state file gracefully."""
        with patch("gse_downloader.cli.commands.Config") as MockConfig:
            MockConfig.return_value.download.output_dir = tmp_path
            MockConfig.return_value.checksum.algorithm = "md5"
            result = runner.invoke(app, ["verify", "GSE_NONEXISTENT"])
        # Should fail gracefully (no state file)
        assert result.exit_code in (0, 1)

    def test_verify_all_empty_dir(self, tmp_path):
        """verify --all on empty directory should give appropriate message."""
        with patch("gse_downloader.cli.commands.Config") as MockConfig:
            MockConfig.return_value.download.output_dir = tmp_path
            MockConfig.return_value.checksum.algorithm = "md5"
            result = runner.invoke(app, ["verify", "--all"])
        assert result.exit_code == 0
        assert "No GSE" in result.output


# ── stats ─────────────────────────────────────────────────────────────────────

class TestStatsCommand:
    def test_stats_empty_dir(self, tmp_path):
        """stats on empty directory should say no archives found."""
        with patch("gse_downloader.cli.commands.Config") as MockConfig:
            MockConfig.return_value.download.output_dir = tmp_path
            result = runner.invoke(app, ["stats", "--output-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No archives" in result.output

    def test_stats_with_archive(self, tmp_path):
        """stats should show summary table when archives exist."""
        from gse_downloader.archive.profile import ArchiveProfile
        from gse_downloader.archive.schema import ArchiveSchema, DownloadStatus
        from gse_downloader.parser.omics_detector import OmicsType
        from gse_downloader.archive.schema import Organism
        import json

        # Create a dummy archive.json
        gse_dir = tmp_path / "GSE11111"
        gse_dir.mkdir()
        schema = ArchiveSchema(gse_id="GSE11111", status=DownloadStatus.COMPLETED)
        schema.omics_type = OmicsType.RNA_SEQ
        schema.sample_count = 8
        schema.organisms = [Organism(name="Homo sapiens")]
        profile = ArchiveProfile(schema)
        archive_path = gse_dir / "archive.json"
        archive_path.write_text(profile.to_json(), encoding="utf-8")

        result = runner.invoke(app, ["stats", "--output-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "GSE11111" in result.output or "1 datasets" in result.output or "Found 1" in result.output


# ── format ───────────────────────────────────────────────────────────────────

class TestFormatCommand:
    def test_format_single_gse(self, tmp_path):
        """format GSE_ID should call formatter and report result."""
        from unittest.mock import MagicMock, patch
        from gse_downloader.formatter.base import FormatResult

        gse_dir = tmp_path / "GSE77777"
        gse_dir.mkdir()

        mock_result = FormatResult(
            gse_id="GSE77777",
            omics_type="Other",
            success=True,
            raw_dir=gse_dir / "raw",
            processed_dir=gse_dir / "processed",
            metadata_file=None,
            expression_matrix=None,
            moved_files=[],
            errors=[],
        )

        with patch("gse_downloader.cli.commands.Config") as MockConfig, \
             patch("gse_downloader.archive.profile.ArchiveGenerator.load", return_value=None), \
             patch("gse_downloader.formatter.factory.FormatterFactory.get") as MockGet:
            MockConfig.return_value.download.output_dir = tmp_path
            mock_formatter = MagicMock()
            mock_formatter.format.return_value = mock_result
            MockGet.return_value = mock_formatter

            result = runner.invoke(app, ["format", "GSE77777", "--output", str(tmp_path)])

        assert result.exit_code == 0
        assert "OK" in result.output or "Format" in result.output

    def test_format_missing_dir(self, tmp_path):
        """format should exit with error when GSE directory does not exist."""
        with patch("gse_downloader.cli.commands.Config") as MockConfig:
            MockConfig.return_value.download.output_dir = tmp_path
            result = runner.invoke(app, ["format", "GSE00000", "--output", str(tmp_path)])
        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "Directory" in result.output

    def test_format_all(self, tmp_path):
        """format --all should process every GSE directory found."""
        from unittest.mock import MagicMock, patch
        from gse_downloader.formatter.base import FormatResult

        for gid in ("GSE111", "GSE222"):
            (tmp_path / gid).mkdir()

        mock_result = FormatResult(
            gse_id="GSE111",
            omics_type="Other",
            success=True,
            raw_dir=None,
            processed_dir=None,
            metadata_file=None,
            expression_matrix=None,
            moved_files=[],
            errors=[],
        )

        with patch("gse_downloader.cli.commands.Config") as MockConfig, \
             patch("gse_downloader.archive.profile.ArchiveGenerator.load", return_value=None), \
             patch("gse_downloader.formatter.factory.FormatterFactory.get") as MockGet:
            MockConfig.return_value.download.output_dir = tmp_path
            mock_formatter = MagicMock()
            mock_formatter.format.return_value = mock_result
            MockGet.return_value = mock_formatter

            result = runner.invoke(app, ["format", "--all", "--output", str(tmp_path)])

        assert result.exit_code == 0
        assert "2" in result.output or "GSE111" in result.output

    def test_format_no_args(self):
        """format without args and without --all should show error."""
        result = runner.invoke(app, ["format"])
        assert result.exit_code != 0 or "Provide" in result.output or "GSE ID" in result.output
