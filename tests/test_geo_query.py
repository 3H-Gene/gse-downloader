"""Tests for GEOQuery — using mocked HTTP responses."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from gse_downloader.parser.geo_query import GEOQuery, GSESeries, GEOFile


# ── GSESeries dataclass ───────────────────────────────────────────────────────

class TestGSESeries:
    def test_default_fields(self):
        s = GSESeries(gse_id="GSE1")
        assert s.gse_id == "GSE1"
        assert s.title == ""
        assert s.pubmed_ids == []
        assert s.keywords == []
        assert s.sample_count == 0

    def test_sample_count_field(self):
        s = GSESeries(gse_id="GSE2", sample_count=15)
        assert s.sample_count == 15


# ── validate_gse_id ───────────────────────────────────────────────────────────

class TestValidateGSEId:
    def test_invalid_format_no_digits(self):
        geo = GEOQuery()
        ok, err = geo.validate_gse_id("GSEXYZ")
        assert not ok
        assert "Invalid GSE ID format" in err

    def test_invalid_format_wrong_prefix(self):
        """Only GSE prefix is accepted, not GPL or GSM."""
        geo = GEOQuery()
        ok, err = geo.validate_gse_id("GPL570")
        assert not ok

    def test_invalid_format_empty(self):
        geo = GEOQuery()
        ok, err = geo.validate_gse_id("")
        assert not ok

    def test_lowercase_gse_accepted(self):
        """Lowercase 'gse123' is case-insensitive and should pass format check
        (the function normalises to upper internally).
        The network call may succeed or fail; we only check format is OK.
        """
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"esearchresult": {"count": "1"}}

        with patch.object(geo.session, "get", return_value=mock_response):
            ok, err = geo.validate_gse_id("gse123")
        assert ok

    def test_valid_format_network_ok(self):
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"esearchresult": {"count": "3"}}

        with patch.object(geo.session, "get", return_value=mock_response):
            ok, err = geo.validate_gse_id("GSE123456")
        assert ok
        assert err is None

    def test_not_found_in_database(self):
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"esearchresult": {"count": "0"}}

        with patch.object(geo.session, "get", return_value=mock_response):
            ok, err = geo.validate_gse_id("GSE999999999")
        assert not ok
        assert "not found" in err

    def test_network_timeout(self):
        geo = GEOQuery()
        with patch.object(geo.session, "get", side_effect=requests.exceptions.Timeout()):
            ok, err = geo.validate_gse_id("GSE123")
        assert not ok
        assert "Timeout" in err


# ── get_series_files ──────────────────────────────────────────────────────────

class TestGetSeriesFiles:
    def test_basic_file_list_structure(self):
        geo = GEOQuery()

        # Mock the suppl directory response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<a href="GSE1_RAW.tar">GSE1_RAW.tar</a>'

        with patch.object(geo.session, "get", return_value=mock_response):
            files = geo.get_series_files("GSE1")

        filenames = [f["filename"] for f in files]
        assert "GSE1_family.soft.gz" in filenames
        assert "GSE1_series_matrix.txt.gz" in filenames
        assert "GSE1_family.xml.tgz" in filenames

    def test_prefix_small_gse(self):
        """GSE1 should use GSEnnn prefix."""
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = ""

        with patch.object(geo.session, "get", return_value=mock_response):
            files = geo.get_series_files("GSE1")

        soft = next(f for f in files if f["filename"].endswith("soft.gz"))
        assert "/GSEnnn/GSE1/" in soft["url"]

    def test_prefix_large_gse(self):
        """GSE134520 should use GSE134nnn prefix."""
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""

        with patch.object(geo.session, "get", return_value=mock_response):
            files = geo.get_series_files("GSE134520")

        soft = next(f for f in files if f["filename"].endswith("soft.gz"))
        assert "/GSE134nnn/GSE134520/" in soft["url"]

    def test_suppl_files_parsed(self):
        """Supplementary files from FTP listing should be included."""
        geo = GEOQuery()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '<a href="GSE1_RAW.tar">GSE1_RAW.tar</a>\n'
            '<a href="GSE1_processed.txt.gz">GSE1_processed.txt.gz</a>'
        )

        with patch.object(geo.session, "get", return_value=mock_response):
            files = geo.get_series_files("GSE1")

        filenames = [f["filename"] for f in files]
        assert "GSE1_RAW.tar" in filenames
        assert "GSE1_processed.txt.gz" in filenames

    def test_suppl_fetch_error_handled(self):
        """If suppl fetch raises, should still return base files."""
        geo = GEOQuery()
        with patch.object(geo.session, "get", side_effect=requests.exceptions.ConnectionError()):
            files = geo.get_series_files("GSE1")

        # Should still have the 3 base files
        assert len(files) >= 3


# ── search_series_detailed ────────────────────────────────────────────────────

class TestSearchSeriesDetailed:
    def _make_esearch_resp(self, ids: list[str]):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {"esearchresult": {"idlist": ids}}
        return m

    def _make_esummary_resp(self, uid: str, accession: str, n_samples: int = 5):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {
            "result": {
                uid: {
                    "accession": accession,
                    "entrytype": "GSE",
                    "title": f"Test {accession}",
                    "summary": "A test study",
                    "gdstype": "Expression profiling by high throughput sequencing",
                    "taxon": [{"scientificname": "Homo sapiens"}],
                    "n_samples": n_samples,
                    "pdat": "2023/06/01",
                    "pubmedids": [12345],
                    "GPL": "GPL24676",
                }
            }
        }
        return m

    def test_returns_results(self):
        geo = GEOQuery()
        esearch = self._make_esearch_resp(["200012345"])
        esummary = self._make_esummary_resp("200012345", "GSE12345", 20)

        with patch.object(geo.session, "get", side_effect=[esearch, esummary]):
            results = geo.search_series_detailed("lung cancer", retmax=5)

        assert len(results) == 1
        assert results[0]["gse_id"] == "GSE12345"
        assert results[0]["sample_count"] == 20
        assert "Homo sapiens" in results[0]["organisms"]

    def test_empty_results(self):
        geo = GEOQuery()
        esearch = self._make_esearch_resp([])

        with patch.object(geo.session, "get", return_value=esearch):
            results = geo.search_series_detailed("no_results_query")

        assert results == []

    def test_skips_non_gse(self):
        """Non-GSE entries (GPL, GSM) should be filtered out."""
        geo = GEOQuery()
        uid = "300001"
        esearch = self._make_esearch_resp([uid])

        m_summary = MagicMock()
        m_summary.raise_for_status = MagicMock()
        m_summary.json.return_value = {
            "result": {
                uid: {
                    "accession": "GPL570",
                    "entrytype": "GPL",   # not GSE
                    "title": "Platform",
                    "summary": "",
                    "gdstype": "",
                    "taxon": [],
                    "n_samples": 0,
                    "pdat": "",
                    "pubmedids": [],
                }
            }
        }

        with patch.object(geo.session, "get", side_effect=[esearch, m_summary]):
            results = geo.search_series_detailed("cancer")

        assert results == []

    def test_network_error_returns_empty(self):
        geo = GEOQuery()
        with patch.object(geo.session, "get", side_effect=requests.exceptions.ConnectionError()):
            results = geo.search_series_detailed("test")
        assert results == []

    def test_retmax_capped_at_100(self):
        """retmax > 100 should be silently capped at 100."""
        geo = GEOQuery()
        captured_params = {}

        def fake_get(url, params=None, **kwargs):
            captured_params.update(params or {})
            m = MagicMock()
            m.raise_for_status = MagicMock()
            m.json.return_value = {"esearchresult": {"idlist": []}}
            return m

        with patch.object(geo.session, "get", side_effect=fake_get):
            geo.search_series_detailed("test", retmax=999)

        assert captured_params.get("retmax", 0) <= 100


# ── _parse_soft_series ────────────────────────────────────────────────────────

class TestParseSoftSeries:
    SOFT_CONTENT = """^SERIES = GSE12345
!Series_title = Test lung cancer study
!Series_summary = This is a test summary.
!Series_overall_design = Comparative analysis
!Series_type = Expression profiling by high throughput sequencing
!Series_submission_date = Jan 01 2023
!Series_last_update_date = Jun 01 2023
!Series_pubmed_id = 12345678
!Series_keyword = RNA-seq
!Series_keyword = lung cancer
!Series_platform_id = GPL24676
!Series_sample_id = GSM1000001
!Series_sample_id = GSM1000002
^SAMPLE = GSM1000001
!Sample_organism_ch1 = Homo sapiens
^SAMPLE = GSM1000002
!Sample_organism_ch1 = Homo sapiens
"""

    def test_parse_basic_fields(self):
        geo = GEOQuery()
        series = geo._parse_soft_series(self.SOFT_CONTENT, "GSE12345")
        assert series.title == "Test lung cancer study"
        assert series.summary == "This is a test summary."
        assert series.series_type == "Expression profiling by high throughput sequencing"

    def test_parse_pubmed_ids(self):
        geo = GEOQuery()
        series = geo._parse_soft_series(self.SOFT_CONTENT, "GSE12345")
        assert "12345678" in series.pubmed_ids

    def test_parse_keywords(self):
        geo = GEOQuery()
        series = geo._parse_soft_series(self.SOFT_CONTENT, "GSE12345")
        assert "RNA-seq" in series.keywords
        assert "lung cancer" in series.keywords

    def test_parse_platforms(self):
        geo = GEOQuery()
        series = geo._parse_soft_series(self.SOFT_CONTENT, "GSE12345")
        assert "GPL24676" in series.platforms

    def test_sample_count(self):
        geo = GEOQuery()
        series = geo._parse_soft_series(self.SOFT_CONTENT, "GSE12345")
        assert series.sample_count >= 2

    def test_empty_content(self):
        geo = GEOQuery()
        series = geo._parse_soft_series("", "GSE0")
        assert series.gse_id == "GSE0"
        assert series.title == ""
        assert series.sample_count == 0
