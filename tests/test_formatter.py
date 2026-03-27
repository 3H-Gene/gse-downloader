"""Tests for formatter module."""

from __future__ import annotations

import csv
import gzip
import json
import tempfile
from pathlib import Path

import pytest

from gse_downloader.formatter.base import BaseFormatter, FormatResult
from gse_downloader.formatter.factory import FormatterFactory
from gse_downloader.formatter.microarray import MicroarrayFormatter
from gse_downloader.formatter.rnaseq import RNASeqFormatter
from gse_downloader.formatter.series_matrix import SeriesMatrixFormatter, _split_matrix_line
from gse_downloader.parser.omics_detector import OmicsType


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def gse_dir(tmp_path: Path) -> Path:
    """Create a temporary GSE directory."""
    d = tmp_path / "GSE99999"
    d.mkdir()
    return d


def _write_archive(gse_dir: Path, omics: str = "RNA-seq", sample_count: int = 3) -> Path:
    """Write a minimal archive.json for testing."""
    samples = [
        {
            "gsm_id": f"GSM{1000 + i}",
            "title": f"Sample {i}",
            "source_name": "blood",
            "organism": "Homo sapiens",
            "extraction_molecule": "total RNA",
            "library_strategy": "RNA-Seq",
            "library_layout": "PAIRED",
            "instrument_model": "Illumina HiSeq 4000",
            "characteristics": {"tissue": "blood", "sex": "male"},
        }
        for i in range(sample_count)
    ]
    archive = {
        "gse_id": gse_dir.name,
        "omics_type": omics,
        "sample_count": sample_count,
        "organisms": [{"name": "Homo sapiens", "taxid": 9606}],
        "metadata": {"title": "Test dataset", "summary": "Test"},
        "samples": samples,
    }
    path = gse_dir / "archive.json"
    path.write_text(json.dumps(archive, indent=2), encoding="utf-8")
    return path


def _write_series_matrix(gse_dir: Path) -> Path:
    """Write a minimal series matrix file."""
    content = (
        "!Series_geo_accession\t\"GSE99999\"\n"
        "!Sample_geo_accession\t\"GSM1001\"\t\"GSM1002\"\t\"GSM1003\"\n"
        "!Sample_title\t\"ctrl1\"\t\"ctrl2\"\t\"trt1\"\n"
        "!Sample_source_name_ch1\t\"blood\"\t\"blood\"\t\"liver\"\n"
        "!Sample_organism_ch1\t\"Homo sapiens\"\t\"Homo sapiens\"\t\"Homo sapiens\"\n"
        "!Sample_characteristics_ch1\t\"tissue: blood\"\t\"tissue: blood\"\t\"tissue: liver\"\n"
        "!series_matrix_table_begin\n"
        "\"ID_REF\"\t\"GSM1001\"\t\"GSM1002\"\t\"GSM1003\"\n"
        "\"GENE1\"\t1.0\t2.0\t3.0\n"
        "\"GENE2\"\t4.0\t5.0\t6.0\n"
        "\"GENE3\"\t7.0\t8.0\t9.0\n"
        "!series_matrix_table_end\n"
    )
    path = gse_dir / "GSE99999_series_matrix.txt"
    path.write_text(content, encoding="utf-8")
    return path


def _write_series_matrix_gz(gse_dir: Path) -> Path:
    """Write a gzip-compressed series matrix file."""
    sm_path = _write_series_matrix(gse_dir)
    gz_path = gse_dir / "GSE99999_series_matrix.txt.gz"
    with open(sm_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        f_out.write(f_in.read())
    sm_path.unlink()
    return gz_path


# ─── _split_matrix_line ────────────────────────────────────────────────────────

class TestSplitMatrixLine:
    def test_simple(self):
        line = '"ID_REF"\t"GSM1"\t"GSM2"'
        result = _split_matrix_line(line)
        assert result == ["ID_REF", "GSM1", "GSM2"]

    def test_numeric(self):
        line = '"GENE1"\t1.23\t4.56'
        result = _split_matrix_line(line)
        assert result == ["GENE1", "1.23", "4.56"]

    def test_empty(self):
        result = _split_matrix_line("")
        assert result == [""]


# ─── SeriesMatrixFormatter ─────────────────────────────────────────────────────

class TestSeriesMatrixFormatter:
    def test_omics_type(self):
        f = SeriesMatrixFormatter()
        assert f.omics_type == "SeriesMatrix"

    def test_format_creates_directories(self, gse_dir):
        _write_archive(gse_dir)
        _write_series_matrix(gse_dir)

        f = SeriesMatrixFormatter()
        result = f.format(gse_dir)

        assert (gse_dir / "raw").exists()
        assert (gse_dir / "processed").exists()
        assert (gse_dir / "metadata").exists()

    def test_format_writes_expression_matrix(self, gse_dir):
        _write_series_matrix(gse_dir)

        f = SeriesMatrixFormatter()
        result = f.format(gse_dir)

        assert result.expression_matrix is not None
        expr = result.expression_matrix
        assert expr.exists()

        with open(expr, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            rows = list(reader)

        # header + 3 data rows
        assert len(rows) == 4
        assert rows[0][0] == "ID_REF"
        assert rows[1][0] == "GENE1"

    def test_format_with_gz_matrix(self, gse_dir):
        _write_series_matrix_gz(gse_dir)

        f = SeriesMatrixFormatter()
        result = f.format(gse_dir)

        assert result.expression_matrix is not None
        assert result.expression_matrix.exists()

    def test_format_writes_metadata_csv(self, gse_dir):
        _write_archive(gse_dir, sample_count=3)

        f = SeriesMatrixFormatter()
        result = f.format(gse_dir)

        assert result.metadata_file is not None
        assert result.metadata_file.exists()

        with open(result.metadata_file, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        assert len(rows) == 3
        assert "gsm_id" in rows[0]

    def test_format_nonexistent_dir(self, tmp_path):
        f = SeriesMatrixFormatter()
        result = f.format(tmp_path / "GSE_NONEXISTENT")

        assert not result.success
        assert result.errors


# ─── MicroarrayFormatter ───────────────────────────────────────────────────────

class TestMicroarrayFormatter:
    def test_omics_type(self):
        f = MicroarrayFormatter()
        assert f.omics_type == "Microarray"

    def test_moves_cel_files(self, gse_dir):
        # Create mock CEL files
        (gse_dir / "sample1.CEL").write_text("mock", encoding="utf-8")
        (gse_dir / "sample2.cel.gz").write_bytes(b"mock")
        _write_series_matrix(gse_dir)

        f = MicroarrayFormatter()
        result = f.format(gse_dir)

        assert (gse_dir / "raw" / "sample1.CEL").exists()
        assert (gse_dir / "raw" / "sample2.cel.gz").exists()

    def test_moves_series_matrix_to_processed(self, gse_dir):
        sm_path = _write_series_matrix(gse_dir)

        f = MicroarrayFormatter()
        result = f.format(gse_dir)

        assert (gse_dir / "processed" / sm_path.name).exists()


# ─── RNASeqFormatter ──────────────────────────────────────────────────────────

class TestRNASeqFormatter:
    def test_omics_type(self):
        f = RNASeqFormatter()
        assert f.omics_type == "RNA-seq"

    def test_moves_fastq_files(self, gse_dir):
        (gse_dir / "sample1.fastq.gz").write_bytes(b"mock")
        (gse_dir / "sample2.bam").write_bytes(b"mock")
        _write_series_matrix(gse_dir)

        f = RNASeqFormatter()
        result = f.format(gse_dir)

        assert (gse_dir / "raw" / "sample1.fastq.gz").exists()
        assert (gse_dir / "raw" / "sample2.bam").exists()

    def test_merge_count_files(self, gse_dir):
        # Create two per-sample count files
        (gse_dir / "sample1_counts.txt").write_text(
            "gene_id\tcount\nGENE1\t100\nGENE2\t200\n", encoding="utf-8"
        )
        (gse_dir / "sample2_counts.txt").write_text(
            "gene_id\tcount\nGENE1\t150\nGENE2\t250\n", encoding="utf-8"
        )

        f = RNASeqFormatter()
        result = f.format(gse_dir)

        assert result.expression_matrix is not None
        assert result.expression_matrix.exists()

        with open(result.expression_matrix, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        # 2 genes (header row skipped during merge)
        assert len(rows) == 2
        assert "gene_id" in rows[0]

    def test_single_count_matrix(self, gse_dir):
        # Single multi-sample count matrix
        content = "gene_id\tsample1\tsample2\nGENE1\t10\t20\nGENE2\t30\t40\n"
        (gse_dir / "counts_matrix.txt").write_text(content, encoding="utf-8")

        f = RNASeqFormatter()
        result = f.format(gse_dir)

        assert result.expression_matrix is not None
        assert result.expression_matrix.exists()


# ─── FormatterFactory ──────────────────────────────────────────────────────────

class TestFormatterFactory:
    @pytest.mark.parametrize("omics_type,expected_cls", [
        (OmicsType.MICROARRAY, MicroarrayFormatter),
        (OmicsType.METHYLATION_ARRAY, MicroarrayFormatter),
        (OmicsType.RNA_SEQ, RNASeqFormatter),
        (OmicsType.ATAC_SEQ, RNASeqFormatter),
        (OmicsType.CHIP_SEQ, RNASeqFormatter),
        (OmicsType.SINGLE_CELL_RNA_SEQ, RNASeqFormatter),
        (OmicsType.WGS, RNASeqFormatter),
        (OmicsType.OTHER, SeriesMatrixFormatter),
    ])
    def test_get_correct_formatter(self, omics_type, expected_cls):
        formatter = FormatterFactory.get(omics_type)
        assert isinstance(formatter, expected_cls)

    def test_get_from_string(self):
        formatter = FormatterFactory.get("RNA-seq")
        assert isinstance(formatter, RNASeqFormatter)

    def test_get_from_unknown_string(self):
        formatter = FormatterFactory.get("UnknownType")
        # Falls back to SeriesMatrixFormatter (OmicsType.OTHER)
        assert isinstance(formatter, SeriesMatrixFormatter)

    def test_get_all_types(self):
        types = FormatterFactory.get_all_types()
        assert OmicsType.RNA_SEQ in types
        assert OmicsType.MICROARRAY in types
        assert len(types) > 5
