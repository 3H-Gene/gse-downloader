"""Tests for the new v1.1.0 modules:
  - core.input_schema
  - profiling.profiler
  - cache.metadata_cache
  - pipeline.pipeline (unit-level, no network calls)
"""

from __future__ import annotations

import csv
import gzip
import json
import os
import time
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# input_schema
# ─────────────────────────────────────────────────────────────────────────────

class TestInputSchema:

    def test_from_string_plain(self):
        from gse_downloader.core.input_schema import GseInput
        inp = GseInput.from_string("gse12345")
        assert inp.gse_id == "GSE12345"

    def test_from_dict_full(self):
        from gse_downloader.core.input_schema import GseInput
        d = {
            "gse_id": "GSE999",
            "title": "test",
            "omics_type": "RNA-seq",
            "sample_count": 10,
            "download_options": {"force": True, "include_sra": False},
        }
        inp = GseInput.from_dict(d)
        assert inp.gse_id == "GSE999"
        assert inp.omics_type == "RNA-seq"
        assert inp.sample_count == 10
        assert inp.options.force is True
        assert inp.options.include_sra is False

    def test_parse_single_string(self):
        from gse_downloader.core.input_schema import parse_input
        result = parse_input("GSE12345")
        assert len(result) == 1
        assert result[0].gse_id == "GSE12345"

    def test_parse_json_string_single(self):
        from gse_downloader.core.input_schema import parse_input
        raw = '{"gse_id": "GSE777", "omics_type": "scRNA-seq"}'
        result = parse_input(raw)
        assert result[0].gse_id == "GSE777"
        assert result[0].omics_type == "scRNA-seq"

    def test_parse_json_string_list(self):
        from gse_downloader.core.input_schema import parse_input
        raw = '[{"gse_id": "GSE1"}, {"gse_id": "GSE2"}]'
        result = parse_input(raw)
        assert len(result) == 2
        assert result[1].gse_id == "GSE2"

    def test_parse_list_of_dicts(self):
        from gse_downloader.core.input_schema import parse_input
        data = [{"gse_id": "GSE100"}, {"gse_id": "GSE200"}]
        result = parse_input(data)
        assert len(result) == 2

    def test_parse_list_of_strings(self):
        from gse_downloader.core.input_schema import parse_input
        result = parse_input(["GSE1", "GSE2", "GSE3"])
        assert len(result) == 3

    def test_parse_file_json(self, tmp_path):
        from gse_downloader.core.input_schema import parse_input
        f = tmp_path / "input.json"
        f.write_text('[{"gse_id": "GSE555"}]', encoding="utf-8")
        result = parse_input(f)
        assert result[0].gse_id == "GSE555"

    def test_parse_file_text(self, tmp_path):
        from gse_downloader.core.input_schema import parse_input
        f = tmp_path / "ids.txt"
        f.write_text("GSE1\nGSE2\nGSE3\n", encoding="utf-8")
        result = parse_input(f)
        assert len(result) == 3

    def test_parse_invalid_raises(self):
        from gse_downloader.core.input_schema import parse_input
        with pytest.raises((ValueError, Exception)):
            parse_input(12345)  # type: ignore[arg-type]

    def test_geo_search_skill_compat(self):
        """Verify compatibility with geo-search-skill output format."""
        from gse_downloader.core.input_schema import parse_input
        geo_search_output = [
            {
                "gse_id": "GSE12345",
                "title": "Lung cancer RNA-seq",
                "summary": "...",
                "organisms": "Homo sapiens",
                "series_type": "Expression profiling by high throughput sequencing",
                "n_samples": 24,
                "GPL": "GPL16791",
            }
        ]
        result = parse_input(geo_search_output)
        assert result[0].gse_id == "GSE12345"
        assert result[0].sample_count == 24
        assert result[0].platform == "GPL16791"


# ─────────────────────────────────────────────────────────────────────────────
# profiling.profiler
# ─────────────────────────────────────────────────────────────────────────────

def _make_matrix_csv(path: Path, n_genes: int = 5, n_samples: int = 3,
                     with_zeros: bool = False, with_missing: bool = False) -> None:
    """Helper: write a small expression matrix CSV."""
    samples = [f"GSM{i+1}" for i in range(n_samples)]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["gene_id"] + samples)
        for g in range(n_genes):
            row = [f"GENE{g+1:03d}"]
            for s in range(n_samples):
                if with_missing and g == 0 and s == 0:
                    row.append("")
                elif with_zeros and g == 1 and s == 1:
                    row.append("0")
                else:
                    row.append(str((g + 1) * (s + 1)))
            writer.writerow(row)


def _make_series_matrix_gz(path: Path, n_genes: int = 4, n_samples: int = 3) -> None:
    """Helper: write a minimal series_matrix.txt.gz."""
    samples = [f"GSM{i+1}" for i in range(n_samples)]
    lines = []
    lines.append(f"!Sample_geo_accession\t" + "\t".join(f'"{s}"' for s in samples))
    lines.append("!series_matrix_table_begin")
    lines.append("ID_REF\t" + "\t".join(f'"{s}"' for s in samples))
    for g in range(n_genes):
        vals = "\t".join(str((g + 1) * (s + 1)) for s in range(n_samples))
        lines.append(f"PROBE{g+1:03d}\t{vals}")
    lines.append("!series_matrix_table_end")
    content = "\n".join(lines).encode("utf-8")
    with gzip.open(path, "wb") as fh:
        fh.write(content)


class TestDataProfiler:

    def test_profile_csv_basic(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE1"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "counts.csv", n_genes=5, n_samples=3)

        profiler = DataProfiler()
        result = profiler.profile(gse_dir)

        assert result.success
        assert result.stats.gene_count == 5
        assert result.stats.sample_count == 3
        assert result.stats.total_cells == 15
        assert result.stats.missing_rate == 0.0
        assert result.matrix_file is not None
        assert result.matrix_file.exists()

    def test_profile_missing_values(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE2"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "expr.csv", n_genes=4, n_samples=4, with_missing=True)

        profiler = DataProfiler()
        result = profiler.profile(gse_dir)

        assert result.success
        assert result.stats.missing_count == 1
        assert result.stats.missing_rate > 0

    def test_profile_zero_rate(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE3"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "matrix.csv", n_genes=4, n_samples=4, with_zeros=True)

        result = DataProfiler().profile(gse_dir)
        assert result.stats.zero_count == 1

    def test_profile_series_matrix_gz(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE4"
        gse_dir.mkdir()
        _make_series_matrix_gz(gse_dir / "GSE4_series_matrix.txt.gz", n_genes=6, n_samples=4)

        result = DataProfiler().profile(gse_dir)
        assert result.success
        assert result.stats.gene_count == 6
        assert result.stats.sample_count == 4

    def test_profile_removes_duplicates(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE5"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        # Write CSV with duplicate gene
        p = processed / "dup.csv"
        with open(p, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["gene_id", "S1", "S2"])
            w.writerow(["GENE1", "10", "20"])
            w.writerow(["GENE1", "11", "21"])  # duplicate
            w.writerow(["GENE2", "30", "40"])

        result = DataProfiler().profile(gse_dir)
        assert result.stats.gene_count == 2
        assert result.stats.duplicate_genes_removed == 1

    def test_profile_removes_empty_rows(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE6"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        p = processed / "e.csv"
        with open(p, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["gene_id", "S1", "S2"])
            w.writerow(["GENE1", "10", "20"])
            w.writerow(["GENE2", "", ""])   # empty
        result = DataProfiler().profile(gse_dir)
        assert result.stats.gene_count == 1
        assert result.stats.empty_genes_removed == 1

    def test_profile_max_rows_truncation(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE7"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "big.csv", n_genes=100, n_samples=2)

        result = DataProfiler(max_rows=10).profile(gse_dir)
        assert result.stats.gene_count == 10
        assert any("Truncated" in w for w in result.warnings)

    def test_profile_nonexistent_dir(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        result = DataProfiler().profile(tmp_path / "DOES_NOT_EXIST")
        assert not result.success
        assert result.errors

    def test_sparsity_calculation(self, tmp_path):
        """Sparsity = (missing + zero) / total_cells."""
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE8"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "m.csv", n_genes=4, n_samples=4,
                         with_missing=True, with_zeros=True)
        result = DataProfiler().profile(gse_dir)
        # 1 missing + 1 zero = 2 non-expressed cells out of 16
        assert result.stats.sparsity == pytest.approx(2 / 16, abs=1e-6)

    def test_profiling_summary_json_written(self, tmp_path):
        from gse_downloader.profiling.profiler import DataProfiler
        gse_dir = tmp_path / "GSE9"
        gse_dir.mkdir()
        processed = gse_dir / "processed"
        processed.mkdir()
        _make_matrix_csv(processed / "c.csv", n_genes=3, n_samples=2)

        result = DataProfiler().profile(gse_dir)
        assert result.summary_file is not None
        assert result.summary_file.exists()
        data = json.loads(result.summary_file.read_text())
        assert data["gse_id"] == "GSE9"
        assert data["stats"]["gene_count"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# cache.metadata_cache
# ─────────────────────────────────────────────────────────────────────────────

class TestMetadataCache:

    def test_set_and_get(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        cache = MetadataCache(cache_dir=tmp_path, ttl_hours=24)
        data = {"title": "test", "sample_count": 5}
        cache.set("GSE123", data)
        result = cache.get("GSE123")
        assert result == data

    def test_miss_returns_none(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        cache = MetadataCache(cache_dir=tmp_path)
        assert cache.get("GSE999") is None

    def test_stale_entry_returns_none(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        import json as _json

        cache = MetadataCache(cache_dir=tmp_path, ttl_hours=0.001)  # 3.6 seconds TTL
        cache.set("GSE456", {"x": 1})
        time.sleep(0.02)  # wait ~20ms
        # Manually set cached_at to old timestamp
        path = tmp_path / "GSE456.json"
        entry = _json.loads(path.read_text())
        entry["cached_at"] = "2020-01-01T00:00:00"
        path.write_text(_json.dumps(entry))
        assert cache.get("GSE456") is None

    def test_invalidate(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        cache = MetadataCache(cache_dir=tmp_path)
        cache.set("GSE789", {"y": 2})
        assert cache.invalidate("GSE789") is True
        assert cache.get("GSE789") is None
        assert cache.invalidate("GSE789") is False  # already gone

    def test_clear(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        cache = MetadataCache(cache_dir=tmp_path)
        cache.set("GSE1", {})
        cache.set("GSE2", {})
        count = cache.clear()
        assert count == 2
        assert cache.get("GSE1") is None

    def test_stats(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        cache = MetadataCache(cache_dir=tmp_path, ttl_hours=100)
        cache.set("GSE1", {"a": 1})
        cache.set("GSE2", {"b": 2})
        stats = cache.stats()
        assert stats["total"] == 2
        assert stats["stale"] == 0
        assert stats["fresh"] == 2

    def test_ttl_zero_never_expires(self, tmp_path):
        from gse_downloader.cache.metadata_cache import MetadataCache
        import json as _json

        cache = MetadataCache(cache_dir=tmp_path, ttl_hours=0)
        cache.set("GSE_OLD", {"data": "old"})
        # Manually set an ancient timestamp
        path = tmp_path / "GSE_OLD.json"
        entry = _json.loads(path.read_text())
        entry["cached_at"] = "2000-01-01T00:00:00"
        path.write_text(_json.dumps(entry))
        # With ttl=0, should still return data
        result = cache.get("GSE_OLD")
        assert result == {"data": "old"}
