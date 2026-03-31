"""Standardized input interface for GSE Downloader.

Defines the canonical input model accepted by all download/pipeline entry points.
Supports two modes:
  - Simple mode:      just a GSE ID string (e.g. "GSE12345")
  - Structured mode:  a JSON dict / list compatible with geo-search-skill output

JSON structure (single dataset):
  {
    "gse_id": "GSE12345",
    "title": "...",           # optional, informational
    "organism": "...",        # optional hint for omics detection
    "omics_type": "RNA-seq",  # optional hint
    "series_type": "...",     # optional
    "download_options": {
      "file_types": ["matrix", "supplementary"],   # optional
      "include_sra": false,                        # opt-in SRA download
      "force": false
    }
  }

JSON list (batch):
  [{"gse_id": "GSE12345", ...}, {"gse_id": "GSE67890", ...}]
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union


@dataclass
class DownloadOptions:
    """Per-dataset download options."""

    # Which file types to fetch: "matrix", "soft", "miniml", "supplementary"
    file_types: list[str] = field(default_factory=lambda: ["soft", "matrix", "supplementary"])
    # Opt-in SRA download — NEVER default True
    include_sra: bool = False
    # Force re-download even if already completed
    force: bool = False
    # Override output directory for this specific dataset
    output_dir: Optional[Path] = None

    @classmethod
    def from_dict(cls, d: dict) -> "DownloadOptions":
        opts = cls()
        if "file_types" in d:
            opts.file_types = [str(t) for t in d["file_types"]]
        if "include_sra" in d:
            opts.include_sra = bool(d["include_sra"])
        if "force" in d:
            opts.force = bool(d["force"])
        if "output_dir" in d:
            opts.output_dir = Path(d["output_dir"])
        return opts


@dataclass
class GseInput:
    """Canonical input model for a single GSE dataset."""

    gse_id: str
    # Optional informational / hint fields (may speed up omics detection)
    title: str = ""
    summary: str = ""
    organism: str = ""
    omics_type: str = ""       # hint — not enforced, used to skip detection if confident
    series_type: str = ""
    sample_count: int = 0
    platform: str = ""
    # Download-level options
    options: DownloadOptions = field(default_factory=DownloadOptions)

    def __post_init__(self) -> None:
        self.gse_id = self.gse_id.upper().strip()

    @classmethod
    def from_dict(cls, d: dict) -> "GseInput":
        """Parse from a dict (e.g. geo-search-skill JSON output)."""
        opts_raw = d.get("download_options", d.get("options", {}))
        opts = DownloadOptions.from_dict(opts_raw) if isinstance(opts_raw, dict) else DownloadOptions()
        return cls(
            gse_id=d.get("gse_id", d.get("accession", "")),
            title=d.get("title", ""),
            summary=d.get("summary", ""),
            organism=str(d.get("organism", d.get("organisms", ""))),
            omics_type=str(d.get("omics_type", d.get("series_type", ""))),
            series_type=d.get("series_type", ""),
            sample_count=int(d.get("sample_count", d.get("n_samples", 0))),
            platform=str(d.get("platform", d.get("GPL", ""))),
            options=opts,
        )

    @classmethod
    def from_string(cls, gse_id: str) -> "GseInput":
        """Create from a plain GSE ID string."""
        return cls(gse_id=gse_id)

    def to_dict(self) -> dict:
        return {
            "gse_id": self.gse_id,
            "title": self.title,
            "summary": self.summary,
            "organism": self.organism,
            "omics_type": self.omics_type,
            "series_type": self.series_type,
            "sample_count": self.sample_count,
            "platform": self.platform,
        }


def parse_input(raw: Union[str, dict, list, Path]) -> list[GseInput]:
    """Parse any supported input format into a list of GseInput objects.

    Accepted formats
    ----------------
    - ``str``  : plain GSE ID (e.g. "GSE12345") OR JSON string
    - ``dict`` : single dataset dict
    - ``list`` : list of dataset dicts or GSE ID strings
    - ``Path`` : path to a JSON file or a plain text file (one GSE ID per line)

    Returns
    -------
    list[GseInput]  — never empty; raises ValueError on parse failure.
    """
    if isinstance(raw, Path):
        raw = _load_path(raw)

    if isinstance(raw, str):
        raw = raw.strip()
        # Try to parse as JSON first
        if raw.startswith("{") or raw.startswith("["):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Failed to parse JSON input: {exc}") from exc
        else:
            # Plain GSE ID or newline-separated list
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            if all(_looks_like_gse_id(ln) for ln in lines):
                return [GseInput.from_string(ln) for ln in lines]
            raise ValueError(f"Cannot parse input string: {raw!r}")

    if isinstance(raw, dict):
        return [GseInput.from_dict(raw)]

    if isinstance(raw, list):
        result = []
        for item in raw:
            if isinstance(item, str) and _looks_like_gse_id(item):
                result.append(GseInput.from_string(item))
            elif isinstance(item, dict):
                result.append(GseInput.from_dict(item))
            else:
                raise ValueError(f"Unsupported list item type: {type(item)} — {item!r}")
        return result

    raise ValueError(f"Unsupported input type: {type(raw)}")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _looks_like_gse_id(s: str) -> bool:
    import re
    return bool(re.match(r"^[Gg][Ss][Ee]\d+$", s.strip()))


def _load_path(path: Path) -> Union[str, dict, list]:
    """Load a file and return its parsed content."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    content = path.read_text(encoding="utf-8").strip()
    # JSON file
    if path.suffix.lower() == ".json" or content.startswith(("{", "[")):
        return json.loads(content)
    # Plain text: one GSE ID per line
    return content
