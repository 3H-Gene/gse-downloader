"""Configuration management for GSE Downloader.

This module handles loading and managing configuration from TOML files
and environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError:
        tomllib = None  # type: ignore[assignment]

from pydantic import BaseModel, Field


class DownloadConfig(BaseModel):
    """Download configuration."""

    output_dir: Path = Field(default=Path("./gse_data"), description="Output directory")
    max_workers: int = Field(default=4, ge=1, le=32, description="Max concurrent downloads")
    retry_times: int = Field(default=3, ge=0, description="Number of retry attempts")
    timeout: int = Field(default=300, ge=30, description="Request timeout in seconds")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    auto_resume: bool = Field(default=True, description="Auto detect and resume incomplete downloads")


class SpeedLimitConfig(BaseModel):
    """Speed limit configuration."""

    enabled: bool = Field(default=False, description="Enable speed limiting")
    max_rate: str = Field(default="10MB/s", description="Max download rate")


class ChecksumConfig(BaseModel):
    """Checksum configuration."""

    enabled: bool = Field(default=True, description="Enable checksum verification")
    algorithm: str = Field(default="md5", description="Checksum algorithm (md5 or sha256)")


class FilesConfig(BaseModel):
    """Files configuration."""

    file_types: list[str] = Field(
        default=["soft", "series_matrix", "miniml"],
        description="File types to download",
    )


class ArchiveConfig(BaseModel):
    """Archive configuration."""

    generate_json: bool = Field(default=True, description="Generate JSON archive")
    generate_readme: bool = Field(default=True, description="Generate README file")
    include_samples: bool = Field(default=True, description="Include sample details")
    include_tissue: bool = Field(default=True, description="Include tissue information")


class NormalizationConfig(BaseModel):
    """Data normalization configuration."""

    enabled: bool = Field(default=True, description="Enable data normalization")
    normalize_matrix: bool = Field(default=True, description="Normalize expression matrix")
    standardize_names: bool = Field(default=True, description="Standardize file names")


class StatsConfig(BaseModel):
    """Statistics configuration."""

    default_view: str = Field(default="summary", description="Default stats view")
    group_by: list[str] = Field(
        default=["organism", "omics_type"],
        description="Default grouping dimensions",
    )


class Config(BaseModel):
    """Main configuration class."""

    download: DownloadConfig = Field(default_factory=DownloadConfig)
    speed_limit: SpeedLimitConfig = Field(default_factory=SpeedLimitConfig)
    checksum: ChecksumConfig = Field(default_factory=ChecksumConfig)
    files: FilesConfig = Field(default_factory=FilesConfig)
    archive: ArchiveConfig = Field(default_factory=ArchiveConfig)
    normalization: NormalizationConfig = Field(default_factory=NormalizationConfig)
    stats: StatsConfig = Field(default_factory=StatsConfig)

    @classmethod
    def from_file(cls, path: Path | str) -> Config:
        """Load configuration from TOML file.

        Args:
            path: Path to TOML configuration file

        Returns:
            Config instance
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        if tomllib is None:
            raise ImportError("tomllib/tomli is required to load TOML config files")

        with open(path, "rb") as f:
            data = tomllib.load(f)

        return cls(**data)

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from environment variables.

        Environment variables:
            GSE_OUTPUT_DIR: Output directory
            GSE_MAX_WORKERS: Max concurrent downloads
            GSE_TIMEOUT: Request timeout

        Returns:
            Config instance
        """
        data: dict = {}

        if output_dir := os.getenv("GSE_OUTPUT_DIR"):
            data["download"] = {"output_dir": output_dir}
        if max_workers := os.getenv("GSE_MAX_WORKERS"):
            data["download"] = data.get("download", {})
            data["download"]["max_workers"] = int(max_workers)
        if timeout := os.getenv("GSE_TIMEOUT"):
            data["download"] = data.get("download", {})
            data["download"]["timeout"] = int(timeout)

        return cls(**data) if data else cls()

    def to_file(self, path: Path | str) -> None:
        """Save configuration to TOML file.

        Args:
            path: Path to save TOML file
        """
        import tomli_w

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # mode="json" ensures Path and other types are TOML-serializable
        data = self.model_dump(mode="json")
        with open(path, "wb") as f:
            tomli_w.dump(data, f)

    def get_output_dir(self, gse_id: str) -> Path:
        """Get output directory for a specific GSE.

        Args:
            gse_id: GSE identifier

        Returns:
            Path to GSE directory
        """
        return self.download.output_dir / gse_id

    def ensure_output_dir(self, gse_id: str) -> Path:
        """Ensure output directory exists for a specific GSE.

        Args:
            gse_id: GSE identifier

        Returns:
            Path to GSE directory
        """
        output_dir = self.get_output_dir(gse_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir


def load_config(config_path: Optional[Path | str] = None) -> Config:
    """Load configuration from file or environment.

    Priority: config_file > environment > defaults

    Args:
        config_path: Path to configuration file

    Returns:
        Config instance
    """
    if config_path:
        return Config.from_file(config_path)

    # Try default locations
    default_locations = [
        Path("config.toml"),
        Path.home() / ".config" / "gse_downloader" / "config.toml",
        Path.home() / ".gse_downloader.toml",
    ]

    for loc in default_locations:
        if loc.exists():
            return Config.from_file(loc)

    # Try environment
    return Config.from_env()


# Global default config instance
_default_config: Optional[Config] = None


def get_config() -> Config:
    """Get the default configuration instance.

    Returns:
        Config instance
    """
    global _default_config
    if _default_config is None:
        _default_config = Config()
    return _default_config


def set_config(config: Config) -> None:
    """Set the default configuration instance.

    Args:
        config: Config instance to set as default
    """
    global _default_config
    _default_config = config
