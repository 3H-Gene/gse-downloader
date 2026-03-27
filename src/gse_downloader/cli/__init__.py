"""CLI module for GSE Downloader."""

from gse_downloader.cli.commands import app


def main():
    """Entry point for CLI."""
    app()


__all__ = ["app", "main"]
