"""Profiling module for GSE Downloader.

Provides structural normalization and basic statistics for GEO datasets.
This module deliberately stays within strict boundaries:

  Allowed:
    - Convert raw files to a 2-D genes × samples matrix
    - Align gene identifiers across samples
    - Remove fully empty rows / duplicate gene entries
    - Compute sample_count, gene_count, missing_rate, zero_rate, sparsity
    - Write metadata.csv and profiling_summary.json

  NOT allowed (out of scope):
    - TPM / CPM / RPKM / log2 / log1p transformation
    - Scaling, centering, or any form of batch correction
    - Clustering, PCA, or any dimensionality reduction
    - Differential expression analysis
    - Any modification that changes the biological meaning of values
"""

from gse_downloader.profiling.profiler import DataProfiler, ProfilingResult

__all__ = ["DataProfiler", "ProfilingResult"]
