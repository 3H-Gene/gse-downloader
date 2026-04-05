"""GEO Query module for fetching data from NCBI.

This module handles querying GEO databases to retrieve metadata
and file information for GSE datasets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import requests

from gse_downloader.utils.logger import get_logger

logger = get_logger("geo_query")


@dataclass
class GEOFile:
    """Represents a downloadable file from GEO."""

    filename: str
    size: int = 0
    url: Optional[str] = None
    file_type: str = "supplementary"


@dataclass
class GSESeries:
    """Represents a GEO Series (GSE) record."""

    gse_id: str
    title: str = ""
    summary: str = ""
    overall_design: str = ""
    series_type: str = ""
    contributor: str = ""
    submission_date: str = ""
    last_update_date: str = ""
    pubmed_ids: list[str] = field(default_factory=list)
    bioproject_id: Optional[str] = None
    sra_id: Optional[str] = None
    platforms: list[str] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)
    organism: list[str] = field(default_factory=list)
    files: list[GEOFile] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    sample_count: int = 0


class GEOQuery:
    """Handles GEO database queries via NCBI E-utilities."""

    EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    GEO_BASE_URL = "https://www.ncbi.nlm.nih.gov/geo/"

    DEFAULT_HEADERS = {
        "User-Agent": "GSE-Downloader/1.0 (https://github.com/3H-Gene/gse-downloader)",
    }

    def __init__(self, email: str = "anonymous@example.com", api_key: Optional[str] = None):
        """Initialize GEO query client.

        Args:
            email: Email for NCBI (required by their policy)
            api_key: Optional NCBI API key for higher rate limits
        """
        self.email = email
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)

        logger.info("GEOQuery initialized")

    def _efetch(self, db: str, id: str, rettype: str = "abstract", retmode: str = "xml") -> str:
        """Fetch records from NCBI E-utilities.

        Args:
            db: Database name (gds, gse, gsm, gpl)
            id: Record ID
            rettype: Return type
            retmode: Return mode (xml or text)

        Returns:
            Response content
        """
        params = {
            "db": db,
            "id": id,
            "rettype": rettype,
            "retmode": retmode,
            "email": self.email,
        }

        if self.api_key:
            params["api_key"] = self.api_key

        url = f"{self.EUTILS_BASE_URL}efetch.fcgi"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Efetch failed for {db}:{id}: {e}")
            raise

    def _esearch(self, db: str, term: str, retmax: int = 20) -> list[str]:
        """Search NCBI databases.

        Args:
            db: Database name
            term: Search term
            retmax: Maximum results

        Returns:
            List of IDs
        """
        params = {
            "db": db,
            "term": term,
            "retmax": retmax,
            "email": self.email,
        }

        if self.api_key:
            params["api_key"] = self.api_key

        url = f"{self.EUTILS_BASE_URL}esearch.fcgi"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Parse IDs from XML response
            ids = re.findall(r"<Id>(\d+)</Id>", response.text)
            return ids
        except requests.exceptions.RequestException as e:
            logger.error(f"ESearch failed for {term}: {e}")
            return []

    def validate_gse_id(self, gse_id: str) -> tuple[bool, Optional[str]]:
        """Validate if a GSE ID exists and is accessible.

        Args:
            gse_id: GSE identifier (e.g., "GSE123456")

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if GSE exists and is public
            - error_message: Error description if invalid, None if valid
        """
        import re as re_module

        # Validate format BEFORE normalising case so we catch lowercase input
        if not re_module.match(r"^[Gg][Ss][Ee]\d+$", gse_id.strip()):
            return False, f"Invalid GSE ID format: {gse_id}. Expected format: GSE123456"

        gse_id = gse_id.upper().strip()

        try:
            # Use E-utilities to check if GSE exists
            url = f"{self.EUTILS_BASE_URL}esearch.fcgi"
            params = {
                "db": "gds",
                "term": gse_id,
                "retmode": "json",
                "email": self.email,
            }
            if self.api_key:
                params["api_key"] = self.api_key

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            count = data.get("esearchresult", {}).get("count", "0")

            if count == "0" or count == 0:
                return False, f"GSE {gse_id} not found in GEO database"

            return True, None

        except requests.exceptions.Timeout:
            return False, f"Timeout while validating {gse_id}. Please check your network connection."
        except requests.exceptions.RequestException as e:
            return False, f"Network error while validating {gse_id}: {str(e)}"
        except Exception as e:
            return False, f"Failed to validate {gse_id}: {str(e)}"

    def get_series_info(self, gse_id: str) -> GSESeries:
        """Get detailed information about a GSE series.

        Args:
            gse_id: GSE identifier (e.g., "GSE123456")

        Returns:
            GSESeries object with metadata
        """
        gse_id = gse_id.upper().strip()
        logger.info(f"Fetching info for {gse_id}")

        series = GSESeries(gse_id=gse_id)

        try:
            # Fetch GSE record
            content = self._efetch("gse", gse_id, rettype="full", retmode="text")

            # Parse fields from SOFT format
            series = self._parse_soft_series(content, gse_id)

            # Fetch related platforms and samples
            if series.platforms:
                # Get first platform for organism info
                platform = series.platforms[0]
                platform_info = self.get_platform_info(platform)
                if platform_info:
                    series.organism = platform_info.get("organism", [])

        except Exception as e:
            logger.error(f"Failed to fetch series info for {gse_id}: {e}")

        return series

    def _parse_soft_series(self, content: str, gse_id: str) -> GSESeries:
        """Parse SOFT format content for series info.

        Args:
            content: SOFT format text
            gse_id: GSE identifier

        Returns:
            GSESeries object
        """
        series = GSESeries(gse_id=gse_id)

        lines = content.split("\n")
        current_sample = None
        in_samples = False

        for line in lines:
            line = line.strip()

            if line.startswith("^SERIES"):
                in_samples = False
                match = re.search(r"(\d+)", line)
                if match:
                    series.gse_id = f"GSE{match.group(1)}"

            elif line.startswith("!Series_title"):
                series.title = self._extract_value(line)

            elif line.startswith("!Series_summary"):
                series.summary = self._extract_value(line)

            elif line.startswith("!Series_overall_design"):
                series.overall_design = self._extract_value(line)

            elif line.startswith("!Series_type"):
                series.series_type = self._extract_value(line)

            elif line.startswith("!Series_contributor"):
                series.contributor = self._extract_value(line)

            elif line.startswith("!Series_pubmed_id"):
                pubmed = self._extract_value(line)
                if pubmed:
                    series.pubmed_ids.append(pubmed)

            elif line.startswith("!Series_submission_date"):
                series.submission_date = self._extract_value(line)

            elif line.startswith("!Series_last_update_date"):
                series.last_update_date = self._extract_value(line)

            elif line.startswith("!Series_platform_id"):
                platform = self._extract_value(line)
                if platform and platform not in series.platforms:
                    series.platforms.append(platform)

            elif line.startswith("!Series_sample_id"):
                sample = self._extract_value(line)
                if sample and sample not in series.samples:
                    series.samples.append(sample)

            elif line.startswith("!Series_keyword"):
                kw = self._extract_value(line)
                if kw and kw not in series.keywords:
                    series.keywords.append(kw)

            elif line.startswith("^SAMPLE"):
                in_samples = True
                match = re.search(r"(GSM\d+)", line)
                if match:
                    current_sample = match.group(1)
                    if current_sample not in series.samples:
                        series.samples.append(current_sample)

            elif in_samples and line.startswith("!Sample_organism"):
                organism = self._extract_value(line)
                if organism and organism not in series.organism:
                    series.organism.append(organism)

        series.sample_count = len(series.samples)
        return series

    def _extract_value(self, line: str) -> str:
        """Extract value from SOFT format line.

        Args:
            line: SOFT format line

        Returns:
            Extracted value
        """
        if " = " in line:
            return line.split(" = ", 1)[1].strip().strip('"')
        return ""

    def get_platform_info(self, gpl_id: str) -> dict:
        """Get platform information.

        Args:
            gpl_id: GPL identifier

        Returns:
            Dictionary with platform info
        """
        gpl_id = gpl_id.upper().strip()

        try:
            content = self._efetch("gpl", gpl_id, rettype="full", retmode="text")

            info = {
                "gpl_id": gpl_id,
                "title": "",
                "organism": [],
                "technology": "",
            }

            lines = content.split("\n")
            for line in lines:
                line = line.strip()

                if line.startswith("!Platform_title"):
                    info["title"] = self._extract_value(line)

                elif line.startswith("!Platform_organism"):
                    organism = self._extract_value(line)
                    if organism:
                        info["organism"].append(organism)

                elif line.startswith("!Platform_technology"):
                    info["technology"] = self._extract_value(line)

            return info

        except Exception as e:
            logger.error(f"Failed to fetch platform info for {gpl_id}: {e}")
            return {"gpl_id": gpl_id}

    def get_sample_info(self, gsm_id: str) -> dict:
        """Get sample information.

        Args:
            gsm_id: GSM identifier

        Returns:
            Dictionary with sample info
        """
        gsm_id = gsm_id.upper().strip()

        try:
            content = self._efetch("gsm", gsm_id, rettype="full", retmode="text")

            info = {
                "gsm_id": gsm_id,
                "title": "",
                "source_name": "",
                "organism": "",
                "characteristics": {},
            }

            lines = content.split("\n")
            for line in lines:
                line = line.strip()

                if line.startswith("!Sample_title"):
                    info["title"] = self._extract_value(line)

                elif line.startswith("!Sample_source_name"):
                    info["source_name"] = self._extract_value(line)

                elif line.startswith("!Sample_organism"):
                    info["organism"] = self._extract_value(line)

                elif line.startswith("!Sample_characteristics"):
                    # Parse characteristics like "tissue: liver"
                    match = re.match(r"!Sample_characteristics_ch1\s+(.+)", line)
                    if match:
                        char_str = match.group(1)
                        if " = " in char_str:
                            key, value = char_str.split(" = ", 1)
                            info["characteristics"][key.strip()] = value.strip().strip('"')

            return info

        except Exception as e:
            logger.error(f"Failed to fetch sample info for {gsm_id}: {e}")
            return {"gsm_id": gsm_id}

    # ── FTP helper ────────────────────────────────────────────────────────────

    @staticmethod
    def _ftp_prefix(gse_id: str) -> str:
        """Return the FTP prefix folder for a GSE ID.

        Examples:
            GSE1      -> GSEnnn
            GSE1234   -> GSE1nnn
            GSE123456 -> GSE123nnn
        """
        num_part = gse_id[3:]
        if len(num_part) <= 3:
            return "GSEnnn"
        return f"GSE{num_part[:-3]}nnn"

    def _list_matrix_files(self, gse_id: str, base_ftp: str) -> list[dict]:
        """Query the GEO FTP /matrix/ directory and return all series_matrix files.

        GEO naming rules:
          - Single-platform GSE: ``{gse_id}_series_matrix.txt.gz``
          - Multi-platform GSE:  ``{gse_id}-{gpl_id}_series_matrix.txt.gz``

        This method resolves the actual filenames by listing the directory,
        so it works correctly for both cases.

        Args:
            gse_id: GSE identifier (already upper-cased)
            base_ftp: FTP base URL, e.g. ``https://ftp.ncbi.nlm.nih.gov/geo/series/GSE158nnn/GSE158702``

        Returns:
            List of file dicts with keys: filename, type, description, url.
            Falls back to the single-file guess if the directory listing fails.
        """
        matrix_url = f"{base_ftp}/matrix/"
        fallback = [{
            "filename": f"{gse_id}_series_matrix.txt.gz",
            "type": "series_matrix",
            "description": "Series Matrix file (gzip-compressed text)",
            "url": f"{matrix_url}{gse_id}_series_matrix.txt.gz",
        }]
        try:
            resp = self.session.get(matrix_url, timeout=30)
            if resp.status_code != 200:
                return fallback
            # Match any href that looks like a series_matrix file for this GSE
            pattern = re.compile(
                r'href="(' + re.escape(gse_id) + r'[^"]*_series_matrix\.txt(?:\.gz)?)"',
                re.IGNORECASE,
            )
            fnames = pattern.findall(resp.text)
            if not fnames:
                return fallback
            return [
                {
                    "filename": fname,
                    "type": "series_matrix",
                    "description": "Series Matrix file (gzip-compressed text)",
                    "url": f"{matrix_url}{fname}",
                }
                for fname in fnames
            ]
        except Exception as exc:
            logger.warning(f"Failed to list matrix files for {gse_id}: {exc}")
            return fallback

    def get_series_files(
        self,
        gse_id: str,
        file_types: Optional[list[str]] = None,
    ) -> list[dict]:
        """Get available download files for a GSE.

        Args:
            gse_id: GSE identifier
            file_types: Subset of ["soft", "matrix", "miniml", "supplementary"].
                        If None, all four types are returned.

        Returns:
            List of file info dictionaries
        """
        gse_id = gse_id.upper().strip()
        _all = file_types is None
        _want = set(file_types) if file_types else set()

        files = []

        FTP_HTTPS = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        base_ftp = f"{FTP_HTTPS}/{self._ftp_prefix(gse_id)}/{gse_id}"

        # 1. SOFT family file
        if _all or "soft" in _want:
            files.append({
                "filename": f"{gse_id}_family.soft.gz",
                "type": "soft",
                "description": "SOFT family file (gzip compressed)",
                "url": f"{base_ftp}/soft/{gse_id}_family.soft.gz",
            })

        # 2. Series matrix – query FTP directory to get actual filename(s)
        # (multi-platform GSEs use the pattern {gse_id}-{gpl}_series_matrix.txt.gz)
        if _all or "matrix" in _want or "series_matrix" in _want:
            files.extend(self._list_matrix_files(gse_id, base_ftp))

        # 3. MINiML tgz archive
        if _all or "miniml" in _want:
            files.append({
                "filename": f"{gse_id}_family.xml.tgz",
                "type": "miniml",
                "description": "MINiML format (tgz archive)",
                "url": f"{base_ftp}/miniml/{gse_id}_family.xml.tgz",
                "is_archive": True,
            })

        # 4. Supplementary files – query the GEO FTP listing page
        if _all or "supplementary" in _want or "suppl" in _want:
            try:
                suppl_url = f"{base_ftp}/suppl/"
                response = self.session.get(suppl_url, timeout=30)

                if response.status_code == 200:
                    content = response.text
                    matches = re.findall(r'href="([^/"]+\.[^"]+)"', content)
                    for fname in matches:
                        if fname.startswith("?") or fname in ("../", "/"):
                            continue
                        files.append({
                            "filename": fname,
                            "type": "supplementary",
                            "description": "Supplementary file",
                            "url": f"{suppl_url}{fname}",
                        })
            except Exception as e:
                logger.warning(f"Failed to get supplementary files for {gse_id}: {e}")

        return files

    def get_series_files_by_strategy(
        self,
        gse_id: str,
        omics_hint: str = "",
        include_sra: bool = False,
    ) -> list[dict]:
        """Multi-path download strategy: matrix → supplementary → SRA (opt-in).

        Priority logic
        --------------
        1. Always include: SOFT family (metadata backbone)
        2. Always include: Series Matrix (expression data for most datasets)
        3. Always include: Supplementary files if any exist on FTP
        4. SRA FASTQ/BAM: ONLY if ``include_sra=True`` (explicit opt-in)

        The function never silently fetches SRA data — bandwidth cost is too
        high and most downstream users only need the processed matrix.

        Args:
            gse_id: GSE identifier
            omics_hint: Optional omics type hint (e.g. "RNA-seq", "scRNA-seq").
                        Used to adjust supplementary file priority.
            include_sra: If True, append SRA run accession info to the file list
                         so the caller can decide whether to download raw reads.

        Returns:
            Ordered list of file info dicts, highest-priority first.
        """
        gse_id = gse_id.upper().strip()
        files: list[dict] = []

        FTP_HTTPS = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        base_ftp = f"{FTP_HTTPS}/{self._ftp_prefix(gse_id)}/{gse_id}"

        # ── Layer 1: SOFT (always) ────────────────────────────────────────────
        files.append({
            "filename": f"{gse_id}_family.soft.gz",
            "type": "soft",
            "priority": 1,
            "description": "SOFT family file (metadata + sample info)",
            "url": f"{base_ftp}/soft/{gse_id}_family.soft.gz",
        })

        # ── Layer 2: Series Matrix ────────────────────────────────────────────
        # Query FTP directory to discover actual filename(s).
        # Multi-platform GSEs use {gse_id}-{gpl}_series_matrix.txt.gz naming.
        for sm_file in self._list_matrix_files(gse_id, base_ftp):
            sm_file["priority"] = 2
            files.append(sm_file)

        # ── Layer 3: Supplementary files ─────────────────────────────────────
        # For sequencing assays the supplementary folder often contains count
        # matrices (*.count.gz, *_counts.txt.gz, *_matrix.mtx.gz …) which are
        # richer than the series matrix.
        try:
            suppl_url = f"{base_ftp}/suppl/"
            resp = self.session.get(suppl_url, timeout=30)
            if resp.status_code == 200:
                fnames = re.findall(r'href="([^/"]+\.[^"]+)"', resp.text)
                for fname in fnames:
                    if fname.startswith("?") or fname in ("../", "/"):
                        continue
                    # Assign slightly higher priority to count/matrix files
                    is_count = any(
                        kw in fname.lower()
                        for kw in ("count", "matrix", "expr", "tpm", "fpkm", "rpkm")
                    )
                    files.append({
                        "filename": fname,
                        "type": "supplementary",
                        "priority": 2 if is_count else 3,
                        "description": "Supplementary file",
                        "url": f"{suppl_url}{fname}",
                    })
        except Exception as exc:
            logger.warning(f"Failed to list supplementary files for {gse_id}: {exc}")

        # ── Layer 4: SRA (explicit opt-in only) ──────────────────────────────
        if include_sra:
            sra_entries = self._get_sra_run_info(gse_id)
            files.extend(sra_entries)

        # Sort by priority (ascending = higher priority first), then filename
        files.sort(key=lambda f: (f.get("priority", 9), f["filename"]))
        return files

    def _get_sra_run_info(self, gse_id: str) -> list[dict]:
        """Fetch SRA run accessions linked to a GSE.

        Returns lightweight dicts with type="sra_run" and no URL — the caller
        is responsible for constructing FASTQ/BAM download commands.
        Does NOT download any files itself.
        """
        try:
            # eSearch: find SRA UIDs linked to this GSE
            params = {
                "db": "sra",
                "term": f"{gse_id}[GSEL]",
                "retmax": 500,
                "retmode": "json",
                "email": self.email,
            }
            if self.api_key:
                params["api_key"] = self.api_key
            resp = self.session.get(
                f"{self.EUTILS_BASE_URL}esearch.fcgi",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            sra_ids = resp.json().get("esearchresult", {}).get("idlist", [])
            if not sra_ids:
                return []

            # eSummary: get run accessions
            sum_params = {
                "db": "sra",
                "id": ",".join(sra_ids[:200]),  # cap to avoid huge requests
                "retmode": "json",
                "email": self.email,
            }
            if self.api_key:
                sum_params["api_key"] = self.api_key
            resp2 = self.session.get(
                f"{self.EUTILS_BASE_URL}esummary.fcgi",
                params=sum_params,
                timeout=30,
            )
            resp2.raise_for_status()
            result = resp2.json().get("result", {})

            runs = []
            for uid in sra_ids[:200]:
                doc = result.get(uid, {})
                runs_str = doc.get("runs", "")
                # runs_str may look like "SRR123456,SRR123457"
                for acc in re.findall(r"(SRR\d+|ERR\d+|DRR\d+)", runs_str):
                    runs.append({
                        "filename": f"{acc}.fastq.gz",
                        "type": "sra_run",
                        "priority": 5,
                        "description": f"SRA run {acc} (opt-in, not auto-downloaded)",
                        "sra_accession": acc,
                        "url": None,   # caller must use fasterq-dump or prefetch
                    })
            logger.info(f"Found {len(runs)} SRA runs for {gse_id}")
            return runs

        except Exception as exc:
            logger.warning(f"Failed to fetch SRA runs for {gse_id}: {exc}")
            return []

    def search_series(self, term: str, retmax: int = 20) -> list[str]:
        """Search for GSE series.

        Args:
            term: Search term
            retmax: Maximum results

        Returns:
            List of GSE IDs
        """
        # Add GSE prefix to search term
        search_term = f"{term}[ALL] AND GSE[DATASET]"

        # NOTE: _esearch("gds", ...) returns GDS internal numeric UIDs, *not* GSE accession
        # numbers. The conversion f"GSE{id}" below works for the common case where the GDS
        # UID matches the GSE numeric suffix, but may occasionally return an incorrect
        # accession for very old or redirected entries.  Use search_series_detailed() for
        # verified GSE accessions via eSummary.
        ids = self._esearch("gds", search_term, retmax)

        # Convert to GSE IDs
        gse_ids = [f"GSE{id}" for id in ids]

        logger.info(f"Found {len(gse_ids)} series for '{term}'")

        return gse_ids

    def search_series_detailed(self, term: str, retmax: int = 10) -> list[dict]:
        """Search GEO and return structured metadata for each hit.

        Uses the GEO DataSets NCBI eSearch + eSummary pipeline.

        Args:
            term: Free-text search query
            retmax: Maximum number of results (default 10, max 100)

        Returns:
            List of dicts with keys: gse_id, title, summary, series_type,
            organisms, sample_count, submission_date, pubmed_ids
        """
        retmax = min(retmax, 100)
        search_term = f"{term}[ALL] AND GSE[DATASET]"

        # 1. eSearch → get GDS UIDs
        params = {
            "db": "gds",
            "term": search_term,
            "retmax": retmax,
            "retmode": "json",
            "email": self.email,
        }
        if self.api_key:
            params["api_key"] = self.api_key

        try:
            resp = self.session.get(
                f"{self.EUTILS_BASE_URL}esearch.fcgi",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            esearch_data = resp.json()
        except Exception as e:
            logger.error(f"eSearch failed: {e}")
            return []

        uids = esearch_data.get("esearchresult", {}).get("idlist", [])
        if not uids:
            return []

        # 2. eSummary → get summary for each UID
        summary_params = {
            "db": "gds",
            "id": ",".join(uids),
            "retmode": "json",
            "email": self.email,
        }
        if self.api_key:
            summary_params["api_key"] = self.api_key

        try:
            resp = self.session.get(
                f"{self.EUTILS_BASE_URL}esummary.fcgi",
                params=summary_params,
                timeout=30,
            )
            resp.raise_for_status()
            summary_data = resp.json()
        except Exception as e:
            logger.error(f"eSummary failed: {e}")
            return []

        results = []
        for uid in uids:
            doc = summary_data.get("result", {}).get(uid, {})
            if not doc:
                continue

            acc = doc.get("accession", "")
            # Only keep GSE records (entrytype == "GSE")
            if doc.get("entrytype", "").upper() != "GSE":
                continue

            entry = {
                "gse_id": acc,
                "title": doc.get("title", ""),
                "summary": doc.get("summary", ""),
                "series_type": doc.get("gdstype", ""),
                "organisms": [t.get("scientificname", "") for t in doc.get("taxon", [])],
                "sample_count": doc.get("n_samples", 0),
                "submission_date": doc.get("pdat", ""),
                "pubmed_ids": [str(p) for p in doc.get("pubmedids", [])],
                "platform": doc.get("GPL", ""),
            }
            results.append(entry)

        logger.info(f"search_series_detailed: {len(results)} GSE results for '{term}'")
        return results

    def close(self) -> None:
        """Close the session."""
        self.session.close()
