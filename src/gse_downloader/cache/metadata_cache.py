"""Metadata cache for GSE Downloader.

Avoids redundant NCBI API calls by caching series metadata locally.
Cache entries expire after ``ttl_hours`` (default 72h).

Cache layout  (one JSON file per GSE ID):
  ~/.cache/gse_downloader/metadata/<GSE_ID>.json
    {
      "gse_id": "GSE12345",
      "cached_at": "<ISO datetime>",
      "ttl_hours": 72,
      "data": { ... }   <- the actual metadata dict
    }
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("cache.metadata")


class MetadataCache:
    """Simple file-based metadata cache.

    Parameters
    ----------
    cache_dir:
        Directory to store cache files.  Defaults to
        ``~/.cache/gse_downloader/metadata/``.
    ttl_hours:
        Time-to-live in hours.  After this the cache entry is stale and
        will be refreshed on next access.  Set to 0 to disable TTL
        (entries never expire).
    """

    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "gse_downloader" / "metadata"

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        ttl_hours: float = 72.0,
    ) -> None:
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.ttl_hours = ttl_hours
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, gse_id: str) -> Optional[dict]:
        """Return cached metadata for *gse_id*, or None if absent / stale."""
        path = self._path(gse_id)
        if not path.exists():
            return None
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))
            if self._is_stale(entry):
                logger.debug(f"Cache stale for {gse_id}, will refresh")
                return None
            logger.debug(f"Cache hit for {gse_id}")
            return entry.get("data")
        except Exception as exc:
            logger.warning(f"Failed to read cache for {gse_id}: {exc}")
            return None

    def set(self, gse_id: str, data: dict) -> None:
        """Store *data* in the cache for *gse_id*."""
        path = self._path(gse_id)
        entry = {
            "gse_id": gse_id,
            "cached_at": datetime.now().isoformat(),
            "ttl_hours": self.ttl_hours,
            "data": data,
        }
        try:
            path.write_text(
                json.dumps(entry, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.debug(f"Cached metadata for {gse_id} → {path}")
        except Exception as exc:
            logger.warning(f"Failed to write cache for {gse_id}: {exc}")

    def invalidate(self, gse_id: str) -> bool:
        """Delete the cache entry for *gse_id*.  Returns True if deleted."""
        path = self._path(gse_id)
        if path.exists():
            path.unlink()
            logger.debug(f"Invalidated cache for {gse_id}")
            return True
        return False

    def clear(self) -> int:
        """Delete all cache entries.  Returns the number of files deleted."""
        count = 0
        for p in self.cache_dir.glob("*.json"):
            p.unlink()
            count += 1
        logger.info(f"Cleared {count} cache entries from {self.cache_dir}")
        return count

    def clear_stale(self) -> int:
        """Delete only expired cache entries.  Returns count deleted."""
        count = 0
        for p in self.cache_dir.glob("*.json"):
            try:
                entry = json.loads(p.read_text(encoding="utf-8"))
                if self._is_stale(entry):
                    p.unlink()
                    count += 1
            except Exception:
                pass
        if count:
            logger.info(f"Cleared {count} stale cache entries")
        return count

    def stats(self) -> dict:
        """Return simple cache statistics."""
        total = 0
        stale = 0
        for p in self.cache_dir.glob("*.json"):
            total += 1
            try:
                entry = json.loads(p.read_text(encoding="utf-8"))
                if self._is_stale(entry):
                    stale += 1
            except Exception:
                stale += 1
        return {"total": total, "stale": stale, "fresh": total - stale, "dir": str(self.cache_dir)}

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _path(self, gse_id: str) -> Path:
        return self.cache_dir / f"{gse_id.upper()}.json"

    def _is_stale(self, entry: dict) -> bool:
        if self.ttl_hours <= 0:
            return False
        cached_at_str = entry.get("cached_at", "")
        if not cached_at_str:
            return True
        try:
            cached_at = datetime.fromisoformat(cached_at_str)
            ttl = timedelta(hours=self.ttl_hours)
            return datetime.now() - cached_at > ttl
        except ValueError:
            return True


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────

_default_cache: Optional[MetadataCache] = None


def get_metadata_cache(
    cache_dir: Optional[Path] = None,
    ttl_hours: float = 72.0,
) -> MetadataCache:
    """Return the module-level MetadataCache singleton.

    Creates it on first call with the supplied parameters.
    """
    global _default_cache
    if _default_cache is None:
        _default_cache = MetadataCache(cache_dir=cache_dir, ttl_hours=ttl_hours)
    else:
        effective_dir = Path(cache_dir) if cache_dir else MetadataCache.DEFAULT_CACHE_DIR
        if _default_cache.cache_dir.resolve() != effective_dir.resolve():
            logger.warning(
                "get_metadata_cache(cache_dir=%r) ignored: singleton already uses %r",
                cache_dir,
                _default_cache.cache_dir,
            )
        if _default_cache.ttl_hours != ttl_hours:
            logger.warning(
                "get_metadata_cache(ttl_hours=%s) ignored: singleton already uses ttl_hours=%s",
                ttl_hours,
                _default_cache.ttl_hours,
            )
    return _default_cache
