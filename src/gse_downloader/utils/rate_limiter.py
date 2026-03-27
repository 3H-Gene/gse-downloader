"""Rate limiter for GSE Downloader.

Provides token-bucket based rate limiting to avoid overwhelming NCBI servers.

Usage::

    limiter = RateLimiter(requests_per_second=2.0)
    limiter.acquire()   # blocks until allowed
    # ... make HTTP request ...
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("rate_limiter")


class RateLimiter:
    """Token bucket rate limiter.

    Allows a configurable number of requests per second. Excess requests
    are throttled by sleeping until a token is available.

    Args:
        requests_per_second: Maximum sustained request rate (default 2.0).
            NCBI recommends ≤ 3 requests/second without an API key.
        burst: Maximum burst size (default equals requests_per_second).
            Allows short bursts above the sustained rate.
    """

    def __init__(
        self,
        requests_per_second: float = 2.0,
        burst: Optional[float] = None,
    ):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.rate = requests_per_second
        self.burst = burst or requests_per_second
        self._tokens: float = self.burst
        self._last_check: float = time.monotonic()
        self._lock = threading.Lock()
        logger.debug(
            f"RateLimiter initialized: rate={self.rate}/s, burst={self.burst}"
        )

    def acquire(self, tokens: float = 1.0) -> float:
        """Block until *tokens* are available, then consume them.

        Args:
            tokens: Number of tokens to consume (default 1.0).

        Returns:
            Time waited in seconds.
        """
        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_check
                self._last_check = now

                # Refill tokens
                self._tokens = min(
                    self.burst,
                    self._tokens + elapsed * self.rate,
                )

                if self._tokens >= tokens:
                    self._tokens -= tokens
                    logger.debug(f"Token acquired. Remaining: {self._tokens:.2f}")
                    return waited

                # Calculate wait time
                wait_time = (tokens - self._tokens) / self.rate

            # Sleep outside the lock
            time.sleep(wait_time)
            waited += wait_time

    def reset(self) -> None:
        """Reset the limiter to a full bucket."""
        with self._lock:
            self._tokens = self.burst
            self._last_check = time.monotonic()

    @property
    def available_tokens(self) -> float:
        """Return current number of available tokens (approximate)."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_check
            return min(self.burst, self._tokens + elapsed * self.rate)


class NoopRateLimiter:
    """A rate limiter that does nothing (disabled rate limiting)."""

    def acquire(self, tokens: float = 1.0) -> float:  # noqa: ARG002
        return 0.0

    def reset(self) -> None:
        pass

    @property
    def available_tokens(self) -> float:
        return float("inf")
