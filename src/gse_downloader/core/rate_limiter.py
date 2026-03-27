"""Rate limiter module for GSE Downloader.

This module handles download rate limiting to avoid overwhelming servers.
"""

from __future__ import annotations

import time
from threading import Lock
from typing import Optional

from gse_downloader.utils.logger import get_logger

logger = get_logger("rate_limiter")


class RateLimiter:
    """Rate limiter for controlling download speed."""

    def __init__(self, max_rate: str = "10MB/s"):
        """Initialize rate limiter.

        Args:
            max_rate: Maximum rate in format like "10MB/s", "1GB/s"

        Raises:
            ValueError: If rate format is invalid
        """
        self.max_rate = self._parse_rate(max_rate)
        self.used_bytes = 0
        self.window_start = time.time()
        self.lock = Lock()

        logger.info(f"Rate limiter initialized: {max_rate}")

    @staticmethod
    def _parse_rate(rate_str: str) -> float:
        """Parse rate string to bytes per second.

        Args:
            rate_str: Rate string like "10MB/s", "1GB/s"

        Returns:
            Rate in bytes per second
        """
        rate_str = rate_str.strip().upper()

        units = {
            "B/S": 1,
            "KB/S": 1024,
            "MB/S": 1024 * 1024,
            "GB/S": 1024 * 1024 * 1024,
        }

        for unit, multiplier in units.items():
            if rate_str.endswith(unit):
                value = float(rate_str[: -len(unit)])
                return value * multiplier

        raise ValueError(f"Invalid rate format: {rate_str}")

    def acquire(self, bytes_requested: int) -> None:
        """Acquire permission to download bytes.

        This will block if the rate limit would be exceeded.

        Args:
            bytes_requested: Number of bytes requested
        """
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.window_start

            # Reset window if expired (1 second)
            if elapsed >= 1.0:
                self.used_bytes = 0
                self.window_start = current_time
                elapsed = 0

            # Calculate available bytes
            available = self.max_rate - self.used_bytes

            # If we would exceed the limit, wait
            if bytes_requested > available:
                wait_time = 1.0 - elapsed
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    self.used_bytes = 0
                    self.window_start = time.time()

            # Update used bytes
            self.used_bytes += bytes_requested

    def update(self, bytes_downloaded: int) -> None:
        """Update used bytes count.

        Args:
            bytes_downloaded: Number of bytes downloaded
        """
        with self.lock:
            self.used_bytes += bytes_downloaded

            # Check if we need to wait
            current_time = time.time()
            elapsed = current_time - self.window_start

            if elapsed < 1.0:
                if self.used_bytes >= self.max_rate:
                    wait_time = 1.0 - elapsed
                    if wait_time > 0:
                        logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                        time.sleep(wait_time)
                    self.used_bytes = 0
                    self.window_start = time.time()

    def reset(self) -> None:
        """Reset the rate limiter."""
        with self.lock:
            self.used_bytes = 0
            self.window_start = time.time()


class TokenBucket:
    """Token bucket algorithm for rate limiting."""

    def __init__(self, rate: float, capacity: Optional[float] = None):
        """Initialize token bucket.

        Args:
            rate: Token refill rate in tokens per second
            capacity: Maximum bucket capacity (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_refill = time.time()
        self.lock = Lock()

    def acquire(self, tokens: float = 1.0) -> bool:
        """Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            True if tokens were acquired, False otherwise
        """
        with self.lock:
            self._refill()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            return False

    def wait_for_tokens(self, tokens: float = 1.0) -> None:
        """Wait until tokens are available.

        Args:
            tokens: Number of tokens to acquire
        """
        while True:
            if self.acquire(tokens):
                return
            # Calculate wait time
            needed = tokens - self.tokens
            wait_time = needed / self.rate
            time.sleep(wait_time)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        current_time = time.time()
        elapsed = current_time - self.last_refill
        self.last_refill = current_time

        # Add tokens based on elapsed time
        new_tokens = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on server responses."""

    def __init__(
        self,
        initial_rate: str = "10MB/s",
        min_rate: str = "1MB/s",
        max_rate: str = "50MB/s",
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ):
        """Initialize adaptive rate limiter.

        Args:
            initial_rate: Initial rate limit
            min_rate: Minimum rate limit
            max_rate: Maximum rate limit
            increase_factor: Factor to increase rate on success
            decrease_factor: Factor to decrease rate on failure
        """
        self.current_rate = self._parse_rate(initial_rate)
        self.min_rate = self._parse_rate(min_rate)
        self.max_rate = self._parse_rate(max_rate)
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.success_threshold = 5
        self.failure_threshold = 1
        self.lock = Lock()

        logger.info(f"Adaptive rate limiter initialized: {initial_rate}")

    @staticmethod
    def _parse_rate(rate_str: str) -> float:
        """Parse rate string to bytes per second."""
        rate_str = rate_str.strip().upper()

        units = {
            "B/S": 1,
            "KB/S": 1024,
            "MB/S": 1024 * 1024,
            "GB/S": 1024 * 1024 * 1024,
        }

        for unit, multiplier in units.items():
            if rate_str.endswith(unit):
                value = float(rate_str[: -len(unit)])
                return value * multiplier

        raise ValueError(f"Invalid rate format: {rate_str}")

    def record_success(self) -> None:
        """Record a successful download."""
        with self.lock:
            self.consecutive_successes += 1
            self.consecutive_failures = 0

            if self.consecutive_successes >= self.success_threshold:
                new_rate = min(self.current_rate * self.increase_factor, self.max_rate)
                if new_rate > self.current_rate:
                    logger.info(f"Increasing rate: {self.current_rate:.0f} -> {new_rate:.0f} bytes/s")
                    self.current_rate = new_rate
                self.consecutive_successes = 0

    def record_failure(self) -> None:
        """Record a failed download."""
        with self.lock:
            self.consecutive_failures += 1
            self.consecutive_successes = 0

            if self.consecutive_failures >= self.failure_threshold:
                new_rate = max(self.current_rate * self.decrease_factor, self.min_rate)
                if new_rate < self.current_rate:
                    logger.warning(f"Decreasing rate: {self.current_rate:.0f} -> {new_rate:.0f} bytes/s")
                    self.current_rate = new_rate
                self.consecutive_failures = 0

    def get_current_rate(self) -> float:
        """Get current rate limit.

        Returns:
            Current rate in bytes per second
        """
        with self.lock:
            return self.current_rate

    def get_current_rate_str(self) -> str:
        """Get current rate limit as formatted string.

        Returns:
            Rate string like "10MB/s"
        """
        rate = self.get_current_rate()
        if rate >= 1024 * 1024:
            return f"{rate / (1024 * 1024):.1f}MB/s"
        elif rate >= 1024:
            return f"{rate / 1024:.1f}KB/s"
        return f"{rate:.0f}B/s"
