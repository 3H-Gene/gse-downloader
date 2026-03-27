"""Tests for rate_limiter module."""

from __future__ import annotations

import time
import threading

import pytest

from gse_downloader.utils.rate_limiter import NoopRateLimiter, RateLimiter


class TestRateLimiter:
    def test_init_default(self):
        limiter = RateLimiter()
        assert limiter.rate == 2.0
        assert limiter.burst == 2.0

    def test_init_custom(self):
        limiter = RateLimiter(requests_per_second=5.0, burst=10.0)
        assert limiter.rate == 5.0
        assert limiter.burst == 10.0

    def test_invalid_rate(self):
        with pytest.raises(ValueError):
            RateLimiter(requests_per_second=0)
        with pytest.raises(ValueError):
            RateLimiter(requests_per_second=-1)

    def test_single_acquire_instant(self):
        """First acquire should be instant (tokens are full)."""
        limiter = RateLimiter(requests_per_second=10.0)
        t0 = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1, f"Expected near-instant acquire, got {elapsed:.3f}s"

    def test_burst_allows_multiple_immediate(self):
        """Burst capacity allows multiple acquires without wait."""
        limiter = RateLimiter(requests_per_second=10.0, burst=5.0)
        t0 = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1, f"Burst should not throttle: {elapsed:.3f}s"

    def test_rate_limiting_slows_down(self):
        """Acquiring more tokens than burst should take > 0 time."""
        limiter = RateLimiter(requests_per_second=20.0, burst=2.0)
        # Consume burst first
        limiter.acquire()
        limiter.acquire()
        # Next acquire must wait
        t0 = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - t0
        # At 20 rps, one token takes 0.05s; allow some tolerance
        assert elapsed > 0.02, f"Expected throttling, got {elapsed:.3f}s"

    def test_reset_refills_bucket(self):
        """After reset, tokens should be full again."""
        limiter = RateLimiter(requests_per_second=10.0, burst=2.0)
        # Drain
        limiter.acquire()
        limiter.acquire()
        # Reset
        limiter.reset()
        t0 = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.05, "After reset, acquire should be instant"

    def test_thread_safety(self):
        """Multiple threads acquiring simultaneously should not raise."""
        limiter = RateLimiter(requests_per_second=100.0, burst=100.0)
        errors = []

        def worker():
            try:
                for _ in range(10):
                    limiter.acquire()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread safety errors: {errors}"

    def test_available_tokens_not_negative(self):
        """available_tokens should never be negative."""
        limiter = RateLimiter(requests_per_second=1.0, burst=1.0)
        for _ in range(3):
            limiter.acquire()
        assert limiter.available_tokens >= 0


class TestNoopRateLimiter:
    def test_acquire_instant(self):
        limiter = NoopRateLimiter()
        t0 = time.monotonic()
        for _ in range(1000):
            limiter.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1

    def test_available_tokens_infinite(self):
        limiter = NoopRateLimiter()
        assert limiter.available_tokens == float("inf")

    def test_reset_no_error(self):
        limiter = NoopRateLimiter()
        limiter.reset()  # Should not raise
