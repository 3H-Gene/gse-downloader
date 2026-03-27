"""Progress tracking utilities."""

from __future__ import annotations

import io
import sys
import time
from typing import Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    FileSizeColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from gse_downloader.utils.logger import get_logger

logger = get_logger("progress")


# ── Windows UTF-8 fix ──────────────────────────────────────────────────────
def _make_progress_console() -> Console:
    """Create a UTF-8-safe Rich Console for progress bars."""
    if sys.platform == "win32":
        if not isinstance(sys.stdout, io.TextIOWrapper) or sys.stdout.encoding.lower().replace("-", "") != "utf8":
            safe_stdout = io.TextIOWrapper(
                sys.stdout.buffer if hasattr(sys.stdout, "buffer") else open(sys.stdout.fileno(), "wb", closefd=False),
                encoding="utf-8",
                errors="replace",
                line_buffering=True,
            )
        else:
            safe_stdout = sys.stdout
        return Console(file=safe_stdout, highlight=False)
    return Console(highlight=False)
# ───────────────────────────────────────────────────────────────────────────


def _make_file_progress(console: Console) -> Progress:
    """Create a Rich Progress bar for per-file downloads."""
    return Progress(
        SpinnerColumn(spinner_name="dots2"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        DownloadColumn(),            # "77.2 MB / 87.5 MB"
        TransferSpeedColumn(),       # "1.23 MB/s"
        TimeRemainingColumn(),       # "ETA 0:01:23"
        TimeElapsedColumn(),         # "00:05:12"
        console=console,
        expand=False,
    )


class FileDownloadProgress:
    """Per-file progress bar. Use as a context manager around download loops.

    Usage::

        with FileDownloadProgress(filename, total_size, resume_from) as prog:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
                prog.advance(len(chunk))
    """

    def __init__(
        self,
        filename: str,
        total_size: int,
        resume_from: int = 0,
        show: bool = True,
    ):
        self.filename = filename
        self.total_size = total_size
        self.resume_from = resume_from
        self.show = show

        self._console: Optional[Console] = None
        self._progress: Optional[Progress] = None
        self._task_id: Optional[TaskID] = None
        self._start_time: float = time.monotonic()
        self._bytes_this_session: int = 0

    def __enter__(self) -> "FileDownloadProgress":
        if self.show:
            self._console = _make_progress_console()
            self._progress = _make_file_progress(self._console)
            self._progress.start()

            # Shorten filename for display
            label = self.filename if len(self.filename) <= 45 else "..." + self.filename[-42:]

            self._task_id = self._progress.add_task(
                description=label,
                total=self.total_size if self.total_size > 0 else None,
                completed=self.resume_from,
            )
        self._start_time = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._progress:
            # Mark as complete if no error
            if exc_type is None and self._task_id is not None:
                try:
                    task = next(t for t in self._progress.tasks if t.id == self._task_id)
                    if task.total is not None:
                        self._progress.update(self._task_id, completed=task.total)
                except StopIteration:
                    pass
            self._progress.stop()

    def advance(self, n_bytes: int) -> None:
        """Call this after writing each chunk."""
        self._bytes_this_session += n_bytes
        if self._progress and self._task_id is not None:
            self._progress.advance(self._task_id, n_bytes)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def speed_bps(self) -> float:
        """Average download speed in bytes/s for this session."""
        elapsed = self.elapsed
        return self._bytes_this_session / elapsed if elapsed > 0 else 0.0

    @property
    def downloaded_this_session(self) -> int:
        return self._bytes_this_session


class MultiFileProgress:
    """Manages a shared Rich Progress for multiple sequential file downloads.

    Shows one persistent progress bar per active file.  Call ``start_file``
    before downloading each file and ``finish_file`` when done.
    """

    def __init__(self, total_files: int, total_size: int = 0, show: bool = True):
        self.total_files = total_files
        self.total_size = total_size
        self.show = show

        self._console: Optional[Console] = None
        self._progress: Optional[Progress] = None
        self._overall_task: Optional[TaskID] = None   # overall bytes bar
        self._file_task: Optional[TaskID] = None      # current file bar
        self._files_done: int = 0
        self._bytes_done: int = 0

    def __enter__(self) -> "MultiFileProgress":
        if self.show:
            self._console = _make_progress_console()
            self._progress = Progress(
                SpinnerColumn(spinner_name="dots2"),
                TextColumn("[bold]{task.description}"),
                BarColumn(bar_width=28),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                TimeElapsedColumn(),
                console=self._console,
                expand=False,
            )
            self._progress.start()

            # Overall progress bar (tracks total bytes across all files)
            if self.total_size > 0:
                self._overall_task = self._progress.add_task(
                    description=f"[bold green]Total ({self.total_files} files)",
                    total=self.total_size,
                    completed=0,
                )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._progress:
            self._progress.stop()

    def start_file(self, filename: str, total_size: int, resume_from: int = 0) -> None:
        """Call before downloading a new file."""
        if not self._progress:
            return
        label = filename if len(filename) <= 42 else "..." + filename[-39:]
        self._file_task = self._progress.add_task(
            description=f"[cyan]{label}",
            total=total_size if total_size > 0 else None,
            completed=resume_from,
        )

    def advance(self, n_bytes: int) -> None:
        """Advance both per-file and overall progress."""
        if not self._progress:
            return
        if self._file_task is not None:
            self._progress.advance(self._file_task, n_bytes)
        if self._overall_task is not None:
            self._progress.advance(self._overall_task, n_bytes)

    def finish_file(self, filename: str, success: bool, size: int = 0) -> None:
        """Call after a file download finishes."""
        if not self._progress:
            return
        self._files_done += 1
        if self._file_task is not None:
            # Complete and remove per-file task
            # Use get_task to safely look up by TaskID
            try:
                task = next(t for t in self._progress.tasks if t.id == self._file_task)
                if task.total is not None:
                    self._progress.update(self._file_task, completed=task.total)
            except StopIteration:
                pass  # task already removed
            try:
                self._progress.remove_task(self._file_task)
            except Exception:
                pass
            self._file_task = None

        # Update overall description
        if self._overall_task is not None:
            self._progress.update(
                self._overall_task,
                description=f"[bold green]Total ({self._files_done}/{self.total_files} files)",
            )

    def log(self, msg: str) -> None:
        """Print a log line without breaking the progress display."""
        if self._progress:
            self._progress.console.log(msg)
        else:
            print(msg)


# ── Legacy compatibility ─────────────────────────────────────────────────────
class DownloadProgress:
    """Backwards-compatible single-task progress tracker (kept for old callers)."""

    def __init__(self, total_files: int, total_size: int, show_progress: bool = True):
        self.total_files = total_files
        self.total_size = total_size
        self.show_progress = show_progress
        self.progress: Optional[Progress] = None
        self.task_id: Optional[TaskID] = None
        self._downloaded_bytes = 0

    def __enter__(self) -> "DownloadProgress":
        if self.show_progress:
            _console = _make_progress_console()
            self.progress = Progress(
                SpinnerColumn(spinner_name="dots2"),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=_console,
            )
            self.progress.start()
            self.task_id = self.progress.add_task(
                f"[cyan]Downloading {self.total_files} files...",
                total=self.total_size,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.progress:
            self.progress.stop()

    def update(self, filename: str, downloaded: int, total: int) -> None:
        if self.progress and self.task_id is not None:
            self.progress.update(
                self.task_id,
                description=f"[cyan]{filename}",
                completed=downloaded,
            )

    def increment(self, bytes_downloaded: int) -> None:
        if self.progress and self.task_id is not None:
            current = self.progress.tasks[self.task_id].completed
            self.progress.update(self.task_id, completed=current + bytes_downloaded)

    def set_description(self, description: str) -> None:
        if self.progress and self.task_id is not None:
            self.progress.update(self.task_id, description=description)

    @property
    def downloaded_bytes(self) -> int:
        if self.progress and self.task_id is not None:
            return self.progress.tasks[self.task_id].completed
        return self._downloaded_bytes


class BatchProgress:
    """Progress tracker for batch operations."""

    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.description = description
        self.progress: Optional[Progress] = None
        self.task_id: Optional[TaskID] = None

    def __enter__(self) -> "BatchProgress":
        _console = _make_progress_console()
        self.progress = Progress(
            SpinnerColumn(spinner_name="dots2"),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[cyan]{task.fields[status]}"),
            console=_console,
        )
        self.progress.start()
        self.task_id = self.progress.add_task(
            self.description,
            total=self.total_items,
            status="",
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.progress:
            self.progress.stop()

    def update(self, description: Optional[str] = None, status: Optional[str] = None) -> None:
        if self.progress and self.task_id is not None:
            kwargs = {}
            if description:
                kwargs["description"] = description
            if status:
                kwargs["status"] = status
            if kwargs:
                self.progress.update(self.task_id, **kwargs)

    def increment(self, status: Optional[str] = None) -> None:
        if self.progress and self.task_id is not None:
            self.progress.advance(self.task_id)
            if status:
                self.progress.update(self.task_id, status=status)
