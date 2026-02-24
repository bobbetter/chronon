"""
Centralized theme module for the Zipline CLI.

Exports a shared Rich Console instance, named style constants,
semantic print helpers (suppressed when format=JSON), and a
status_spinner context manager.
"""

from contextlib import contextmanager

from rich.console import Console

from ai.chronon.cli.formatter import Format

# ── Shared console ────────────────────────────────────────────────────

console = Console()

# ── Style constants ───────────────────────────────────────────────────

STYLE_SUCCESS = "bold green"
STYLE_ERROR = "bold red"
STYLE_WARNING = "bold yellow"
STYLE_INFO = "cyan"
STYLE_DIM = "dim"
STYLE_ACCENT = "cyan"
STYLE_URL = "blue underline"
STYLE_KEY = "bold"


# ── Icon / label constants ────────────────────────────────────────────


class Icons:
    SUCCESS = f"[{STYLE_SUCCESS}]✅ SUCCESS[/]"
    ERROR = f"[{STYLE_ERROR}]❌ ERROR[/]"
    WARNING = f"[{STYLE_WARNING}]⚠️  WARNING[/]"
    INFO = f"[{STYLE_INFO}]ℹ️  INFO[/]"
    STEP = f"[{STYLE_DIM}].. [/]"


# ── Semantic print helpers ────────────────────────────────────────────
# All suppress output when format is JSON so that Rich markup never
# leaks into machine-readable output.


def print_success(msg, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"{Icons.SUCCESS} {msg}")


def print_error(msg, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"{Icons.ERROR} {msg}")


def print_warning(msg, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"{Icons.WARNING} {msg}")


def print_info(msg, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"{Icons.INFO} {msg}")


def print_step(msg, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"{Icons.STEP} {msg}")


def print_url(label, url, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"  [{STYLE_KEY}]{label}:[/{STYLE_KEY}] [{STYLE_URL}]{url}[/{STYLE_URL}]")


def print_key_value(key, value, format=Format.TEXT):
    if format == Format.JSON:
        return
    console.print(f"  [{STYLE_KEY}]{key}:[/{STYLE_KEY}] {value}")


# ── Spinner context manager ──────────────────────────────────────────


@contextmanager
def status_spinner(message, format=Format.TEXT):
    """Wrap a block with a Rich spinner. No-op when format is JSON."""
    if format == Format.JSON:
        yield
    else:
        with console.status(f"[{STYLE_INFO}]{message}[/{STYLE_INFO}]"):
            yield
