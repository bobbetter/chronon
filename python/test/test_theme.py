"""Tests for the centralized CLI theme module."""

from io import StringIO
from unittest.mock import patch

import pytest

from ai.chronon.cli.formatter import Format
from ai.chronon.cli.theme import (
    console,
    print_error,
    print_info,
    print_key_value,
    print_step,
    print_success,
    print_url,
    print_warning,
    status_spinner,
)


class TestThemeHelpers:
    """Verify that theme helpers respect the format flag."""

    @pytest.fixture(autouse=True)
    def _capture(self):
        """Redirect the shared console to a string buffer for assertions."""
        self.buf = StringIO()
        self._orig_file = console.file
        console.file = self.buf
        yield
        console.file = self._orig_file

    # ── JSON suppression ──────────────────────────────────────────────

    def test_print_success_suppressed_for_json(self):
        print_success("ok", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_error_suppressed_for_json(self):
        print_error("fail", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_warning_suppressed_for_json(self):
        print_warning("warn", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_info_suppressed_for_json(self):
        print_info("info", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_step_suppressed_for_json(self):
        print_step("step", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_url_suppressed_for_json(self):
        print_url("Link", "http://example.com", format=Format.JSON)
        assert self.buf.getvalue() == ""

    def test_print_key_value_suppressed_for_json(self):
        print_key_value("key", "value", format=Format.JSON)
        assert self.buf.getvalue() == ""

    # ── TEXT output ───────────────────────────────────────────────────

    def test_print_success_text(self):
        print_success("done", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "SUCCESS" in output
        assert "done" in output

    def test_print_error_text(self):
        print_error("broken", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "ERROR" in output
        assert "broken" in output

    def test_print_warning_text(self):
        print_warning("careful", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "WARNING" in output
        assert "careful" in output

    def test_print_info_text(self):
        print_info("note", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "INFO" in output
        assert "note" in output

    def test_print_step_text(self):
        print_step("working", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "working" in output

    def test_print_url_text(self):
        print_url("Docs", "http://docs.example.com", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "Docs" in output
        assert "http://docs.example.com" in output

    def test_print_key_value_text(self):
        print_key_value("Name", "Alice", format=Format.TEXT)
        output = self.buf.getvalue()
        assert "Name" in output
        assert "Alice" in output


class TestStatusSpinner:
    """Verify the status_spinner context manager."""

    def test_spinner_noop_for_json(self):
        """status_spinner should be a no-op when format is JSON."""
        entered = False
        with status_spinner("loading...", format=Format.JSON):
            entered = True
        assert entered

    def test_spinner_runs_for_text(self):
        """status_spinner should execute the body for TEXT format."""
        executed = False
        with status_spinner("loading...", format=Format.TEXT):
            executed = True
        assert executed
