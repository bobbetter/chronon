"""Unit tests for integration test template generation."""

import os

import pytest
from click.testing import CliRunner

from integration.helpers.cli import compile_configs
from integration.helpers.templates import (
    AWS_CONFIGS,
    AZURE_CONFIGS,
    GCP_CONFIGS,
    _apply_renames,
    _find_imported_stems,
    cleanup_test_configs,
    generate_test_configs,
    get_confs,
)

_canary_root = os.path.join(os.path.dirname(__file__), "canary")


class TestCompiledConfsExist:
    """Generate templates, compile, and verify all confs declared in ConfigTemplate are produced."""

    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", _canary_root)
        monkeypatch.setenv("ARTIFACT_PREFIX", "gs://test-artifacts")
        monkeypatch.setenv("CUSTOMER_ID", "test")
        monkeypatch.syspath_prepend(_canary_root)

    def _cleanup_compiled(self, tid):
        compiled_dir = os.path.join(_canary_root, "compiled")
        if not os.path.isdir(compiled_dir):
            return
        for root, _, files in os.walk(compiled_dir):
            for f in files:
                if tid in f:
                    os.remove(os.path.join(root, f))

    def _compile_and_check(self, cloud):
        tid = "ctest"
        confs = get_confs(cloud, tid)
        assert confs, f"No confs declared for {cloud} in ConfigTemplate.confs"
        try:
            generate_test_configs(tid, _canary_root, cloud=cloud)
            runner = CliRunner()
            compile_configs(runner, _canary_root, clean=False)

            compiled_dir = os.path.join(_canary_root, "compiled")
            # Collect all compiled files that contain the test_id so we can
            # show the developer what was actually produced.
            actually_produced = []
            for root, _, files in os.walk(compiled_dir):
                for f in files:
                    if tid in f:
                        actually_produced.append(
                            os.path.relpath(os.path.join(root, f), _canary_root)
                        )

            for base_conf, resolved_conf in sorted(confs.items()):
                path = os.path.join(_canary_root, resolved_conf)
                assert os.path.exists(path), (
                    f"\n"
                    f"Compiled conf not produced: {resolved_conf}\n"
                    f"  Expected because '{base_conf}' is declared in a\n"
                    f"  ConfigTemplate.confs list in integration/helpers/templates.py.\n"
                    f"\n"
                    f"  This usually means the source .py file was modified (e.g. a\n"
                    f"  variable was renamed or its version was bumped) but the confs\n"
                    f"  list was not updated to match.\n"
                    f"\n"
                    f"  To fix: open integration/helpers/templates.py, find the\n"
                    f"  ConfigTemplate whose source produces this conf, and update\n"
                    f"  its confs list to match the new compiled output.\n"
                    f"\n"
                    f"  Compiled confs actually produced for test_id='{tid}':\n"
                    + "\n".join(f"    - {p}" for p in sorted(actually_produced))
                )
        finally:
            cleanup_test_configs(tid, _canary_root, cloud=cloud)
            self._cleanup_compiled(tid)

    def test_gcp_confs(self):
        self._compile_and_check("gcp")

    def test_aws_confs(self):
        self._compile_and_check("aws")

    def test_azure_confs(self):
        self._compile_and_check("azure")


class TestConfigSourcesExist:
    """Ensure every ConfigTemplate.source points to a real file in the canary dir."""

    _SOURCE_MISSING_MSG = (
        "\n"
        "Source file missing: {source}\n"
        "  Expected at: {path}\n"
        "\n"
        "  This file is referenced by a ConfigTemplate in\n"
        "  integration/helpers/templates.py ({cloud}_CONFIGS).\n"
        "  Integration tests depend on this file to generate\n"
        "  test-isolated configs.\n"
        "\n"
        "  To fix: either restore the source file, or remove\n"
        "  the ConfigTemplate entry from {cloud}_CONFIGS in\n"
        "  integration/helpers/templates.py (and remove any\n"
        "  integration tests that reference its confs).\n"
    )

    @pytest.mark.parametrize(
        "cfg",
        GCP_CONFIGS,
        ids=[c.source for c in GCP_CONFIGS],
    )
    def test_gcp_source_exists(self, cfg):
        path = os.path.join(_canary_root, cfg.source)
        assert os.path.isfile(path), self._SOURCE_MISSING_MSG.format(
            source=cfg.source, path=path, cloud="GCP",
        )

    @pytest.mark.parametrize(
        "cfg",
        AWS_CONFIGS,
        ids=[c.source for c in AWS_CONFIGS],
    )
    def test_aws_source_exists(self, cfg):
        path = os.path.join(_canary_root, cfg.source)
        assert os.path.isfile(path), self._SOURCE_MISSING_MSG.format(
            source=cfg.source, path=path, cloud="AWS",
        )

    @pytest.mark.parametrize(
        "cfg",
        AZURE_CONFIGS,
        ids=[c.source for c in AZURE_CONFIGS],
    )
    def test_azure_source_exists(self, cfg):
        path = os.path.join(_canary_root, cfg.source)
        assert os.path.isfile(path), self._SOURCE_MISSING_MSG.format(
            source=cfg.source, path=path, cloud="AZURE",
        )


class TestRewriteImports:
    def test_simple_rename(self):
        content = "from staging_queries.gcp import purchases_import\nv = purchases_import.v1"
        renames = {"purchases_import": "purchases_import_abc123"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "import purchases_import_abc123" in result
        assert "purchases_import_abc123.v1" in result

    def test_multiple_renames(self):
        content = "import purchases_import, purchases_notds_import"
        renames = {
            "purchases_import": "purchases_import_xyz",
            "purchases_notds_import": "purchases_notds_import_xyz",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert result == "import purchases_import_xyz, purchases_notds_import_xyz"

    def test_word_boundary_prevents_partial_match(self):
        """purchases should not match inside purchases_import."""
        content = "from staging_queries.gcp import purchases_import, purchases_notds_import"
        renames = {"purchases": "purchases_test1"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        # purchases_import and purchases_notds_import should NOT be affected
        assert result == content

    def test_word_boundary_matches_standalone(self):
        """purchases should match when it appears as a standalone word."""
        content = "from group_bys.gcp import purchases\nJoinPart(group_by=purchases.v1_test)"
        renames = {"purchases": "purchases_test1"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "import purchases_test1" in result
        assert "purchases_test1.v1_test" in result

    def test_longest_first_ordering(self):
        """Longer names are replaced first to prevent double-suffixing."""
        content = "import checkouts_import, checkouts_notds_import\ncheckouts_import and checkouts_notds_import"
        renames = {
            "checkouts_import": "checkouts_import_t1",
            "checkouts_notds_import": "checkouts_notds_import_t1",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "checkouts_import_t1 and checkouts_notds_import_t1" in result

    def test_only_imported_stems_are_rewritten(self):
        """Variable names matching a module stem should not be rewritten
        if the file doesn't import that module."""
        content = (
            "from staging_queries.gcp import exports\n"
            "user_activities = exports.user_activities\n"
        )
        renames = {
            "exports": "exports_t1",
            "user_activities": "user_activities_t1",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        # exports is imported -> should be renamed
        assert "import exports_t1" in result
        assert "exports_t1.user_activities" in result
        # user_activities is NOT imported -> the variable name stays
        assert "user_activities = " in result
        assert "user_activities_t1 = " not in result

    def test_no_renames_passthrough(self):
        content = "some arbitrary content"
        result = _apply_renames(content, {})
        assert result == content

    def test_jinja2_literals_survive(self):
        """{{ start_date }} and {{ end_date }} pass through unchanged."""
        content = 'import some_module\nquery="SELECT * WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}"'
        found = _find_imported_stems(content, {"some_module": "some_module_test1"})
        result = _apply_renames(content, found)
        assert "{{ start_date }}" in result
        assert "{{ end_date }}" in result


class TestGenerateAndCleanup:
    def test_round_trip(self, tmp_path):
        """generate_test_configs renames files in-place, cleanup restores them."""
        # Set up a minimal canary structure
        sq_dir = tmp_path / "staging_queries" / "gcp"
        sq_dir.mkdir(parents=True)
        gb_dir = tmp_path / "group_bys" / "gcp"
        gb_dir.mkdir(parents=True)
        join_dir = tmp_path / "joins" / "gcp"
        join_dir.mkdir(parents=True)

        # Create minimal source files
        (sq_dir / "purchases_import.py").write_text("v1 = 'purchases_import'")
        (sq_dir / "purchases_notds_import.py").write_text("v1 = 'purchases_notds_import'")
        (sq_dir / "checkouts_import.py").write_text("v1 = 'checkouts_import'")
        (sq_dir / "checkouts_notds_import.py").write_text("v1 = 'checkouts_notds_import'")
        (sq_dir / "exports.py").write_text("v1 = 'exports'")
        (gb_dir / "purchases.py").write_text(
            "from staging_queries.gcp import purchases_import\nv1 = purchases_import"
        )
        (join_dir / "training_set.py").write_text(
            "from group_bys.gcp import purchases\nv1 = purchases"
        )
        (join_dir / "demo.py").write_text("v1 = 'demo'")

        tid = "abc12345"
        generated = generate_test_configs(tid, str(tmp_path), cloud="gcp")
        assert len(generated) == 8

        # Original files should exist
        assert (sq_dir / "exports.py").exists()
        assert (gb_dir / "purchases.py").exists()

        # Renamed files should exist
        assert (sq_dir / f"exports_{tid}.py").exists()
        assert (gb_dir / f"purchases_{tid}.py").exists()

        # Verify import renames propagated into file contents
        gb_content = (gb_dir / f"purchases_{tid}.py").read_text()
        assert f"purchases_import_{tid}" in gb_content

        join_content = (join_dir / f"training_set_{tid}.py").read_text()
        assert f"purchases_{tid}" in join_content

        # Cleanup should restore originals
        restored = cleanup_test_configs(tid, str(tmp_path), cloud="gcp")
        assert len(restored) == 8

        # Original files should be back
        assert (sq_dir / "exports.py").exists()
        assert (gb_dir / "purchases.py").exists()

        # Renamed files should be gone
        assert not (sq_dir / f"exports_{tid}.py").exists()
        assert not (gb_dir / f"purchases_{tid}.py").exists()

        # Import references should be reverted
        gb_content = (gb_dir / "purchases.py").read_text()
        assert "purchases_import" in gb_content
        assert f"purchases_import_{tid}" not in gb_content
