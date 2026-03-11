"""Generate test config files from source configs with test_id isolation.

The strategy is to *rename* source files in-place so that the compiler
produces compiled configs whose internal names contain the test_id hash.
After compilation the original filenames are restored.

For example, ``exports.py`` is temporarily renamed to ``exports_abc123.py``.
Any other source file that imports ``exports`` gets its import rewritten to
``exports_abc123`` so the dependency chain stays consistent.
"""

import glob
import logging
import os
import re
import shutil
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ConfigTemplate:
    source: str  # relative path to source .py (e.g. "group_bys/gcp/purchases.py")
    # base compiled conf paths this source produces (e.g. "compiled/joins/gcp/demo.v1__1").
    # These are the keys integration tests use: confs["compiled/joins/gcp/demo.v1__1"]
    confs: list[str] = field(default_factory=list)

    @property
    def stem(self) -> str:
        """Module name derived from source filename."""
        return os.path.splitext(os.path.basename(self.source))[0]

    @property
    def renamed_source(self) -> str:
        """Source path template with test_id inserted."""
        return self.source.replace(".py", "_{test_id}.py")

    def resolve_confs(self, test_id: str) -> dict[str, str]:
        """Map base compiled conf path -> test_id-resolved path."""
        result = {}
        for base_conf in self.confs:
            resolved = base_conf.replace(f"{self.stem}.", f"{self.stem}_{test_id}.")
            result[base_conf] = resolved
        return result


GCP_CONFIGS = [
    ConfigTemplate(
        source="staging_queries/gcp/purchases_import.py",
        confs=["compiled/staging_queries/gcp/purchases_import.v1__0"],
    ),
    ConfigTemplate(
        source="staging_queries/gcp/purchases_notds_import.py",
        confs=["compiled/staging_queries/gcp/purchases_notds_import.v1__0"],
    ),
    ConfigTemplate(
        source="staging_queries/gcp/checkouts_import.py",
        confs=["compiled/staging_queries/gcp/checkouts_import.v1__0"],
    ),
    ConfigTemplate(
        source="staging_queries/gcp/checkouts_notds_import.py",
        confs=["compiled/staging_queries/gcp/checkouts_notds_import.v1__0"],
    ),
    ConfigTemplate(
        source="staging_queries/gcp/exports.py",
        confs=[
            "compiled/staging_queries/gcp/exports.user_activities__0",
            "compiled/staging_queries/gcp/exports.checkouts__0",
        ],
    ),
    ConfigTemplate(
        source="group_bys/gcp/purchases.py",
        confs=[
            "compiled/group_bys/gcp/purchases.v1_test__0",
            "compiled/group_bys/gcp/purchases.v1_dev__0",
        ],
    ),
    ConfigTemplate(
        source="joins/gcp/training_set.py",
        confs=[
            "compiled/joins/gcp/training_set.v1_test__0",
            "compiled/joins/gcp/training_set.v1_dev__0",
            "compiled/joins/gcp/training_set.v1_dev_notds__0",
        ],
    ),
    ConfigTemplate(
        source="joins/gcp/demo.py",
        confs=[
            "compiled/joins/gcp/demo.v1__1",
            "compiled/joins/gcp/demo.derivations_v1__2",
        ],
    ),
]

AWS_CONFIGS = [
    ConfigTemplate(
        source="staging_queries/aws/exports.py",
        confs=[
            "compiled/staging_queries/aws/exports.user_activities__0",
            "compiled/staging_queries/aws/exports.checkouts__0",
            "compiled/staging_queries/aws/exports.dim_listings__0",
            "compiled/staging_queries/aws/exports.dim_merchants__0",
        ],
    ),
    ConfigTemplate(
        source="group_bys/aws/user_activities.py",
        confs=[
            "compiled/group_bys/aws/user_activities.v1__1",
        ],
    ),
    ConfigTemplate(
        source="joins/aws/demo.py",
        confs=[
            "compiled/joins/aws/demo.v1__1",
            "compiled/joins/aws/demo.derivations_v1__2",
        ],
    ),
]

AZURE_CONFIGS = [
    ConfigTemplate(
        source="staging_queries/azure/exports.py",
        confs=[
            "compiled/staging_queries/azure/exports.user_activities__0",
            "compiled/staging_queries/azure/exports.checkouts__0",
        ],
    ),
    ConfigTemplate(
        source="group_bys/azure/purchases.py",
        confs=[
            "compiled/group_bys/azure/purchases.v1_test__0",
            "compiled/group_bys/azure/purchases.v1_dev__0",
        ],
    ),
    ConfigTemplate(
        source="joins/azure/training_set.py",
        confs=[
            "compiled/joins/azure/training_set.v1_test__0",
            "compiled/joins/azure/training_set.v1_dev__0",
            "compiled/joins/azure/training_set.v1_dev_notds__0",
        ],
    ),
    ConfigTemplate(
        source="joins/azure/demo.py",
        confs=[
            "compiled/joins/azure/demo.v2",
            "compiled/joins/azure/demo.derivations_v3",
        ],
    ),
]


def _get_configs(cloud: str) -> list[ConfigTemplate]:
    if cloud == "gcp":
        return GCP_CONFIGS
    if cloud == "aws":
        return AWS_CONFIGS
    if cloud == "azure":
        return AZURE_CONFIGS
    raise ValueError(f"Unsupported cloud: {cloud}")


def get_confs(cloud: str, test_id: str) -> dict[str, str]:
    """Return compiled conf paths keyed by base conf path, resolved with *test_id*."""
    result: dict[str, str] = {}
    for cfg in _get_configs(cloud):
        result.update(cfg.resolve_confs(test_id))
    return result


def _find_imported_stems(content: str, stem_renames: dict[str, str]) -> dict[str, str]:
    """Return the subset of *stem_renames* whose keys appear in import statements."""
    result: dict[str, str] = {}
    for line in content.splitlines():
        m = re.match(r".*\bimport\s+(.+)", line.strip())
        if not m:
            continue
        imported_names = m.group(1)
        for old, new in stem_renames.items():
            if re.search(rf"\b{re.escape(old)}\b", imported_names):
                result[old] = new
    return result


def _apply_renames(content: str, renames: dict[str, str]) -> str:
    """Replace module names with renamed versions throughout *content*.

    Uses a negative lookbehind for '.' so that attribute access like
    ``exports.user_activities`` is left alone while import-level names
    (``import user_activities``, ``user_activities.v1``) are renamed.
    """
    for old, new in sorted(renames.items(), key=lambda kv: len(kv[0]), reverse=True):
        content = re.sub(rf"(?<!\.)\b{re.escape(old)}\b", new, content)
    return content


def _rewrite_file(path: str, stem_renames: dict[str, str], dest: str = None) -> bool:
    """Rewrite a single file, only renaming stems that appear in its imports.

    Returns True if the file was modified.
    """
    dest_write = dest or path
    with open(path) as f:
        original = f.read()
    applicable = _find_imported_stems(original, stem_renames)
    if not applicable:
        return False
    updated = _apply_renames(original, applicable)
    if updated == original:
        return False
    with open(dest_write, "w") as f:
        f.write(updated)
    return True


def _collect_py_files(chronon_root: str, cloud: str) -> list[str]:
    """Return all .py config files under *chronon_root* for *cloud* that may need import rewriting."""
    pattern = os.path.join(chronon_root, "**", cloud, "*.py")
    return [p for p in glob.glob(pattern, recursive=True) if os.path.basename(p) != "__init__.py"]


def generate_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Rename source config files in-place, inserting *test_id* into filenames.

    All ``.py`` files under the cloud's config directories have their import
    statements rewritten so references to renamed modules stay consistent.
    Only stems that a file actually imports are rewritten, so local variable
    names that happen to match a module stem are left untouched.

    Returns the list of renamed file paths (for bookkeeping; cleanup uses
    the same configs list to reverse the renames).
    """
    configs = _get_configs(cloud)
    stem_renames = {cfg.stem: f"{cfg.stem}_{test_id}" for cfg in configs}

    all_py = _collect_py_files(chronon_root, cloud)
    renamed: dict[str,str] = {}
    for cfg in configs:
        src = os.path.join(chronon_root, cfg.source)
        dst = os.path.join(chronon_root, cfg.renamed_source.format(test_id=test_id))
        if os.path.exists(src):
            shutil.copy(src, dst)
            logger.info("Renamed %s -> %s", src, dst)
            renamed[src] = dst

    print(renamed)
    for path in all_py:
        if path in renamed:
            if _rewrite_file(path, stem_renames, renamed[path]):
                logger.info("Rewrote imports from %s in %s", path, renamed[path])

    return list(renamed.values())


def cleanup_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Restore renamed source files and revert import rewrites.

    Reverses the changes made by ``generate_test_configs``.
    """
    configs = _get_configs(cloud)
    reverse_renames = {f"{cfg.stem}_{test_id}": cfg.stem for cfg in configs}
    restored: list[str] = []

    # Phase 1: rename files back to originals.
    for cfg in configs:
        dst = os.path.join(chronon_root, cfg.renamed_source.format(test_id=test_id))
        src = os.path.join(chronon_root, cfg.source)
        if os.path.exists(dst):
            os.rename(dst, src)
            logger.info("Restored %s -> %s", dst, src)
            restored.append(src)

    # Phase 2: revert import rewrites in ALL .py files.
    all_py = _collect_py_files(chronon_root, cloud)
    for path in all_py:
        if _rewrite_file(path, reverse_renames):
            logger.info("Reverted imports in %s", path)

    return restored
