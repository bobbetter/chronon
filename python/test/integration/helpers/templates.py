"""Generate test config files from source configs with test_id isolation."""

import logging
import os
import re
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
    def output(self) -> str:
        """Output path template, derived from source."""
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


def _find_imported_stems(content: str, test_isolated_stems: set[str]) -> list[str]:
    """Find module names in import statements that are also being test-isolated.

    Scans each ``import`` line and returns any imported names that appear
    in *test_isolated_stems*, sorted longest-first for safe replacement.
    """
    renames: set[str] = set()
    for line in content.splitlines():
        m = re.match(r".*\bimport\s+(.+)", line.strip())
        if not m:
            continue
        imported_names = m.group(1)
        for stem in test_isolated_stems:
            if re.search(rf"\b{re.escape(stem)}\b", imported_names):
                renames.add(stem)
    return sorted(renames, key=len, reverse=True)


def _apply_test_id(content: str, renames: list[str], test_id: str) -> str:
    """Replace module names with test_id-suffixed versions.

    Uses a negative lookbehind for '.' so that attribute access like
    ``exports.user_activities`` is left alone while import-level names
    (``import user_activities``, ``user_activities.v1``) are renamed.
    """
    for name in sorted(renames, key=len, reverse=True):
        content = re.sub(rf"(?<!\.)\b{re.escape(name)}\b", f"{name}_{test_id}", content)
    return content


def generate_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Generate test config files from source configs with test_id suffixes.

    Imports referencing other test-isolated modules are automatically
    renamed so the whole dependency chain uses test_id-isolated tables.

    Parameters
    ----------
    test_id : str
        Unique suffix injected into output filenames and module references.
    chronon_root : str
        Absolute path to the canary config root
        (e.g. ``<repo>/python/test/canary``).
    cloud : str
        Cloud provider — determines which config set to use.

    Returns
    -------
    list[str]
        Absolute paths of generated files.
    """
    configs = _get_configs(cloud)
    test_isolated_stems = {cfg.stem for cfg in configs}
    generated: list[str] = []

    for cfg in configs:
        source_path = os.path.join(chronon_root, cfg.source)
        with open(source_path) as f:
            content = f.read()

        renames = _find_imported_stems(content, test_isolated_stems)
        if renames:
            content = _apply_test_id(content, renames, test_id)

        output_path = os.path.join(chronon_root, cfg.output.format(test_id=test_id))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w") as f:
            f.write(content)

        logger.info("Generated %s", output_path)
        generated.append(output_path)

    return generated


def cleanup_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Remove previously generated config files for *test_id*.

    Returns a list of paths that were deleted.
    """
    configs = _get_configs(cloud)
    removed: list[str] = []

    for cfg in configs:
        output_path = os.path.join(chronon_root, cfg.output.format(test_id=test_id))
        if os.path.exists(output_path):
            os.remove(output_path)
            logger.info("Removed %s", output_path)
            removed.append(output_path)

    return removed
