"""Generate test config files from source configs with test_id isolation."""

import logging
import os
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ConfigTemplate:
    source: str  # relative path to source .py (e.g. "group_bys/gcp/purchases.py")
    renames: list[str] = field(default_factory=list)  # module names to suffix with _{test_id}
    # base compiled conf paths this source produces (e.g. "compiled/joins/gcp/demo.v1__1").
    # These are the keys integration tests use: confs["compiled/joins/gcp/demo.v1__1"]
    confs: list[str] = field(default_factory=list)

    @property
    def output(self) -> str:
        """Output path template, derived from source."""
        return self.source.replace(".py", "_{test_id}.py")

    def resolve_confs(self, test_id: str) -> dict[str, str]:
        """Map base compiled conf path -> test_id-resolved path."""
        stem = os.path.splitext(os.path.basename(self.source))[0]
        result = {}
        for base_conf in self.confs:
            resolved = base_conf.replace(f"{stem}.", f"{stem}_{test_id}.")
            result[base_conf] = resolved
        return result


GCP_CONFIGS = [
    ConfigTemplate(source="staging_queries/gcp/purchases_import.py"),
    ConfigTemplate(source="staging_queries/gcp/purchases_notds_import.py"),
    ConfigTemplate(source="staging_queries/gcp/checkouts_import.py"),
    ConfigTemplate(source="staging_queries/gcp/checkouts_notds_import.py"),
    ConfigTemplate(
        source="staging_queries/gcp/exports.py",
        confs=["compiled/staging_queries/gcp/exports.user_activities__0"],
    ),
    ConfigTemplate(
        source="group_bys/gcp/purchases.py",
        renames=["purchases_import", "purchases_notds_import"],
    ),
    ConfigTemplate(
        source="joins/gcp/training_set.py",
        renames=["purchases", "checkouts_import", "checkouts_notds_import"],
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
    ConfigTemplate(source="staging_queries/aws/exports.py"),
    ConfigTemplate(source="group_bys/aws/purchases.py"),
    ConfigTemplate(
        source="joins/aws/training_set.py",
        renames=["purchases"],
    ),
    ConfigTemplate(
        source="joins/aws/demo.py",
        confs=["compiled/joins/aws/demo.derivations_v1__2"],
    ),
]


def _get_configs(cloud: str) -> list[ConfigTemplate]:
    if cloud == "gcp":
        return GCP_CONFIGS
    if cloud == "aws":
        return AWS_CONFIGS
    raise ValueError(f"Unsupported cloud: {cloud}")


def get_confs(cloud: str, test_id: str) -> dict[str, str]:
    """Return compiled conf paths keyed by base conf path, resolved with *test_id*."""
    result: dict[str, str] = {}
    for cfg in _get_configs(cloud):
        result.update(cfg.resolve_confs(test_id))
    return result


def _apply_test_id(content: str, renames: list[str], test_id: str) -> str:
    """Replace module names with test_id-suffixed versions using word boundaries."""
    for name in sorted(renames, key=len, reverse=True):
        content = re.sub(rf"\b{re.escape(name)}\b", f"{name}_{test_id}", content)
    return content


def generate_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Generate test config files from source configs with test_id suffixes.

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
    generated: list[str] = []

    for cfg in configs:
        source_path = os.path.join(chronon_root, cfg.source)
        with open(source_path) as f:
            content = f.read()

        if cfg.renames:
            content = _apply_test_id(content, cfg.renames, test_id)

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
