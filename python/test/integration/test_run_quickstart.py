"""Cloud-agnostic integration test for the full zipline run pipeline.

Exercises: compile -> import staging queries -> backfill group_bys -> backfill joins
-> check partitions -> upload -> upload-to-kv -> metadata-upload
-> staging query -> fetch

Replaces the former test_gcp_template_quickstart.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import (
    compile_configs,
    submit_check_partitions,
    submit_fetch,
    submit_metadata_upload,
    submit_run,
    submit_upload,
    submit_upload_to_kv,
)

START_DS = "2023-11-01"
END_DS = "2023-11-30"

STAGING_QUERY_IMPORT_KEYS = [
    "compiled/staging_queries/gcp/purchases_import.v1__0",
    "compiled/staging_queries/gcp/checkouts_import.v1__0",
    "compiled/staging_queries/gcp/purchases_notds_import.v1__0",
    "compiled/staging_queries/gcp/checkouts_notds_import.v1__0",
]


@pytest.mark.integration
def test_run_quickstart(test_id, confs, chronon_root, version, cloud):
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    # 1. Import staging queries (only present in GCP configs)
    for key in STAGING_QUERY_IMPORT_KEYS:
        if key in confs:
            submit_run(runner, chronon_root, confs[key], version,
                        start_ds=START_DS, end_ds=END_DS)

    # 2. Backfill group_by
    gb_conf = confs[f"compiled/group_bys/{cloud}/purchases.v1_test__0"]
    submit_run(runner, chronon_root, gb_conf, version,
               start_ds=START_DS, end_ds=END_DS)

    # 3. Backfill joins
    join_conf = confs[f"compiled/joins/{cloud}/training_set.v1_test__0"]
    submit_run(runner, chronon_root, join_conf, version,
               start_ds=START_DS, end_ds=END_DS)

    notds_key = f"compiled/joins/{cloud}/training_set.v1_dev_notds__0"
    if notds_key in confs:
        submit_run(runner, chronon_root, confs[notds_key], version,
                   start_ds=START_DS, end_ds=END_DS)

    # Steps 4-9 are only supported by the GCP runner (Dataproc).
    # The AWS runner (EMR) does not implement metastore, upload, or fetch modes.
    if cloud == "gcp":
        # 4. Check partitions
        partition_name = f"data.{cloud}_purchases_{test_id}_v1_test__0/ds={END_DS}"
        submit_check_partitions(
            runner, chronon_root,
            f"compiled/teams_metadata/{cloud}/{cloud}_team_metadata",
            version, partition_name,
        )

        # 5. Upload
        submit_upload(runner, chronon_root, gb_conf, version, ds=END_DS)

        # 6. Upload to KV
        submit_upload_to_kv(runner, chronon_root, gb_conf, version, ds=END_DS)

        # 7. Metadata upload
        submit_metadata_upload(runner, chronon_root, gb_conf, version)

        # 8. Staging query (exports)
        exports_conf = confs[f"compiled/staging_queries/{cloud}/exports.checkouts__0"]
        submit_run(runner, chronon_root, exports_conf, version,
                   start_ds=START_DS, end_ds=END_DS)

        # 9. Fetch and verify
        fetch_name = f"{cloud}.purchases_{test_id}.v1_test__0"
        result = submit_fetch(runner, chronon_root, gb_conf, version,
                              keys='{"user_id":"5"}', name=fetch_name)
        assert "purchase_price_average_7d" in result.output, \
            f"Expected purchase_price_average_7d in fetch output:\n{result.output}"
