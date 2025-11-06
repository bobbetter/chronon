from ai.chronon.source import EntitySource
from ai.chronon.query import Query, selects
from ai.chronon.group_by import (
    GroupBy
)


source = EntitySource(
    snapshot_table="data.profiles",
    query=Query(
        selects=selects("user_id", "country", "company_size", "is_public_profile"),
        time_column="ts",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=None,
    online=True,
    version=1,
    backfill_start_date="2025-09-20",
)