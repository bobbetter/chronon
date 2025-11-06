from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)


source = EventSource(
    table="data.page_views", 
    topic=None,
    query=Query(
        selects=selects("user_id", "page_name"),
        time_column="ts",
        start_partition="2025-09-20",
    ),
)

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [3, 14, 30]] 

v1 = GroupBy(
    version=1,
    sources=[source],
    keys=["user_id"],
    online=True,
    backfill_start_date="2025-09-20",
    aggregations=[
        Aggregation(
            input_column="page_name",
            operation=Operation.UNIQUE_COUNT,
            windows=window_sizes,
        ),
        Aggregation(
            input_column="user_id",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
    ],
)