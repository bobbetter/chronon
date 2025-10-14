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
    table="data.page_views", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
    topic=None, # See the 'returns' GroupBy for an example that has a streaming source configured. In this case, this would be the streaming source topic that can be listened to for realtime events
    query=Query(
        selects=selects("user_id", "session_id", "product_id"),  # Select the fields we care about
        time_column="ts",
        start_partition="2025-10-01",
    ),
)

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [3, 14, 30]]  # Define some window sizes to use below

v1 = GroupBy(
    version=1,
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    online=True,
    backfill_start_date="2025-10-01",
    aggregations=[
        Aggregation(
            input_column="session_id",
            operation=Operation.APPROX_UNIQUE_COUNT,
            windows=window_sizes
    ),
        Aggregation(
            input_column="session_id",
            operation=Operation.COUNT,
            windows=window_sizes,
            buckets=["product_id"]
        ),
        Aggregation(
            input_column="product_id",
            operation=Operation.LAST_K(5),
            windows=window_sizes,
        ),
    ],
)