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
    table="data.logins", 
    topic="events.logins/fields=ts,event_id,user_id,login_methd,device_type,ip_address/host=kafka/port=9092/serde=custom/provider_class=ai.chronon.flink.deser.LoginsSerDe",
    query=Query(
        selects=selects("user_id", "login_method", "device_type"),  # Select the fields we care about
        time_column="ts",
        start_partition="2025-09-16",
    ),
)

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [3, 14, 30]] 

v1 = GroupBy(
    version=1,
    sources=[source],
    keys=["user_id"],
    online=True,
    backfill_start_date="2025-09-16",
    aggregations=[
        Aggregation(
            input_column="user_id",
            operation=Operation.COUNT,
            windows=window_sizes,
            buckets=["device_type"]
    ),
        Aggregation(
            input_column="login_method",
            operation=Operation.LAST_K(5),
            windows=window_sizes,
        ),
    ],
)