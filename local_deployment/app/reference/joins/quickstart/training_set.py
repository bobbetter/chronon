from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects
from ai.chronon.join import Join, JoinPart
from group_bys.quickstart.purchases import v1 as purchases_v1
from group_bys.quickstart.returns import v1 as returns_v1


source = EventSource(
    table="data.checkouts",
    query=Query(
        selects=selects("user_id"),
        time_column="ts",
    ),
)

v1 = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=purchases_v1), 
        JoinPart(group_by=returns_v1)
    ],
    row_ids="user_id",
    version=1,
)