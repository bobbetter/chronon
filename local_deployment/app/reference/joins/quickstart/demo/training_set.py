from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects
from ai.chronon.join import Join, JoinPart
from group_bys.quickstart.profiles import v1 as profiles_v1
from group_bys.quickstart.page_views import v1 as page_views_v1
from group_bys.quickstart.logins import v1 as logins_v1


source = EventSource(
    table="data.fraud_labels",
    query=Query(
        selects=selects("user_id", "is_fraud"),
        time_column="ts",
    ),
)

v1 = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=profiles_v1), 
        JoinPart(group_by=page_views_v1),
        JoinPart(group_by=logins_v1)
    ],
    row_ids="user_id",
    version=1,
)