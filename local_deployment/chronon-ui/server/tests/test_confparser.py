import copy
from pathlib import Path
from server.services.confparser import ConfParser, ConfType

parent_dir = Path(__file__).parent
expected_one_groupby = [
    {
        "name": "quickstart.page_views.v1__1",
        "conf_type": ConfType.GROUP_BY,
        "primary_keys": ["user_id"],
    },
]


def test_graphparser_with_directory_path():
    test_dir = parent_dir / "compiled" / "one_groupby"
    graph_parser = ConfParser(str(test_dir))
    graph = graph_parser.parse()
    assert graph == expected_one_groupby


expected_two_groupbys = [
    {
        "name": "quickstart.page_views.v1__1",
        "conf_type": ConfType.GROUP_BY,
        "primary_keys": ["user_id"],
    },
    {
        "name": "quickstart.purchases.v1__1",
        "conf_type": ConfType.GROUP_BY,
        "primary_keys": ["user_id"],
    },
]


def test_graphparser_with_two_group_bys():
    test_dir = parent_dir / "compiled" / "two_groupby"
    conf_parser = ConfParser(str(test_dir))
    result = conf_parser.parse()
    assert result == expected_two_groupbys


expected_one_join = [
    {
        "name": "quickstart.training_set.v1__1",
        "conf_type": ConfType.JOIN,
        "primary_keys": ["user_id"],
    },
]


def test_graphparser_with_one_join():
    test_dir = parent_dir / "compiled" / "one_join"
    conf_parser = ConfParser(str(test_dir))
    result = conf_parser.parse()
    assert result == expected_one_join
