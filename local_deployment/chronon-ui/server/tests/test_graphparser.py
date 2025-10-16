import copy
from pathlib import Path
from server.services.graphparser import GraphParser

expected_graph = {
  "nodes": [
    {
      "name": "quickstart.page_views.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": True
    },
  ]
}

parent_dir = Path(__file__).parent
def test_graphparser_with_directory_path():
    test_dir = parent_dir / "compiled" / "one_groupby"
    graph_parser = GraphParser(str(test_dir))
    graph = graph_parser.parse()
    assert graph == expected_graph


second_gb = {
  "nodes": [
     {
      "name": "quickstart.page_views.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"],
      'config_file_path': 'compiled/two_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.purchases.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"],
      'config_file_path': 'compiled/two_groupby/purchases.v1__1',
    },
    {
      "name": "data.purchases",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_purchases_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_purchases_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": True
    },
    {
      "source": "data.purchases",
      "target": "quickstart.purchases.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.purchases.v1__1",
      "target": "quickstart.quickstart_purchases_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": True
    },
    {
      "source": "quickstart.purchases.v1__1",
      "target": "quickstart.quickstart_purchases_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": True
    },
  ]
}


def test_graphparser_two_gb():
    test_dir = parent_dir / "compiled" / "two_groupby"
    graph_parser = GraphParser(str(test_dir))
    graph = graph_parser.parse()
    assert graph == second_gb



expected_graph_with_joins = {
  "nodes": [
    {
      "name": "quickstart.page_views.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      'actions': [
          "backfill",
      ],
      'config_file_path': 'compiled/one_join/training_set.v1__1',
      'exists': True,
      'name': 'quickstart.training_set.v1__1',
      'type': 'conf-join',
      'type_visual': 'conf',
    },
    {
      'actions': [
        "show",
      ],
      'config_file_path': None,
      'exists': False,
      'name': 'training_data.quickstart.training_set.v1__1',
      'type': 'batch-data',
      'type_visual': 'backfill-join',
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": True
    },
    {
      'exists': 'False',
      'source': 'quickstart.training_set.v1__1',
      'target': 'training_data.quickstart.training_set.v1__1',
      'type': 'conf-to-training-data-set',
    },
    {
      'exists': 'True',
      'source': 'quickstart.purchases.v1__1',
      'target': 'quickstart.training_set.v1__1',
      'type': 'conf-to-conf',
    },
    {
      'exists': 'True',
      'source': 'quickstart.returns.v1__1',
      'target': 'quickstart.training_set.v1__1',
      'type': 'conf-to-conf',
    },
  ]
}


parent_dir = Path(__file__).parent
def test_graphparser_gb_with_joins():
    test_dir_gb = parent_dir / "compiled" / "one_groupby"
    test_dir_joins = parent_dir / "compiled" / "one_join"
    graph_parser = GraphParser(str(test_dir_gb), str(test_dir_joins))
    assert graph_parser.parse() == expected_graph_with_joins
