import copy
from pathlib import Path
from server.services.graphparser import GraphParser

expected_graph = {
  "nodes": [
    {
      "name": "quickstart.page_views.v1__1",
      "type": "group_by",
      "type_visual": "configuration",
      "exists": True,
      "actions": ["backfill", "pre-compute-upload", "show-online-data"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw_data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill_group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "pre_computed_upload",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show", "upload-to-kv"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',
    },
    {
      "name": "quickstart_page_views_v1__1_batch",
      "type": "batch_uploaded",
      "type_visual": "online-data",
      "exists": False,
      "actions": None,
      'config_file_path': None,
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "exists": True
    },
    {
      "source": "quickstart.quickstart_page_views_v1__1__upload",
      "target": "quickstart_page_views_v1__1_batch",
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
      "type": "group_by",
      "type_visual": "configuration",
      "exists": True,
      "actions": ["backfill", "pre-compute-upload", "show-online-data"],
      'config_file_path': 'compiled/two_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw_data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill_group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "pre_computed_upload",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show", "upload-to-kv"],
      'config_file_path': 'compiled/two_groupby/page_views.v1__1',
    },
    {
      "name": "quickstart_page_views_v1__1_batch",
      "type": "batch_uploaded",
      "type_visual": "online-data",
      "exists": False,
      "actions": None,
      'config_file_path': None,
    },
    {
      "name": "quickstart.purchases.v1__1",
      "type": "group_by",
      "type_visual": "configuration",
      "exists": True,
      "actions": ["backfill", "pre-compute-upload", "show-online-data"],
      'config_file_path': 'compiled/two_groupby/purchases.v1__1',
    },
    {
      "name": "data.purchases",
      "type": "raw_data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_purchases_v1__1",
      "type": "backfill_group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_purchases_v1__1__upload",
      "type": "pre_computed_upload",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show", "upload-to-kv"],
      'config_file_path': 'compiled/two_groupby/purchases.v1__1',
    },
    {
      "name": "quickstart_purchases_v1__1_batch",
      "type": "batch_uploaded",
      "type_visual": "online-data",
      "exists": False,
      "actions": None,
      'config_file_path': None,
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "exists": True
    },
    {
      "source": "quickstart.quickstart_page_views_v1__1__upload",
      "target": "quickstart_page_views_v1__1_batch",
      "exists": True
    },
    {
      "source": "data.purchases",
      "target": "quickstart.purchases.v1__1",
      "exists": True
    },
    {
      "source": "quickstart.purchases.v1__1",
      "target": "quickstart.quickstart_purchases_v1__1",
      "exists": True
    },
    {
      "source": "quickstart.purchases.v1__1",
      "target": "quickstart.quickstart_purchases_v1__1__upload",
      "exists": True
    },
    {
      "source": "quickstart.quickstart_purchases_v1__1__upload",
      "target": "quickstart_purchases_v1__1_batch",
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
      "type": "group_by",
      "type_visual": "configuration",
      "exists": True,
      "actions": ["backfill", "pre-compute-upload", "show-online-data"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',

    },
    {
      "name": "data.page_views",
      "type": "raw_data",
      "type_visual": "batch-data",
      "exists": False,  # No datascanner provided in test
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1",
      "type": "backfill_group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      "name": "quickstart.quickstart_page_views_v1__1__upload",
      "type": "pre_computed_upload",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show", "upload-to-kv"],
      'config_file_path': 'compiled/one_groupby/page_views.v1__1',
    },
    {
      "name": "quickstart_page_views_v1__1_batch",
      "type": "batch_uploaded",
      "type_visual": "online-data",
      "exists": False,
      "actions": None,
      'config_file_path': None,
    },
    {
      "name": "data.checkouts",
      "type": "raw_data",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"],
      'config_file_path': None,
    },
    {
      'actions': [
          "backfill",
          "show-online-data",
      ],
      'config_file_path': 'compiled/one_join/training_set.v1__1',
      'exists': True,
      'name': 'quickstart.training_set.v1__1',
      'type': 'join',
      'type_visual': 'configuration',
    },
    {
      'actions': [
        "show",
      ],
      'config_file_path': None,
      'exists': False,
      'name': 'quickstart.quickstart_training_set_v1__1',
      'type': 'backfill_join',
      'type_visual': 'batch-data',
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart.quickstart_page_views_v1__1__upload",
      "exists": True
    },
    {
      "source": "quickstart.quickstart_page_views_v1__1__upload",
      "target": "quickstart_page_views_v1__1_batch",
      "exists": True
    },
    {
      'exists': True,
      'source': 'data.checkouts',
      'target': 'quickstart.training_set.v1__1',
    },
    {
      'exists': False,
      'source': 'quickstart.training_set.v1__1',
      'target': 'quickstart.quickstart_training_set_v1__1',
    },
    {
      'exists': True,
      'source': 'quickstart.purchases.v1__1',
      'target': 'quickstart.training_set.v1__1',  
    },
    {
      'exists': True,
      'source': 'quickstart.returns.v1__1',
      'target': 'quickstart.training_set.v1__1',
    },
  ]
}


parent_dir = Path(__file__).parent
def test_graphparser_gb_with_joins():
    test_dir_gb = parent_dir / "compiled" / "one_groupby"
    test_dir_joins = parent_dir / "compiled" / "one_join"
    graph_parser = GraphParser(str(test_dir_gb), str(test_dir_joins))
    assert graph_parser.parse() == expected_graph_with_joins
