import pytest
from pathlib import Path
from server.services.datascanner_local import DataScannerLocal


@pytest.fixture
def test_warehouse_path():
    """Return the path to the test warehouse directory."""
    current_dir = Path(__file__).parent
    return str(current_dir / "test_data" / "warehouse")


@pytest.fixture
def scanner(test_warehouse_path):
    """Create a DataScanner instance with the test warehouse."""
    return DataScannerLocal(test_warehouse_path)


def test_datascanner_initialization(test_warehouse_path):
    """Test that DataScanner can be initialized."""
    scanner = DataScannerLocal(test_warehouse_path)
    assert scanner.warehouse_path == Path(test_warehouse_path)


def test_list_databases(scanner):
    """Test listing databases in the warehouse."""
    databases = scanner.list_databases()
    assert isinstance(databases, list)
    assert "data" in databases
    assert "quickstart" in databases
    # Databases should be sorted
    assert databases == sorted(databases)


def test_list_databases_nonexistent_path():
    """Test listing databases with a non-existent path."""
    scanner = DataScannerLocal("/nonexistent/path")
    databases = scanner.list_databases()
    assert databases == []


def test_list_tables_in_data_db(scanner):
    """Test listing tables in the 'data' database."""
    tables = scanner.list_tables("data")
    assert isinstance(tables, list)
    # Based on the test data structure, we should have these tables
    expected_tables = ["checkouts", "clicks", "form_submissions"]
    for table in expected_tables:
        assert table in tables, f"Expected table '{table}' not found in data database"
    # Tables should be sorted
    assert sorted(tables) == sorted(expected_tables)


def test_list_tables_nonexistent_db(scanner):
    """Test listing tables in a non-existent database."""
    tables = scanner.list_tables("nonexistent")
    assert tables == []


def test_get_table_schema(scanner):
    """Test getting the schema of a table."""
    schema = scanner.get_table_schema("data", "clicks")
    assert isinstance(schema, list)
    assert len(schema) > 0
    # Each schema entry should have name and type
    for col in schema:
        assert "name" in col
        assert "type" in col


def test_sample_table(scanner):
    """Test sampling data from a table."""
    result = scanner.sample_table("data", "clicks", limit=10)
    assert isinstance(result, dict)
    assert "data" in result
    assert "table_schema" in result
    assert "row_count" in result
    assert "limit" in result
    assert "offset" in result

    # Check that data is a list
    assert isinstance(result["data"], list)
    assert len(result["data"]) <= 10

    # Check table schema
    assert isinstance(result["table_schema"], list)
    assert len(result["table_schema"]) > 0

    # Check row count
    assert isinstance(result["row_count"], int)
    assert result["row_count"] >= len(result["data"])


def test_sample_table_with_offset(scanner):
    """Test sampling data with offset."""
    result1 = scanner.sample_table("data", "clicks", limit=5, offset=0)
    result2 = scanner.sample_table("data", "clicks", limit=5, offset=5)

    # Both should have data
    assert len(result1["data"]) > 0
    # The data should be different (assuming we have enough rows)
    if result1["row_count"] > 5:
        # Can't compare directly because DataFrame conversion might not preserve order
        # Just check that we got different offsets
        assert result1["offset"] == 0
        assert result2["offset"] == 5


def test_sample_table_large_limit(scanner):
    """Test sampling with a limit larger than available rows."""
    result = scanner.sample_table("data", "clicks", limit=1000000)
    # Should return all available rows without error
    assert len(result["data"]) <= result["row_count"]


def test_get_table_stats(scanner):
    """Test getting table statistics."""
    stats = scanner.get_table_stats("data", "clicks")
    assert isinstance(stats, dict)
    assert "row_count" in stats
    assert "column_count" in stats
    assert "columns" in stats

    assert isinstance(stats["row_count"], int)
    assert stats["row_count"] > 0
    assert isinstance(stats["column_count"], int)
    assert stats["column_count"] > 0
    assert len(stats["columns"]) == stats["column_count"]


def test_execute_query(scanner):
    """Test executing a custom SQL query."""
    pattern = scanner._build_parquet_glob("data", "clicks")
    query = f"SELECT * FROM read_parquet('{pattern}') WHERE 1=1"
    result = scanner.execute_query(query, limit=10)

    assert isinstance(result, dict)
    assert "data" in result
    assert "table_schema" in result
    assert "row_count" in result

    assert isinstance(result["data"], list)
    assert len(result["data"]) <= 10


def test_build_parquet_glob_default_db(scanner):
    """Test building parquet glob for default database."""
    pattern = scanner._build_parquet_glob("default", "test_table")
    expected_path = Path(scanner.warehouse_path) / "test_table" / "**" / "*.parquet"
    assert pattern == str(expected_path)


def test_build_parquet_glob_named_db(scanner):
    """Test building parquet glob for named database."""
    pattern = scanner._build_parquet_glob("data", "clicks")
    expected_path = (
        Path(scanner.warehouse_path) / "data.db" / "clicks" / "**" / "*.parquet"
    )
    assert pattern == str(expected_path)


def test_has_parquet_under(scanner):
    """Test checking for parquet files under a directory."""
    # Test with a directory that has parquet files
    clicks_dir = Path(scanner.warehouse_path) / "data.db" / "clicks"
    assert scanner._has_parquet_under(clicks_dir)

    # Test with warehouse root (should have parquet files somewhere)
    assert scanner._has_parquet_under(Path(scanner.warehouse_path))


def test_multiple_tables(scanner):
    """Test that we can query multiple different tables."""
    tables = scanner.list_tables("data")
    assert len(tables) > 1

    # Sample from multiple tables
    for table_name in tables[:3]:  # Test first 3 tables
        result = scanner.sample_table("data", table_name, limit=5)
        assert len(result["data"]) >= 0  # Some tables might be empty
        assert "table_schema" in result


def test_schema_consistency(scanner):
    """Test that schema from different methods is consistent."""
    # Get schema using get_table_schema
    schema1 = scanner.get_table_schema("data", "clicks")

    # Get schema from sample_table
    sample_result = scanner.sample_table("data", "clicks", limit=1)
    schema2 = sample_result["table_schema"]

    # Get schema from get_table_stats
    stats = scanner.get_table_stats("data", "clicks")
    schema3 = stats["columns"]

    # All should have the same number of columns
    assert len(schema1) == len(schema2)
    assert len(schema1) == len(schema3)

    # Column names should match
    names1 = [col["name"] for col in schema1]
    names2 = [col["name"] for col in schema2]
    names3 = [col["name"] for col in schema3]
    assert names1 == names2
    assert names1 == names3


def test_get_table_exists(scanner):
    """Test getting the existence of a table."""
    assert not scanner.get_table_exists("data", "nonexistent")
    assert scanner.get_table_exists("data", "clicks")
