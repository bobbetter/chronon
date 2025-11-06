# import pytest
# from pathlib import Path
# from fastapi.testclient import TestClient
# from server.main import app

# # Override the warehouse path for testing
# import os
# test_warehouse = str(Path(__file__).parent / "test_data" / "warehouse")
# os.environ["SPARK_WAREHOUSE_PATH"] = test_warehouse

# client = TestClient(app)


# def test_list_databases():
#     """Test the /databases endpoint."""
#     response = client.get("/v1/spark-data/databases")
#     assert response.status_code == 200
#     data = response.json()
#     assert "databases" in data
#     assert isinstance(data["databases"], list)
#     assert "data" in data["databases"]
#     assert "quickstart" in data["databases"]


# def test_list_tables():
#     """Test the /databases/{db_name}/tables endpoint."""
#     response = client.get("/v1/spark-data/databases/data/tables")
#     assert response.status_code == 200
#     data = response.json()
#     assert "database" in data
#     assert "tables" in data
#     assert data["database"] == "data"
#     assert isinstance(data["tables"], list)
#     assert "clicks" in data["tables"]
#     assert "page_views" in data["tables"]


# def test_get_table_stats():
#     """Test the /databases/{db_name}/tables/{table_name}/stats endpoint."""
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/stats")
#     assert response.status_code == 200
#     data = response.json()
#     assert "database" in data
#     assert "table" in data
#     assert "row_count" in data
#     assert "column_count" in data
#     assert "columns" in data
#     assert data["database"] == "data"
#     assert data["table"] == "clicks"
#     assert data["row_count"] > 0
#     assert data["column_count"] > 0


# def test_sample_table():
#     """Test the /databases/{db_name}/tables/{table_name}/sample endpoint."""
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/sample?limit=10&offset=0")
#     assert response.status_code == 200
#     data = response.json()
#     assert "database" in data
#     assert "table" in data
#     assert "data" in data
#     assert "schema" in data
#     assert "row_count" in data
#     assert "limit" in data
#     assert "offset" in data
#     assert data["database"] == "data"
#     assert data["table"] == "clicks"
#     assert isinstance(data["data"], list)
#     assert len(data["data"]) <= 10
#     assert data["limit"] == 10
#     assert data["offset"] == 0


# def test_sample_table_with_custom_params():
#     """Test sampling with custom limit and offset."""
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/sample?limit=5&offset=5")
#     assert response.status_code == 200
#     data = response.json()
#     assert len(data["data"]) <= 5
#     assert data["limit"] == 5
#     assert data["offset"] == 5


# def test_get_table_schema():
#     """Test the /databases/{db_name}/tables/{table_name}/schema endpoint."""
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/schema")
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, list)
#     assert len(data) > 0
#     # Each schema entry should have name and type
#     for col in data:
#         assert "name" in col
#         assert "type" in col


# def test_execute_query():
#     """Test the /query endpoint."""
#     query_request = {
#         "query": "SELECT 1 as test_col",
#         "limit": 10
#     }
#     response = client.post("/v1/spark-data/query", json=query_request)
#     assert response.status_code == 200
#     data = response.json()
#     assert "data" in data
#     assert "schema" in data
#     assert "row_count" in data
#     assert len(data["data"]) == 1
#     assert data["data"][0]["test_col"] == 1


# def test_execute_query_with_read_parquet():
#     """Test the /query endpoint with read_parquet."""
#     warehouse_path = Path(test_warehouse)
#     pattern = str(warehouse_path / "data.db" / "clicks" / "**" / "*.parquet")
#     query_request = {
#         "query": f"SELECT * FROM read_parquet('{pattern}') LIMIT 5"
#     }
#     response = client.post("/v1/spark-data/query", json=query_request)
#     assert response.status_code == 200
#     data = response.json()
#     assert len(data["data"]) <= 5


# def test_nonexistent_database():
#     """Test accessing a non-existent database."""
#     response = client.get("/v1/spark-data/databases/nonexistent/tables")
#     assert response.status_code == 200  # Should return empty list, not error
#     data = response.json()
#     assert data["tables"] == []


# def test_invalid_query():
#     """Test executing an invalid SQL query."""
#     query_request = {
#         "query": "SELECT * FROM nonexistent_table"
#     }
#     response = client.post("/v1/spark-data/query", json=query_request)
#     assert response.status_code == 400  # Bad request for invalid query
#     data = response.json()
#     assert "detail" in data


# def test_sample_table_limit_validation():
#     """Test that limit validation works."""
#     # Test with limit too high
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/sample?limit=5000")
#     assert response.status_code == 422  # Validation error
    
#     # Test with valid limit at boundary
#     response = client.get("/v1/spark-data/databases/data/tables/clicks/sample?limit=1000")
#     assert response.status_code == 200

