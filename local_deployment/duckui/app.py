import os
from pathlib import Path
from typing import List

import duckdb
import pandas as pd
import streamlit as st


def get_project_root() -> Path:
  return Path(__file__).resolve().parents[2]


def get_warehouse_path() -> Path:
  env_path = os.environ.get("SPARK_WAREHOUSE_PATH")
  if env_path:
    return Path(env_path)
  # Default to project_root/warehouse (mounted from the Spark container)
  return get_project_root() / "warehouse"


@st.cache_data(show_spinner=False)
def list_databases(warehouse_dir: Path) -> List[str]:
  if not warehouse_dir.exists():
    return []
  dbs = [p.stem for p in warehouse_dir.iterdir() if p.is_dir() and p.suffix == ".db"]
  # Detect presence of top-level tables which belong to the implicit 'default' db
  def has_parquet_under(path: Path) -> bool:
    try:
      return any(path.rglob("*.parquet"))
    except Exception:
      return False
  default_candidates = []
  for p in warehouse_dir.iterdir():
    if not p.is_dir():
      continue
    if p.suffix == ".db":
      continue
    if p.name in {"metastore_db"}:
      continue
    if has_parquet_under(p):
      default_candidates.append(p.name)
  if default_candidates and "default" not in dbs:
    dbs.append("default")
  return sorted(set(dbs))


@st.cache_data(show_spinner=False)
def list_tables(warehouse_dir: Path, db_name: str) -> List[str]:
  if db_name == "default":
    def has_parquet_under(path: Path) -> bool:
      try:
        return any(path.rglob("*.parquet"))
      except Exception:
        return False
    tables = []
    for p in warehouse_dir.iterdir():
      if not p.is_dir():
        continue
      if p.suffix == ".db":
        continue
      if p.name in {"metastore_db"}:
        continue
      if has_parquet_under(p):
        tables.append(p.name)
    return sorted(tables)
  db_dir = warehouse_dir / f"{db_name}.db"
  if not db_dir.exists():
    return []
  tables = [p.name for p in db_dir.iterdir() if p.is_dir()]
  return sorted(tables)


def build_parquet_glob(warehouse_dir: Path, db_name: str, table_name: str) -> str:
  # Spark writes Parquet files under partition directories like ds=YYYY-MM-DD
  if db_name == "default":
    table_dir = warehouse_dir / table_name
  else:
    table_dir = warehouse_dir / f"{db_name}.db" / table_name
  # Use a recursive glob to capture all partitions
  return str(table_dir / "**" / "*.parquet")


@st.cache_data(show_spinner=False)
def sample_table(pattern: str, limit: int = 100) -> pd.DataFrame:
  con = duckdb.connect()
  try:
    # read_parquet supports glob patterns
    query = "SELECT * FROM read_parquet(? ) LIMIT ?"
    df = con.execute(query, [pattern, limit]).fetch_df()
    return df
  finally:
    con.close()


@st.cache_data(show_spinner=False)
def table_row_count(pattern: str) -> int:
  con = duckdb.connect()
  try:
    query = "SELECT COUNT(*) AS cnt FROM read_parquet(?)"
    cnt = con.execute(query, [pattern]).fetchone()[0]
    return int(cnt)
  finally:
    con.close()


def main() -> None:
  st.set_page_config(page_title="Local data ", layout="wide")
  st.title("Local datasets")

  warehouse_dir = get_warehouse_path()
  st.caption(f"Warehouse: {warehouse_dir}")

  if not warehouse_dir.exists():
    st.error("Warehouse directory not found. Ensure the Spark container has the warehouse bind-mounted to the host and that data has been loaded.")
    st.stop()

  with st.sidebar:
    st.header("Catalog")
    if st.button("Refresh data", type="secondary", use_container_width=True):
      st.cache_data.clear()
      st.rerun()
    dbs = list_databases(warehouse_dir)
    if not dbs:
      st.warning("No databases found (expected directories ending with .db).")
      st.stop()
    db_name = st.selectbox("Database", options=dbs, index=min(dbs.index("data") if "data" in dbs else 0, len(dbs)-1))
    tables = list_tables(warehouse_dir, db_name)
    if not tables:
      st.warning("No tables found in selected database.")
      st.stop()
    table_name = st.selectbox("Table", options=tables)
    limit = st.slider("Preview rows", min_value=10, max_value=1000, value=100, step=10)

  st.subheader(f"{db_name}.{table_name}")
  pattern = build_parquet_glob(warehouse_dir, db_name, table_name)
  st.caption(f"Pattern: {pattern}")

  try:
    count = table_row_count(pattern)
    st.write(f"Total rows: {count}")
  except Exception as e:
    st.warning(f"Could not compute row count: {e}")

  try:
    df = sample_table(pattern, limit=limit)
    if df.shape[0] == 0:
      st.info("No rows returned. Check that Parquet files exist under the table directory.")
    else:
      st.dataframe(df, width="stretch")
  except Exception as e:
    st.error(f"Failed to read table sample: {e}")

  with st.expander("Run ad-hoc query (read-only)"):
    st.caption("Use DuckDB SQL. Hint: FROM read_parquet('<glob>')")
    sql = st.text_area(
      label="SQL",
      value=f"SELECT * FROM read_parquet('{pattern}') LIMIT 100",
      height=120,
    )
    if st.button("Execute", type="primary"):
      con = duckdb.connect()
      try:
        result_df = con.execute(sql).fetch_df()
        st.dataframe(result_df, width="stretch")
      except Exception as e:
        st.error(f"Query failed: {e}")
      finally:
        con.close()


if __name__ == "__main__":
  main()


