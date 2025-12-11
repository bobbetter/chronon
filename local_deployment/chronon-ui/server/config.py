"""Central configuration for path locations and settings."""
import os
from pathlib import Path
from enum import Enum
server_root = Path(__file__).parent

class ComputeEngine(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"

# Spark warehouse path (can be overridden via environment variable)
warehouse_path = os.environ.get("SPARK_WAREHOUSE_PATH") or str(
    server_root / "spark-data/warehouse"
)

chronon_compiled_dir = server_root / "chronon_config" / "compiled"

teams_metadata_dir = chronon_compiled_dir / "teams_metadata"

compiled_dir_gbs = chronon_compiled_dir / "group_bys"
compiled_dir_joins = chronon_compiled_dir / "joins"


EMR_SCRIPTS_DIR = "/app/server/scripts"
EMR_SCRIPTS_S3_KEY_PREFIX = "chronon-files/scripts/"
EMR_STARTER_S3_KEY = "chronon-files/scripts/emr_chronon_starter.py"

COMPILED_CONFIGS_S3_KEY_PREFIX = "chronon-files/compiled/"

# S3 path for scripts
DELETE_TABLE_SCRIPT_S3_KEY = "chronon-files/scripts/delete_table.py"

S3_BUCKET = os.environ.get("CHRONON_S3_BUCKET", "det-data")
CHRONON_JAR_S3_KEY_PREFIX = "chronon-files/jars/"
CHRONON_JAR_DIR = os.environ.get("CHRONON_JAR_DIR", "/srv/chronon/jars")
CHRONON_SPARK_JAR = "chronon-spark-assembly.jar"
CHRONON_AWS_JAR = "chronon-aws-assembly.jar"
CHRONON_JAR_FILES = [CHRONON_SPARK_JAR, CHRONON_AWS_JAR]
CHRONON_ONLINE_CLASS = os.environ.get("CHRONON_ONLINE_CLASS")