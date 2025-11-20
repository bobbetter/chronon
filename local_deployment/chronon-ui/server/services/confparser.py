from typing import List, Dict, Any
from enum import Enum
import json
import os


class ConfType(Enum):
    GROUP_BY = "group_by"
    JOIN = "join"


class Conf:
    def __init__(self, name: str, conf_type: ConfType, primary_keys: List[str]):
        self.name = name
        self.conf_type = conf_type
        self.primary_keys = primary_keys

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "conf_type": self.conf_type,
            "primary_keys": self.primary_keys,
        }


class ConfParser:
    IGNORE_FILES = ["schema.v1__1"]

    def __init__(self, directory_path: str):
        self.directory_path = directory_path

    def _parse_conf_file(self, file_path: str) -> Conf:
        """Parse a single configuration file and extract name and primary keys."""
        with open(file_path, "r", encoding="utf-8") as f:
            compiled_data = json.load(f)

        # Extract name from metaData
        name = compiled_data["metaData"]["name"]

        # Determine conf_type based on whether it's a join or group_by
        if "joinParts" in compiled_data:
            conf_type = ConfType.JOIN
            # For joins, primary keys come from left.keys
            primary_keys = compiled_data.get("rowIds", [])
        else:
            conf_type = ConfType.GROUP_BY
            # For group_bys, primary keys come from keyColumns
            primary_keys = compiled_data.get("keyColumns", [])

        return Conf(name, conf_type, primary_keys)

    def parse(self) -> List[Dict[str, Any]]:
        """Parse all configuration files in the directory and return a list of conf dictionaries."""
        confs = []

        if not os.path.isdir(self.directory_path):
            return confs

        for entry in sorted(os.listdir(self.directory_path)):
            file_path = os.path.join(self.directory_path, entry)

            # Skip if not a file or if it's in the ignore list
            if not os.path.isfile(file_path) or entry in self.IGNORE_FILES:
                continue

            try:
                conf = self._parse_conf_file(file_path)
                confs.append(conf.to_dict())
            except Exception as e:
                # Skip files that can't be parsed
                print(f"Skipping file {file_path}: {e}")
                continue

        return confs
