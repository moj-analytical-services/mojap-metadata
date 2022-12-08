from dataclasses import dataclass
from typing import Dict, List, Optional
from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters import BaseConverter

_mojap_to_rapid_types = {
    "null": None,
    "bool": "boolean",
    "bool_": "boolean",
    "int8": "Int64",
    "int16": "Int64",
    "int32": "Int64",
    "int64": "Int64",
    "uint8": "Int64",
    "uint16": "Int64",
    "uint32": "Int64",
    "uint64": "Int64",
    "decimal128": "Float64",
    "float16": "Float64",
    "float32": "Float64",
    "float64": "Float64",
    "time32": None,
    "time32(s)": None,
    "time32(ms)": None,
    "time64(us)": None,
    "time64(ns)": None,
    "date32": "date",
    "date64": "date",
    "timestamp(s)": None,
    "timestamp(ms)": None,
    "timestamp(ns)": None,
    "timestamp(us)": None,
    "string": "object",
    "large_string": "object",
    "utf8": "object",
    "large_utf8": "object",
    "binary": None,
    "large_binary": None,
    "list": None,
    "list_": None,
    "large_list": None,
    "struct": None,
}


@dataclass
class Owner:
    name: str
    email: str


@dataclass
class RapidTableMeta:
    domain: str
    dataset: str
    sensitivity: str
    version: Optional[int]
    key_value_tags: Dict[str, str] = dict()
    key_only_tags: List[str] = list()
    owners: Optional[List[Owner]] = None


class RapidApiConverter(BaseConverter):
    def __init__(self):
        super().__init__(None)
        super()._mojap_to_rapid_types = _mojap_to_rapid_types

    def convert_col_type(self, coltype: str) -> str:
        out_col = _mojap_to_rapid_types(coltype)
        if out_col is None:
            raise NotImplementedError(f"{coltype} is not a valid rAPId type.")
        return out_col

    def generate_from_meta(
        self,
        metadata: Metadata,
        rapid_table_meta: RapidTableMeta,
        date_format: str = "%Y/%m/%d",
    ) -> dict:
        cols = []

        partition_indices = {v: k for k, v in enumerate(metadata.partitions)}

        for col in metadata.columns:
            rapid_col = {
                "name": col["name"],
                "partition_index": partition_indices.get(col["name"], None),
                "data_type": self.convert_col_type(col["type"]),
                "allow_null": col.get["nullable", True],
            }
            if rapid_col["data_type"] == "date":
                rapid_col["format"] = date_format

            cols.append(rapid_col)
        return {"metadata": rapid_table_meta, "columns": cols}
