from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters import BaseConverter
import pyarrow as pa
from typing import Tuple, List, Any


def _get_pa_type(meta_type: str) -> pa.DataType:
    """Returns a pyarrow type from the name
    of the type in our metadata (which is based on
    arrow types)

    Args:
        meta_type (str): The metadata name for the datatype

    Returns:
        [pa.DataType]: A pyarrow datatype obj
    """
    is_time_type = meta_type.startswith("time")
    is_decimal_type = meta_type.startswith("decimal128")
    is_binary_type = meta_type.startswith("binary")
    if is_time_type or is_decimal_type or is_binary_type:
        attr_name, values = _extract_bracket_params(meta_type)
    else:
        attr_name = meta_type
        values = []

    return getattr(pa, attr_name)(*values)


def _extract_bracket_params(meta_type: str) -> Tuple[str, List[Any]]:
    """
    Gets parameters from the string representation of the type

    Args:
        meta_type (str): The string name of the metadata type

    Returns:
        Tuple[str, List[Any]]: A tuple, first arg is a string of the type
        name only and then the second value is a list of values (if any)
        inside the brackets of the meta_type. e.g. "int64" returns ("int64", [])
        and "decimal128(1,2)" returns ("decimal128", [1, 2])
    """
    is_decimal_type = meta_type.startswith("decimal128")
    is_binary_type = meta_type.startswith("binary")

    if "(" in meta_type:
        attr_name, value_str = meta_type.split("(", 1)
        value_str = value_str.split(")")[0]
        values = value_str.split(",")
        if not any([bool(v) for v in values]):
            values = []

        # cast input to int for specific types
        if (is_decimal_type or is_binary_type) and values:
            values = [int(v.strip()) for v in values]
    else:
        attr_name = meta_type
        values = []
    return attr_name, values


class ArrowConverter(BaseConverter):
    def __init__(self):
        """
        Converts metadata objects to an Arrow Schema.
        Note that this converter has no options
        (i.e. ArrowConverter().options returns
        the BaseCoverterOptions)

        Example:
        from mojap_metadata.converters.arrow_converter import (
            ArrowConverter,
        )

        ac = ArrowConverter()
        metadata = Metadata.from_json("my-table-metadata.json")
        pyarrow_schema = ac.generate_from_meta(metadata) # get pyArrow Schema
        """
        super().__init__(None)

    def generate_from_meta(
        self,
        metadata: Metadata,
        drop_partitions: bool = True,
    ) -> pa.Schema:
        """Generates the Hive DDL from our metadata

        Args:
            metadata (Metadata): metadata object from the Metadata class
            drop_partitions (bool): Drop partitions from the outputted pyarrow schema.
              defaults to True.

        Returns:
            pa.Schema: A Schema for a pyArrow table
        """

        arrow_cols = []
        for col in metadata.columns:
            if drop_partitions and (col["name"] in metadata.partitions):
                pass
            else:
                arrow_cols.append(
                    pa.field(
                        col["name"],
                        _get_pa_type(col["type"]),
                        nullable=col.get("nullable", True)
                    )
                )

        return pa.schema(arrow_cols)
