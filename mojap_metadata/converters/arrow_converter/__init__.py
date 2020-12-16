from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters import BaseConverter
import pyarrow as pa


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
    if is_time_type or is_decimal_type:
        attr_name, values = _extract_bracket_params(meta_type)
        if is_decimal_type:
            values = [int(v.strip()) for v in values]
            pa_type = getattr(pa, attr_name)(*values)
    else:
        pa_type = getattr(pa, meta_type)()

    return pa_type


def _extract_bracket_params(meta_type):
    attr_name, value_str = meta_type.split("(", 1)
    value_str = value_str.split(")")[0]
    values = value_str.split(",")
    return attr_name, values


class ArrowConverter(BaseConverter):
    def __init__(self):
        """
        Converts metadata objects to an Arrow Schema.

        options (GlueConverterOptions, optional): See ?GlueConverterOptions
        for more details. If not set a default GlueConverterOptions is set to
        the options parameter.

        Example:
        from mojap_metadata.converters.arrow_converter import (
            ArrowConverter,
        )
        options = GlueConverterOptions(csv_ddl = _create_open_csv_ddl)
        ac = ArrowConverter()
        metadata = Metadata.from_json("my-table-metadata.json")
        ddl = ac.generate_from_meta(metadata) # get pyArrow Schema
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
                arrow_cols.append((col["name"], _get_pa_type(col["type"])))

        return pa.schema(arrow_cols)
