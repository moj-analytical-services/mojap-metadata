import warnings

from mojap_metadata.metadata.metadata import (
    Metadata,
    _unpack_complex_data_type,
    _metadata_complex_dtype_names,
)
from mojap_metadata.converters import (
    BaseConverter,
    _flatten_and_convert_complex_data_type,
)

import pyarrow as pa
from typing import Tuple, List, Any, Union, Callable


_arrow_id_to_static_metatype = {
    0: "null",
    1: "bool",
    2: "uint8",
    3: "int8",
    4: "uint16",
    5: "int16",
    6: "uint32",
    7: "int32",
    8: "uint64",
    9: "int64",
    10: "float16",
    11: "float32",
    12: "float64",
    13: "string",
    14: "binary",  # Non fixed width
    16: "date32",
    17: "date64",
    34: "large_string",
    35: "large_binary",
}


def _get_arrow_time(arrow_type: Union[pa.lib.Time32Type, pa.lib.Time64Type]):
    return f"time{arrow_type.bit_width}({arrow_type.unit})"


def _get_arrow_timestamp(arrow_type: pa.lib.TimestampType):
    return f"timestamp({arrow_type.unit})"


def _get_decimal128(arrow_type: pa.lib.Decimal128Type):
    return f"decimal128({arrow_type.precision}, {arrow_type.scale})"


def _get_fixed_width_binary(arrow_type: pa.lib.FixedSizeBinaryType):
    return f"binary({arrow_type.byte_width})"


_arrow_id_to_callable_metatype = {
    15: _get_fixed_width_binary,
    18: _get_arrow_timestamp,
    19: _get_arrow_time,
    20: _get_arrow_time,
    23: _get_decimal128,
}


def _rename_data_type_to_arrow_type(data_type: str):
    if data_type == "bool":
        return "bool_"
    elif data_type == "list":
        return "list_"
    else:
        return data_type


def _simple_arrow_type_conversion(arrow_type: pa.lib.DataType) -> str:
    if arrow_type.id in _arrow_id_to_static_metatype:
        mtype = _arrow_id_to_static_metatype[arrow_type.id]
    elif arrow_type.id in _arrow_id_to_callable_metatype:
        mtype = _arrow_id_to_callable_metatype[arrow_type.id](arrow_type)
    else:
        raise NotImplementedError(
            f"No conversion available for arrow_type: {str(arrow_type)}"
        )
    return mtype


def _is_pa_struct(thing: Any):
    return isinstance(thing, pa.lib.StructType)


def _is_pa_list(thing: Any):
    return isinstance(thing, (pa.lib.ListType, pa.lib.LargeListType))


def _convert_complex_pa_to_data_type(arrow_type: pa.lib.DataType) -> Union[dict, str]:
    """Recursive function that jumps into complex arrow types (structs and lists)
    and returns complex types as a dict. Non complex types are returned as a str.
    Names returned are those given to the agnostic data types in Metadata.

    Args:
        arrow_type (pa.lib.DataType): An arrow type

    Returns:
        Union[dict, str]: unpacked representation of data type as dict
            or str.
    """
    d = {}
    # For struct iterate through each key value binding
    # until each field name has its corresponding type returned
    if _is_pa_struct(arrow_type):
        d["struct"] = {}
        for field in arrow_type:
            d["struct"][field.name] = _convert_complex_pa_to_data_type(field.type)
        return d
    # For list get the data type for the lists content and return the list
    elif _is_pa_list(arrow_type):
        k = "list" if arrow_type.id == 25 else "large_list"
        d[k] = _convert_complex_pa_to_data_type(arrow_type.value_type)
        return d
    else:
        # If the data type is simple then return the simple conversion
        # this return is what ends the recursion
        return _simple_arrow_type_conversion(arrow_type)


def _convert_complex_data_type_to_pa(
    data_type: Union[dict, str], converter_fun: Callable
) -> Any:
    """
    Recursive function to unpack complex and basic datatypes.
    """
    if isinstance(data_type, str):
        return converter_fun(data_type)

    else:
        fields = []
        for k, v in data_type.items():
            if k in _metadata_complex_dtype_names:
                inner_data_type = _convert_complex_data_type_to_pa(v, converter_fun)
                arrow_attr = _rename_data_type_to_arrow_type(k)
                return getattr(pa, arrow_attr)(inner_data_type)
            else:
                new_v = _convert_complex_data_type_to_pa(v, converter_fun)
                fields.append((k, new_v))

        return fields


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

    def convert_col_type(self, coltype: str) -> pa.DataType:
        """Converts our metadata types to arrow data type object

        Args:
            coltype (str): str representation of our metadata column types

        Returns:
            pa.DataType: Arrow data type
        """

        data_type = _unpack_complex_data_type(coltype)

        return _convert_complex_data_type_to_pa(data_type, self.convert_basic_col_type)

    def convert_basic_col_type(self, coltype: str) -> pa.DataType:
        """
        Returns a pyarrow type from the name
        of the type in our metadata (which is based on
        arrow types)

        Args:
            coltype ([str]): str representation of our metadata column types

        Returns:
            [pa.DataType]: The equivalent type object in pyArrow
        """
        is_time_type = coltype.startswith("time")
        is_decimal_type = coltype.startswith("decimal128")
        is_binary_type = coltype.startswith("binary")
        if is_time_type or is_decimal_type or is_binary_type:
            attr_name, values = _extract_bracket_params(coltype)
        else:
            attr_name = coltype
            values = []

        # Allowing for types without underscores
        if coltype == "bool":
            attr_name = "bool_"
        elif coltype == "list":
            attr_name = "list_"
        else:
            pass

        return getattr(pa, attr_name)(*values)

    def generate_from_meta(
        self,
        metadata: Metadata,
        drop_partitions: bool = True,
    ) -> pa.Schema:
        """Generates an arrow schema from our metadata

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
                        self.convert_col_type(col["type"]),
                        nullable=col.get("nullable", True),
                    )
                )

        return pa.schema(arrow_cols)

    def generate_to_meta(
        self, arrow_schema: pa.Schema, meta_init_dict: dict = None
    ) -> Metadata:
        """Generates our metadata instance from an arrow schema

        Args:
            arrow_schema (pa.Schema): pa.Schema from an arrow table

        Returns:
            Metadata: An agnostic metadata instance
        """
        if not meta_init_dict:
            meta_init_dict = {}

        if "columns" in meta_init_dict:
            warnings.warn("columns key found in meta_init_dict will be overwritten")

        meta_init_dict["columns"] = []
        meta_init_dict["_converted_from"] = "arrow_schema"

        for field in arrow_schema:
            meta_init_dict["columns"].append(
                {"name": field.name, "type": self.reverse_convert_col_type(field.type)}
            )

        m = Metadata.from_dict(meta_init_dict)
        return m

    def reverse_convert_col_type(self, arrow_type: pa.lib.DataType) -> str:
        """Converts an arrow type to a metadata col type

        Args:
            arrow_field (pa.Field): an arrow field to convert

        Returns:
            str: str representation of Metadata col type
        """
        data_type_dict = _convert_complex_pa_to_data_type(arrow_type)
        # Types in are data_type_dict have already beenm converted
        # to our agnostic metadata type names so can flatten with
        # no converter (aka use this lambda fun)
        data_type = _flatten_and_convert_complex_data_type(
            data_type=data_type_dict, converter_fun=lambda x: x
        )
        return data_type
