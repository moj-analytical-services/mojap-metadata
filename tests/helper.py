import pytest
from typing import Any, Optional
from mojap_metadata import Metadata

valid_types = (
    "null",
    "int8",
    "bool",
    "bool_",  # bool_ DEPRECATED
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "decimal128(1,38)",
    "time32(s)",
    "time32(ms)",
    "time64(us)",
    "time64(ns)",
    "timestamp(s)",
    "timestamp(ms)",
    "timestamp(us)",
    "timestamp(ns)",
    "date32",
    "date64",
    "string",
    "large_string",
    "utf8",
    "large_utf8",
    "binary",
    "binary(128)",
    "large_binary",
    "struct<num:int64>",
    "list<int64>",
    "list_<int64>",  # list_ DEPRECATED
    "list<list_<int64>>",
    "large_list<int64>",
    "large_list<large_list<int64>>",
    "struct<num:int64,newnum:int64>",
    "struct<num:int64,arr:list_<int64>>",
    "list<struct<num:int64,desc:string>>",
    "struct<num:int64,desc:string>",
    "list<decimal128(38,0)>",
    "large_list<decimal128(38,0)>",
)


def assert_meta_col_conversion(
    converter_class: Any,
    in_type: Any,
    out_type: Any,
    expect_raises: str = None,
    reverse: bool = False,
):
    """Generic function to test conversion from our
    agnostic meta_type string name to a new type (whatever converter)
    you have plugged into it.

    Args:
        converter_class: Class used to instantiate a new converter object
        in_type (str): The metadata type that will converted
        out_type (str): The expected output from converting in_type
        expect_raises (str, optional): Can "error", "warning" or None.
        Tells pytest what to expect, a raised error or warning or None
        for nothing raised.
        reverse (bool, optional): If False (default) call the convert_col_type method
        of the converter object. If True use the reverse_convert_col_type method.
    """

    # Test standard coverter_class
    converter = converter_class()
    yolo_converter = converter_class()
    yolo_converter.options.ignore_warnings = True

    funname = "reverse_convert_col_type" if reverse else "convert_col_type"

    if expect_raises == "error":
        with pytest.raises(ValueError):
            getattr(converter, funname)(in_type)
            getattr(yolo_converter, funname)(in_type)

    elif expect_raises == "warning":
        with pytest.warns(UserWarning):
            assert getattr(converter, funname)(in_type) == out_type

        with pytest.warns(None) as record:
            assert getattr(yolo_converter, funname)(in_type) == out_type

        if len(record) != 0:
            fail_info = "Expected no warning as options.ignore_warnings = True."
            pytest.fail(fail_info)
    else:
        with pytest.warns(None) as record:
            assert getattr(converter, funname)(in_type) == out_type
            assert getattr(yolo_converter, funname)(in_type) == out_type

        if len(record) != 0:
            fail_info = "Warnings raised when expected no warning on these conversions"
            pytest.fail(fail_info)


def get_meta(ff: str, additional_params: Optional[dict] = None):
    additional_params = {} if not additional_params else additional_params
    md = Metadata.from_dict(
        {
            "name": "test_table",
            "file_format": ff,
            "columns": [
                {
                    "name": "my_int",
                    "type": "int64",
                    "description": "This is an integer",
                },
                {"name": "my_double", "type": "float64"},
                {"name": "my_date", "type": "date64"},
                {"name": "my_decimal", "type": "decimal128(10,2)"},
                {
                    "name": "my_timestamp",
                    "type": "timestamp(s)",
                    "description": "Partition column",
                },
            ],
            "partitions": ["my_timestamp"],
            **additional_params,
        }
    )

    return md
