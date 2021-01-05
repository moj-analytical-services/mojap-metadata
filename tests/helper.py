import pytest
from typing import Any


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
            fail_info = "Explected no warning as options.ignore_warnings = True."
            pytest.fail(fail_info)
    else:
        with pytest.warns(None) as record:
            assert getattr(converter, funname)(in_type) == out_type
            assert getattr(yolo_converter, funname)(in_type) == out_type

        if len(record) != 0:
            fail_info = "Warnings raised when expected no warning on these conversions"
            pytest.fail(fail_info)
