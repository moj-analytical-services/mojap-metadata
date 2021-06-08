from mojap_metadata.metadata.metadata import (
    Metadata,
    _metadata_complex_dtype_names,
)
from typing import Union, Any, Callable, Tuple
from collections.abc import Mapping
from dataclasses import dataclass


# https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def _dict_merge(dct, merge_dct):
    """Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            _dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


def _flatten_and_convert_complex_data_type(
    data_type: Union[dict, str],
    converter_fun: Callable,
    complex_dtype_names: Tuple[str] = None,
    field_sep: str = ", ",
) -> str:
    """Recursive function to flattern a complex datatype in a dictionary
    format i.e. output from (from Metadata.unpack_complex_data_type).
    And flattern it down to a string but with the converted data types.

    Args:
        data_type (dict): complex data type as a dictionary
        converter_fun (Callable): standard converter function to change
            a str data_type to the new data_type
        complex_dtype_names (Tuple[str]): A set of names that define a complex
            datatype (the name given before <>). If None, defaults to
            _metadata_complex_dtype_names from the metadata module
            which are the agnostic dtype names

    Returns:
        str: Complex datatype converted back into a flattened str of
            converted datatypes
    """
    if complex_dtype_names is None:
        complex_dtype_names = _metadata_complex_dtype_names

    if isinstance(data_type, str):
        return converter_fun(data_type)

    else:
        fields = []
        for k, v in data_type.items():
            if k in complex_dtype_names:
                inner_data_type = _flatten_and_convert_complex_data_type(
                    v, converter_fun, complex_dtype_names, field_sep=field_sep
                )
                return f"{converter_fun(k)}<{inner_data_type}>"
            else:
                new_v = _flatten_and_convert_complex_data_type(
                    v, converter_fun, complex_dtype_names, field_sep=field_sep
                )
                fields.append(f"{k}:{new_v}")
                del new_v
        return field_sep.join(fields)


@dataclass
class BaseConverterOptions:
    ignore_warnings = False


class BaseConverter:
    def __init__(self, options: Union[BaseConverterOptions, Any] = None):
        """
        Base class to be used as standard for parsing in an object, say DDL
        or oracle db connection and then outputting a Metadata class. Not sure
        if needed or will be too strict for generalisation.

        options (BaseConverterOptions): A simple class that lets users set or get
        particular paramters. Each one will specific to the converter but each
        converter uses this standard class to access and set parameters.
        """
        if options is None:
            self.options = BaseConverterOptions()
        else:
            self.options = options

    def generate_to_meta(self, item, **kwargs) -> Metadata:
        """
        Should be overwritten to transform item into the Metadata object
        """
        raise NotImplementedError("This function has not been overwritten")

    def generate_from_meta(self, metadata: Metadata, **kwargs) -> object:
        """
        Should be overwritten to transform metadata into the MetaData object
        """
        raise NotImplementedError("This function has not been overwritten")

    def convert_col_type(self, coltype: str) -> Any:
        """
        Should be overwritten to transform the col type (str) from the our agnostic
        metadata types to the equivalent type for the converter
        """
        raise NotImplementedError("This function has not been overwritten")

    def reverse_convert_col_type(self, coltype: Any) -> str:
        """
        Should be overwritten to transform a coltype object (Any) to our agnostic
        metadata types
        """
        raise NotImplementedError("This function has not been overwritten")
