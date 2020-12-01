from mojap_metadata.metadata.metadata import Metadata
from typing import IO, Union, Any
from collections.abc import Mapping
from dataclasses import dataclass


# https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def _dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
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


@dataclass
class ConverterOptions:
    ignore_warnings = False


class BaseConverter:
    def __init__(self, options: Union[ConverterOptions, Any] = None):
        """
        Base class to be used as standard for parsing in an object, say DDL
        or oracle db connection and then outputting a Metadata class. Not sure
        if needed or will be too strict for generalisation.

        options (ConverterOptions): A simple class that lets users set or get
        particular paramters. Each one will specific to the converter but each
        converter uses this standard class to access and set parameters.
        """
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

    def to_json(self, filepath: Union[IO, str]):
        """
        Should be overwritten to write Converter parameters to a json config
        """
        raise NotImplementedError("This function has not been overwritten")

    def to_yaml(self, filepath: Union[IO, str]):
        """
        Should be overwritten to write Converter parameters to a yaml config
        """
        raise NotImplementedError("This function has not been overwritten")

    def read_config(self, file: IO):
        """
        Should be overwritten to read a config and parameterise itself.
        Configs should be json or yaml. Can just use yaml to read both:

        with open(file) as f:
            converter.read_config(f)
        """
        raise NotImplementedError("This function has not been overwritten")
