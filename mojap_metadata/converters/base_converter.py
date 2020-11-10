from metadata.base_meta import Metadata
from typing import IO, Union

class BaseConverter(object):
    """
    Base class to be used as standard for parsing in an object, say DDL
    or oracle db connection and then outputting a Metadata class. Not sure 
    if needed or will be too strict for generalisation.
    """
    def __init__(self, **kwargs):
        pass


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
