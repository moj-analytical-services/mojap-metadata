from typing import Union

from converters.base_converter import BaseConverter

import pandas as pd

class PandasConverter(BaseConverter):
    """
    Will either convert a pandas dataframe to the correct metadata
    Or will infer the metadata from the pandas dataframe
    """
    def __init__(self, type_assumption:str = "csv", **kwargs):
        super().__init__()
        self.pandas_v1 = pd.__version__ >= "1.0.0"
        self.type_assumption = type_assumption


    def generate_to_meta(self, item: Union[str, pd.DataFrame], **kwargs) -> Metadata:
        """
        Infers what the metadata should be based on the dataframe or filepath provided.
        """
        if isinstance(item, str):
            if self.type_assumption == "csv":
                df = pd.read_csv(item, infer_datetime_format=True, **kwargs)
            else:
                raise ValueError("Only supports CSVs")

        elif isinstance(item, pd.DataFrame):
            df = item
        else:
            raise ValueError("item is wrong")

        meta = Metadata()

        for c in df.columns:
            # as an example. (Obvs would actually get types)
            meta.add_column(c, type="character")

        return meta


    def generate_from_meta(self, metadata: Metadata, df: pd.DataFrame, **kwargs) -> object:
        """
        Casts the types in a dataframe given the metadata. 
        """
        # Have this code but will come back to this
        pass

        return df
