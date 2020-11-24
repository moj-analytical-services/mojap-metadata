from typing import Union

from converters.base_converter import BaseConverter

import boto3


class GlueConverter(BaseConverter):
    """
    Will either convert a glue table DDL to our metadata
    Or will generate glue table DDL from our metadata
    """

    def __init__(self, database_config: dict, **kwargs):
        super().__init__()
        self.database_config = database_config

    def generate_to_meta(self, item: Union[dict, str], **kwargs) -> Metadata:
        """
        Converts table DDL to our metadata
        """
        pass

        return Metadata

    def generate_from_meta(
        self,
        metadata: Metadata,
        custom_ddl_spec=None,
        full_database_path=None,
        glue_specific=None,
    ):
        "Creates a glue ddl from the metadata"
        glue_table_definition = _get_spec("base")

        # should allow custom specs otherwise default to this
        if custom_ddl_spec:
            specific = custom_ddl_spec
        else:
            specific = _get_spec(metadata.data_format)

        _dict_merge(glue_table_definition, specific)

        # Create glue specific variables from meta data
        glue_table_definition["Name"] = metadata.name
        glue_table_definition["Description"] = metadata.description

        glue_table_definition["StorageDescriptor"][
            "Columns"
        ] = self._generate_glue_columns(exclude_columns=metadata.partitions)

        if metadata.data_format == "json":
            non_partition_names = [
                c for c in metadata.column_names if c not in metadata.partitions
            ]
            glue_table_definition["StorageDescriptor"]["SerdeInfo"]["Parameters"][
                "paths"
            ] = ",".join(non_partition_names)

        if full_database_path:
            glue_table_definition["StorageDescriptor"]["Location"] = os.path.join(
                full_database_path, metadata.location
            )
        else:
            raise ValueError(
                "Need to provide a database or full database path to generate glue table def"
            )

        if glue_specific:
            _dict_merge(glue_table_definition, glue_specific)

        if len(metadata.partitions) > 0:
            not_partitions = [
                c for c in metadata.column_names if c not in metadata.partitions
            ]
            glue_partition_cols = self._generate_glue_columns(
                exclude_columns=not_partitions
            )

            glue_table_definition["PartitionKeys"] = glue_partition_cols

        return glue_table_definition

    def _generate_glue_columns(self, metadata: Metadata, exclude_columns=[]):

        glue_columns = []
        for c in metadata.columns:
            if c["name"] not in exclude_columns:
                new_c = {}
                new_c["Name"] = c["name"]
                new_c["Comment"] = c["description"]
                new_c["Type"] = _agnostic_to_glue_spark_dict[
                    trim_complex_type(c["type"])
                ]["glue"]
                glue_columns.append(new_c)

        return glue_columns
