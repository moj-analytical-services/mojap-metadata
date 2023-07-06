import importlib.resources as pkg_resources
import os
import re
from typing import Dict, List, Optional, Tuple, Union

import awswrangler as wr
import boto3
from boto3.session import Session
from dataengineeringutils3.s3 import read_json_from_s3
from jinja2 import Template

from mojap_metadata.converters.aws_iceberg_converter import specs
from mojap_metadata.converters.aws_iceberg_converter.exceptions import (
    GlueIcebergTableExists,
    MalformedIcebergPartition,
    NonIcebergGlueTable,
    UnsupportedIcebergSchemaEvolution,
)
from mojap_metadata.converters.aws_iceberg_converter.iceberg_metadata import (
    IcebergMetadata,
)
from mojap_metadata.converters.glue_converter import GlueConverter, GlueTable
from mojap_metadata.metadata.metadata import Metadata

_supported_changes = [
    (
        "integer",
        "bigint",
    ),
    (
        "int",
        "bigint",
    ),
    (
        "float",
        "double",
    ),
]


class AthenaIcebergSqlConverter(GlueConverter):
    @staticmethod
    def _combine_partition_name_and_transform(name: str, transform: str) -> str:
        transform_arg_search = re.search("\\[[0-9]*\\]", transform)

        if "bucket" in transform or "truncate" in transform:
            if transform_arg_search is not None:
                transform_arg = re.sub("\\[|\\]", "", transform_arg_search.group(0))
                transform_fn = re.sub("\\[.*\\]", "", transform)

            else:
                raise MalformedIcebergPartition(
                    f"{transform} should have a length / width variable specified"
                )

        if "bucket" in transform:
            full_partition = f"{transform_fn}({transform_arg}, {name})"
        elif "truncate" in transform:
            full_partition = f"{transform_fn}({name}, {transform_arg})"
        elif transform == "identity":
            full_partition = name
        else:
            full_partition = f"{transform}({name})"

        return full_partition

    def convert_columns(self, metadata: IcebergMetadata) -> Tuple[List, List]:
        cols = [
            (
                c["name"],
                self.convert_col_type(c["type"]),
                c["description"] if "description" in c else "",
            )
            for c in metadata.columns
        ]

        if metadata.partition_transforms is not None:
            partitions = [
                self._combine_partition_name_and_transform(
                    name,
                    transform,
                )
                for name, transform in zip(
                    metadata.partitions,
                    metadata.partition_transforms,
                )
            ]
        else:
            partitions = []

        return cols, partitions

    def generate_from_meta(
        self,
        metadata: Union[IcebergMetadata, str, dict],
        database_name: str = None,
        table_location: str = None,
        create_not_alter: bool = True,
        existing_metadata: Optional[Union[IcebergMetadata, str, dict, None]] = None,
        **kwargs,
    ) -> List[str]:
        method_id = "create" if create_not_alter else "alter"
        method = getattr(self, f"generate_{method_id}_from_meta")

        metadata_to_use = IcebergMetadata.from_infer(metadata)
        table_location_to_use = (
            table_location if table_location is not None else metadata.table_location
        )
        database_name_to_use = (
            database_name if database_name is not None else metadata.database_name
        )

        if database_name_to_use is None:
            raise ValueError("Database name must be set in metadata or via method")

        method_kwargs = {
            "metadata": metadata_to_use,
            "database_name": database_name_to_use,
            "table_location": table_location_to_use,
            **kwargs,
        }

        if not create_not_alter:
            if existing_metadata is None:
                raise ValueError("existing_metadata must be specified")

            method_kwargs["existing_metadata"] = existing_metadata

        queries = method(**method_kwargs)
        if isinstance(queries, str):
            queries = [queries]

        return queries

    def generate_create_from_meta(
        self,
        metadata: Union[IcebergMetadata, str, dict],
        database_name: str = None,
        table_location: str = None,
        **kwargs,
    ) -> str:
        metadata_to_use = IcebergMetadata.from_infer(metadata)
        table_location_to_use = (
            table_location if table_location is not None else metadata.table_location
        )
        database_name_to_use = (
            database_name if database_name is not None else metadata.database_name
        )

        if table_location_to_use is None:
            raise ValueError("Table location must be set in metadata or via method")

        columns, partitions = self.convert_columns(metadata_to_use)

        raw_sql = pkg_resources.open_text(specs, "create.sql").read()

        sql_template = Template(raw_sql)

        sql = sql_template.render(
            table_name=metadata_to_use.name,
            database_name=database_name_to_use,
            table_location=table_location_to_use,
            columns=columns,
            partitions=partitions,
            **kwargs,
        )

        return sql

    def generate_alter_from_meta(
        self,
        metadata: Union[IcebergMetadata, str, dict],
        existing_metadata: Union[IcebergMetadata, str, dict],
        database_name: str = None,
        **kwargs,
    ) -> List[str]:
        meta = IcebergMetadata.from_infer(metadata)
        existing_meta = IcebergMetadata.from_infer(existing_metadata)

        database_name_to_use = (
            database_name if database_name is not None else metadata.database_name
        )

        add_columns, changed_columns = self._find_new_or_updated_columns(
            metadata=meta,
            existing_metadata=existing_meta,
        )

        removed_columns = self._find_removed_columns(
            metadata=meta,
            existing_metadata=existing_meta,
        )

        queries = []
        if add_columns:
            add_query = self._generate_alter_add_query(
                columns=add_columns,
                table_name=meta.name,
                database_name=database_name_to_use,
                **kwargs,
            )
            queries.append(add_query)

        queries += self._generate_alter_change_queries(
            changed_columns,
            table_name=meta.name,
            database_name=database_name_to_use,
        )

        queries += self._generate_alter_drop_queries(
            removed_columns,
            table_name=meta.name,
            database_name=database_name_to_use,
        )

        return queries

    @staticmethod
    def _generate_alter_add_query(
        columns: List[Tuple[str, str]], table_name: str, database_name: str, **kwargs
    ) -> str:
        raw_sql = pkg_resources.open_text(specs, "alter_add_columns.sql").read()

        sql_template = Template(raw_sql)

        sql = sql_template.render(
            table_name=table_name,
            database_name=database_name,
            columns=columns,
            **kwargs,
        )

        return sql

    @staticmethod
    def _generate_alter_change_queries(
        changed_columns: List[Tuple[str, str]],
        table_name: str,
        database_name: str,
        **kwargs,
    ) -> List[str]:
        alter_change_queries = []

        raw_sql = pkg_resources.open_text(specs, "alter_change_column.sql").read()

        sql_template = Template(raw_sql)

        for column_name, column_type in changed_columns:
            sql = sql_template.render(
                table_name=table_name,
                database_name=database_name,
                column_old_name=column_name,
                column_new_name=column_name,
                column_type=column_type,
                **kwargs,
            )
            alter_change_queries.append(sql)

        return alter_change_queries

    @staticmethod
    def _generate_alter_drop_queries(
        removed_columns: List[str], table_name: str, database_name: str, **kwargs
    ) -> List[str]:
        alter_drop_queries = []

        raw_sql = pkg_resources.open_text(specs, "alter_drop_column.sql").read()

        sql_template = Template(raw_sql)

        for column_name in removed_columns:
            sql = sql_template.render(
                table_name=table_name,
                database_name=database_name,
                column_name=column_name,
                **kwargs,
            )
            alter_drop_queries.append(sql)

        return alter_drop_queries

    def _find_new_or_updated_columns(
        self, metadata: Metadata, existing_metadata: Metadata
    ) -> List[Tuple[str, str]]:
        add_columns = []
        changed_columns = []

        for column in metadata.columns:
            column_name = column["name"]
            column_type = self.convert_col_type(column["type"])

            if column_name not in existing_metadata.column_names:
                add_columns.append((column_name, column_type))

            else:
                old_column_type = self.convert_col_type(
                    existing_metadata.get_column(column_name)["type"]
                )

                if column_type != old_column_type:
                    supported = (old_column_type, column_type) in _supported_changes

                    if not supported:
                        raise UnsupportedIcebergSchemaEvolution(
                            f"Can't implement change from {old_column_type}"
                            + f" to {column_type}"
                        )

                    changed_columns.append((column_name, column_type))

        return add_columns, changed_columns

    @staticmethod
    def _find_removed_columns(
        metadata: Metadata, existing_metadata: Metadata
    ) -> List[str]:
        return [
            column_name
            for column_name in existing_metadata.column_names
            if column_name not in metadata.column_names
        ]


class AwsIcebergTable(GlueTable):
    def __init__(self):
        super().__init__(glue_converter_options=None)
        self.sql_converter = AthenaIcebergSqlConverter()

    @staticmethod
    def _pre_generation_setup(
        database_name: str,
        table_name: str,
        delete_table_if_exists: bool,
        table_location: Union[str, None] = None,
        boto3_session: Union[Session, None] = None,
    ) -> bool:
        # create database if it doesn't exist
        existing_databases = wr.catalog.databases(None).Database.to_list()
        if database_name not in existing_databases:
            wr.catalog.create_database(name=database_name, boto3_session=boto3_session)

        if delete_table_if_exists:
            if table_location is None:
                raise ValueError("Table location must be set to re-create the table")

            _ = wr.catalog.delete_table_if_exists(
                database=database_name, table=table_name, boto3_session=boto3_session
            )
            wr.s3.delete_objects(table_location, boto3_session=boto3_session)
            table_exists = False

        else:
            table_exists = wr.catalog.does_table_exist(
                database=database_name, table=table_name, boto3_session=boto3_session
            )

        return table_exists

    @staticmethod
    def _execute_queries(
        queries: List[str], boto3_session: Union[Session, None] = None
    ):
        for query in queries:
            try:
                query_id = wr.athena.start_query_execution(
                    query, boto3_session=boto3_session
                )
                response = wr.athena.wait_query(
                    query_execution_id=query_id, boto3_session=boto3_session
                )
            except Exception:
                raise wr.exceptions.QueryFailed(f"Failed to execute:\n{query}")

            if response["Status"]["State"] == "FAILED":
                raise wr.exceptions.QueryFailed(f"Failed to execute:\n{query}")

            if response["Status"]["State"] == "CANCELLED":
                raise wr.exceptions.QueryCancelled(f"Query cancelled:\n{query}")

    def generate_from_meta(
        self,
        metadata: Union[Metadata, str, dict],
        database_name: str = None,
        table_location: str = None,
        delete_table_if_exists: Optional[bool] = False,
        alter_table_if_exists: Optional[bool] = False,
        boto3_session: Union[Session, None] = None,
    ):
        """
        Creates an Iceberg table in AWS from metadata
        arguments:
            - metadata: Metadata object, string path, or dictionary metadata.
            - database_name (optional): name of the glue database the table is to be
            created in. can also be a property of the metadata.
            - table_location (optional): the s3 location of the table. can also be a
            property of the metadata.
            - delete_table_if_exists (optional): whether to delete the iceberg table and
            underlying data if it exists. Defaults to False.
            - alter_table_if_exists (optional): whether to alter the iceberg table
            schema if it already exists. Defaults to False.
        """

        # set database_name to metadata.database_name if none
        database_name = database_name if database_name else metadata.database_name
        # do the same with table_location
        table_location = table_location if table_location else metadata.table_location

        if database_name is None:
            raise ValueError("Database name must be set in metadata or via method")

        metadata = Metadata.from_infer(metadata)

        table_exists = self._pre_generation_setup(
            database_name=database_name,
            table_name=metadata.name,
            table_location=table_location,
            delete_table_if_exists=delete_table_if_exists,
            boto3_session=boto3_session,
        )

        if not table_exists or alter_table_if_exists:
            queries = self.sql_converter.generate_from_meta(
                metadata=metadata,
                database_name=database_name,
                table_location=table_location,
                create_not_alter=not table_exists,
                existing_metadata=self.generate_to_meta(
                    database=database_name,
                    table=metadata.name,
                )
                if alter_table_if_exists
                else None,
            )

            _ = self._execute_queries(queries, boto3_session=boto3_session)

        else:
            raise GlueIcebergTableExists(
                f"{metadata.name} already exists and delete_table_if_exists"
                + " and alter_table_if_exists both set to False"
            )

    def generate_to_meta(self, database: str, table: str) -> IcebergMetadata:
        glue_client = boto3.client(
            "glue",
            region_name=os.getenv(
                "AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
            ),
        )

        resp = glue_client.get_table(
            DatabaseName=database,
            Name=table,
        )

        table_details = resp["Table"]
        table_storage_details = table_details["StorageDescriptor"]

        table_parameters = table_details.get("Parameters", {})
        table_type = table_parameters.get("table_type", "").lower()

        if table_type != "iceberg":
            raise NonIcebergGlueTable(
                "Please specify an iceberg table registered with Glue"
            )

        table_metadata_location = table_parameters["metadata_location"]
        (
            table_partitions,
            table_partition_transforms,
        ) = self._get_partitions_from_iceberg_metadata(table_metadata_location)

        table_location = table_storage_details["Location"]
        table_iceberg_columns = table_storage_details["Columns"]

        table_current_columns = self.convert_columns(
            [
                {"Name": c["Name"], "Type": c["Type"]}
                for c in table_iceberg_columns
                if c["Parameters"]["iceberg.field.current"] == "true"
            ]
        )

        table_noncurrent_columns = self.convert_columns(
            [
                {"Name": c["Name"], "Type": c["Type"]}
                for c in table_iceberg_columns
                if c["Parameters"]["iceberg.field.current"] == "false"
            ]
        )

        meta_dict = {
            "columns": table_current_columns,
            "table_type": "iceberg",
            "file_format": "parquet",
            "table_location": table_location,
            "noncurrent_columns": table_noncurrent_columns,
            "partitions": table_partitions,
            "partition_transforms": table_partition_transforms,
            "name": table,
            "database_name": database,
        }

        meta = IcebergMetadata.from_dict(meta_dict)

        return meta

    @classmethod
    def _get_partitions_from_iceberg_metadata(
        cls,
        iceberg_metadata_filepath: str,
    ) -> List[str]:
        table_iceberg_metadata = read_json_from_s3(iceberg_metadata_filepath)

        table_partition_spec_id = table_iceberg_metadata.get("default-spec-id")
        table_partition_specs = table_iceberg_metadata.get("partition-specs", [])

        raw_partitions = cls._get_current_partition_spec(
            table_partition_specs, table_partition_spec_id
        )
        partitions = [p["name"] for p in raw_partitions]
        partiton_transforms = [p["transform"] for p in raw_partitions]

        return partitions, partiton_transforms

    @staticmethod
    def _get_current_partition_spec(
        partition_specs: List[Dict], current_partition_spec_id: int
    ) -> List[Dict]:
        partitions = []
        for partition_spec in partition_specs:
            partition_spec_id = partition_spec["spec-id"]
            if partition_spec_id == current_partition_spec_id:
                partitions += partition_spec["fields"]
        return partitions
