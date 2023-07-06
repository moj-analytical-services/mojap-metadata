import re

from mojap_metadata.metadata.metadata import Metadata, MetadataProperty


class IcebergMetadata(Metadata):
    table_type = MetadataProperty()
    noncurrent_columns = MetadataProperty()
    partition_transforms = MetadataProperty()

    def validate(self):
        super().validate()
        self._validate_partition_transform_is_supported()
        self._validate_same_number_of_partitions_as_transforms_if_given()

    def _validate_same_number_of_partitions_as_transforms_if_given(self):
        if self._data.get("partition_transforms") is not None and not self._data.get(
            "partition_transforms"
        ):
            if len(self._data["partitions"]) != len(self._data["partition_transforms"]):
                raise ValueError(
                    "The number of partitions and partition transforms do not match"
                )

    def _validate_partition_transform_is_supported(
        self,
        transform_regex: str = "|".join(
            [
                "(identity)",
                "(year)",
                "(day)",
                "(hour)",
                "(bucket\\[[0-9]*\\])",
                "(truncate\\[[0-9]*\\])",
            ]
        ),
    ):
        for transform in self._data.get("partition_transforms", []):
            matches = re.search(
                transform_regex,
                transform,
            )
            if matches is None:
                raise ValueError(
                    f"{transform} does not match pattern {transform_regex}"
                )
