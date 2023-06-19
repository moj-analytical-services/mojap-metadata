class NonIcebergGlueTable(Exception):
    pass


class MalformedIcebergPartition(Exception):
    pass


class GlueIcebergTableExists(Exception):
    pass


class UnsupportedIcebergSchemaEvolution(Exception):
    pass
