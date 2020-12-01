import pytest
import warnings

from mojap_metadata.converters.glue_converter import GlueConverter, GlueConverterOptions


@pytest.mark.parametrize(
    argnames="meta_type,glue_type,expect_raises",
    argvalues=[
        ("int8", "TINYINT", None)
        ("int16", "SMALLINT", None)
        ("int32", "INT", None)
        ("int64", "BIGINT", None)
        ("uint8", "SMALLINT", "warning")
        ("uint16", "INT", "warning")
        ("uint32", "BIGINT", "warning")
        ("uint64", None, "error")
        ("float16", "FLOAT", "warning")
        ("float32", "FLOAT", None)
        ("float64", "DOUBLE", None)
        ("decimal128(0,38)", "DECIMAL(0,38)", None)
        ("decimal128(1,2)", "DECIMAL(2,2)", None)
        ("time32(s)",  None, True, True)
        ("time32(ms)", None, True, True)
        ("time64(us)", None, True, True)
        ("time64(ns)", None, True, True)
        ("timestamp(s)", "TIMESTAMP", None)
        ("timestamp(ms)", "TIMESTAMP", None)
        ("timestamp(us)", "TIMESTAMP", None)
        ("timestamp(ns)", "TIMESTAMP", None)
        ("date32", "DATE", None)
        ("date64", "DATE", None)
        ("string", "STRING", None)
        ("large_string", "STRING", None)
        ("utf8", "STRING", None)
        ("large_utf8", "STRING", None)
        ("binary", "BINARY", None)
        ("binary(128)", "BINARY", "warn")
        ("large_binary", "BINARY", "warn")
    ],
)
def test_meta_to_glue_type(meta_type, glue_type, expect_raises):
    gc = GlueConverter()
    gc_yolo = GlueConverter(
        GlueConverterOptions(ignore_warnings=True)
    )
    if expect_raises == "error":
        with pytest.raises(ValueError):
            gc.convert_col_type(meta_type)
            gc_yolo.convert_col_type(meta_type)

    elif expect_raises == "warn":
        with pytest.raises(warnings.warn):
            assert gc.convert_col_type(meta_type) == glue_type

        assert gc_yolo.convert_col_type(meta_type) == glue_type

    else:
        assert gc.convert_col_type(meta_type) == glue_type
        assert gc_yolo.convert_col_type(meta_type) == glue_type

