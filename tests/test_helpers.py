import pytest
import warnings
from tests.helper import assert_meta_col_conversion
from mojap_metadata.converters import BaseConverter

from _pytest.outcomes import Failed


class DummyConverter(BaseConverter):
    def __init__(self):
        super().__init__(None)

    def convert_col_type(self, coltype: str) -> str:
        """
        Just returns uppercase version on coltype.
        Will raise a warning if name starts with warn
        and will raise an error if name starts with error
        """

        if coltype.startswith("error"):
            raise ValueError("coltype starts with error")

        if coltype.startswith("warn") and not self.options.ignore_warnings:
            warnings.warn("coltype starts with warn")

        return coltype.upper()


@pytest.mark.parametrize(
    argnames="meta_type,new_type,expect_raises",
    argvalues=[
        ("test", "TEST", None),
        ("warning_test", "WARNING_TEST", "warning"),
        ("error_test", None, "error"),
    ],
)
def test_assert_meta_to_new_type_conversion(meta_type, new_type, expect_raises):
    assert_meta_col_conversion(DummyConverter, meta_type, new_type, expect_raises)


def test_assert_meta_to_new_type_conversion_edge_cases():
    with pytest.raises(Failed, match="DID NOT WARN."):
        assert_meta_col_conversion(DummyConverter, "test", "TEST", "warning")

    with pytest.raises(Failed, match="DID NOT RAISE"):
        assert_meta_col_conversion(DummyConverter, "test", "TEST", "error")
