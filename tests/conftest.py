import json
import os
from typing import Optional


import boto3
import psycopg2
import pytest
from _pytest.mark.structures import MarkDecorator
import testing.postgresql
from moto import mock_glue
from sqlalchemy import create_engine

from mojap_metadata import Metadata


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    mocked_envs = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]
    for menv in mocked_envs:
        os.environ[menv] = "testing"

    yield  # Allows us to close down envs on exit

    for menv in mocked_envs:
        del os.environ[menv]


@pytest.fixture(scope="function")
def glue_client(aws_credentials):
    with mock_glue():
        yield boto3.client("glue", region_name="eu-west-1")


@pytest.fixture(scope="session")
def postgres_connection():

    psql = testing.postgresql.Postgresql(port=5433)
    info = psql.dsn()

    connection = psycopg2.connect(
        user=info["user"],
        host=info["host"],
        database=info["database"],
        port=info["port"],
        password="postgres",  # pragma: allowlist secret
    )
    connection.autocommit = True
    engine = create_engine(psql.url())
    yield engine, psql, connection  # Allows us to close down envs on exit
    psql = psql.stop()
    connection.close()


# test input and expected metadata for the Metadata.column_names_to_lower
# and columns_names_to_upper tests
@pytest.fixture(scope="function")
def meta_input():
    meta = Metadata(
        columns=[
            {"name": "A", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "C", "type": "date32"},
            {"name": "D", "type": "date32"},
            {"name": "e", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="function")
def expected_meta_out_lower():
    meta = Metadata(
        columns=[
            {"name": "a", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "date32"},
            {"name": "d", "type": "date32"},
            {"name": "e", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="function")
def expected_meta_out_upper():
    meta = Metadata(
        columns=[
            {"name": "A", "type": "int8"},
            {"name": "B", "type": "string"},
            {"name": "C", "type": "date32"},
            {"name": "D", "type": "date32"},
            {"name": "E", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="module")
def iceberg_metadata_dictionary():
    with open("tests/data/aws_iceberg_converter/metadata.json", "r") as f:
        meta_dict = json.load(f)
    yield meta_dict


class Case:
    """Container for a test case, with optional test ID.

    Attributes
    ----------
        label
            Optional test ID. Will be displayed for each test when
            running `pytest -v`.
        kwargs
            Parameters used for the test cases.

    Examples
    --------
    >>> Case(label="some test name", foo=10, bar="some value")
    >>> Case(foo=99, bar="some other value")   # no name given

    See also
    --------
    source: https://github.com/ckp95/pytest-parametrize-cases
    """

    def __init__(
        self,
        label: Optional[str] = None,
        marks: Optional[MarkDecorator] = None,
        **kwargs,
    ):
        """Initialise objects."""
        self.label = label
        self.kwargs = kwargs
        self.marks = marks
        # Makes kwargs accessible with dot notation.
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        """Return string."""
        return f"Case({self.label!r}, **{self.kwargs!r})"


def parametrize_cases(*cases: Case):
    """More user friendly parameterize cases testing.

    See: https://github.com/ckp95/pytest-parametrize-cases
    """

    if not all(isinstance(case, Case) for case in cases):
        raise TypeError("All arguments must be instances of Case")

    all_arguments = sorted({arg for case in cases for arg in case.kwargs})

    case_values = []
    case_ids = []
    for case in cases:
        # If a test is missing an argument, fill it with `None`
        case_kwargs = {arg: case.kwargs.get(arg, None) for arg in all_arguments}
        case_tuple = tuple(case_kwargs[arg] for arg in all_arguments)

        # If marks are given, wrap the case tuple.
        if case.marks:
            case_tuple = pytest.param(*case_tuple, marks=case.marks)

        case_values.append(case_tuple)
        case_ids.append(case.label)

    if len(all_arguments) == 1:
        # otherwise it gets passed to the test function as a singleton tuple
        case_values = [values[0] for values in case_values]

    return pytest.mark.parametrize(
        argnames=",".join(sorted(all_arguments)), argvalues=case_values, ids=case_ids
    )
