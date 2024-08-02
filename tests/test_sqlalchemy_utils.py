from .conftest import Case, parametrize_cases
import pytest
import mojap_metadata.converters.sqlalchemy_converter.utils as su


class TestCamelToSnake:
    """Tests for the camel_to_snake function."""

    @parametrize_cases(
        Case(
            label="snake_case",
            input_string="hello_there",
            # Note the extra _.
            expected="hello__there",
        ),
        Case(
            label="camel_case",
            input_string="HelloThereWorld",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_at_end",
            input_string="HelloThereWORLD",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_in_middle",
            input_string="HelloTHEREWorld",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_at_start",
            input_string="HELLOThereWorld",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_string_with_numbers",
            input_string="1Hello2There34World5",
            expected="1_hello2_there34_world5",
        ),
        Case(
            label="camel_case_string_with_non_alphanumeric",
            input_string="!Hello?There@World~",
            expected="_!_hello_?_there_@_world_~",
        ),
        Case(label="empty_string", input_string="", expected=""),
        Case(label="single_underscore", input_string="_", expected="__"),
    )
    def test_expected(
        self,
        input_string,
        expected,
    ):
        """Test the expected functionality."""
        actual = su.camel_to_snake(input_string)

        assert actual == expected

    def test_raises_error_for_all_upper_case(self):
        """Test VasueError is raised when all upper case string passed."""
        input_string = "HELLOWORLD"
        with pytest.raises(
            ValueError,
            match=f"{input_string} is all upper case. Cannot convert to snake case.",
        ):
            su.camel_to_snake(input_string)


class TestMakeSnake:
    """Tests for the make_snake function."""

    @parametrize_cases(
        Case(
            label="snake_case",
            input_string="hello_there",
            # Note the extra _.
            expected="hello_there",
        ),
        Case(
            label="camel_case",
            input_string="hello_ThereWorld",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_at_end",
            input_string="HelloThere_WORLD",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_in_middle",
            input_string="Hello_THERE_World",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_with_upper_case_word_at_start",
            input_string="HELLO_ThereWorld",
            expected="hello_there_world",
        ),
        Case(
            label="camel_case_string_with_numbers",
            input_string="1_Hello2There34World5",
            expected="1_hello2_there34_world5",
        ),
        Case(
            label="camel_case_string_with_non_alphanumeric",
            input_string="!Hello?There@World~",
            expected="_!_hello_?_there_@_world_~",
        ),
        Case(label="empty_string", input_string="", expected=""),
        Case(label="single_underscore", input_string="_", expected="_"),
    )
    def test_expected(
        self,
        input_string,
        expected,
    ):
        """Test the expected functionality."""
        actual = su.make_snake(input_string)

        assert actual == expected
