def _camel_to_snake(camel_case: str) -> str:
    """Convert a CamelCase string to snake_case.

    Parameters
    ----------
    camel_case
        The CamelCase string to be converted to snake_case.

    Returns
    -------
    str
        The snake_case representation of the input string.

    Raises
    ------
    ValueError
        If camel_case is an all upper case string.

    Example
    -------
    >>> snake_string = _camel_to_snake('CamelCase')
    >>> print(snake_string)
    'camel_case'
    """
    if camel_case.isupper():
        msg = f"{camel_case} is all upper case. Cannot convert to snake case."
        raise ValueError(msg)

    snake_case = ""

    for i, char in enumerate(camel_case):
        if (
            i > 0
            and i != len(camel_case) - 1
            and char.isupper()
            and camel_case[i - 1].isupper()
            and camel_case[i + 1].islower()
        ):
            # Character is not the first or last character and is upper case
            # and is preceded by upper case but followed by lower case so
            # presume is start of a new word.
            snake_case += "_"

        elif i > 0 and char.isupper() and camel_case[i - 1].isupper():
            # Character is not the first character and is upper case
            # and is preceded by upper case character so presume is part of a
            # "shout-y" word and so don't precede the character with _
            pass

        elif char.isupper() and i > 0:
            # Not the first character in the string so want to put an _ before
            # it.
            snake_case += "_"

        if not char.isalnum():
            snake_case += "_"

        snake_case += char.lower()

    return snake_case


def _make_snake(string: str) -> str:
    """Convert given string to snake_case.

    This will attempt to convert in order:
    1. if string already contains `_` then just ensure all characters are lower
       case and then return it
    2. if the string is all upper case, convert to lower and return
    3. otherwise pass through to the _camel_to_snake function.
    """
    if "_" in string:
        string_elements = string.split("_")
        return "_".join([_make_snake(element) for element in string_elements])
    elif string.isupper():
        return string.lower()
    else:
        return _camel_to_snake(string)
