import pytest

from source.utils.sanitization.sanitization import sanitize_columns


@pytest.mark.parametrize("input_list,expected", [(['foo'], ['foo']),
                                                 ([' foo '], ['_foo_']),
                                                 (['foo bar'], ['foo_bar'])])
def test_ingest_should_sanitize_columns_correctly(input_list, expected) -> None:
    assert expected == sanitize_columns(input_list)
