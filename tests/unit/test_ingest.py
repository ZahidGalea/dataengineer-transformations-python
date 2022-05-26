import pytest

from data_transformations.citibike import ingest


@pytest.mark.parametrize("input,expected", [(['foo'], ['foo']),
                                            ([' foo '], ['_foo_']),
                                            (['foo bar'], ['foo_bar'])])
def test_ingest_should_sanitize_columns_correctly(input, expected) -> None:
    assert expected == ingest.sanitize_columns(input)
