from data_transformations.citibike import ingest


def test_should_sanitize_nothing() -> None:
    input = ['foo']
    expected = ['foo']
    assert expected == ingest.sanitize_columns(input)


def test_should_sanitize_whitespace_outside() -> None:
    input = [' foo ']
    expected = ['_foo_']
    assert expected == ingest.sanitize_columns(input)


def test_should_sanitize_whitespace_in_between() -> None:
    input = ['foo bar']
    expected = ['foo_bar']
    assert expected == ingest.sanitize_columns(input)
