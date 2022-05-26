from typing import List


def sanitize_columns(columns: List[str]) -> List[str]:
    return [column.replace(" ", "_") for column in columns]
