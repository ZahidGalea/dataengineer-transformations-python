import csv
import os
import tempfile
from typing import Tuple, List
import pytest
from pyspark.sql import SparkSession


class Helpers:
    @staticmethod
    def create_input_and_output_folders() -> Tuple[str, str]:
        return tempfile.mkdtemp(), tempfile.mkdtemp()

    @staticmethod
    def write_csv_file(file_path: str, content: List[List[str]]) -> None:
        with open(file_path, 'w') as csv_file:
            input_csv_writer = csv.writer(csv_file)
            input_csv_writer.writerows(content)
            csv_file.close()

    @staticmethod
    def write_lines_file(input_text_path, input_file_lines: List[str]) -> None:
        with open(input_text_path, 'w') as input_file:
            input_file.writelines(input_file_lines)

    @staticmethod
    def write_parquet_file(spark_session, data, columns, target_path) -> None:
        ingest_dataframe = spark_session.createDataFrame(data, columns)
        ingest_dataframe.write.parquet(target_path, mode='overwrite')


@pytest.fixture
def spark_session() -> SparkSession:
    spark_session = SparkSession.builder.appName("IntegrationTests").getOrCreate()
    return spark_session


@pytest.fixture(autouse=True)
def helpers():
    return Helpers
