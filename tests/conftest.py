import csv
import os
import tempfile
from typing import Tuple, List
import pytest
from pyspark.sql import SparkSession


class Helpers:
    @staticmethod
    def create_input_and_output_folders() -> Tuple[str, str]:
        base_path = tempfile.mkdtemp()
        ingest_folder = "%s%sinput" % (base_path, os.path.sep)
        transform_folder = "%s%soutput" % (base_path, os.path.sep)
        return ingest_folder, transform_folder

    @staticmethod
    def write_csv_file(file_path: str, content: List[List[str]]) -> None:
        with open(file_path, 'w') as csv_file:
            input_csv_writer = csv.writer(csv_file)
            input_csv_writer.writerows(content)
            csv_file.close()

    @staticmethod
    def get_file_paths(input_file_lines: List[str]) -> Tuple[str, str]:
        base_path = tempfile.mkdtemp()

        input_text_path = "%s%sinput.txt" % (base_path, os.path.sep)
        with open(input_text_path, 'w') as input_file:
            input_file.writelines(input_file_lines)

        output_path = "%s%soutput" % (base_path, os.path.sep)
        return input_text_path, output_path

    @staticmethod
    def write_as_parquet_file(spark_session, data, columns, target_folder) -> None:
        ingest_dataframe = spark_session.createDataFrame(data, columns)
        ingest_dataframe.write.parquet(target_folder, mode='overwrite')


@pytest.fixture
def spark_session() -> SparkSession:
    spark_session = SparkSession.builder.appName("IntegrationTests").getOrCreate()
    return spark_session


@pytest.fixture(autouse=True)
def helpers():
    return Helpers
