import logging
from source.utils.sanitization.sanitization import sanitize_columns

from pyspark.sql import SparkSession


def csv_to_parquet(spark: SparkSession,
                   ingest_path: str,
                   transformation_path: str,
                   header: bool) -> str:
    logging.info("Reading text file from: %s", ingest_path)
    input_df = spark.read.format("org.apache.spark.csv").option("header", header).csv(ingest_path)
    renamed_columns = sanitize_columns(input_df.columns)
    ref_df = input_df.toDF(*renamed_columns)
    ref_df.printSchema()
    ref_df.show()
    ref_df.write.parquet(transformation_path)
    return transformation_path


def txt_to_dataframe(spark: SparkSession, input_path: str):
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)
    return input_df
