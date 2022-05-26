import logging

import sys
from pyspark.sql import SparkSession

from source.utils.ingestion.local_file_read import csv_to_parquet
from datetime import datetime

APP_NAME = "Citibike Pipeline: Ingest"


def run(ss: SparkSession, input_file_path, output_file_path, header: bool = True):
    return csv_to_parquet(ss, input_file_path, output_file_path, header)


if __name__ == '__main__':
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d%H:%M:%S")
    # TODO: Logs must be on a external storage or folder
    logging.basicConfig(filename=f'logs/job_{APP_NAME}_{current_time}.log', level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 3:
        logging.warning("Input source and output path are required")
        sys.exit(1)

    # know_args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName

    logging.info("Application Initialized: " + app_name)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run(spark, input_path, output_path, True)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
