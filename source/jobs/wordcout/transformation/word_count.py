import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from source.utils.ingestion.local_file_read import txt_to_dataframe

APP_NAME = "WordCount"


def run(ss: SparkSession, input_file_path: str, output_file_path: str) -> None:
    logging.info("Reading text file from: %s", output_file_path)
    input_df = txt_to_dataframe(ss, input_file_path)
    logging.info("Writing csv to directory: %s", output_file_path)
    input_df.coalesce(1).write.csv(output_file_path, header=True)


if __name__ == '__main__':
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d%H:%M:%S")
    # TODO: Logs must be on a external storage or folder
    logging.basicConfig(filename=f'logs/job_{APP_NAME}_{current_time}.log', level=logging.INFO)
    logging.info(sys.argv)
    logging.info(sys.argv)

    if len(sys.argv) != 3:
        logging.warning("Input .txt file and output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run(spark, input_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
