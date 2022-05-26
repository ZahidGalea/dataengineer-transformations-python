import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

APP_NAME = "Citibike Pipeline: Distance Calculation"


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    return dataframe


def run(ss: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = ss.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(ss, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')


if __name__ == '__main__':
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d%H:%M:%S")
    # TODO: Logs must be on a external storage or folder
    logging.basicConfig(filename=f'job_{APP_NAME}_{current_time}.log', level=logging.INFO)
    logging.info(sys.argv)
    arguments = sys.argv

    if len(arguments) != 3:
        logging.warning("Dataset file path and output path not specified!")
        sys.exit(1)

    dataset_path = arguments[2]
    output_path = arguments[3]

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    run(spark, dataset_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)

    spark.stop()
