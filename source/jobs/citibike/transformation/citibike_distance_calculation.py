import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
import numpy as np
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

APP_NAME = "Citibike Pipeline: Distance Calculation"


# TODO: Must use the formula of Thoughtworks
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)
    r = 6371
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)

    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    return str(np.round(res, 2))


# TODO: The data should be preprocessed with type of data correctly
def compute_distance(_spark: SparkSession, df: DataFrame) -> DataFrame:
    harvesine_distance_udf = udf(haversine_distance)

    df = df.withColumn("distance",
                       harvesine_distance_udf(col('start_station_latitude'),
                                              col('start_station_longitude'),
                                              col('end_station_latitude'),
                                              col('end_station_longitude')))
    return df.withColumn("distance", col("distance").cast(DoubleType()))


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
    logging.basicConfig(filename=f'logs/job_{APP_NAME}_{current_time}.log', level=logging.INFO)
    logging.info(sys.argv)
    arguments = sys.argv

    if len(arguments) != 3:
        logging.warning("Dataset file path and output path not specified!")
        sys.exit(1)

    dataset_path = arguments[1]
    output_path = arguments[2]

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    run(spark, dataset_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)

    spark.stop()
