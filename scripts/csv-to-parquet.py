#!/usr/bin/env python
"""Convert itineraries.csv (~31 GB) to Snappy-compressed Parquet.

Why: Parquet is columnar + compressed → ~5-10x smaller, ~10-100x faster
for the column-selective scans we'll do in EDA and ML feature engineering.

Run inside the jupyter (or spark-master) container:
    docker compose exec jupyter spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.sql.shuffle.partitions=200 \
        /home/jovyan/work/../scripts/csv-to-parquet.py

Or simpler from host (uses the cluster):
    docker compose exec jupyter python /scripts/csv-to-parquet.py

Inputs/outputs are configurable via env vars:
    INPUT_CSV   default /data/itineraries.csv
    OUTPUT_DIR  default /data/itineraries.parquet
    PARTITION_BY default startingAirport (set to '' to disable)
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

INPUT_CSV = os.environ.get("INPUT_CSV", "/data/itineraries.csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/itineraries.parquet")
PARTITION_BY = os.environ.get("PARTITION_BY", "startingAirport")

# Explicit schema: at 31 GB, schema inference would do a full extra pass.
# Pipe-delimited "segments*" columns stay as strings here — parsing happens
# downstream in the EDA/feature-engineering notebooks.
SCHEMA = StructType([
    StructField("legId", StringType(), False),
    StructField("searchDate", StringType(), True),           # parse to DateType later
    StructField("flightDate", StringType(), True),           # parse to DateType later
    StructField("startingAirport", StringType(), True),
    StructField("destinationAirport", StringType(), True),
    StructField("fareBasisCode", StringType(), True),
    StructField("travelDuration", StringType(), True),       # ISO-8601 duration text
    StructField("elapsedDays", IntegerType(), True),
    StructField("isBasicEconomy", BooleanType(), True),
    StructField("isRefundable", BooleanType(), True),
    StructField("isNonStop", BooleanType(), True),
    StructField("baseFare", DoubleType(), True),
    StructField("totalFare", DoubleType(), True),            # target
    StructField("seatsRemaining", IntegerType(), True),
    StructField("totalTravelDistance", DoubleType(), True),  # has NaNs
    StructField("segmentsDepartureTimeEpochSeconds", StringType(), True),
    StructField("segmentsDepartureTimeRaw", StringType(), True),
    StructField("segmentsArrivalTimeEpochSeconds", StringType(), True),
    StructField("segmentsArrivalTimeRaw", StringType(), True),
    StructField("segmentsArrivalAirportCode", StringType(), True),
    StructField("segmentsDepartureAirportCode", StringType(), True),
    StructField("segmentsAirlineName", StringType(), True),
    StructField("segmentsAirlineCode", StringType(), True),
    StructField("segmentsEquipmentDescription", StringType(), True),
    StructField("segmentsDurationInSeconds", StringType(), True),
    StructField("segmentsDistance", StringType(), True),     # contains 'None'
    StructField("segmentsCabinCode", StringType(), True),
])


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("itineraries-csv-to-parquet")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Avoid tiny files: target ~128 MB blocks
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading CSV : {INPUT_CSV}")
    print(f"Writing Parquet: {OUTPUT_DIR}")
    print(f"Partition by   : {PARTITION_BY or '(none)'}")

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", False)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA)
        .csv(INPUT_CSV)
    )

    writer = df.write.mode("overwrite")
    if PARTITION_BY:
        writer = writer.partitionBy(*[c.strip() for c in PARTITION_BY.split(",") if c.strip()])
    writer.parquet(OUTPUT_DIR)

    print("Done. Row count summary:")
    spark.read.parquet(OUTPUT_DIR).groupBy().count().show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
