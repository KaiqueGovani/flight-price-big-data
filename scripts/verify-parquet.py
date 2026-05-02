#!/usr/bin/env python
"""Read the Parquet output and print a sanity check: row count + schema + sample.

Run via:  make verify
or:       spark-submit --master spark://spark-master:7077 \
              --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
              /scripts/verify-parquet.py
"""
import os

from pyspark.sql import SparkSession

PARQUET = os.environ.get("PARQUET_DIR", "hdfs://namenode:9000/data/itineraries.parquet")

spark = SparkSession.builder.appName("verify-parquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(PARQUET)

print(f"\n=== {PARQUET} ===")
print(f"ROW COUNT: {df.count():,}")
print()
df.printSchema()

print("\n=== Sample rows ===")
df.select("legId", "startingAirport", "destinationAirport", "totalFare", "flightDate") \
  .show(5, truncate=False)

print("\n=== Rows per partition ===")
df.groupBy("startingAirport").count().orderBy("startingAirport").show(20, truncate=False)

spark.stop()
