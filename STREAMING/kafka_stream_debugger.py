"""
Kafka Flight Stream Debugger.

Consumes raw JSON flight telemetry from Apache Kafka using Spark Structured Streaming,
enforces a strict schema, and outputs throttled micro-batches to the console for live inspection.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    BooleanType
)

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")


def get_flight_schema() -> StructType:
    """Define explicit schema for incoming OpenSky ADS-B JSON payloads."""
    return StructType([
        StructField("event_time", LongType(), True),
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True)
    ])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("KafkaFlightStreamDebugger")
        .getOrCreate()
    )

    # Suppress verbose INFO logs from Spark/JVM
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    flights_df = (
        kafka_df
        .select(from_json(col("value").cast("string"), get_flight_schema()).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
    )

    # Throttle micro-batch execution to optimize CPU/memory usage on single-node Linux host
    query = (
        flights_df.writeStream
        .format("console")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
