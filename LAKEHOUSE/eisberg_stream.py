"""
Kafka to Apache Iceberg Streaming Sink.

Consumes live flight telemetry from Apache Kafka, applies an explicit schema,
and persists micro-batches into an ACID-compliant Apache Iceberg table.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "30000")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "./checkpoints/flight_events")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "flight_catalog.flights.flight_events")


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
        .appName("FlightKafkaToIceberg")
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
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    flights_df = (
        kafka_df
        .select(from_json(col("value").cast("string"), get_flight_schema()).alias("data"))
        .select("data.*")
    )

    query = (
        flights_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .toTable(ICEBERG_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
