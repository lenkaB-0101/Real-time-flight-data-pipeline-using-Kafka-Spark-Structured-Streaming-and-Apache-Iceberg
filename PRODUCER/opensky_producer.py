"""
OpenSky Network Telemetry Producer.

Polls live ADS-B aircraft state vectors from the OpenSky Network REST API
and publishes structured JSON events to an Apache Kafka topic partitioned by aircraft ICAO24 ID.
"""

import json
import logging
import os
import time
import requests
from kafka import KafkaProducer

# Configure production-grade logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration (defaults to local self-hosted AlmaLinux setup)
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")
OPENSKY_URL = OPENSKY_URL = "https://opensky-network.org/api/states/all"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "10"))


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Initialize Kafka producer with JSON value serialization and UTF-8 key encoding."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3
    )


def fetch_and_publish(producer: KafkaProducer) -> None:
    """Fetch current flight states from OpenSky API and stream them to Kafka."""
    response = requests.get(OPENSKY_URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    event_time = data.get("time")
    states = data.get("states") or []

    sent_count = 0
    for state in states:
        icao24 = state[0]
        flight = {
            "event_time": event_time,
            "icao24": icao24,
            "callsign": state[1].strip() if state[1] else None,
            "origin_country": state[2],
            "longitude": state[5],
            "latitude": state[6],
            "baro_altitude": state[7],
            "on_ground": state[8],
            "velocity": state[9],
            "true_track": state[10],
            "vertical_rate": state[11]
        }

        # Partition by icao24 to guarantee event ordering per aircraft
        producer.send(KAFKA_TOPIC, key=icao24, value=flight)
        sent_count += 1

    producer.flush()
    logger.info("Published %d flight state vectors to Kafka topic '%s'", sent_count, KAFKA_TOPIC)


def main() -> None:
    logger.info("Starting OpenSky Telemetry Producer targeting %s...", KAFKA_SERVER)
    producer = create_producer(KAFKA_SERVER)

    try:
        while True:
            try:
                fetch_and_publish(producer)
            except requests.RequestException as req_err:
                logger.error("OpenSky API request failed: %s", req_err)
            except Exception as err:
                logger.exception("Unexpected error during ingestion cycle: %s", err)

            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Shutting down producer gracefully...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
