✈️ Flight Streaming Pipeline


🚧 **Status: IN PROGRESS**

This project is a hands-on data engineering lab focused on building an
end-to-end streaming pipeline using real-world flight data.

The goal is to understand and demonstrate how streaming data flows from
an external API through Kafka and Spark into an analytical storage layer
based on Apache Iceberg.

## Architecture

```text
OpenSky Network
       │
       ▼
  Python Producer
       │
       ▼
     Kafka
       │
       ▼
Spark Structured Streaming
       │
       ▼
   Apache Iceberg
       │
       ▼
      SQL
