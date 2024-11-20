# Kafka Beam Application

## Description
This is a sample application that demonstrates Apache Beam with Kafka for message processing.

- Consumes messages from an input Kafka topic.
- Parses payloads containing Name, Address, and Date of Birth.
- Calculates age and classifies messages into `EVEN_TOPIC` and `ODD_TOPIC`.
- Writes processed messages back to Kafka.
