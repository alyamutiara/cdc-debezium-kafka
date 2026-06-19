"""CDC transform service.

Consumes Debezium CDC events from Kafka, applies column mapping and filtering
per YAML configuration, handles schema-evolution DDL events, and produces
transformed events to output topics for the JDBC sink connector.
"""
__version__ = "0.1.0"
