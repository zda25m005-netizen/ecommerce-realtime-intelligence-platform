"""
Kafka to Bronze - Layer 2/3
Structured Streaming job that reads from all 3 Kafka topics
and writes raw events to Bronze Delta Lake tables on MinIO.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from config.spark_config import get_spark_session, BRONZE_PATH, KAFKA_CONFIG, CHECKPOINT_BASE


def stream_topic_to_bronze(spark, topic_name, bronze_table_path, checkpoint_path):
    """
    Read from a Kafka topic and write raw JSON to Bronze Delta table.
    Bronze = raw, unprocessed, exactly as received.
    """
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse Kafka message: keep key, value (as string), topic, partition, offset, timestamp
    bronze_df = (
        kafka_df
        .select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.col("value").cast(StringType()).alias("raw_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.lit(topic_name).alias("source_topic"),
        )
    )

    # Write to Delta Lake Bronze layer
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="10 seconds")
        .start(bronze_table_path)
    )

    return query


def main():
    spark = get_spark_session("KafkaToBronze")
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    # Stream all 3 topics to Bronze
    for topic_key, topic_name in KAFKA_CONFIG['topics'].items():
        bronze_path = f"{BRONZE_PATH}/{topic_key}"
        checkpoint_path = f"{CHECKPOINT_BASE}/bronze/{topic_key}"

        print(f"Starting stream: {topic_name} -> {bronze_path}")
        q = stream_topic_to_bronze(spark, topic_name, bronze_path, checkpoint_path)
        queries.append(q)

    print(f"All {len(queries)} streams started. Waiting for termination...")

    # Wait for any stream to terminate (they run indefinitely)
    for q in queries:
        q.awaitTermination()


if __name__ == '__main__':
    main()
