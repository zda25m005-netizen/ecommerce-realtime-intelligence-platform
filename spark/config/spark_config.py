"""
Spark Configuration - Shared across all Spark jobs.
Configures Delta Lake, MinIO (S3), and Kafka connectivity.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name="EcommercePlatform", master=None):
    """
    Create a SparkSession with Delta Lake + MinIO + Kafka configured.
    """
    master_url = master or os.getenv('SPARK_MASTER_URL', 'local[*]')

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)

        # Delta Lake
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.kafka:kafka-clients:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO / S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minio_admin'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minio_password'))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Performance tuning
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")

        # Delta Lake auto-compact
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )

    return builder.getOrCreate()


# S3A paths for Delta tables (Medallion architecture)
BRONZE_PATH = "s3a://bronze"
SILVER_PATH = "s3a://silver"
GOLD_PATH = "s3a://gold"

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'topics': {
        'clickstream': 'clickstream',
        'inventory': 'inventory',
        'user_events': 'user_events',
    }
}

# Checkpoint paths (for Spark Structured Streaming)
CHECKPOINT_BASE = "s3a://checkpoints"
