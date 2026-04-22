"""
Bronze to Silver - Layer 2/3
Batch job that reads raw Bronze data, parses JSON,
cleans/validates, and writes structured Silver Delta tables.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)
from delta.tables import DeltaTable
from config.spark_config import get_spark_session, BRONZE_PATH, SILVER_PATH


# ── Schema Definitions ──────────────────────────────────────────────────────

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("timestamp", StringType(), True),
    StructField("original_timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
])

INVENTORY_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("stock_level", IntegerType(), True),
    StructField("units_sold", IntegerType(), True),
    StructField("old_price", DoubleType(), True),
    StructField("new_price", DoubleType(), True),
    StructField("change_pct", DoubleType(), True),
    StructField("restock_quantity", IntegerType(), True),
    StructField("new_stock_level", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("supplier", StringType(), True),
    StructField("estimated_arrival", StringType(), True),
    StructField("timestamp", StringType(), True),
])

USER_EVENTS_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("session_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("landing_page", StringType(), True),
    StructField("page", StringType(), True),
    StructField("pages_viewed", IntegerType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("items_viewed", IntegerType(), True),
    StructField("items_carted", IntegerType(), True),
    StructField("purchased", BooleanType(), True),
    StructField("is_returning", BooleanType(), True),
    StructField("ip_hash", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("time_on_page_seconds", IntegerType(), True),
    StructField("scroll_depth_pct", IntegerType(), True),
    StructField("timestamp", StringType(), True),
])


def process_clickstream(spark):
    """Parse and clean clickstream Bronze -> Silver."""
    bronze_path = f"{BRONZE_PATH}/clickstream"
    silver_path = f"{SILVER_PATH}/clickstream"

    print("Processing clickstream: Bronze -> Silver")

    bronze_df = spark.read.format("delta").load(bronze_path)

    silver_df = (
        bronze_df
        .select(
            F.from_json(F.col("raw_value"), CLICKSTREAM_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
            F.col("ingestion_timestamp"),
        )
        .select("data.*", "kafka_timestamp", "ingestion_timestamp")
        # Data quality: drop nulls on required fields
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("item_id").isNotNull())
        # Validate event types
        .filter(F.col("event_type").isin("view", "addtocart", "transaction"))
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
    )

    # Write as Delta with MERGE (upsert) for idempotency
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                silver_df.alias("source"),
                """target.user_id = source.user_id
                   AND target.item_id = source.item_id
                   AND target.timestamp = source.timestamp
                   AND target.event_type = source.event_type"""
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        silver_df.write.format("delta").mode("overwrite").save(silver_path)

    count = spark.read.format("delta").load(silver_path).count()
    print(f"Clickstream Silver: {count} records")


def process_inventory(spark):
    """Parse and clean inventory Bronze -> Silver."""
    bronze_path = f"{BRONZE_PATH}/inventory"
    silver_path = f"{SILVER_PATH}/inventory"

    print("Processing inventory: Bronze -> Silver")

    bronze_df = spark.read.format("delta").load(bronze_path)

    silver_df = (
        bronze_df
        .select(
            F.from_json(F.col("raw_value"), INVENTORY_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
            F.col("ingestion_timestamp"),
        )
        .select("data.*", "kafka_timestamp", "ingestion_timestamp")
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("item_id").isNotNull())
        .filter(F.col("event_type").isin("stock_level", "price_change", "restock"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
    )

    silver_df.write.format("delta").mode("overwrite").save(silver_path)

    count = spark.read.format("delta").load(silver_path).count()
    print(f"Inventory Silver: {count} records")


def process_user_events(spark):
    """Parse and clean user_events Bronze -> Silver."""
    bronze_path = f"{BRONZE_PATH}/user_events"
    silver_path = f"{SILVER_PATH}/user_events"

    print("Processing user_events: Bronze -> Silver")

    bronze_df = spark.read.format("delta").load(bronze_path)

    silver_df = (
        bronze_df
        .select(
            F.from_json(F.col("raw_value"), USER_EVENTS_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
            F.col("ingestion_timestamp"),
        )
        .select("data.*", "kafka_timestamp", "ingestion_timestamp")
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
    )

    silver_df.write.format("delta").mode("overwrite").save(silver_path)

    count = spark.read.format("delta").load(silver_path).count()
    print(f"User Events Silver: {count} records")


def main():
    spark = get_spark_session("BronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    process_clickstream(spark)
    process_inventory(spark)
    process_user_events(spark)

    print("Bronze -> Silver processing complete!")
    spark.stop()


if __name__ == '__main__':
    main()
