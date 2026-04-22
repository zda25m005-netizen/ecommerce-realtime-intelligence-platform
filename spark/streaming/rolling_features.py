"""
Rolling Features - Layer 3 (Streaming)
Spark Structured Streaming job that computes rolling 1-hour CTR features
and writes them to the Gold feature store via Delta MERGE.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable
from config.spark_config import get_spark_session, KAFKA_CONFIG, GOLD_PATH, CHECKPOINT_BASE


CLICKSTREAM_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
])


def compute_rolling_ctr(spark):
    """
    Compute rolling 1-hour CTR per item from the clickstream Kafka topic.
    Writes to Gold: rolling_item_ctr
    Updates every 10 minutes via trigger.
    """
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
        .option("subscribe", "clickstream")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON from Kafka
    parsed = (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                CLICKSTREAM_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_ts")
        )
        .select("data.*", "kafka_ts")
        .withWatermark("kafka_ts", "10 minutes")
    )

    # Rolling 1-hour window aggregation per item
    rolling_ctr = (
        parsed
        .groupBy(
            F.window("kafka_ts", "1 hour", "10 minutes"),
            "item_id"
        )
        .agg(
            F.count("*").alias("total_events_1h"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views_1h"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("carts_1h"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("purchases_1h"),
            F.countDistinct("user_id").alias("unique_users_1h"),
        )
        .withColumn("ctr_1h",
                     F.when(F.col("views_1h") > 0,
                            F.col("carts_1h") / F.col("views_1h"))
                     .otherwise(0.0))
        .withColumn("conversion_1h",
                     F.when(F.col("views_1h") > 0,
                            F.col("purchases_1h") / F.col("views_1h"))
                     .otherwise(0.0))
        .select(
            "item_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_events_1h", "views_1h", "carts_1h", "purchases_1h",
            "unique_users_1h", "ctr_1h", "conversion_1h",
        )
    )

    # Write to Gold using foreachBatch for Delta MERGE
    def merge_to_gold(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        gold_path = f"{GOLD_PATH}/rolling_item_ctr"
        batch_with_ts = batch_df.withColumn("updated_at", F.current_timestamp())

        try:
            delta_table = DeltaTable.forPath(spark, gold_path)
            (
                delta_table.alias("target")
                .merge(
                    batch_with_ts.alias("source"),
                    "target.item_id = source.item_id AND target.window_start = source.window_start"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            # First write - table doesn't exist yet
            batch_with_ts.write.format("delta").mode("overwrite").save(gold_path)

        print(f"Batch {batch_id}: merged {batch_df.count()} rolling CTR records")

    checkpoint_path = f"{CHECKPOINT_BASE}/gold/rolling_ctr"

    query = (
        rolling_ctr.writeStream
        .foreachBatch(merge_to_gold)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 minutes")
        .start()
    )

    return query


def main():
    spark = get_spark_session("RollingFeatures")
    spark.sparkContext.setLogLevel("WARN")

    print("Starting rolling CTR feature computation...")
    query = compute_rolling_ctr(spark)
    query.awaitTermination()


if __name__ == '__main__':
    main()
