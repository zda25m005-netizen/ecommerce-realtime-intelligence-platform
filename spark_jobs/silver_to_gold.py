"""
Silver to Gold - Layer 2/3
Builds feature store tables for ML models:
  - user_item_interactions: interaction matrix for ALS recommender
  - product_features: features for GBT dynamic pricing model
  - user_profiles: aggregated user behavior profiles
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql import Window
from config.spark_config import get_spark_session, SILVER_PATH, GOLD_PATH


def build_user_item_interactions(spark):
    """
    Build interaction matrix for ALS collaborative filtering.
    Gold table: user_item_interactions
    Columns: user_id, item_id, rating (implicit), interaction_count, last_interaction
    """
    print("Building Gold: user_item_interactions")

    clickstream = spark.read.format("delta").load(f"{SILVER_PATH}/clickstream")

    # Assign implicit ratings based on event type
    # view=1, addtocart=3, transaction=5
    rated = clickstream.withColumn(
        "implicit_rating",
        F.when(F.col("event_type") == "view", 1.0)
        .when(F.col("event_type") == "addtocart", 3.0)
        .when(F.col("event_type") == "transaction", 5.0)
        .otherwise(0.0)
    )

    interactions = (
        rated
        .groupBy("user_id", "item_id")
        .agg(
            F.max("implicit_rating").alias("rating"),
            F.count("*").alias("interaction_count"),
            F.max("timestamp").alias("last_interaction"),
            F.collect_set("event_type").alias("event_types"),
            F.countDistinct("session_id").alias("num_sessions"),
        )
        .withColumn("has_purchased", F.array_contains(F.col("event_types"), "transaction"))
        .withColumn("has_carted", F.array_contains(F.col("event_types"), "addtocart"))
        .drop("event_types")
        .withColumn("processed_at", F.current_timestamp())
    )

    gold_path = f"{GOLD_PATH}/user_item_interactions"
    interactions.write.format("delta").mode("overwrite").save(gold_path)

    count = interactions.count()
    print(f"  user_item_interactions: {count} records")
    return count


def build_product_features(spark):
    """
    Build product feature table for GBT dynamic pricing.
    Gold table: product_features
    Combines clickstream demand signals with inventory/price data.
    """
    print("Building Gold: product_features")

    clickstream = spark.read.format("delta").load(f"{SILVER_PATH}/clickstream")
    inventory = spark.read.format("delta").load(f"{SILVER_PATH}/inventory")

    # Demand features from clickstream
    demand = (
        clickstream
        .groupBy("item_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("purchase_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
        )
        .withColumn("view_to_cart_rate",
                     F.when(F.col("view_count") > 0,
                            F.col("cart_count") / F.col("view_count"))
                     .otherwise(0.0))
        .withColumn("cart_to_purchase_rate",
                     F.when(F.col("cart_count") > 0,
                            F.col("purchase_count") / F.col("cart_count"))
                     .otherwise(0.0))
    )

    # Latest inventory state per item
    window_latest = Window.partitionBy("item_id").orderBy(F.col("timestamp").desc())

    latest_stock = (
        inventory
        .filter(F.col("event_type") == "stock_level")
        .withColumn("rn", F.row_number().over(window_latest))
        .filter(F.col("rn") == 1)
        .select("item_id",
                F.col("stock_level").alias("current_stock"),
                F.col("category"))
    )

    latest_price = (
        inventory
        .filter(F.col("event_type") == "price_change")
        .withColumn("rn", F.row_number().over(window_latest))
        .filter(F.col("rn") == 1)
        .select("item_id",
                F.col("new_price").alias("current_price"),
                F.col("old_price").alias("previous_price"),
                F.col("change_pct").alias("last_price_change_pct"))
    )

    # Price history stats
    price_stats = (
        inventory
        .filter(F.col("event_type") == "price_change")
        .groupBy("item_id")
        .agg(
            F.avg("new_price").alias("avg_price"),
            F.min("new_price").alias("min_price"),
            F.max("new_price").alias("max_price"),
            F.stddev("new_price").alias("price_stddev"),
            F.count("*").alias("num_price_changes"),
        )
    )

    # Join everything
    product_features = (
        demand
        .join(latest_stock, "item_id", "left")
        .join(latest_price, "item_id", "left")
        .join(price_stats, "item_id", "left")
        .withColumn("demand_score",
                     F.col("view_count") * 0.1 + F.col("cart_count") * 0.3 + F.col("purchase_count") * 0.6)
        .withColumn("stock_velocity",
                     F.when(F.col("current_stock") > 0,
                            F.col("purchase_count") / F.col("current_stock"))
                     .otherwise(0.0))
        .withColumn("processed_at", F.current_timestamp())
    )

    gold_path = f"{GOLD_PATH}/product_features"
    product_features.write.format("delta").mode("overwrite").save(gold_path)

    count = product_features.count()
    print(f"  product_features: {count} records")
    return count


def build_user_profiles(spark):
    """
    Build aggregated user behavior profiles.
    Gold table: user_profiles
    """
    print("Building Gold: user_profiles")

    clickstream = spark.read.format("delta").load(f"{SILVER_PATH}/clickstream")

    try:
        user_events = spark.read.format("delta").load(f"{SILVER_PATH}/user_events")
        has_user_events = True
    except Exception:
        has_user_events = False

    # Clickstream-based profile
    user_activity = (
        clickstream
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_actions"),
            F.countDistinct("item_id").alias("unique_items_interacted"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("total_carts"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("total_purchases"),
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen"),
        )
        .withColumn("engagement_score",
                     F.col("total_views") * 0.1 + F.col("total_carts") * 0.3 + F.col("total_purchases") * 0.6)
        .withColumn("conversion_rate",
                     F.when(F.col("total_views") > 0,
                            F.col("total_purchases") / F.col("total_views"))
                     .otherwise(0.0))
    )

    # Add device/region info if available
    if has_user_events:
        user_meta = (
            user_events
            .filter(F.col("device_type").isNotNull())
            .groupBy("user_id")
            .agg(
                F.first("device_type").alias("primary_device"),
                F.first("region").alias("primary_region"),
                F.avg("duration_seconds").alias("avg_session_duration"),
                F.avg("pages_viewed").alias("avg_pages_per_session"),
            )
        )
        user_profiles = user_activity.join(user_meta, "user_id", "left")
    else:
        user_profiles = user_activity

    user_profiles = user_profiles.withColumn("processed_at", F.current_timestamp())

    gold_path = f"{GOLD_PATH}/user_profiles"
    user_profiles.write.format("delta").mode("overwrite").save(gold_path)

    count = user_profiles.count()
    print(f"  user_profiles: {count} records")
    return count


def main():
    spark = get_spark_session("SilverToGold")
    spark.sparkContext.setLogLevel("WARN")

    build_user_item_interactions(spark)
    build_product_features(spark)
    build_user_profiles(spark)

    print("Silver -> Gold processing complete! Feature store is ready.")
    spark.stop()


if __name__ == '__main__':
    main()
