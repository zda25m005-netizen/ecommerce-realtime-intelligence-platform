"""
GBT Dynamic Pricing Model - Layer 5
Model B: Gradient Boosted Trees for optimal price prediction.
Reads from Gold feature store + Olist pricing data.
Metrics: RMSE, R-squared, Revenue Impact
"""

import os
import sys
sys.path.insert(0, '/opt/spark-apps')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'spark'))

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
from config.spark_config import get_spark_session, GOLD_PATH

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = "gbt_dynamic_pricing"
OLIST_DATA_PATH = os.getenv('OLIST_DATA_PATH', '/opt/data/raw/olist')


def load_olist_data(spark):
    """
    Load and prepare Olist pricing data for training.
    Target: optimal price based on demand, competition, reviews.
    """
    orders_path = f"{OLIST_DATA_PATH}/olist_order_items_dataset.csv"
    products_path = f"{OLIST_DATA_PATH}/olist_products_dataset.csv"
    reviews_path = f"{OLIST_DATA_PATH}/olist_order_reviews_dataset.csv"

    if os.path.exists(orders_path):
        print("Loading Olist dataset from CSV...")
        order_items = spark.read.csv(orders_path, header=True, inferSchema=True)
        products = spark.read.csv(products_path, header=True, inferSchema=True)
        reviews = spark.read.csv(reviews_path, header=True, inferSchema=True)

        # Aggregate review scores per product
        review_agg = (
            reviews.join(order_items, "order_id")
            .groupBy("product_id")
            .agg(
                F.avg("review_score").alias("avg_review_score"),
                F.count("*").alias("num_reviews"),
            )
        )

        # Build pricing dataset
        pricing_data = (
            order_items
            .join(products, "product_id", "left")
            .join(review_agg, "product_id", "left")
            .select(
                "product_id",
                F.col("price").alias("target_price"),
                F.col("freight_value"),
                F.col("product_name_lenght").alias("name_length"),
                F.col("product_description_lenght").alias("desc_length"),
                F.col("product_photos_qty").alias("photo_count"),
                F.col("product_weight_g").alias("weight_g"),
                F.col("product_length_cm").alias("length_cm"),
                F.col("product_height_cm").alias("height_cm"),
                F.col("product_width_cm").alias("width_cm"),
                "avg_review_score",
                "num_reviews",
            )
            .na.fill(0)
            .filter(F.col("target_price") > 0)
        )

        return pricing_data

    else:
        print("Olist data not found, using Gold feature store...")
        return load_gold_pricing_data(spark)


def load_gold_pricing_data(spark):
    """
    Build pricing training data from Gold feature store.
    Used when Olist CSV is not available.
    """
    product_features = spark.read.format("delta").load(f"{GOLD_PATH}/product_features")

    pricing_data = (
        product_features
        .filter(F.col("current_price").isNotNull())
        .select(
            "item_id",
            F.col("current_price").alias("target_price"),
            "view_count",
            "cart_count",
            "purchase_count",
            "unique_users",
            "view_to_cart_rate",
            "cart_to_purchase_rate",
            F.coalesce("current_stock", F.lit(100)).alias("current_stock"),
            "demand_score",
            "stock_velocity",
            F.coalesce("avg_price", F.col("current_price")).alias("avg_price"),
            F.coalesce("price_stddev", F.lit(0.0)).alias("price_stddev"),
            F.coalesce("num_price_changes", F.lit(0)).alias("num_price_changes"),
        )
        .na.fill(0)
    )

    return pricing_data


def train_gbt(spark, pricing_data, params):
    """Train GBT model with given hyperparameters."""

    # Determine feature columns (exclude target and IDs)
    id_cols = {"product_id", "item_id", "target_price"}
    string_cols = [f.name for f in pricing_data.schema.fields
                   if f.dataType.typeName() == "string" and f.name not in id_cols]
    numeric_cols = [f.name for f in pricing_data.schema.fields
                    if f.dataType.typeName() in ("integer", "long", "double", "float")
                    and f.name not in id_cols]

    # Build pipeline stages
    stages = []

    # Index string columns
    indexed_cols = []
    for col in string_cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        stages.append(indexer)
        indexed_cols.append(f"{col}_idx")

    # Assemble features
    feature_cols = numeric_cols + indexed_cols
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    stages.append(assembler)

    # Scale features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
    stages.append(scaler)

    # GBT Regressor
    gbt = GBTRegressor(
        labelCol="target_price",
        featuresCol="features",
        maxIter=params['maxIter'],
        maxDepth=params['maxDepth'],
        stepSize=params['stepSize'],
        subsamplingRate=params.get('subsamplingRate', 0.8),
        seed=42,
    )
    stages.append(gbt)

    pipeline = Pipeline(stages=stages)

    # Train/test split
    train, test = pricing_data.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train)

    # Predictions
    predictions = model.transform(test)

    # Metrics
    rmse_eval = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="rmse")
    r2_eval = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="r2")
    mae_eval = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="mae")

    rmse = rmse_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)

    # Revenue impact estimation
    revenue_impact = (
        predictions
        .withColumn("price_diff", F.col("prediction") - F.col("target_price"))
        .withColumn("revenue_change_pct", F.col("price_diff") / F.col("target_price") * 100)
        .agg(
            F.avg("revenue_change_pct").alias("avg_revenue_change_pct"),
            F.avg("price_diff").alias("avg_price_diff"),
        )
        .collect()[0]
    )

    return model, {
        'rmse': rmse,
        'r2': r2,
        'mae': mae,
        'avg_revenue_change_pct': float(revenue_impact['avg_revenue_change_pct'] or 0),
        'avg_price_diff': float(revenue_impact['avg_price_diff'] or 0),
        'train_count': train.count(),
        'test_count': test.count(),
        'num_features': len(feature_cols),
    }


def run_experiment(spark, num_runs=5):
    """
    Run multiple GBT experiments with different hyperparameters.
    Track all runs in MLflow.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    pricing_data = load_olist_data(spark)
    print(f"Pricing dataset: {pricing_data.count()} records")

    # Hyperparameter grid
    param_grid = [
        {'maxIter': 50, 'maxDepth': 5, 'stepSize': 0.1},
        {'maxIter': 100, 'maxDepth': 5, 'stepSize': 0.1},
        {'maxIter': 100, 'maxDepth': 8, 'stepSize': 0.05},
        {'maxIter': 150, 'maxDepth': 5, 'stepSize': 0.05, 'subsamplingRate': 0.7},
        {'maxIter': 100, 'maxDepth': 10, 'stepSize': 0.1, 'subsamplingRate': 0.9},
    ]

    best_model = None
    best_r2 = -float('inf')
    best_run_id = None

    for i, params in enumerate(param_grid):
        print(f"\n--- Run {i+1}/{len(param_grid)}: {params} ---")

        with mlflow.start_run(run_name=f"gbt_run_{i+1}") as run:
            mlflow.log_params(params)
            mlflow.set_tag("model_type", "GBT")
            mlflow.set_tag("experiment_variant", f"v{i+1}")

            model, metrics = train_gbt(spark, pricing_data, params)

            mlflow.log_metrics(metrics)
            print(f"  RMSE: {metrics['rmse']:.4f}")
            print(f"  R2: {metrics['r2']:.4f}")
            print(f"  MAE: {metrics['mae']:.4f}")

            # Save model directly to MinIO via s3a:// (avoids JVM s3:// Py4J crash)
            model_save_path = f"s3a://mlflow/gbt-models/run_{i+1}"
            model.write().overwrite().save(model_save_path)
            mlflow.log_param("model_path", model_save_path)
            print(f"  Model saved to {model_save_path}")

            if metrics['r2'] > best_r2:
                best_r2 = metrics['r2']
                best_model = model
                best_run_id = run.info.run_id

    # Save best model to fixed production path on MinIO
    print(f"\nBest model: run_id={best_run_id}, R2={best_r2:.4f}")
    best_model.write().overwrite().save("s3a://mlflow/gbt-models/best")
    print("Best model saved to s3a://mlflow/gbt-models/best")

    # Tag best run in MLflow
    client = mlflow.tracking.MlflowClient()
    client.set_tag(best_run_id, "production", "true")
    client.set_tag(best_run_id, "model_path", "s3a://mlflow/gbt-models/best")
    print("Best run tagged as production in MLflow")

    return best_model, best_run_id


def predict_price(model, features_df):
    """Predict optimal price for given product features."""
    predictions = model.transform(features_df)
    return predictions.select("prediction").collect()


def main():
    spark = get_spark_session("GBTPricing")
    spark.sparkContext.setLogLevel("WARN")

    model, run_id = run_experiment(spark, num_runs=5)
    print(f"\nTraining complete. Best run: {run_id}")

    spark.stop()


if __name__ == '__main__':
    main()
