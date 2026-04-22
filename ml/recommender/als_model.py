"""
ALS Collaborative Filtering Recommender - Layer 5
Model A: Alternating Least Squares for implicit feedback.
Reads from Gold feature store, trains with MLflow tracking.
Metrics: MAP@10, NDCG@10
"""

import os
import sys
sys.path.insert(0, '/opt/spark-apps')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'spark'))

import mlflow
import mlflow.spark
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
from pyspark.sql import Window
from config.spark_config import get_spark_session, GOLD_PATH

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = "als_recommender"


def load_training_data(spark):
    """Load interaction matrix from Gold feature store."""
    interactions = spark.read.format("delta").load(f"{GOLD_PATH}/user_item_interactions")

    print(f"Loaded {interactions.count()} interactions")
    print(f"Unique users: {interactions.select('user_id').distinct().count()}")
    print(f"Unique items: {interactions.select('item_id').distinct().count()}")

    return interactions


def compute_ndcg_at_k(predictions_df, k=10):
    """
    Compute NDCG@K for recommendation quality.
    Uses actual purchases as ground truth relevance.
    """
    window = Window.partitionBy("user_id").orderBy(F.col("prediction").desc())

    ranked = (
        predictions_df
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= k)
    )

    # DCG: sum(relevance / log2(rank + 1))
    dcg = (
        ranked
        .withColumn("dcg_contrib",
                     F.col("rating") / F.log2(F.col("rank") + 1))
        .groupBy("user_id")
        .agg(F.sum("dcg_contrib").alias("dcg"))
    )

    # Ideal DCG: sort by actual rating
    ideal_window = Window.partitionBy("user_id").orderBy(F.col("rating").desc())
    ideal_ranked = (
        predictions_df
        .withColumn("rank", F.row_number().over(ideal_window))
        .filter(F.col("rank") <= k)
    )

    idcg = (
        ideal_ranked
        .withColumn("idcg_contrib",
                     F.col("rating") / F.log2(F.col("rank") + 1))
        .groupBy("user_id")
        .agg(F.sum("idcg_contrib").alias("idcg"))
    )

    ndcg = (
        dcg.join(idcg, "user_id")
        .withColumn("ndcg",
                     F.when(F.col("idcg") > 0, F.col("dcg") / F.col("idcg"))
                     .otherwise(0.0))
        .agg(F.avg("ndcg").alias("avg_ndcg"))
        .collect()[0]["avg_ndcg"]
    )

    return ndcg or 0.0


def compute_map_at_k(predictions_df, k=10):
    """
    Compute Mean Average Precision @ K.
    Considers items with rating >= 3 as relevant.
    """
    window = Window.partitionBy("user_id").orderBy(F.col("prediction").desc())

    ranked = (
        predictions_df
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= k)
        .withColumn("is_relevant", F.when(F.col("rating") >= 3.0, 1).otherwise(0))
    )

    # Precision at each position
    cum_window = Window.partitionBy("user_id").orderBy("rank").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    ap = (
        ranked
        .withColumn("cum_relevant", F.sum("is_relevant").over(cum_window))
        .withColumn("precision_at_k",
                     F.when(F.col("is_relevant") == 1,
                            F.col("cum_relevant") / F.col("rank"))
                     .otherwise(0.0))
        .groupBy("user_id")
        .agg(F.avg("precision_at_k").alias("avg_precision"))
    )

    map_score = ap.agg(F.avg("avg_precision").alias("map")).collect()[0]["map"]
    return map_score or 0.0


def train_als(spark, interactions, params):
    """Train ALS model with given hyperparameters."""
    als = ALS(
        maxIter=params['maxIter'],
        regParam=params['regParam'],
        rank=params['rank'],
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        implicitPrefs=True,
        coldStartStrategy="drop",
        nonnegative=True,
        seed=42,
    )

    # Train/test split
    train, test = interactions.randomSplit([0.8, 0.2], seed=42)
    model = als.fit(train)

    # Predictions
    predictions = model.transform(test)

    # Metrics
    rmse_eval = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    rmse = rmse_eval.evaluate(predictions)
    map_10 = compute_map_at_k(predictions, k=10)
    ndcg_10 = compute_ndcg_at_k(predictions, k=10)

    return model, {
        'rmse': rmse,
        'map_at_10': map_10,
        'ndcg_at_10': ndcg_10,
        'train_count': train.count(),
        'test_count': test.count(),
    }


def run_experiment(spark, num_runs=5):
    """
    Run multiple ALS experiments with different hyperparameters.
    Track all runs in MLflow. Return best model.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    interactions = load_training_data(spark)

    # Hyperparameter grid
    param_grid = [
        {'rank': 10, 'maxIter': 10, 'regParam': 0.01},
        {'rank': 20, 'maxIter': 10, 'regParam': 0.01},
        {'rank': 20, 'maxIter': 15, 'regParam': 0.05},
        {'rank': 50, 'maxIter': 10, 'regParam': 0.1},
        {'rank': 50, 'maxIter': 15, 'regParam': 0.01},
    ]

    best_model = None
    best_ndcg = -1
    best_run_id = None

    for i, params in enumerate(param_grid):
        print(f"\n--- Run {i+1}/{len(param_grid)}: {params} ---")

        with mlflow.start_run(run_name=f"als_run_{i+1}") as run:
            # Log parameters
            mlflow.log_params(params)
            mlflow.set_tag("model_type", "ALS")
            mlflow.set_tag("experiment_variant", f"v{i+1}")

            # Train
            model, metrics = train_als(spark, interactions, params)

            # Log metrics
            mlflow.log_metrics(metrics)
            print(f"  RMSE: {metrics['rmse']:.4f}")
            print(f"  MAP@10: {metrics['map_at_10']:.4f}")
            print(f"  NDCG@10: {metrics['ndcg_at_10']:.4f}")

            # Save model directly to MinIO via s3a:// (avoids JVM s3:// Py4J crash)
            model_save_path = f"s3a://mlflow/als-models/run_{i+1}"
            model.write().overwrite().save(model_save_path)
            mlflow.log_param("model_path", model_save_path)
            print(f"  Model saved to {model_save_path}")

            # Track best
            if metrics['ndcg_at_10'] > best_ndcg:
                best_ndcg = metrics['ndcg_at_10']
                best_model = model
                best_run_id = run.info.run_id

    # Register best model
    print(f"\nBest model: run_id={best_run_id}, NDCG@10={best_ndcg:.4f}")

    # Save best model to a fixed "production" path on MinIO
    best_model.write().overwrite().save("s3a://mlflow/als-models/best")
    print("Best model saved to s3a://mlflow/als-models/best")

    # Tag the best run in MLflow
    client = mlflow.tracking.MlflowClient()
    client.set_tag(best_run_id, "production", "true")
    client.set_tag(best_run_id, "model_path", "s3a://mlflow/als-models/best")
    print("Best run tagged as production in MLflow")

    return best_model, best_run_id


def generate_recommendations(spark, model, user_id, n=10):
    """Generate top-N recommendations for a given user."""
    user_recs = model.recommendForAllUsers(n)
    user_rec = user_recs.filter(F.col("user_id") == user_id)

    if user_rec.count() == 0:
        return []

    recs = (
        user_rec
        .select(F.explode("recommendations").alias("rec"))
        .select(
            F.col("rec.item_id").alias("item_id"),
            F.col("rec.rating").alias("score"),
        )
        .collect()
    )

    return [{"item_id": r["item_id"], "score": float(r["score"])} for r in recs]


def main():
    spark = get_spark_session("ALSRecommender")
    spark.sparkContext.setLogLevel("WARN")

    model, run_id = run_experiment(spark, num_runs=5)
    print(f"\nTraining complete. Best run: {run_id}")

    # Test recommendation
    sample_recs = generate_recommendations(spark, model, user_id=1, n=10)
    print(f"\nSample recommendations for user 1: {sample_recs}")

    spark.stop()


if __name__ == '__main__':
    main()
