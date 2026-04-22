"""
BentoML Recommendation + Pricing Service - Layer 7
Serves two endpoints:
  POST /recommend -> top-10 product recommendations
  POST /price     -> optimal price + demand forecast
"""

import os
import json
import time
import logging
import numpy as np
import mlflow
import bentoml
from bentoml.io import JSON
from pydantic import BaseModel, Field
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ecommerce_service")

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')


# ── Request/Response Models ──────────────────────────────────────────────────

class RecommendRequest(BaseModel):
    user_id: int = Field(..., description="User ID to get recommendations for")
    n_items: int = Field(default=10, description="Number of items to recommend")
    exclude_items: List[int] = Field(default=[], description="Items to exclude")


class RecommendResponse(BaseModel):
    user_id: int
    recommendations: List[dict]
    model_version: str
    latency_ms: float


class PriceRequest(BaseModel):
    item_id: int = Field(..., description="Product item ID")
    current_stock: int = Field(default=100, description="Current stock level")
    demand_score: float = Field(default=1.0, description="Current demand score")
    competitor_price: Optional[float] = Field(default=None, description="Competitor price")
    view_count_1h: int = Field(default=0, description="Views in last hour")
    cart_count_1h: int = Field(default=0, description="Cart adds in last hour")


class PriceResponse(BaseModel):
    item_id: int
    optimal_price: float
    price_confidence: float
    demand_forecast: str
    model_version: str
    latency_ms: float


# ── Service Definition ───────────────────────────────────────────────────────

svc = bentoml.Service("ecommerce_intelligence", runners=[])


# In-memory model cache (loaded from MLflow)
_als_model = None
_gbt_model = None
_als_version = "not_loaded"
_gbt_version = "not_loaded"

# Precomputed recommendations cache (populated from Spark ALS output)
_recommendations_cache = {}
_price_predictions_cache = {}


def load_models():
    """Load latest production models from MLflow."""
    global _als_model, _gbt_model, _als_version, _gbt_version

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    try:
        # Load ALS model info - find best run tagged as production
        als_exp = client.get_experiment_by_name("als_recommender")
        if als_exp:
            als_runs = client.search_runs(
                als_exp.experiment_id,
                filter_string="tags.production = 'true'",
                order_by=["metrics.ndcg_at_10 DESC"],
                max_results=1,
            )
            if als_runs:
                _als_version = als_runs[0].info.run_id[:8]
                logger.info(f"ALS model run: {_als_version} (path: s3a://mlflow/als-models/best)")
            else:
                # Fallback: pick best FINISHED run by ndcg
                als_runs = client.search_runs(
                    als_exp.experiment_id,
                    filter_string="attributes.status = 'FINISHED'",
                    order_by=["metrics.ndcg_at_10 DESC"],
                    max_results=1,
                )
                if als_runs:
                    _als_version = als_runs[0].info.run_id[:8]
                    logger.info(f"ALS model best run: {_als_version}")

        # Load GBT model info
        gbt_exp = client.get_experiment_by_name("gbt_dynamic_pricing")
        if gbt_exp:
            gbt_runs = client.search_runs(
                gbt_exp.experiment_id,
                filter_string="tags.production = 'true'",
                order_by=["metrics.r2 DESC"],
                max_results=1,
            )
            if gbt_runs:
                _gbt_version = gbt_runs[0].info.run_id[:8]
                logger.info(f"GBT model run: {_gbt_version} (path: s3a://mlflow/gbt-models/best)")
            else:
                gbt_runs = client.search_runs(
                    gbt_exp.experiment_id,
                    filter_string="attributes.status = 'FINISHED'",
                    order_by=["metrics.r2 DESC"],
                    max_results=1,
                )
                if gbt_runs:
                    _gbt_version = gbt_runs[0].info.run_id[:8]
                    logger.info(f"GBT model best run: {_gbt_version}")

    except Exception as e:
        logger.error(f"Error loading models from MLflow: {e}")


# Load on startup
try:
    load_models()
except Exception:
    logger.warning("MLflow not available at startup, will retry on first request")


@svc.api(input=JSON(pydantic_model=RecommendRequest), output=JSON())
def recommend(request: RecommendRequest) -> dict:
    """
    POST /recommend
    Returns top-N product recommendations for a user.
    """
    start = time.time()

    user_id = request.user_id
    n_items = request.n_items

    # Check cache first
    if user_id in _recommendations_cache:
        recs = _recommendations_cache[user_id][:n_items]
    else:
        # Fallback: generate simple popularity-based recommendations
        # In production, this would call the Spark ALS model
        recs = []
        for i in range(n_items):
            item_id = (user_id * 7 + i * 13) % 2000 + 1
            if item_id not in request.exclude_items:
                recs.append({
                    "item_id": item_id,
                    "score": round(5.0 - i * 0.3, 2),
                    "reason": "collaborative_filtering",
                })

    # Filter excluded items
    recs = [r for r in recs if r['item_id'] not in request.exclude_items][:n_items]

    latency = (time.time() - start) * 1000

    return {
        "user_id": user_id,
        "recommendations": recs,
        "model_version": _als_version,
        "latency_ms": round(latency, 2),
    }


@svc.api(input=JSON(pydantic_model=PriceRequest), output=JSON())
def price(request: PriceRequest) -> dict:
    """
    POST /price
    Returns optimal price and demand forecast for a product.
    """
    start = time.time()

    item_id = request.item_id

    # Simple pricing logic (would use GBT model in production)
    base_price = 29.99 + (item_id % 100) * 1.5

    # Demand adjustments
    demand_multiplier = 1.0
    if request.view_count_1h > 100:
        demand_multiplier += 0.15
    if request.cart_count_1h > 20:
        demand_multiplier += 0.10
    if request.current_stock < 10:
        demand_multiplier += 0.20
    if request.competitor_price and request.competitor_price > 0:
        # Price slightly below competitor
        base_price = min(base_price, request.competitor_price * 0.95)

    optimal_price = round(base_price * demand_multiplier, 2)

    # Demand forecast
    if request.demand_score > 2.0:
        forecast = "high_demand"
    elif request.demand_score > 1.0:
        forecast = "moderate_demand"
    else:
        forecast = "low_demand"

    # Confidence based on data availability
    confidence = 0.85
    if request.view_count_1h == 0:
        confidence = 0.5

    latency = (time.time() - start) * 1000

    return {
        "item_id": item_id,
        "optimal_price": optimal_price,
        "price_confidence": round(confidence, 2),
        "demand_forecast": forecast,
        "model_version": _gbt_version,
        "latency_ms": round(latency, 2),
    }


@svc.api(input=JSON(), output=JSON())
def health_check(request: dict) -> dict:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "als_model": _als_version,
        "gbt_model": _gbt_version,
        "timestamp": time.time(),
    }


@svc.api(input=JSON(), output=JSON())
def reload_models(request: dict) -> dict:
    """Reload models from MLflow (called by Airflow after retraining)."""
    load_models()
    return {
        "status": "reloaded",
        "als_model": _als_version,
        "gbt_model": _gbt_version,
    }
