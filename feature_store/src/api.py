"""
FastAPI Feature Serving API

What this does:
    Serves pre-computed fraud features from Redis with sub-millisecond latency.
    This is the API that a fraud scoring model would call to get features
    before making a fraud/not-fraud decision.

Endpoints:
    GET /health              → Service health + feature freshness
    GET /features/user/{id}  → All fraud features for a user
    GET /features/merchant/{id} → Latest stats for a merchant
    POST /features/batch     → Batch feature lookup for multiple users

How it's used in production:
    1. Transaction comes in from payment gateway
    2. Fraud scoring service calls GET /features/user/{user_id}
    3. Gets 25+ pre-computed features in <1ms
    4. Feeds features to ML model
    5. Model returns fraud probability
    6. If probability > threshold → block transaction

Interview talking point:
    "I built a FastAPI feature serving layer backed by Redis that returns
    25+ fraud features per user in sub-millisecond latency. The same features
    computed by dbt in the batch layer are served here for real-time scoring,
    maintaining consistency between training and serving environments."
"""

import json
import logging
import os
import time
from datetime import datetime

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("feature_api")

# ---------- App Setup ----------
app = FastAPI(
    title="Fraud Feature Store API",
    description="Serves pre-computed fraud features from Redis for real-time scoring",
    version="1.0.0",
)

# ---------- Redis Connection ----------
redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=0,
    decode_responses=True,
)


# ---------- Request/Response Models ----------
class BatchRequest(BaseModel):
    """Request body for batch feature lookup."""
    user_ids: list[str]


class FeatureResponse(BaseModel):
    """Wrapper for feature responses with metadata."""
    user_id: str
    features: dict
    served_at: str
    latency_ms: float


# ---------- Health Endpoint ----------
@app.get("/health")
def health_check():
    """
    Health check with feature freshness monitoring.

    Returns:
        - status: healthy / degraded / unhealthy
        - redis: connected / disconnected
        - features_age_hours: how old the loaded features are
        - feature_counts: number of user and merchant features in Redis

    Why freshness matters:
        If the feature loader hasn't run in 25+ hours, features are stale.
        A fraud model using day-old features will miss recent behavioral
        changes. The health endpoint lets monitoring tools (Prometheus,
        Datadog) detect this and alert.
    """
    try:
        redis_conn.ping()
        redis_status = "connected"
    except redis.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "redis": "disconnected"},
        )

    # Check feature freshness
    meta_raw = redis_conn.get("_meta:features:last_loaded")
    if meta_raw:
        meta = json.loads(meta_raw)
        last_loaded = datetime.fromisoformat(meta["last_loaded_at"])
        age_hours = (datetime.utcnow() - last_loaded).total_seconds() / 3600

        status = "healthy" if age_hours < 25 else "degraded"

        return {
            "status": status,
            "redis": redis_status,
            "features_age_hours": round(age_hours, 2),
            "last_loaded_at": meta["last_loaded_at"],
            "user_features_count": meta["user_features_count"],
            "merchant_stats_count": meta["merchant_stats_count"],
        }

    return {
        "status": "degraded",
        "redis": redis_status,
        "message": "Features not loaded yet. Run the feature loader.",
    }


# ---------- User Features Endpoint ----------
@app.get("/features/user/{user_id}", response_model=FeatureResponse)
def get_user_features(user_id: str):
    """
    Get all fraud features for a single user.

    This is the primary endpoint called by the fraud scoring service.
    Returns 25+ pre-computed features in sub-millisecond latency.

    Args:
        user_id: User identifier (e.g., user_2765df9165)

    Returns:
        FeatureResponse with all features and serving metadata.

    Raises:
        404 if user_id not found in Redis.
    """
    start = time.perf_counter()

    key = f"user:features:{user_id}"
    raw = redis_conn.get(key)

    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for user '{user_id}'. "
                   f"User may not exist or features haven't been loaded.",
        )

    features = json.loads(raw)
    latency = (time.perf_counter() - start) * 1000  # Convert to ms

    return FeatureResponse(
        user_id=user_id,
        features=features,
        served_at=datetime.utcnow().isoformat(),
        latency_ms=round(latency, 3),
    )


# ---------- Merchant Stats Endpoint ----------
@app.get("/features/merchant/{merchant_id}")
def get_merchant_stats(merchant_id: str):
    """
    Get the latest daily stats for a merchant.

    Used by the fraud scoring service to check merchant-level risk signals
    (failure rate, refund rate, anomalous ticket ratios).
    """
    start = time.perf_counter()

    key = f"merchant:latest:{merchant_id}"
    raw = redis_conn.get(key)

    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No stats found for merchant '{merchant_id}'.",
        )

    stats = json.loads(raw)
    latency = (time.perf_counter() - start) * 1000

    return {
        "merchant_id": merchant_id,
        "stats": stats,
        "served_at": datetime.utcnow().isoformat(),
        "latency_ms": round(latency, 3),
    }


# ---------- Batch Features Endpoint ----------
@app.post("/features/batch")
def get_batch_features(request: BatchRequest):
    """
    Get features for multiple users in a single call.

    Why batch?
        When scoring a batch of transactions (e.g., end-of-day review),
        making 10,000 individual API calls is slow. The batch endpoint
        uses Redis MGET for a single round-trip.

    Args:
        request: BatchRequest with list of user_ids (max 100).

    Returns:
        List of feature responses + summary metadata.
    """
    start = time.perf_counter()

    if len(request.user_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Batch size limited to 100 users. Use pagination for larger batches.",
        )

    # Redis MGET: single round-trip for all keys
    keys = [f"user:features:{uid}" for uid in request.user_ids]
    results = redis_conn.mget(keys)

    responses = []
    found = 0
    missing = []

    for user_id, raw in zip(request.user_ids, results):
        if raw is not None:
            features = json.loads(raw)
            responses.append({
                "user_id": user_id,
                "features": features,
            })
            found += 1
        else:
            missing.append(user_id)

    latency = (time.perf_counter() - start) * 1000

    return {
        "results": responses,
        "summary": {
            "requested": len(request.user_ids),
            "found": found,
            "missing": len(missing),
            "missing_ids": missing,
        },
        "served_at": datetime.utcnow().isoformat(),
        "latency_ms": round(latency, 3),
    }


# ---------- Startup/Shutdown Events ----------
@app.on_event("startup")
def startup():
    """Log startup and verify Redis connection."""
    try:
        redis_conn.ping()
        logger.info("Feature API started. Redis connected.")
    except redis.ConnectionError:
        logger.error("Feature API started but Redis is NOT connected!")


@app.on_event("shutdown")
def shutdown():
    """Clean up Redis connection."""
    redis_conn.close()
    logger.info("Feature API stopped.")
