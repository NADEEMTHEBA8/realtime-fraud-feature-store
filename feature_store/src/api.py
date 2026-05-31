"""
Feature serving API.

Reads precomputed fraud features from Redis (loaded by feature_store.loader)
and exposes them over HTTP for a fraud scoring service to call per
transaction. Read-only; no feature computation happens here.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("feature_api")

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=0,
    decode_responses=True,
)

# Health degrades past this age; matches the loader's 24h user-feature TTL
# plus the 4h DAG interval.
FRESHNESS_LIMIT_HOURS = 25


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        redis_conn.ping()
        logger.info("Feature API started; Redis reachable.")
    except redis.ConnectionError:
        logger.error("Feature API started but Redis is unreachable.")
    yield
    redis_conn.close()


app = FastAPI(
    title="Fraud Feature Store API",
    description="Serves precomputed fraud features from Redis.",
    version="1.0.0",
    lifespan=lifespan,
)


class BatchRequest(BaseModel):
    user_ids: list[str]


class FeatureResponse(BaseModel):
    user_id: str
    features: dict
    served_at: str


@app.get("/health")
def health_check():
    """Redis reachability plus feature freshness from loader metadata."""
    try:
        redis_conn.ping()
    except redis.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "redis": "disconnected"},
        )

    meta_raw = redis_conn.get("_meta:features:last_loaded")
    if not meta_raw:
        return {
            "status": "degraded",
            "redis": "connected",
            "message": "Features not loaded yet.",
        }

    meta = json.loads(meta_raw)
    last_loaded = datetime.fromisoformat(meta["last_loaded_at"])
    age_hours = (datetime.utcnow() - last_loaded).total_seconds() / 3600

    return {
        "status": "healthy" if age_hours < FRESHNESS_LIMIT_HOURS else "degraded",
        "redis": "connected",
        "features_age_hours": round(age_hours, 2),
        "last_loaded_at": meta["last_loaded_at"],
        "user_features_count": meta["user_features_count"],
        "merchant_stats_count": meta["merchant_stats_count"],
    }


@app.get("/features/user/{user_id}", response_model=FeatureResponse)
def get_user_features(user_id: str):
    """All fraud features for one user. 404 if the user is not in Redis."""
    raw = redis_conn.get(f"user:features:{user_id}")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features for user '{user_id}'.",
        )
    return FeatureResponse(
        user_id=user_id,
        features=json.loads(raw),
        served_at=datetime.utcnow().isoformat(),
    )


@app.get("/features/merchant/{merchant_id}")
def get_merchant_stats(merchant_id: str):
    """Latest daily stats for one merchant. 404 if not in Redis."""
    raw = redis_conn.get(f"merchant:latest:{merchant_id}")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No stats for merchant '{merchant_id}'.",
        )
    return {
        "merchant_id": merchant_id,
        "stats": json.loads(raw),
        "served_at": datetime.utcnow().isoformat(),
    }


@app.post("/features/batch")
def get_batch_features(request: BatchRequest):
    """Multi-user lookup via a single Redis MGET. Capped at 100 ids."""
    if len(request.user_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Batch size limited to 100 users.",
        )

    keys = [f"user:features:{uid}" for uid in request.user_ids]
    results = redis_conn.mget(keys)

    found = []
    missing = []
    for user_id, raw in zip(request.user_ids, results):
        if raw is not None:
            found.append({"user_id": user_id, "features": json.loads(raw)})
        else:
            missing.append(user_id)

    return {
        "results": found,
        "summary": {
            "requested": len(request.user_ids),
            "found": len(found),
            "missing": len(missing),
            "missing_ids": missing,
        },
        "served_at": datetime.utcnow().isoformat(),
    }
