"""
Fraud Feature Serving API.

Exposes precomputed fraud features from Redis over HTTP.
Includes basic latency monitoring and API versioning.
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Generator

import redis
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("feature_api")

# --- Configuration & State ---
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FRESHNESS_LIMIT_HOURS = 25
API_KEY_NAME = "X-API-Key"
DUMMY_API_KEY = "sk_test_123"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# Global connection pool managed by lifespan
redis_client: redis.Redis = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global redis_client
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True,
    )
    try:
        redis_client.ping()
        logger.info("Feature API started; Redis pool initialized.")
    except redis.ConnectionError:
        logger.error("Feature API started but Redis is unreachable.")
    yield
    if redis_client:
        redis_client.close()
        logger.info("Redis pool closed.")


app = FastAPI(
    title="Fraud Feature Store API",
    description="Low-latency feature serving API for ML inference.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
)


# --- Middleware ---
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Simple latency monitor."""
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    # Record process time in milliseconds
    response.headers["X-Process-Time-Ms"] = f"{process_time * 1000:.2f}"
    return response


# --- Dependencies ---
def get_api_key(api_key: str = Security(api_key_header)) -> str:
    """Validate the incoming API key."""
    if api_key != DUMMY_API_KEY:
        raise HTTPException(
            status_code=403, detail="Invalid API Key. Access denied."
        )
    return api_key


def get_redis() -> Generator[redis.Redis, None, None]:
    """Dependency injection for the Redis client."""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")
    yield redis_client


# --- Models ---
class BatchRequest(BaseModel):
    user_ids: list[str] = Field(..., max_length=100, description="List of max 100 user IDs")


class FeatureResponse(BaseModel):
    user_id: str
    features: dict
    served_at: str


class HealthResponse(BaseModel):
    status: str
    redis: str
    features_age_hours: float | None = None
    last_loaded_at: str | None = None


# --- V1 Router ---
v1_router = APIRouter(prefix="/v1", tags=["Features V1"])


@v1_router.get("/features/user/{user_id}", response_model=FeatureResponse)
def get_user_features(
    user_id: str,
    api_key: str = Depends(get_api_key),
    r: redis.Redis = Depends(get_redis)
):
    """Retrieve all precomputed fraud features for a single user."""
    raw = r.get(f"user:features:{user_id}")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for user '{user_id}'.",
        )
    return FeatureResponse(
        user_id=user_id,
        features=json.loads(raw),
        served_at=datetime.utcnow().isoformat(),
    )


@v1_router.post("/features/batch")
def get_batch_features(
    request: BatchRequest,
    api_key: str = Depends(get_api_key),
    r: redis.Redis = Depends(get_redis)
):
    """Multi-user feature lookup via a single Redis MGET for high-throughput scoring."""
    keys = [f"user:features:{uid}" for uid in request.user_ids]
    results = r.mget(keys)

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


# --- Base Router (Unauthenticated) ---
@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check(r: redis.Redis = Depends(get_redis)):
    """System health and data freshness monitor. Unauthenticated."""
    try:
        r.ping()
    except redis.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "redis": "disconnected"},
        )

    meta_raw = r.get("_meta:features:last_loaded")
    if not meta_raw:
        return HealthResponse(
            status="degraded",
            redis="connected"
        )

    meta = json.loads(meta_raw)
    last_loaded = datetime.fromisoformat(meta["last_loaded_at"])
    age_hours = (datetime.utcnow() - last_loaded).total_seconds() / 3600

    return HealthResponse(
        status="healthy" if age_hours < FRESHNESS_LIMIT_HOURS else "degraded",
        redis="connected",
        features_age_hours=round(age_hours, 2),
        last_loaded_at=meta["last_loaded_at"]
    )


# Attach routers
app.include_router(v1_router)
