from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import os
import json
import time
from typing import List, Optional
import logging
from .models import PopularityModel, ItemItemCFModel, ModelRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Recommender API", version="1.0.0")

reqs = Counter('recommend_requests_total', 'Total recommendation requests', ['status', 'model_type'])
lat = Histogram('recommend_latency_seconds', 'Recommendation latency')
kafka_events = Counter('kafka_events_total', 'Kafka events produced', ['topic'])

model_registry = ModelRegistry()

class RecommendationRequest(BaseModel):
    user_id: int
    k: int = 20
    model: Optional[str] = "popularity"

class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[int]
    model_used: str
    latency_ms: float

@app.on_event("startup")
async def startup_event():
    try:
        await model_registry.initialize_models()
        logger.info("Models initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize models: {e}")

@app.get('/healthz')
def healthz():
    return {
        "status": "ok",
        "version": os.getenv('MODEL_VERSION', 'v1.0'),
        "models_loaded": list(model_registry.get_available_models().keys())
    }

@app.post('/recommend/{user_id}')
async def recommend(user_id: int, request: RecommendationRequest, background_tasks: BackgroundTasks):
    start_time = time.time()
    
    try:
        
        if user_id != request.user_id:
            raise HTTPException(status_code=400, detail="User ID mismatch")

        model = model_registry.get_model(request.model)
        if not model:
            raise HTTPException(status_code=400, detail=f"Model {request.model} not available")

        recommendations = model.recommend(user_id, request.k)
        
        latency_ms = (time.time() - start_time) * 1000
        
        response = RecommendationResponse(
            user_id=user_id,
            recommendations=recommendations,
            model_used=request.model,
            latency_ms=latency_ms
        )

        background_tasks.add_task(
            log_recommendation_event,
            "reco_responses",
            {
                "user_id": user_id,
                "recommendations": recommendations,
                "model_used": request.model,
                "latency_ms": latency_ms,
                "timestamp": time.time()
            }
        )
        
        reqs.labels('200', request.model).inc()
        lat.observe(latency_ms / 1000)  
        
        return response
        
    except HTTPException:
        reqs.labels('400', request.model or 'unknown').inc()
        raise
    except Exception as e:
        logger.error(f"Recommendation error: {e}")
        reqs.labels('500', request.model or 'unknown').inc()
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get('/models')
def list_models():
    return {
        "available_models": model_registry.get_available_models(),
        "default_model": "popularity"
    }

@app.get('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

async def log_recommendation_event(topic_suffix: str, data: dict):
   
    try:
        logger.info(f"Would send to Kafka topic {topic_suffix}: {data}")
        kafka_events.labels(topic_suffix).inc()
    except Exception as e:
        logger.error(f"Failed to log to Kafka: {e}")