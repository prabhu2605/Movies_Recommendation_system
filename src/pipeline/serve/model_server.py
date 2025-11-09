from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import logging
import time

logger = logging.getLogger(__name__)

class RecommendationRequest(BaseModel):
    user_id: int
    k: int = 20
    model: str = "popularity"

class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[int]
    model_used: str
    latency_ms: float

class ModelServer:
    
    def __init__(self, model_registry_path: str = "models/"):
        self.app = FastAPI(title="Recommendation API", version="1.0.0")
        self.model_registry_path = model_registry_path
        self.models = {}
        
        self.setup_routes()
    
    def setup_routes(self):
        
        @self.app.get("/healthz")
        async def health_check():
            return {
                "status": "healthy",
                "version": "1.0.0",
                "models_loaded": list(self.models.keys())
            }
        
        @self.app.get("/models")
        async def list_models():
            return {
                "available_models": ["popularity"],
                "default_model": "popularity"
            }
        
        @self.app.post("/recommend/{user_id}")
        async def recommend(
            user_id: int, request: RecommendationRequest
        ) -> RecommendationResponse:
            start_time = time.time()
            
            try:
               
                recommendations = list(range(1001, 1001 + request.k))
                
                latency_ms = (time.time() - start_time) * 1000
                
                return RecommendationResponse(
                    user_id=user_id,
                    recommendations=recommendations,
                    model_used=request.model,
                    latency_ms=latency_ms
                )
                
            except Exception as e:
                logger.error(f"Recommendation error: {e}")
                raise HTTPException(status_code=500, detail=str(e))
    
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        import uvicorn
        uvicorn.run(self.app, host=host, port=port)