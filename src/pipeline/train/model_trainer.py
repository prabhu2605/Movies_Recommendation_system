import pandas as pd
import numpy as np
import logging
from typing import Dict, Any
import pickle
import os

logger = logging.getLogger(__name__)

class ModelTrainer:

    
    def __init__(self, model_registry_path: str = "models/"):
        self.model_registry_path = model_registry_path
        self.models = {}

        os.makedirs(model_registry_path, exist_ok=True)
    
    class PopularityModel:
        def __init__(self):
            self.item_popularity = None
            self.is_trained = False
        
        def train(self, train_data: pd.DataFrame):
            self.item_popularity = train_data.groupby('itemId')['rating'].agg([
                'count', 'mean'
            ]).reset_index()

            self.item_popularity['popularity_score'] = (
                self.item_popularity['count'] * self.item_popularity['mean']
            )
            
            self.item_popularity = self.item_popularity.sort_values(
                'popularity_score', ascending=False
            )
            self.is_trained = True
        
        def recommend(self, user_id: int, k: int = 20) -> list:
            if not self.is_trained or self.item_popularity is None:
                return []
            
            return self.item_popularity.head(k)['itemId'].tolist()
    
    def train_popularity_model(self, train_data: pd.DataFrame) -> PopularityModel:
        model = self.PopularityModel()
        model.train(train_data)
 
        model_path = os.path.join(self.model_registry_path, "popularity_model.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        logger.info(f"Popularity model trained and saved to {model_path}")
        return model
    
    def load_model(self, model_name: str):
        model_path = os.path.join(self.model_registry_path, f"{model_name}.pkl")
        
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                return pickle.load(f)
        else:
            logger.warning(f"Model {model_name} not found at {model_path}")
            return None