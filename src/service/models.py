import numpy as np
import pandas as pd
from typing import List, Dict, Optional
import logging
import pickle
import os

logger = logging.getLogger(__name__)

class BaseModel:
    def __init__(self, name: str):
        self.name = name
        self.is_trained = False
    
    def train(self, ratings_data: pd.DataFrame):
        raise NotImplementedError
    
    def recommend(self, user_id: int, k: int = 20) -> List[int]:
        raise NotImplementedError
    
    def save(self, path: str):
        with open(path, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, path: str):
        with open(path, 'rb') as f:
            return pickle.load(f)

class PopularityModel(BaseModel):
    def __init__(self):
        super().__init__("popularity")
        self.popular_items = []
    
    def train(self, ratings_data: pd.DataFrame):
        logger.info("Training popularity model")
        item_stats = ratings_data.groupby('itemId').agg({
            'rating': ['count', 'mean']
        }).reset_index()
        item_stats.columns = ['itemId', 'rating_count', 'rating_mean']

        max_count = item_stats['rating_count'].max()
        item_stats['score'] = (item_stats['rating_mean'] * item_stats['rating_count']) / max_count

        self.popular_items = item_stats.nlargest(1000, 'score')['itemId'].tolist()
        self.is_trained = True
        logger.info(f"Popularity model trained with {len(self.popular_items)} items")
    
    def recommend(self, user_id: int, k: int = 20) -> List[int]:
        if not self.is_trained:
            raise ValueError("Model not trained")
        return self.popular_items[:k]

class ItemItemCFModel(BaseModel):
    def __init__(self):
        super().__init__("item_item_cf")
        self.similarity_matrix = None
        self.item_mapping = None
        self.ratings_matrix = None
    
    def train(self, ratings_data: pd.DataFrame):
        logger.info("Training Item-Item Collaborative Filtering model")

        self.ratings_matrix = ratings_data.pivot_table(
            index='userId', 
            columns='itemId', 
            values='rating',
            fill_value=0
        )
        
        from sklearn.metrics.pairwise import cosine_similarity
        self.similarity_matrix = cosine_similarity(self.ratings_matrix.T)
        self.item_mapping = {i: item_id for i, item_id in enumerate(self.ratings_matrix.columns)}
        
        self.is_trained = True
        logger.info("Item-Item CF model trained")
    
    def recommend(self, user_id: int, k: int = 20) -> List[int]:
        if not self.is_trained:
            raise ValueError("Model not trained")
        
        try:
            user_ratings = self.ratings_matrix.loc[user_id].values
            scores = self.similarity_matrix.dot(user_ratings)

            top_indices = np.argsort(scores)[::-1][:k]
            recommendations = [self.item_mapping[i] for i in top_indices]
            
            return recommendations
        except KeyError:
            return list(self.ratings_matrix.columns[:k])

class ModelRegistry:
    def __init__(self):
        self.models: Dict[str, BaseModel] = {}
    
    async def initialize_models(self):
        sample_data = self._create_sample_data()
        pop_model = PopularityModel()
        pop_model.train(sample_data)
        self.models["popularity"] = pop_model
        
        cf_model = ItemItemCFModel()
        cf_model.train(sample_data)
        self.models["item_item_cf"] = cf_model
    
    def get_model(self, model_name: str) -> Optional[BaseModel]:
        return self.models.get(model_name)
    
    def get_available_models(self) -> Dict[str, str]:
        return {name: model.__class__.__name__ for name, model in self.models.items()}
    
    def _create_sample_data(self) -> pd.DataFrame:
        np.random.seed(42)
        n_users = 1000
        n_items = 500
        n_ratings = 10000
        
        data = {
            'userId': np.random.randint(1, n_users + 1, n_ratings),
            'itemId': np.random.randint(1, n_items + 1, n_ratings),
            'rating': np.random.randint(1, 6, n_ratings),
            'timestamp': np.random.randint(1609459200, 1640995200, n_ratings)  # 2021-2022
        }
        
        return pd.DataFrame(data)