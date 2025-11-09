import pandas as pd
import numpy as np
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class FeatureEngineer:
    
    def __init__(self):
        self.feature_stats = {}
    
    def create_user_features(self, ratings_data: pd.DataFrame) -> pd.DataFrame:
        user_features = ratings_data.groupby('user_id').agg({
            'rating': ['count', 'mean', 'std'],
            'timestamp': ['min', 'max']
        }).reset_index()
        
        user_features.columns = [
            'user_id', 'rating_count', 'rating_mean', 'rating_std',
            'first_activity', 'last_activity'
        ]
        
        user_features['user_activity_days'] = (
            user_features['last_activity'] - user_features['first_activity']
        ) / (24 * 60 * 60) 
        
        return user_features
    
    def create_item_features(self, ratings_data: pd.DataFrame) -> pd.DataFrame:
     
        item_features = ratings_data.groupby('item_id').agg({
            'rating': ['count', 'mean', 'std'],
            'user_id': 'nunique'
        }).reset_index()
        
        item_features.columns = [
            'item_id', 'rating_count', 'rating_mean', 'rating_std', 'unique_users'
        ]
        
        item_features['popularity_score'] = (
            item_features['rating_count'] * item_features['rating_mean']
        )
        
        return item_features
    
    def create_interaction_features(self, ratings_data: pd.DataFrame) -> pd.DataFrame:
        ratings_data['hour_of_day'] = pd.to_datetime(
            ratings_data['timestamp'], unit='s'
        ).dt.hour
        
        ratings_data['day_of_week'] = pd.to_datetime(
            ratings_data['timestamp'], unit='s'
        ).dt.dayofweek
        
        return ratings_data