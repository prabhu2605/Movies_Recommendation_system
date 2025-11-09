import pandas as pd
import numpy as np
from sklearn.metrics import ndcg_score
from sklearn.model_selection import train_test_split
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class OfflineEvaluator:

    def __init__(self, split_ratio: float = 0.8):
        self.split_ratio = split_ratio
    
    def chronological_split(self, ratings_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if 'timestamp' not in ratings_data.columns:
            raise ValueError("Data must contain 'timestamp' for chronological split")

        data_sorted = ratings_data.sort_values('timestamp')
        split_point = int(len(data_sorted) * self.split_ratio)
        
        train_data = data_sorted[:split_point]
        test_data = data_sorted[split_point:]
        
        logger.info(f"Chronological split: {len(train_data)} train, {len(test_data)} test")
        return train_data, test_data
    
    def calculate_ndcg(self, true_items: list, pred_items: list, k: int = 20) -> float:
        if not pred_items:
            return 0.0
        
        true_vector = [1 if item in true_items else 0 for item in pred_items[:k]]
        pred_vector = list(range(len(pred_items[:k]), 0, -1))
        
        if sum(true_vector) == 0:  
            return 0.0
            
        return ndcg_score([true_vector], [pred_vector], k=k)
    
    def evaluate_subpopulations(self, test_data: pd.DataFrame, recommendations: Dict) -> Dict:
        user_interaction_counts = test_data.groupby('userId').size()
 
        cold_start_users = user_interaction_counts[user_interaction_counts < 5].index
        power_users = user_interaction_counts[user_interaction_counts > 50].index
        regular_users = user_interaction_counts[(user_interaction_counts >= 5) & (user_interaction_counts <= 50)].index
        
        results = {}
        
        for segment_name, user_segment in [
            ('cold_start', cold_start_users),
            ('power_users', power_users), 
            ('regular_users', regular_users)
        ]:
            segment_ndcgs = []
            for user_id in user_segment:
                if user_id in recommendations:
                    user_ratings = test_data[test_data['userId'] == user_id]
                    true_items = user_ratings.nlargest(20, 'rating')['itemId'].tolist()
                    pred_items = recommendations[user_id]
                    
                    ndcg = self.calculate_ndcg(true_items, pred_items)
                    segment_ndcgs.append(ndcg)
            
            if segment_ndcgs:
                results[f'{segment_name}_ndcg'] = np.mean(segment_ndcgs)
                results[f'{segment_name}_count'] = len(segment_ndcgs)
            else:
                results[f'{segment_name}_ndcg'] = 0.0
                results[f'{segment_name}_count'] = 0
        
        return results
    
    def comprehensive_evaluation(self, model, ratings_data: pd.DataFrame) -> Dict:
      
        train_data, test_data = self.chronological_split(ratings_data)

        model.train(train_data)
        

        test_users = test_data['userId'].unique()
        recommendations = {}
        for user_id in test_users[:100]: 
            try:
                recommendations[user_id] = model.recommend(user_id, k=20)
            except:
                recommendations[user_id] = []

        overall_ndcgs = []
        for user_id, pred_items in recommendations.items():
            user_ratings = test_data[test_data['userId'] == user_id]
            true_items = user_ratings.nlargest(20, 'rating')['itemId'].tolist()
            ndcg = self.calculate_ndcg(true_items, pred_items)
            overall_ndcgs.append(ndcg)

        subpopulation_results = self.evaluate_subpopulations(test_data, recommendations)
        
        return {
            'overall_ndcg': np.mean(overall_ndcgs) if overall_ndcgs else 0.0,
            'users_evaluated': len(overall_ndcgs),
            **subpopulation_results
        }