import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append('src')

from pipeline.evaluate.offline_eval import OfflineEvaluator

class MockModel:
    def __init__(self):
        self.is_trained = False
    
    def train(self, data):
        self.is_trained = True
    
    def recommend(self, user_id, k=20):
        return list(range(1, k+1))

def test_chronological_split():
    evaluator = OfflineEvaluator()

    data = pd.DataFrame({
        'userId': [1, 1, 2, 2, 3, 3],
        'itemId': [101, 102, 101, 103, 102, 104],
        'rating': [5, 4, 3, 5, 4, 3],
        'timestamp': [1000, 2000, 3000, 4000, 5000, 6000]
    })
    
    train, test = evaluator.chronological_split(data)

    assert train['timestamp'].max() <= test['timestamp'].min()
    assert len(train) == 4  
    assert len(test) == 2

def test_ndcg_calculation():
   
    evaluator = OfflineEvaluator()
    
    true_items = [1, 2, 3]
    pred_items = [1, 4, 2, 5, 3] 
    
    ndcg = evaluator.calculate_ndcg(true_items, pred_items, k=3)
    assert 0 <= ndcg <= 1

def test_subpopulation_analysis():
    evaluator = OfflineEvaluator()
    
    test_data = pd.DataFrame({
        'userId': [1, 1, 2, 2, 3, 3, 4, 4, 4, 4],  # User 4 is power user
        'itemId': [101, 102, 101, 103, 102, 104, 101, 102, 103, 104],
        'rating': [5, 4, 3, 5, 4, 3, 5, 4, 3, 2],
        'timestamp': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    })
    
    recommendations = {
        1: [101, 102, 103],
        2: [101, 104, 105],
        3: [102, 104, 106],
        4: [101, 102, 103, 104]
    }
    
    results = evaluator.evaluate_subpopulations(test_data, recommendations)
    
    assert 'cold_start_ndcg' in results
    assert 'power_users_ndcg' in results
    assert 'regular_users_ndcg' in results