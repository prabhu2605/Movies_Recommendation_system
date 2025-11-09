import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append('src')

from pipeline.evaluate.offline_eval import OfflineEvaluator

class MockModel:
    """Mock model for comprehensive testing"""
    def __init__(self, name="mock_model"):
        self.name = name
        self.is_trained = False
    
    def train(self, data):
        self.is_trained = True
        return f"Trained on {len(data)} records"
    
    def recommend(self, user_id, k=20):
        return list(range(100, 100 + k))

class TestOfflineEvalComprehensive:
    def test_chronological_split_edge_cases(self):
        """Test chronological split with edge cases"""
        evaluator = OfflineEvaluator(split_ratio=0.8)
        
      
        empty_data = pd.DataFrame(columns=['userId', 'itemId', 'rating', 'timestamp'])
       
        train, test = evaluator.chronological_split(empty_data)
        assert len(train) == 0
        assert len(test) == 0
        
        
        single_data = pd.DataFrame({
            'userId': [1], 'itemId': [101], 'rating': [5], 'timestamp': [1000]
        })
        train, test = evaluator.chronological_split(single_data)
       
        assert len(train) == 0
        assert len(test) == 1

    def test_ndcg_edge_cases(self):
        """Test NDCG calculation with edge cases"""
        evaluator = OfflineEvaluator()
        
       
        assert evaluator.calculate_ndcg([1, 2, 3], [], k=5) == 0.0
        
      
        assert evaluator.calculate_ndcg([], [1, 2, 3], k=5) == 0.0
        
        
        true_items = [1, 2, 3]
        pred_items = [1, 2, 3, 4, 5]
        ndcg = evaluator.calculate_ndcg(true_items, pred_items, k=3)
        assert ndcg > 0.9  

    def test_comprehensive_evaluation_integration(self):
        """Test complete evaluation workflow"""
        evaluator = OfflineEvaluator(split_ratio=0.8)
        
        
        np.random.seed(42)
        n_samples = 500
        data = pd.DataFrame({
            'userId': np.random.randint(1, 50, n_samples),
            'itemId': np.random.randint(1, 100, n_samples),
            'rating': np.random.choice([1, 2, 3, 4, 5], n_samples),
            'timestamp': np.random.randint(1609459200, 1640995200, n_samples)
        })
        
        model = MockModel()
        results = evaluator.comprehensive_evaluation(model, data)
        
        assert 'overall_ndcg' in results
        assert 'users_evaluated' in results
        assert 'cold_start_ndcg' in results
        assert 'power_users_ndcg' in results
        assert 'regular_users_ndcg' in results
        assert model.is_trained == True

    def test_evaluate_subpopulations_edge_cases(self):
        """Test subpopulation analysis with edge cases"""
        evaluator = OfflineEvaluator()
        
       
        empty_data = pd.DataFrame(columns=['userId', 'itemId', 'rating', 'timestamp'])
        empty_recommendations = {}
        results = evaluator.evaluate_subpopulations(empty_data, empty_recommendations)
        
        assert results['cold_start_ndcg'] == 0.0
        assert results['power_users_ndcg'] == 0.0
        assert results['regular_users_ndcg'] == 0.0
        
        
        test_data = pd.DataFrame({
            'userId': [1, 2, 3],
            'itemId': [101, 102, 103],
            'rating': [5, 4, 3],
            'timestamp': [1000, 2000, 3000]
        })
        empty_recs = {}
        results = evaluator.evaluate_subpopulations(test_data, empty_recs)
        
        assert results['cold_start_count'] == 0
        assert results['power_users_count'] == 0
        assert results['regular_users_count'] == 0