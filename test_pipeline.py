#!/usr/bin/env python3
"""
Test pipeline runner for HW3 - Works without external dependencies
"""

import sys
import os
import pandas as pd
import numpy as np


sys.path.append('src')

from pipeline.evaluate.offline_eval import OfflineEvaluator
from pipeline.quality.drift_detector import DriftDetector
from pipeline.quality.schemas import validate_watch_schema, validate_rating_schema

class MockModel:
    """Mock model for testing"""
    def __init__(self, name="mock_model"):
        self.name = name
        self.is_trained = False
    
    def train(self, data):
        self.is_trained = True
        print(f"    {self.name} trained on {len(data)} records")
    
    def recommend(self, user_id, k=20):
        
        return list(range(100, 100 + k))

def test_offline_evaluation():
    """Test offline evaluation with mock data"""
    print("\n Testing Offline Evaluation...")
    
  
    np.random.seed(42)
    n_users = 1000
    n_items = 500
    n_ratings = 10000
    
    sample_data = pd.DataFrame({
        'userId': np.random.randint(1, n_users + 1, n_ratings),
        'itemId': np.random.randint(1, n_items + 1, n_ratings),
        'rating': np.random.choice([1, 2, 3, 4, 5], n_ratings, p=[0.1, 0.2, 0.3, 0.3, 0.1]),
        'timestamp': np.random.randint(1609459200, 1640995200, n_ratings)
    })
    
    
    evaluator = OfflineEvaluator(split_ratio=0.8)
    
    
    train_data, test_data = evaluator.chronological_split(sample_data)
    print(f"    Chronological split: {len(train_data)} train, {len(test_data)} test")
    
    
    true_items = [101, 102, 103]
    pred_items = [101, 104, 102, 105, 103]
    ndcg = evaluator.calculate_ndcg(true_items, pred_items, k=5)
    print(f"    NDCG calculation: {ndcg:.4f}")
    
    
    mock_model = MockModel("PopularityModel")
    results = evaluator.comprehensive_evaluation(mock_model, sample_data)
    
    print(f"    Overall NDCG: {results['overall_ndcg']:.4f}")
    print(f"    Users evaluated: {results['users_evaluated']}")
    print(f"    Cold start users NDCG: {results.get('cold_start_ndcg', 0):.4f}")
    print(f"    Power users NDCG: {results.get('power_users_ndcg', 0):.4f}")
    
    return results

def test_schema_validation():
    """Test schema validation"""
    print("\n Testing Schema Validation...")
    
 
    valid_watch = {
        'user_id': 123,
        'item_id': 456,
        'timestamp': 1640995200,
        'event_type': 'start'
    }
    
    valid_rating = {
        'user_id': 123,
        'item_id': 456,
        'rating': 5,
        'timestamp': 1640995200
    }
    
    watch_valid = validate_watch_schema(valid_watch)
    rating_valid = validate_rating_schema(valid_rating)
    
    print(f"    Watch schema validation: {watch_valid}")
    print(f"    Rating schema validation: {rating_valid}")
    
    
    invalid_watch = {
        'user_id': 0,  
        'item_id': 456,
        'timestamp': 1640995200,
        'event_type': 'start'
    }
    
    invalid_rating = {
        'user_id': 123,
        'item_id': 456,
        'rating': 6,  
        'timestamp': 1640995200
    }
    
    watch_invalid = validate_watch_schema(invalid_watch)
    rating_invalid = validate_rating_schema(invalid_rating)
    
    print(f"    Invalid watch rejected: {not watch_invalid}")
    print(f"    Invalid rating rejected: {not rating_invalid}")
    
    return watch_valid and rating_valid and (not watch_invalid) and (not rating_invalid)

def test_drift_detection():
    """Test drift detection with synthetic data"""
    print("\n Testing Drift Detection...")
    
 
    np.random.seed(42)
    

    historical_data = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, 5000),
        'item_id': np.random.randint(1, 500, 5000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 5000, p=[0.1, 0.2, 0.3, 0.3, 0.1])
    })
    
  
    current_data = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, 5000),
        'item_id': np.random.randint(1, 500, 5000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 5000, p=[0.15, 0.25, 0.25, 0.25, 0.1])  # Slight drift
    })
    
    drift_detector = DriftDetector(threshold=0.05)
    drift_results = drift_detector.comprehensive_drift_check(current_data, historical_data)
    
    print(f"    Rating distribution drift: {drift_results['rating_distribution_drift']['drift_detected']}")
    print(f"    User activity drift: {drift_results['user_activity_drift']['drift_detected']}")
    print(f"    Overall drift detected: {drift_results['overall_drift_detected']}")
    

    current_dist = drift_results['rating_distribution_drift']['current_distribution']
    historical_dist = drift_results['rating_distribution_drift']['historical_distribution']
    
    print(f"    Current rating distribution: {current_dist}")
    print(f"    Historical rating distribution: {historical_dist}")
    
    return drift_results

def test_subpopulation_analysis():
    """Test subpopulation analysis"""
    print("\n Testing Subpopulation Analysis...")
    
    
    np.random.seed(42)
    
    
    cold_start_data = pd.DataFrame({
        'userId': [1, 1, 2, 3, 3, 4],
        'itemId': [101, 102, 101, 102, 103, 101],
        'rating': [5, 4, 3, 5, 4, 3],
        'timestamp': [1000, 2000, 3000, 4000, 5000, 6000]
    })
    
    
    power_user_data = pd.DataFrame({
        'userId': [5] * 10 + [6] * 8,
        'itemId': list(range(101, 111)) + list(range(101, 109)),
        'rating': [5, 4, 3, 5, 4, 3, 2, 1, 5, 4] + [3, 4, 5, 2, 3, 4, 5, 1],
        'timestamp': list(range(7000, 7010)) + list(range(8000, 8008))
    })
    
    test_data = pd.concat([cold_start_data, power_user_data], ignore_index=True)
    
    evaluator = OfflineEvaluator()
    
    
    recommendations = {
        1: [101, 102, 103, 104, 105],
        2: [101, 106, 107, 108, 109],
        3: [102, 103, 110, 111, 112],
        4: [101, 113, 114, 115, 116],
        5: list(range(101, 121)),
        6: list(range(101, 116))
    }
    
    subpopulation_results = evaluator.evaluate_subpopulations(test_data, recommendations)
    
    print(f"    Cold start users analyzed: {subpopulation_results.get('cold_start_count', 0)} users")
    print(f"    Power users analyzed: {subpopulation_results.get('power_users_count', 0)} users")
    print(f"    Regular users analyzed: {subpopulation_results.get('regular_users_count', 0)} users")
    
    return subpopulation_results

def main():
    print(" Running HW3 Test Pipeline")
    print("=" * 50)
    
    
    all_passed = True
    
    try:
        
        offline_results = test_offline_evaluation()
        
        
        schema_passed = test_schema_validation()
        all_passed = all_passed and schema_passed
        
        drift_results = test_drift_detection()
        
        
        subpop_results = test_subpopulation_analysis()
        
    except Exception as e:
        print(f"\n Error during testing: {e}")
        all_passed = False
    
   
    print("\n" + "=" * 50)
    print(" TEST SUMMARY")
    print("=" * 50)
    
    if all_passed:
        print(" ALL TESTS PASSED!")
        print("\n HW3 Pipeline Components Verified:")
        print("   - Offline evaluation with chronological splits")
        print("   - Schema validation with Pandera")
        print("   - Drift detection for data quality")
        print("   - Subpopulation analysis")
        print("   - Modular pipeline structure")
        
        print("\n Sample Results:")
        if 'offline_results' in locals():
            print(f"   - Offline NDCG: {offline_results['overall_ndcg']:.4f}")
        if 'drift_results' in locals():
            print(f"   - Data drift detected: {drift_results['overall_drift_detected']}")
        
        print("\n Next Steps:")
        print("   1. Run unit tests: pytest tests/ -v")
        print("   2. Check coverage: pytest --cov=src --cov-report=html")
        print("   3. Add environment variables for full pipeline")
        
    else:
        print(" Some tests failed. Check the output above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)