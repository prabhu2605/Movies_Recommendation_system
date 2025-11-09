import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append('src')

from pipeline.quality.drift_detector import DriftDetector

def test_rating_distribution_drift():
    """Test rating distribution drift detection"""
    drift_detector = DriftDetector(threshold=0.05)
    
    
    np.random.seed(42)
    
    
    historical_data = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, 5000),
        'item_id': np.random.randint(1, 500, 5000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 5000, p=[0.1, 0.2, 0.3, 0.3, 0.1])
    })
    
   
    current_data = pd.DataFrame({
        'user_id': np.random.randint(1000, 2000, 5000),  
        'item_id': np.random.randint(500, 1000, 5000),   
        'rating': np.random.choice([1, 2, 3, 4, 5], 5000, p=[0.1, 0.2, 0.3, 0.3, 0.1])  
    })
    
    result = drift_detector.detect_rating_distribution_drift(current_data, historical_data)
    

    assert 'drift_detected' in result
    assert 'p_value' in result
    assert 'current_distribution' in result
    assert 'historical_distribution' in result
    

def test_user_activity_drift():
    """Test user activity drift detection"""
    drift_detector = DriftDetector(threshold=0.05)
    
    np.random.seed(42)
    historical_data = pd.DataFrame({
        'user_id': np.random.randint(1, 100, 1000),
        'item_id': np.random.randint(1, 500, 1000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 1000)
    })
    
    current_data = pd.DataFrame({
        'user_id': np.random.randint(101, 200, 1000),
        'item_id': np.random.randint(501, 1000, 1000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 1000)
    })
    
    result = drift_detector.detect_user_activity_drift(current_data, historical_data)
    
    assert 'drift_detected' in result
    assert 'p_value' in result
    assert 'current_avg_activity' in result
    assert 'historical_avg_activity' in result

def test_comprehensive_drift_check():
    """Test comprehensive drift analysis"""
    drift_detector = DriftDetector(threshold=0.05)
    
    np.random.seed(42)
    historical_data = pd.DataFrame({
        'user_id': np.random.randint(1, 500, 1000),
        'item_id': np.random.randint(1, 500, 1000),
        'rating': np.ones(1000)  
    })
    
    current_data = pd.DataFrame({
        'user_id': np.random.randint(501, 1000, 1000),
        'item_id': np.random.randint(501, 1000, 1000),
        'rating': np.full(1000, 5)  
    })
    
    result = drift_detector.comprehensive_drift_check(current_data, historical_data)
    
    assert 'rating_distribution_drift' in result
    assert 'user_activity_drift' in result
    assert 'overall_drift_detected' in result
    
    assert result['rating_distribution_drift']['drift_detected'] == True