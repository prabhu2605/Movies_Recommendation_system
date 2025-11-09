import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append('src')

from pipeline.quality.drift_detector import DriftDetector

class TestDriftDetectorComprehensive:
    def test_drift_detector_edge_cases(self):
        """Test drift detector with edge cases"""
        detector = DriftDetector(threshold=0.05)
        
        
        empty_df = pd.DataFrame(columns=['user_id', 'item_id', 'rating'])
        result = detector.detect_rating_distribution_drift(empty_df, empty_df)
        
        assert 'drift_detected' in result
        assert 'p_value' in result
        
       
        single_record = pd.DataFrame({
            'user_id': [1], 'item_id': [101], 'rating': [5]
        })
        result = detector.detect_rating_distribution_drift(single_record, single_record)
        assert 'drift_detected' in result

    def test_different_thresholds(self):
        """Test drift detection with different thresholds"""
        np.random.seed(42)
        
        historical = pd.DataFrame({
            'user_id': np.random.randint(1, 100, 100),
            'item_id': np.random.randint(1, 50, 100),
            'rating': np.random.choice([1, 2, 3, 4, 5], 100, p=[0.1, 0.2, 0.3, 0.3, 0.1])
        })
        
        current = pd.DataFrame({
            'user_id': np.random.randint(101, 200, 100),
            'item_id': np.random.randint(51, 100, 100),
            'rating': np.random.choice([1, 2, 3, 4, 5], 100, p=[0.15, 0.25, 0.25, 0.25, 0.1]) 
        })
        
        
        strict_detector = DriftDetector(threshold=0.01)
        strict_result = strict_detector.comprehensive_drift_check(current, historical)
        
        
        lenient_detector = DriftDetector(threshold=0.10)
        lenient_result = lenient_detector.comprehensive_drift_check(current, historical)
        
        
        assert 'overall_drift_detected' in strict_result
        assert 'overall_drift_detected' in lenient_result

    def test_user_activity_edge_cases(self):
        """Test user activity drift with edge cases"""
        detector = DriftDetector(threshold=0.05)
        
        
        historical = pd.DataFrame({
            'user_id': np.repeat(range(1, 11), 5),  
            'item_id': range(50),
            'rating': np.ones(50)
        })
        
        current = pd.DataFrame({
            'user_id': np.repeat(range(11, 21), 5),  
            'item_id': range(50),
            'rating': np.ones(50)
        })
        
        result = detector.detect_user_activity_drift(current, historical)
        assert result['drift_detected'] == False  

    def test_comprehensive_drift_with_identical_data(self):
        """Test comprehensive drift with identical data (no drift)"""
        detector = DriftDetector(threshold=0.05)
        
        np.random.seed(42)
        data = pd.DataFrame({
            'user_id': np.random.randint(1, 100, 200),
            'item_id': np.random.randint(1, 50, 200),
            'rating': np.random.choice([1, 2, 3, 4, 5], 200, p=[0.1, 0.2, 0.3, 0.3, 0.1])
        })
        
        
        historical = data.iloc[:100]
        current = data.iloc[100:]
        
        result = detector.comprehensive_drift_check(current, historical)
        
        
        assert 'rating_distribution_drift' in result
        assert 'user_activity_drift' in result
        assert 'overall_drift_detected' in result