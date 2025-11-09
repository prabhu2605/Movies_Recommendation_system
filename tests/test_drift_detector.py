import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append('src')

from pipeline.quality.drift_detector import DriftDetector

class TestDriftDetector:
    def test_initialization(self):
        """Test drift detector initialization"""
        detector = DriftDetector(threshold=0.05)
        assert detector.threshold == 0.05
        
        
        detector_default = DriftDetector()
        assert detector_default.threshold == 0.05

    def test_rating_distribution_drift_no_drift(self):
        """Test rating distribution drift detection with no drift"""
        detector = DriftDetector(threshold=0.05)
        
        np.random.seed(42)
        historical = pd.DataFrame({
            'user_id': np.random.randint(1, 1000, 1000),
            'item_id': np.random.randint(1, 500, 1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000, p=[0.1, 0.2, 0.3, 0.3, 0.1])
        })
        
        current = pd.DataFrame({
            'user_id': np.random.randint(1000, 2000, 1000),
            'item_id': np.random.randint(500, 1000, 1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000, p=[0.1, 0.2, 0.3, 0.3, 0.1])
        })
        
        result = detector.detect_rating_distribution_drift(current, historical)
        assert result['drift_detected'] == False
        assert 'p_value' in result
        assert 'current_distribution' in result
        assert 'historical_distribution' in result

    def test_rating_distribution_drift_with_drift(self):
        """Test rating distribution drift detection with actual drift"""
        detector = DriftDetector(threshold=0.05)
        
        np.random.seed(42)
        historical = pd.DataFrame({
            'user_id': np.random.randint(1, 1000, 1000),
            'item_id': np.random.randint(1, 500, 1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000, p=[0.1, 0.2, 0.3, 0.3, 0.1])
        })
        
        
        current = pd.DataFrame({
            'user_id': np.random.randint(1000, 2000, 1000),
            'item_id': np.random.randint(500, 1000, 1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000, p=[0.3, 0.3, 0.2, 0.1, 0.1])  
        })
        
        result = detector.detect_rating_distribution_drift(current, historical)
        
        assert 'drift_detected' in result
        assert 'p_value' in result

    def test_user_activity_drift(self):
        """Test user activity drift detection"""
        detector = DriftDetector(threshold=0.05)
        
        np.random.seed(42)
        historical = pd.DataFrame({
            'user_id': np.repeat(range(1, 101), 10),  
            'item_id': range(1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000)
        })
        
        current = pd.DataFrame({
            'user_id': np.repeat(range(101, 301), 5),   
            'item_id': range(1000),
            'rating': np.random.choice([1, 2, 3, 4, 5], 1000)
        })
        
        result = detector.detect_user_activity_drift(current, historical)
        assert 'drift_detected' in result
        assert 'p_value' in result
        assert 'current_avg_activity' in result
        assert 'historical_avg_activity' in result

    def test_comprehensive_drift_check(self):
        """Test comprehensive drift analysis"""
        detector = DriftDetector(threshold=0.05)
        
        np.random.seed(42)
        historical = pd.DataFrame({
            'user_id': np.random.randint(1, 500, 1000),
            'item_id': np.random.randint(1, 500, 1000),
            'rating': np.ones(1000) 
        })
        
        current = pd.DataFrame({
            'user_id': np.random.randint(501, 1000, 1000),
            'item_id': np.random.randint(501, 1000, 1000),
            'rating': np.full(1000, 5)  
        })
        
        result = detector.comprehensive_drift_check(current, historical)
        
        assert 'rating_distribution_drift' in result
        assert 'user_activity_drift' in result  
        assert 'overall_drift_detected' in result
        
        assert result['rating_distribution_drift']['drift_detected'] == True