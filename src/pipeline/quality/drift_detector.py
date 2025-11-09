import pandas as pd
import numpy as np
from scipy import stats
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class DriftDetector:
    def __init__(self, threshold: float = 0.05):
        self.threshold = threshold
    
    def detect_rating_distribution_drift(self, current_data: pd.DataFrame, historical_data: pd.DataFrame) -> Dict:

        current_ratings = current_data['rating'].value_counts().sort_index()
        historical_ratings = historical_data['rating'].value_counts().sort_index()

        all_ratings = set(current_ratings.index) | set(historical_ratings.index)
        current_aligned = current_ratings.reindex(all_ratings, fill_value=0)
        historical_aligned = historical_ratings.reindex(all_ratings, fill_value=0)

        chi2, p_value = stats.chisquare(current_aligned, historical_aligned)
        
        return {
            'drift_detected': p_value < self.threshold,
            'p_value': p_value,
            'current_distribution': current_ratings.to_dict(),
            'historical_distribution': historical_ratings.to_dict()
        }
    
    def detect_user_activity_drift(self, current_data: pd.DataFrame, historical_data: pd.DataFrame) -> Dict:
        current_user_activity = current_data.groupby('user_id').size()
        historical_user_activity = historical_data.groupby('user_id').size()

        ks_stat, p_value = stats.ks_2samp(current_user_activity, historical_user_activity)
        
        return {
            'drift_detected': p_value < self.threshold,
            'p_value': p_value,
            'current_avg_activity': current_user_activity.mean(),
            'historical_avg_activity': historical_user_activity.mean()
        }
    
    def comprehensive_drift_check(self, current_data: pd.DataFrame, historical_data: pd.DataFrame) -> Dict:
      
        rating_drift = self.detect_rating_distribution_drift(current_data, historical_data)
        activity_drift = self.detect_user_activity_drift(current_data, historical_data)
        
        return {
            'rating_distribution_drift': rating_drift,
            'user_activity_drift': activity_drift,
            'overall_drift_detected': rating_drift['drift_detected'] or activity_drift['drift_detected']
        }