import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json

sys.path.append('src')

from pipeline.evaluate.online_eval import OnlineEvaluator

class TestOnlineEvalComprehensive:
    def test_calculate_success_rate_integration(self):
        """Test success rate calculation with mock data"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config, time_window_minutes=30)
        
      
        mock_reco_events = [
            {
                'user_id': 1,
                'recommendations': [101, 102, 103],
                'timestamp': '2024-01-01T00:00:00',
                'model_used': 'personalized'
            },
            {
                'user_id': 2,
                'recommendations': [104, 105, 106],
                'timestamp': '2024-01-01T00:01:00',
                'model_used': 'popularity'
            }
        ]
        
        mock_watch_events = [
            {
                'user_id': 1,
                'item_id': 101,  
                'timestamp': '2024-01-01T00:05:00',
                'event_type': 'start'
            },
            {
                'user_id': 2,
                'item_id': 201,  
                'timestamp': '2024-01-01T00:06:00',
                'event_type': 'start'
            }
        ]
        
        with patch.object(evaluator, 'consume_kafka_events') as mock_consume:
            mock_consume.side_effect = [mock_reco_events, mock_watch_events]
            
            result = evaluator.calculate_success_rate()
            
            assert 'success_rate' in result
            assert 'success_count' in result
            assert 'total_recommendations' in result
            assert result['total_recommendations'] == 2
           
            assert result['success_count'] == 1
            assert result['success_rate'] == 0.5

    def test_calculate_personalization_rate_edge_cases(self):
        """Test personalization rate with various edge cases"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config)
        
       
        mock_events_popularity = [
            {'user_id': 1, 'model_used': 'popularity'},
            {'user_id': 2, 'model_used': 'popularity'},
            {'user_id': 3, 'model_used': 'popularity'}
        ]
        
        with patch.object(evaluator, 'consume_kafka_events', return_value=mock_events_popularity):
            rate = evaluator.calculate_personalization_rate()
            assert rate == 0.0  
        
        
        mock_events_personalized = [
            {'user_id': 1, 'model_used': 'collaborative'},
            {'user_id': 2, 'model_used': 'content-based'},
            {'user_id': 3, 'model_used': 'hybrid'}
        ]
        
        with patch.object(evaluator, 'consume_kafka_events', return_value=mock_events_personalized):
            rate = evaluator.calculate_personalization_rate()
            assert rate == 1.0  

    def test_time_window_functionality(self):
        """Test different time window configurations"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        
        evaluator_15min = OnlineEvaluator(mock_config, time_window_minutes=15)
        assert evaluator_15min.time_window.total_seconds() == 15 * 60
        
        
        evaluator_60min = OnlineEvaluator(mock_config, time_window_minutes=60)
        assert evaluator_60min.time_window.total_seconds() == 60 * 60
        
      
        evaluator_default = OnlineEvaluator(mock_config)
        assert evaluator_default.time_window.total_seconds() == 30 * 60