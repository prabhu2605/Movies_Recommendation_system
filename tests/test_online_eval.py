import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json

sys.path.append('src')

from pipeline.evaluate.online_eval import OnlineEvaluator

class TestOnlineEvaluator:
    def test_initialization(self):
        """Test online evaluator initialization"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key' 
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config, time_window_minutes=30)
        
        assert evaluator.time_window.total_seconds() == 30 * 60
        assert evaluator.config == mock_config

    def test_default_time_window(self):
        """Test online evaluator with default time window"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config)  
        assert evaluator.time_window.total_seconds() == 30 * 60  

    @patch('pipeline.evaluate.online_eval.Consumer')
    def test_consume_kafka_events(self, mock_consumer):
        """Test Kafka event consumption"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config)
        
        
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
       
        mock_msg1 = MagicMock()
        mock_msg1.error.return_value = None
        mock_msg1.value.return_value = json.dumps({
            'user_id': 123, 
            'recommendations': [1, 2, 3],
            'timestamp': '2024-01-01T00:00:00'
        }).encode('utf-8')
        
        mock_msg2 = MagicMock()
        mock_msg2.error.return_value = None
        mock_msg2.value.return_value = json.dumps({
            'user_id': 124,
            'recommendations': [4, 5, 6], 
            'timestamp': '2024-01-01T00:01:00'
        }).encode('utf-8')
        
        
        mock_consumer_instance.poll.side_effect = [mock_msg1, mock_msg2, None]
        
        events = evaluator.consume_kafka_events(['test-topic'], limit=2)
        
        assert len(events) == 2
        assert events[0]['user_id'] == 123
        assert events[1]['user_id'] == 124

    def test_calculate_personalization_rate_no_events(self):
        """Test personalization rate with no events"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config)
        
        with patch.object(evaluator, 'consume_kafka_events', return_value=[]):
            personalization_rate = evaluator.calculate_personalization_rate()
            assert personalization_rate == 0.0

    def test_calculate_personalization_rate_mixed_models(self):
        """Test personalization rate with mixed models"""
        mock_config = Mock()
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        
        evaluator = OnlineEvaluator(mock_config)
        
        mock_events = [
            {'user_id': 1, 'model_used': 'personalized', 'recommendations': [1, 2, 3]},
            {'user_id': 2, 'model_used': 'popularity', 'recommendations': [4, 5, 6]},
            {'user_id': 3, 'model_used': 'personalized', 'recommendations': [7, 8, 9]},
        ]
        
        with patch.object(evaluator, 'consume_kafka_events', return_value=mock_events):
            personalization_rate = evaluator.calculate_personalization_rate()
            assert personalization_rate == 2/3  