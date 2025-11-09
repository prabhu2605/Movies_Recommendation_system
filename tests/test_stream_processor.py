import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import json

sys.path.append('src')

from pipeline.ingest.stream_processor import StreamProcessor
from pipeline.config.settings import PipelineConfig

class TestStreamProcessor:
    def test_initialization(self):
        """Test stream processor initialization"""
        mock_config = Mock(spec=PipelineConfig)
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        mock_config.MAX_QUEUE_SIZE = 1000
        
        processor = StreamProcessor(mock_config)
        
        assert processor.config == mock_config
        assert processor.backpressure_handler.max_queue_size == 1000

    @patch('pipeline.ingest.stream_processor.Consumer')
    def test_setup_consumer(self, mock_consumer):
        """Test Kafka consumer setup"""
        mock_config = Mock(spec=PipelineConfig)
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        mock_config.MAX_QUEUE_SIZE = 1000
        
        processor = StreamProcessor(mock_config)
        
        
        mock_consumer.assert_called_once()
        call_args = mock_consumer.call_args[0][0]
        assert call_args['bootstrap.servers'] == 'test-server'
        assert call_args['sasl.username'] == 'test-key'
        assert call_args['sasl.password'] == 'test-secret'

    def test_flush_batches(self):
        """Test batch flushing"""
        mock_config = Mock(spec=PipelineConfig)
        mock_config.KAFKA_BOOTSTRAP_SERVERS = 'test-server'
        mock_config.KAFKA_API_KEY = 'test-key'
        mock_config.KAFKA_API_SECRET = 'test-secret'
        mock_config.TEAM_NAME = 'test-team'
        mock_config.MAX_QUEUE_SIZE = 1000
        
        processor = StreamProcessor(mock_config)
        
       
        watch_data = [{'user_id': 1, 'item_id': 101, 'timestamp': 1640995200, 'event_type': 'start'}]
        rate_data = [{'user_id': 1, 'item_id': 101, 'rating': 5, 'timestamp': 1640995200}]
        
        
        processor.flush_batches(watch_data, rate_data)