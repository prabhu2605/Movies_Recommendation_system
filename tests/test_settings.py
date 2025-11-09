import pytest
import os
import sys
from unittest.mock import patch

sys.path.append('src')

from pipeline.config.settings import PipelineConfig

class TestPipelineConfig:
    def test_default_initialization(self):
        """Test pipeline config with default values"""
        config = PipelineConfig()
        
        assert config.KAFKA_BOOTSTRAP_SERVERS == ''
        assert config.KAFKA_API_KEY == ''
        assert config.KAFKA_API_SECRET == ''
        assert config.TEAM_NAME == 'AIMLGROUP'
        assert config.OFFLINE_EVAL_SPLIT_RATIO == 0.8
        assert config.ONLINE_EVAL_WINDOW_MINUTES == 30
        assert config.DRIFT_DETECTION_THRESHOLD == 0.05
        assert config.MAX_QUEUE_SIZE == 1000

    def test_environment_variables(self):
        """Test pipeline config with environment variables"""
       
        with patch.dict(os.environ, {}, clear=True):
            with patch.dict(os.environ, {
                'KAFKA_BOOTSTRAP_SERVERS': 'test-server',
                'KAFKA_API_KEY': 'test-key',
                'KAFKA_API_SECRET': 'test-secret',
                'TEAM_NAME': 'TEST_TEAM',
                'OFFLINE_EVAL_RATIO': '0.7',
                'ONLINE_EVAL_WINDOW': '60',
                'DRIFT_THRESHOLD': '0.01',
                'MAX_QUEUE_SIZE': '500'
            }):
                config = PipelineConfig()
                
                assert config.KAFKA_BOOTSTRAP_SERVERS == 'test-server'
                assert config.KAFKA_API_KEY == 'test-key'
                assert config.KAFKA_API_SECRET == 'test-secret'
                assert config.TEAM_NAME == 'TEST_TEAM'
                assert config.OFFLINE_EVAL_SPLIT_RATIO == 0.7
                assert config.ONLINE_EVAL_WINDOW_MINUTES == 60
                assert config.DRIFT_DETECTION_THRESHOLD == 0.01
                assert config.MAX_QUEUE_SIZE == 500

    def test_validate_missing_required_vars(self):
        """Test validation with missing required environment variables"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                PipelineConfig.validate()
            
            assert "Missing required environment variables" in str(exc_info.value)

    def test_validate_with_all_vars(self):
        """Test validation with all required environment variables"""
        with patch.dict(os.environ, {}, clear=True):
            with patch.dict(os.environ, {
                'KAFKA_BOOTSTRAP_SERVERS': 'test-server',
                'KAFKA_API_KEY': 'test-key',
                'KAFKA_API_SECRET': 'test-secret',
                'AWS_ACCESS_KEY_ID': 'test-aws-key',
                'AWS_SECRET_ACCESS_KEY': 'test-aws-secret',
                'S3_BUCKET': 'test-bucket'
            }):
                config = PipelineConfig.validate()
                
                assert config.KAFKA_BOOTSTRAP_SERVERS == 'test-server'
                assert config.KAFKA_API_KEY == 'test-key'
                assert config.KAFKA_API_SECRET == 'test-secret'