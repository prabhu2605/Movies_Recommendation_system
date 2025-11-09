import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class PipelineConfig:

    KAFKA_BOOTSTRAP_SERVERS: str = None
    KAFKA_API_KEY: str = None
    KAFKA_API_SECRET: str = None
    TEAM_NAME: str = None

    AWS_ACCESS_KEY_ID: str = None
    AWS_SECRET_ACCESS_KEY: str = None
    S3_BUCKET: str = None
    S3_ENDPOINT_URL: str = None
    
    MODEL_REGISTRY_PATH: str = None
    OFFLINE_EVAL_SPLIT_RATIO: float = None
    ONLINE_EVAL_WINDOW_MINUTES: int = None
    
 
    DRIFT_DETECTION_THRESHOLD: float = None
    MAX_QUEUE_SIZE: int = None
    
    def __post_init__(self):
        
        self.KAFKA_BOOTSTRAP_SERVERS = self.KAFKA_BOOTSTRAP_SERVERS or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')
        self.KAFKA_API_KEY = self.KAFKA_API_KEY or os.getenv('KAFKA_API_KEY', '')
        self.KAFKA_API_SECRET = self.KAFKA_API_SECRET or os.getenv('KAFKA_API_SECRET', '')
        self.TEAM_NAME = self.TEAM_NAME or os.getenv('TEAM_NAME', 'AIMLGROUP')
        
        self.AWS_ACCESS_KEY_ID = self.AWS_ACCESS_KEY_ID or os.getenv('AWS_ACCESS_KEY_ID', '')
        self.AWS_SECRET_ACCESS_KEY = self.AWS_SECRET_ACCESS_KEY or os.getenv('AWS_SECRET_ACCESS_KEY', '')
        self.S3_BUCKET = self.S3_BUCKET or os.getenv('S3_BUCKET', 'movierecommendationsystem')
        self.S3_ENDPOINT_URL = self.S3_ENDPOINT_URL or os.getenv('S3_ENDPOINT_URL', 'https://s3.amazonaws.com')
        
        self.MODEL_REGISTRY_PATH = self.MODEL_REGISTRY_PATH or os.getenv('MODEL_REGISTRY_PATH', 'models/')
        self.OFFLINE_EVAL_SPLIT_RATIO = self.OFFLINE_EVAL_SPLIT_RATIO or float(os.getenv('OFFLINE_EVAL_RATIO', '0.8'))
        self.ONLINE_EVAL_WINDOW_MINUTES = self.ONLINE_EVAL_WINDOW_MINUTES or int(os.getenv('ONLINE_EVAL_WINDOW', '30'))
        
        self.DRIFT_DETECTION_THRESHOLD = self.DRIFT_DETECTION_THRESHOLD or float(os.getenv('DRIFT_THRESHOLD', '0.05'))
        self.MAX_QUEUE_SIZE = self.MAX_QUEUE_SIZE or int(os.getenv('MAX_QUEUE_SIZE', '1000'))
    
    @classmethod
    def validate(cls):
        required_vars = [
            'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_API_KEY', 'KAFKA_API_SECRET',
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET'
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        
        return cls()