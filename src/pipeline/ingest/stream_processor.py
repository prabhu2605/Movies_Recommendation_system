import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import pandas as pd
from collections import deque

from pipeline.config.settings import PipelineConfig
from pipeline.quality.schemas import validate_watch_schema, validate_rating_schema
from pipeline.quality.backpressure import BackpressureHandler

logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.consumer = None
        self.backpressure_handler = BackpressureHandler(config.MAX_QUEUE_SIZE)
        self.setup_consumer()
    
    def setup_consumer(self):
        
        self.consumer = Consumer({
            'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.config.KAFKA_API_KEY,
            'sasl.password': self.config.KAFKA_API_SECRET,
            'group.id': f"{self.config.TEAM_NAME}-ingestor",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        topics = [
            f"{self.config.TEAM_NAME}.WATCH",
            f"{self.config.TEAM_NAME}.RATE",
            f"{self.config.TEAM_NAME}.RECO_REQUESTS",
            f"{self.config.TEAM_NAME}.RECO_RESPONSES"
        ]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
    
    def process_messages(self):
      
        watch_data = []
        rate_data = []
        batch_size = 100
        batch_timeout = 30
        
        last_flush = datetime.now()
        
        logger.info("Starting modular stream processing")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:

                    if (datetime.now() - last_flush).seconds > batch_timeout:
                        self.flush_batches(watch_data, rate_data)
                        watch_data.clear()
                        rate_data.clear()
                        last_flush = datetime.now()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                if self.backpressure_handler.should_apply_backpressure(len(watch_data) + len(rate_data)):
                    logger.warning("Applying backpressure - sampling incoming events")
                    continue
                
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                 
                    if 'WATCH' in topic:
                        if validate_watch_schema(message_data):
                            watch_data.append(message_data)
                    elif 'RATE' in topic:
                        if validate_rating_schema(message_data):
                            rate_data.append(message_data)
                    elif 'RECO' in topic:
        
                        logger.debug(f"Recommendation event: {message_data}")
                    
        
                    self.consumer.commit(asynchronous=False)
                    
                    if len(watch_data) + len(rate_data) >= batch_size:
                        self.flush_batches(watch_data, rate_data)
                        watch_data.clear()
                        rate_data.clear()
                        last_flush = datetime.now()
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down stream processor")
        finally:
            if watch_data or rate_data:
                self.flush_batches(watch_data, rate_data)
            self.consumer.close()
    
    def flush_batches(self, watch_data: list, rate_data: list):
        logger.info(f"Flushing {len(watch_data)} watch events, {len(rate_data)} rate events")
    