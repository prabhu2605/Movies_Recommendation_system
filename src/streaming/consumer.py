import os
import json
import logging
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import boto3
from .schemas import validate_watch_schema, validate_rating_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamIngestor:
    def __init__(self):
        self.consumer = None
        self.s3_client = None
        self.setup_consumer()
        self.setup_s3()
        
    def setup_consumer(self):
        self.consumer = Consumer({
            'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ['KAFKA_API_KEY'],
            'sasl.password': os.environ['KAFKA_API_SECRET'],
            'group.id': f"{os.environ.get('TEAM_NAME', 'default')}-ingestor",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        topics = [
            f"{os.environ.get('TEAM_NAME', 'default')}.watch",
            f"{os.environ.get('TEAM_NAME', 'default')}.rate"
        ]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
    
    def setup_s3(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            endpoint_url=os.environ.get('S3_ENDPOINT_URL') 
        )
        self.bucket_name = os.environ.get('S3_BUCKET', 'recommender-snapshots')
    
    def process_messages(self):
        watch_data = []
        rate_data = []
        batch_size = 100
        batch_timeout = 30 
        
        last_flush = datetime.now()
        
        logger.info("Starting message processing loop")
        
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
                
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    if 'watch' in topic:
                        if validate_watch_schema(message_data):
                            watch_data.append(message_data)
                            logger.debug(f"Processed watch event for user {message_data.get('user_id')}")
                    
                    elif 'rate' in topic:
                        if validate_rating_schema(message_data):
                            rate_data.append(message_data)
                            logger.debug(f"Processed rating event for user {message_data.get('user_id')}")

                    self.consumer.commit(async=False)
                    
        
                    if len(watch_data) + len(rate_data) >= batch_size:
                        self.flush_batches(watch_data, rate_data)
                        watch_data.clear()
                        rate_data.clear()
                        last_flush = datetime.now()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            if watch_data or rate_data:
                self.flush_batches(watch_data, rate_data)
            self.consumer.close()
    
    def flush_batches(self, watch_data: list, rate_data: list):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        date_prefix = datetime.now().strftime("%Y-%m-%d")
        
        try:
            if watch_data:
                df_watch = pd.DataFrame(watch_data)
                table_watch = pa.Table.from_pandas(df_watch)

                watch_filename = f"/tmp/watch_{timestamp}.parquet"
                pq.write_table(table_watch, watch_filename)
                
                s3_key = f"snapshots/{date_prefix}/watch_{timestamp}.parquet"
                self.s3_client.upload_file(watch_filename, self.bucket_name, s3_key)
                logger.info(f"Uploaded {len(watch_data)} watch events to {s3_key}")
            
            if rate_data:
                df_rate = pd.DataFrame(rate_data)
                table_rate = pa.Table.from_pandas(df_rate)
                
                rate_filename = f"/tmp/rate_{timestamp}.parquet"
                pq.write_table(table_rate, rate_filename)
                
                s3_key = f"snapshots/{date_prefix}/rate_{timestamp}.parquet"
                self.s3_client.upload_file(rate_filename, self.bucket_name, s3_key)
                logger.info(f"Uploaded {len(rate_data)} rating events to {s3_key}")
                
        except Exception as e:
            logger.error(f"Error flushing batches to S3: {e}")

def main():
    ingestor = StreamIngestor()
    ingestor.process_messages()

if __name__ == "__main__":
    main()