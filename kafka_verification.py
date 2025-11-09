# kafka_verification_fixed.py
from confluent_kafka import Consumer, KafkaError
import os
import json
from dotenv import load_dotenv

load_dotenv()

def verify_kafka_topics():
    print(" Verifying Kafka Topics and Messages...")
    
    config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
        'group.id': 'verification-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(config)
    
   
    team_name = os.getenv('TEAM_NAME', 'AIMLGROUP')
    topics = [
        f'{team_name}.RECO_REQUESTS', 
        f'{team_name}.RECO_RESPONSES',
        f'{team_name}.WATCH',
        f'{team_name}.RATE'
    ]
    
    for topic in topics:
        print(f"\n Checking topic: {topic}")
        try:
            consumer.subscribe([topic])
            
            messages_received = 0
            for i in range(10):
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"    Error: {msg.error()}")
                        break
                
                messages_received += 1
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    print(f"    Message {messages_received}:")
                    print(f"      User: {message_data.get('user_id', 'N/A')}")
                    print(f"      Model: {message_data.get('model_used', message_data.get('model_requested', 'N/A'))}")
                    print(f"      Timestamp: {message_data.get('timestamp', 'N/A')}")
                except:
                    print(f"    Message {messages_received}: {len(msg.value())} bytes")
                
            if messages_received > 0:
                print(f"    Topic {topic} is ACTIVE with {messages_received} messages")
            else:
                print(f"    Topic {topic} exists but no messages found")
                
        except Exception as e:
            print(f"   Failed to access topic {topic}: {e}")
    
    consumer.close()

def list_topics():
    print("\n Listing ALL available topics...")
    
    config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
    }
    
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(config)
    
    try:
        metadata = admin.list_topics(timeout=10)
        topics = metadata.topics
        
        print("All topics in cluster:")
        for topic_name, topic in topics.items():
            print(f"  {topic_name} (partitions: {len(topic.partitions)})")
            
    except Exception as e:
        print(f" Failed to list topics: {e}")

if __name__ == "__main__":
    list_topics()
    verify_kafka_topics()
    print("\n Kafka verification complete!")