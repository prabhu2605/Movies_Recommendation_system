import json
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)

class OnlineEvaluator:
    
    def __init__(self, config, time_window_minutes: int = 30):
        self.config = config
        self.time_window = timedelta(minutes=time_window_minutes)
    
    def consume_kafka_events(self, topics: List[str], limit: int = 100) -> List[Dict]:
        consumer = Consumer({
            'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.config.KAFKA_API_KEY,
            'sasl.password': self.config.KAFKA_API_SECRET,
            'group.id': f"{self.config.TEAM_NAME}-online-eval",
            'auto.offset.reset': 'earliest'
        })
        
        consumer.subscribe(topics)
        events = []
        
        try:
            while len(events) < limit:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                try:
                    event_data = json.loads(msg.value().decode('utf-8'))
                    events.append(event_data)
                except json.JSONDecodeError:
                    continue
        
        finally:
            consumer.close()
        
        return events
    
    def calculate_success_rate(self) -> Dict:

        reco_events = self.consume_kafka_events([f"{self.config.TEAM_NAME}.RECO_RESPONSES"])

        watch_events = self.consume_kafka_events([f"{self.config.TEAM_NAME}.WATCH"])
        
        success_count = 0
        total_recommendations = len(reco_events)
        
        for reco in reco_events:
            user_id = reco.get('user_id')
            recommended_items = reco.get('recommendations', [])
            reco_time = datetime.fromisoformat(reco.get('timestamp', datetime.now().isoformat()))
            
            if not user_id or not recommended_items:
                continue
            
            user_watches = [
                w for w in watch_events 
                if w.get('user_id') == user_id
                and datetime.fromisoformat(w.get('timestamp', datetime.now().isoformat())) - reco_time <= self.time_window
            ]
            
            watched_items = {w.get('item_id') for w in user_watches}
            
            if any(item in watched_items for item in recommended_items):
                success_count += 1
        
        success_rate = success_count / total_recommendations if total_recommendations > 0 else 0
        
        return {
            'success_rate': success_rate,
            'success_count': success_count,
            'total_recommendations': total_recommendations,
            'time_window_minutes': self.time_window.total_seconds() / 60
        }
    
    def calculate_personalization_rate(self) -> float:

        reco_events = self.consume_kafka_events([f"{self.config.TEAM_NAME}.RECO_RESPONSES"])
        
        if not reco_events:
            return 0.0
        
        personalized_count = sum(
            1 for event in reco_events 
            if event.get('model_used') != 'popularity'
        )
        
        return personalized_count / len(reco_events)