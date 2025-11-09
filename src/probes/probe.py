import requests
import json
import time
import os
import logging
from datetime import datetime
import random
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationProbe:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.kafka_producer = None
        self.setup_kafka()
    
    def setup_kafka(self):
        try:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
            api_key = os.getenv('KAFKA_API_KEY')
            api_secret = os.getenv('KAFKA_API_SECRET')
            
            if not all([bootstrap_servers, api_key, api_secret]):
                logger.warning("Kafka environment variables not set. Using console logging only.")
                return
            
            self.kafka_producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': api_key,
                'sasl.password': api_secret
            })
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            self.kafka_producer = None
    
    def send_kafka_event(self, topic_suffix: str, data: dict):
        if not self.kafka_producer:
        
            logger.info(f"[KAFKA] {topic_suffix}: {data}")
            return
        
        try:
            team_name = os.getenv('TEAM_NAME', 'AIMLGROUP')
            topic = f"{team_name}.{topic_suffix.upper()}"
            self.kafka_producer.produce(
                topic,
                json.dumps(data).encode('utf-8')
            )
            self.kafka_producer.poll(0)
            logger.debug(f"Sent event to Kafka topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to send Kafka event: {e}")
    
    def test_recommendation(self, user_id: int, model: str = "popularity"):
        start_time = time.time()
        
        try:
           
            response = requests.post(
                f"{self.api_url}/recommend/{user_id}",
                json={
                    "user_id": user_id,
                    "k": 20,
                    "model": model
                },
                timeout=10
            )
            
            latency_ms = (time.time() - start_time) * 1000

            self.send_kafka_event("reco_requests", {
                "user_id": user_id,
                "model_requested": model,
                "timestamp": datetime.now().isoformat(),
                "response_time_ms": latency_ms,
                "status_code": response.status_code
            })
            
            if response.status_code == 200:
                data = response.json()
                
                self.send_kafka_event("reco_responses", {
                    "user_id": user_id,
                    "model_used": data.get("model_used"),
                    "recommendations": data.get("recommendations", []),
                    "latency_ms": data.get("latency_ms"),
                    "timestamp": datetime.now().isoformat()
                })
                
                return {
                    "success": True,
                    "latency_ms": latency_ms,
                    "model_used": data.get("model_used"),
                    "recommendations_count": len(data.get("recommendations", [])),
                    "personalized": data.get("model_used") != "popularity"
                }
            else:
                return {
                    "success": False,
                    "latency_ms": latency_ms,
                    "error": f"HTTP {response.status_code}"
                }
                
        except requests.exceptions.RequestException as e:
            latency_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "latency_ms": latency_ms,
                "error": str(e)
            }
    
    def run_probes(self, num_probes: int = 10):
        results = {
            "total": 0,
            "successful": 0,
            "personalized": 0,
            "total_latency": 0,
            "models_used": {}
        }
        
        for i in range(num_probes):
           
            user_id = random.randint(1, 1000)
            model = random.choice(["popularity", "item_item_cf"])
            
            logger.info(f"Probe {i+1}/{num_probes}: User {user_id}, Model {model}")
            
            result = self.test_recommendation(user_id, model)
            
            results["total"] += 1
            if result["success"]:
                results["successful"] += 1
                results["total_latency"] += result["latency_ms"]
                
                model_used = result.get("model_used", "unknown")
                results["models_used"][model_used] = results["models_used"].get(model_used, 0) + 1
                
                if result.get("personalized", False):
                    results["personalized"] += 1
            
            time.sleep(1)

        if results["successful"] > 0:
            results["avg_latency_ms"] = results["total_latency"] / results["successful"]
            results["success_rate"] = results["successful"] / results["total"]
            results["personalization_rate"] = results["personalized"] / results["successful"]
        else:
            results["avg_latency_ms"] = 0
            results["success_rate"] = 0
            results["personalization_rate"] = 0
        
        return results

def main():
    api_url = os.getenv('API_URL', 'http://localhost:8000')
    num_probes = int(os.getenv('NUM_PROBES', '5'))
    
    probe = RecommendationProbe(api_url)
    
    logger.info(f"Starting probe tests to {api_url}")
    results = probe.run_probes(num_probes)

    print("\n=== Probe Results ===")
    print(f"Total probes: {results['total']}")
    print(f"Successful: {results['successful']}")
    print(f"Success rate: {results['success_rate']:.1%}")
    print(f"Personalized responses: {results['personalized']}")
    print(f"Personalization rate: {results['personalization_rate']:.1%}")
    print(f"Average latency: {results['avg_latency_ms']:.2f}ms")
    print(f"Models used: {results['models_used']}")

    if probe.kafka_producer:
        probe.kafka_producer.flush()

if __name__ == "__main__":
    main()