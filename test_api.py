import requests


print("=== Testing Health Endpoint ===")
health = requests.get("http://localhost:8000/healthz").json()
print(f"Status: {health['status']}")
print(f"Version: {health['version']}")
print(f"Models: {health['models_loaded']}")


print("\n=== Testing Models Endpoint ===")
models = requests.get("http://localhost:8000/models").json()
print(f"Available: {models['available_models']}")
print(f"Default: {models['default_model']}")


print("\n=== Testing Recommendation ===")
response = requests.post(
    "http://localhost:8000/recommend/123",
    json={"user_id": 123, "k": 5, "model": "popularity"}
)
data = response.json()
print(f"Recommendations: {data['recommendations']}")
print(f"Model Used: {data['model_used']}")
print(f"Latency: {data['latency_ms']}ms")