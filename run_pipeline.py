#!/usr/bin/env python3
"""
Main pipeline runner for HW3 - Handles missing environment variables
"""

import sys
import os
sys.path.append('src')

try:
    from pipeline.config.settings import PipelineConfig
    from pipeline.evaluate.offline_eval import OfflineEvaluator
    from pipeline.evaluate.online_eval import OnlineEvaluator
    from pipeline.quality.drift_detector import DriftDetector
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("💡 Make sure you've created all the required pipeline modules")
    sys.exit(1)

def main():
    print("🚀 Running HW3 Modular Pipeline")
    
    
    try:
        config = PipelineConfig.validate()
        print("✅ Configuration loaded successfully")
        full_pipeline = True
    except ValueError as e:
        print(f"⚠️  Configuration warning: {e}")
        print("💡 Using default configuration for testing")
        config = PipelineConfig()
        full_pipeline = False
    
    
    print("\n📊 Running Offline Evaluation...")
    np.random.seed(42)
    sample_data = pd.DataFrame({
        'userId': np.random.randint(1, 1000, 10000),
        'itemId': np.random.randint(1, 500, 10000),
        'rating': np.random.choice([1, 2, 3, 4, 5], 10000, p=[0.1, 0.2, 0.3, 0.3, 0.1]),
        'timestamp': np.random.randint(1609459200, 1640995200, 10000)
    })
    
    
    offline_evaluator = OfflineEvaluator()
    train_data, test_data = offline_evaluator.chronological_split(sample_data)
    
    print(f"   Training data: {len(train_data)} records")
    print(f"   Test data: {len(test_data)} records")
    
    
    print("\n🔄 Running Drift Detection...")
    drift_detector = DriftDetector()
    
    
    historical_data = sample_data.iloc[:8000]
    current_data = sample_data.iloc[8000:]
    
    drift_results = drift_detector.comprehensive_drift_check(current_data, historical_data)
    
    print(f"   Rating distribution drift: {drift_results['rating_distribution_drift']['drift_detected']}")
    print(f"   User activity drift: {drift_results['user_activity_drift']['drift_detected']}")
    
    if full_pipeline:
       
        print("\n🌐 Online Evaluation Setup...")
        online_evaluator = OnlineEvaluator(config)
        print("   Online evaluator ready - run with actual Kafka events")
    else:
        print("\n🌐 Online Evaluation: Requires environment variables")
        print("   Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET")
    
    print("\n🎉 HW3 Pipeline Components Verified!")
    print("   Next steps:")
    print("   1. Run complete test: python test_pipeline.py")
    print("   2. Run unit tests: pytest tests/ -v")
    print("   3. Check coverage: pytest --cov=src --cov-report=html")
    print("   4. Add environment variables for full pipeline")

if __name__ == "__main__":
    main()