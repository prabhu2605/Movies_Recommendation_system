import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import logging
import time
from sklearn.model_selection import train_test_split
from sklearn.metrics import ndcg_score
import pickle
import os
from service.models import PopularityModel, ItemItemCFModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelTrainer:
    def __init__(self, data_path: str = None):
        self.data_path = data_path
        self.ratings_data = None
        self.train_data = None
        self.test_data = None
        
    def load_data(self):
        if self.data_path and os.path.exists(self.data_path):
            logger.info(f"Loading data from {self.data_path}")
            self.ratings_data = pd.read_csv(self.data_path)
        else:
            logger.info("No data path provided, generating sample data")
            self.ratings_data = self._generate_sample_data()

        self.train_data, self.test_data = train_test_split(
            self.ratings_data, test_size=0.2, random_state=42
        )
        
        logger.info(f"Data loaded: {len(self.ratings_data)} total ratings")
        logger.info(f"Train set: {len(self.train_data)}, Test set: {len(self.test_data)}")
    
    def train_models(self):
        models = {}
        training_times = {}

        logger.info("Training Popularity Model")
        start_time = time.time()
        pop_model = PopularityModel()
        pop_model.train(self.train_data)
        training_times['popularity'] = time.time() - start_time
        models['popularity'] = pop_model

        logger.info("Training Item-Item Collaborative Filtering Model")
        start_time = time.time()
        cf_model = ItemItemCFModel()
        cf_model.train(self.train_data)
        training_times['item_item_cf'] = time.time() - start_time
        models['item_item_cf'] = cf_model
        
        return models, training_times
    
    def evaluate_models(self, models: dict, test_data: pd.DataFrame):
        results = {}
        
        for name, model in models.items():
            logger.info(f"Evaluating {name}")
            
            sample_users = test_data['userId'].unique()[:100]
            
            actual = []
            predicted = []
            
            for user_id in sample_users:
                user_ratings = test_data[test_data['userId'] == user_id]
                true_items = user_ratings.nlargest(20, 'rating')['itemId'].tolist()
                
                try:
                    pred_items = model.recommend(user_id, k=20)
                   
                    true_vector = [1 if item in true_items else 0 for item in pred_items]
                    pred_vector = list(range(len(pred_items), 0, -1)) 
                    
                    actual.append(true_vector)
                    predicted.append(pred_vector)
                except Exception as e:
                    logger.warning(f"Could not get recommendations for user {user_id}: {e}")
                    continue
            
            if actual and predicted:
        
                ndcg = ndcg_score(actual, predicted)
                results[name] = {
                    'ndcg': ndcg,
                    'users_evaluated': len(actual)
                }
            else:
                results[name] = {
                    'ndcg': 0.0,
                    'users_evaluated': 0
                }
        
        return results
    
    def _generate_sample_data(self) -> pd.DataFrame:
        np.random.seed(42)
        n_users = 1000
        n_items = 1700 
        n_ratings = 100000
        
        data = {
            'userId': np.random.randint(1, n_users + 1, n_ratings),
            'itemId': np.random.randint(1, n_items + 1, n_ratings),
            'rating': np.random.choice([1, 2, 3, 4, 5], n_ratings, p=[0.1, 0.2, 0.3, 0.3, 0.1]),
            'timestamp': np.random.randint(1609459200, 1640995200, n_ratings)
        }
        
        return pd.DataFrame(data)
    
    def save_models(self, models: dict, output_dir: str = "models"):
        os.makedirs(output_dir, exist_ok=True)
        
        for name, model in models.items():
            model_path = os.path.join(output_dir, f"{name}_model.pkl")
            model.save(model_path)
            logger.info(f"Saved {name} model to {model_path}")

def main():
    trainer = ModelTrainer()
    trainer.load_data()

    models, training_times = trainer.train_models()

    evaluation_results = trainer.evaluate_models(models, trainer.test_data)

    print("\n=== Model Training Results ===")
    for name in models.keys():
        print(f"\n{name.upper()}:")
        print(f"  Training Time: {training_times[name]:.2f}s")
        print(f"  NDCG@20: {evaluation_results[name]['ndcg']:.4f}")
        print(f"  Users Evaluated: {evaluation_results[name]['users_evaluated']}")

    trainer.save_models(models)

if __name__ == "__main__":
    main()