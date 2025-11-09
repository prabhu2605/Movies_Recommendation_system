import pytest
import pandas as pd
import sys
import os

sys.path.append('src')

from pipeline.quality.schemas import validate_watch_schema, validate_rating_schema, watch_schema, rating_schema

class TestSchemasComprehensive:
    def test_watch_schema_comprehensive_validation(self):
        """Test comprehensive watch schema validation"""
        
        valid_watches = [
            {
                'user_id': 1,
                'item_id': 101,
                'timestamp': 1640995200,
                'event_type': 'start'
            },
            {
                'user_id': 999999,
                'item_id': 999999,
                'timestamp': 1640995201,
                'event_type': 'stop'
            },
            {
                'user_id': 123,
                'item_id': 456,
                'timestamp': 1640995202,
                'event_type': 'complete'
            }
        ]
        
        for watch_data in valid_watches:
            assert validate_watch_schema(watch_data) == True
        
      
        invalid_watches = [
            {
                'user_id': 0,  
                'item_id': 101,
                'timestamp': 1640995200,
                'event_type': 'start'
            },
            {
                'user_id': 123,
                'item_id': 0,  
                'timestamp': 1640995200,
                'event_type': 'start'
            },
            {
                'user_id': 123,
                'item_id': 101,
                'timestamp': 1000,  
                'event_type': 'start'
            },
            {
                'user_id': 123,
                'item_id': 101,
                'timestamp': 1640995200,
                'event_type': 'invalid' 
            }
        ]
        
        for watch_data in invalid_watches:
            assert validate_watch_schema(watch_data) == False

    def test_rating_schema_comprehensive_validation(self):
        """Test comprehensive rating schema validation"""
       
        valid_ratings = [
            {
                'user_id': 1,
                'item_id': 101,
                'rating': 1,
                'timestamp': 1640995200
            },
            {
                'user_id': 999999,
                'item_id': 999999,
                'rating': 5,
                'timestamp': 1640995201
            },
            {
                'user_id': 123,
                'item_id': 456,
                'rating': 3,
                'timestamp': 1640995202
            }
        ]
        
        for rating_data in valid_ratings:
            assert validate_rating_schema(rating_data) == True
        
       
        invalid_ratings = [
            {
                'user_id': 0,  
                'item_id': 101,
                'rating': 5,
                'timestamp': 1640995200
            },
            {
                'user_id': 123,
                'item_id': 101,
                'rating': 0,  
                'timestamp': 1640995200
            },
            {
                'user_id': 123,
                'item_id': 101,
                'rating': 6,  
                'timestamp': 1640995200
            },
            {
                'user_id': 123,
                'item_id': 101,
                'rating': 5,
                'timestamp': 1000  
            }
        ]
        
        for rating_data in invalid_ratings:
            assert validate_rating_schema(rating_data) == False

    def test_dataframe_schemas_comprehensive(self):
        """Test DataFrame schema validation comprehensively"""
        
        valid_watch_df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'item_id': [101, 102, 103, 104, 105],
            'timestamp': [1640995200, 1641081600, 1641168000, 1641254400, 1641340800],
            'event_type': ['start', 'stop', 'complete', 'start', 'stop']
        })
        
       
        watch_schema.validate(valid_watch_df)
        
        
        valid_rating_df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'item_id': [101, 102, 103, 104, 105],
            'rating': [5, 4, 3, 2, 1],
            'timestamp': [1640995200, 1641081600, 1641168000, 1641254400, 1641340800]
        })
        
        
        rating_schema.validate(valid_rating_df)