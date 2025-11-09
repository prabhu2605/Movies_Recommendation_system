import pytest
import pandas as pd
import sys
import os

sys.path.append('src')

from pipeline.quality.schemas import watch_schema, rating_schema, validate_watch_schema, validate_rating_schema

def test_watch_schema_validation():
   
    valid_data = {
        'user_id': 1,
        'item_id': 101,
        'timestamp': 1640995200,
        'event_type': 'start'
    }
    
    assert validate_watch_schema(valid_data) == True

    invalid_data = {
        'user_id': 0,  
        'item_id': 101,
        'timestamp': 1640995200,
        'event_type': 'start'
    }
    
    assert validate_watch_schema(invalid_data) == False

def test_rating_schema_validation():

    valid_data = {
        'user_id': 1,
        'item_id': 101,
        'rating': 5,
        'timestamp': 1640995200
    }
    
    assert validate_rating_schema(valid_data) == True
 
    invalid_data = {
        'user_id': 1,
        'item_id': 101,
        'rating': 6,  
        'timestamp': 1640995200
    }
    
    assert validate_rating_schema(invalid_data) == False

def test_dataframe_schemas():
    valid_watch_df = pd.DataFrame({
        'user_id': [1, 2, 3],
        'item_id': [101, 102, 103],
        'timestamp': [1640995200, 1641081600, 1641168000],
        'event_type': ['start', 'stop', 'complete']
    })

    watch_schema.validate(valid_watch_df)