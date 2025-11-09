from typing import Dict, Any

def validate_watch_schema(data: Dict[str, Any]) -> bool:
    required_fields = ['user_id', 'item_id', 'timestamp', 'event_type']
    
    for field in required_fields:
        if field not in data:
            return False

    if not isinstance(data['user_id'], int):
        return False
    if not isinstance(data['item_id'], int):
        return False
    if data['event_type'] not in ['start', 'stop', 'complete']:
        return False
    
    return True

def validate_rating_schema(data: Dict[str, Any]) -> bool:
    required_fields = ['user_id', 'item_id', 'rating', 'timestamp']
    
    for field in required_fields:
        if field not in data:
            return False

    if not isinstance(data['user_id'], int):
        return False
    if not isinstance(data['item_id'], int):
        return False
    if not (0 <= data['rating'] <= 5):
        return False
    
    return True