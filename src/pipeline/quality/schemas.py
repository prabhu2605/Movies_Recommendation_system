import pandera as pa
from pandera import DataFrameSchema, Column, Check
import pandas as pd

watch_schema = DataFrameSchema({
    "user_id": Column(pa.Int, checks=Check.greater_than(0)),
    "item_id": Column(pa.Int, checks=Check.greater_than(0)),
    "timestamp": Column(pa.Int, checks=Check.greater_than(1609459200)),
    "event_type": Column(pa.String, checks=Check.isin(["start", "stop", "complete"]))
})

rating_schema = DataFrameSchema({
    "user_id": Column(pa.Int, checks=Check.greater_than(0)),
    "item_id": Column(pa.Int, checks=Check.greater_than(0)),
    "rating": Column(pa.Int, checks=Check.in_range(1, 5)),
    "timestamp": Column(pa.Int, checks=Check.greater_than(1609459200))
})

def validate_watch_schema(data: dict) -> bool:
    try:
        df = pd.DataFrame([data])
        watch_schema.validate(df)
        return True
    except:
        return False

def validate_rating_schema(data: dict) -> bool:
    try:
        df = pd.DataFrame([data])
        rating_schema.validate(df)
        return True
    except:
        return False