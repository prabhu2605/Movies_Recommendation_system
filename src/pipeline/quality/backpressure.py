import logging
from typing import Deque
from collections import deque

logger = logging.getLogger(__name__)

class BackpressureHandler:
    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self.processing_queue = deque(maxlen=max_queue_size)
        self.backpressure_applied = False
    
    def should_apply_backpressure(self, current_queue_size: int) -> bool:
        if current_queue_size >= self.max_queue_size:
            if not self.backpressure_applied:
                logger.warning(f"Applying backpressure - queue size {current_queue_size} >= {self.max_queue_size}")
                self.backpressure_applied = True
            return True
        else:
            self.backpressure_applied = False
            return False
    
    def handle_overload(self, incoming_events: list) -> list:
        if len(incoming_events) > self.max_queue_size // 2:
   
            sampled_events = incoming_events[::2]
            logger.warning(f"Sampling events due to overload: {len(sampled_events)}/{len(incoming_events)}")
            return sampled_events
        return incoming_events