import pytest
import sys
import os

sys.path.append('src')

from pipeline.quality.backpressure import BackpressureHandler

class TestBackpressureComprehensive:
    def test_backpressure_with_different_queue_sizes(self):
        """Test backpressure with various queue size configurations"""
       
        small_handler = BackpressureHandler(max_queue_size=10)
        assert small_handler.max_queue_size == 10
        assert small_handler.should_apply_backpressure(5) == False
        assert small_handler.should_apply_backpressure(10) == True
        
       
        large_handler = BackpressureHandler(max_queue_size=10000)
        assert large_handler.max_queue_size == 10000
        assert large_handler.should_apply_backpressure(5000) == False
        assert large_handler.should_apply_backpressure(10000) == True
        
       
        default_handler = BackpressureHandler()
        assert default_handler.max_queue_size == 1000

    def test_handle_overload_various_scenarios(self):
        """Test overload handling in various scenarios"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        events = list(range(50))  
        processed = handler.handle_overload(events)
        assert processed == events 
        assert len(processed) == 50
        
       
        events = list(range(51)) 
        processed = handler.handle_overload(events)
        
        assert len(processed) in [25, 26]  
        assert len(processed) <= len(events) // 2 + 1 
        
        events = list(range(200))  
        processed = handler.handle_overload(events)
        assert len(processed) == 100  

    def test_backpressure_state_management(self):
        """Test backpressure state transitions"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        assert handler.backpressure_applied == False
        
        
        handler.should_apply_backpressure(100)
        assert handler.backpressure_applied == True
        
     
        handler.should_apply_backpressure(50)
        assert handler.backpressure_applied == False
        
        
        handler.should_apply_backpressure(150)
        assert handler.backpressure_applied == True