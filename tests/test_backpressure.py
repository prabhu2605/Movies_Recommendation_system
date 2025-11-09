import pytest
import sys
import os

sys.path.append('src')

from pipeline.quality.backpressure import BackpressureHandler

class TestBackpressureHandler:
    def test_initialization(self):
        """Test backpressure handler initialization"""
        handler = BackpressureHandler(max_queue_size=1000)
        assert handler.max_queue_size == 1000
        assert handler.backpressure_applied == False
        
       
        handler_default = BackpressureHandler()
        assert handler_default.max_queue_size == 1000

    def test_should_apply_backpressure_below_threshold(self):
        """Test backpressure when below threshold"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        assert handler.should_apply_backpressure(50) == False
        assert handler.backpressure_applied == False

    def test_should_apply_backpressure_at_threshold(self):
        """Test backpressure at threshold"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        assert handler.should_apply_backpressure(100) == True
        assert handler.backpressure_applied == True

    def test_should_apply_backpressure_above_threshold(self):
        """Test backpressure above threshold"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        assert handler.should_apply_backpressure(150) == True
        assert handler.backpressure_applied == True

    def test_should_apply_backpressure_back_below_threshold(self):
        """Test backpressure removal when back below threshold"""
        handler = BackpressureHandler(max_queue_size=100)
        
     
        handler.should_apply_backpressure(100)
        assert handler.backpressure_applied == True
        
        
        assert handler.should_apply_backpressure(50) == False
        assert handler.backpressure_applied == False

    def test_handle_overload_normal_load(self):
        """Test overload handling with normal load"""
        handler = BackpressureHandler(max_queue_size=100)
        
       
        events = list(range(10))
        processed = handler.handle_overload(events)
        assert processed == events
        assert len(processed) == 10

    def test_handle_overload_heavy_load(self):
        """Test overload handling with heavy load"""
        handler = BackpressureHandler(max_queue_size=100)
        
        
        events = list(range(60))  
        processed = handler.handle_overload(events)
        assert len(processed) == 30  
        assert processed == events[::2]  

    def test_handle_overload_light_load(self):
        """Test overload handling with light load"""
        handler = BackpressureHandler(max_queue_size=100)
        
       
        events = list(range(40))  
        processed = handler.handle_overload(events)
        assert processed == events
        assert len(processed) == 40