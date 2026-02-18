"""
Concurrency manager for TPC-C benchmark.
Handles concurrent query execution with different load patterns.
"""

import time
import logging
import threading
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

logger = logging.getLogger(__name__)


class LoadPattern(Enum):
    """Supported load patterns."""
    CONSTANT = "constant"
    RAMP_UP = "ramp-up"
    SPIKE = "spike"
    WAVE = "wave"


class ConcurrencyManager:
    """Manages concurrent execution of queries."""
    
    def __init__(self, concurrency: int = 50):
        """
        Initialize concurrency manager.
        
        Args:
            concurrency: Number of concurrent threads
        """
        self.concurrency = concurrency
        self.executor: ThreadPoolExecutor = None
        self.active_threads = 0
        self.lock = threading.Lock()
    
    def execute_concurrent(self, task: Callable, 
                          task_args: List[Any],
                          duration_seconds: int = 60,
                          load_pattern: LoadPattern = LoadPattern.CONSTANT) -> List[Dict[str, Any]]:
        """
        Execute tasks concurrently with specified load pattern.
        
        Args:
            task: Callable task to execute
            task_args: List of arguments for each task
            duration_seconds: Duration to run
            load_pattern: Load pattern to use
            
        Returns:
            List of task results
        """
        results = []
        start_time = time.time()
        
        if load_pattern == LoadPattern.CONSTANT:
            results = self._execute_constant_load(task, task_args, duration_seconds)
        elif load_pattern == LoadPattern.RAMP_UP:
            results = self._execute_ramp_up_load(task, task_args, duration_seconds)
        elif load_pattern == LoadPattern.SPIKE:
            results = self._execute_spike_load(task, task_args, duration_seconds)
        elif load_pattern == LoadPattern.WAVE:
            results = self._execute_wave_load(task, task_args, duration_seconds)
        
        elapsed = time.time() - start_time
        logger.info(f"Concurrent execution completed in {elapsed:.2f}s, "
                   f"executed {len(results)} tasks")
        
        return results
    
    def _execute_constant_load(self, task: Callable, task_args: List[Any],
                               duration_seconds: int) -> List[Dict[str, Any]]:
        """
        Execute with constant load.
        
        Args:
            task: Task to execute
            task_args: Task arguments
            duration_seconds: Duration to run
            
        Returns:
            List of results
        """
        results = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            task_index = 0
            
            while (time.time() - start_time) < duration_seconds:
                # Submit tasks up to concurrency limit
                while len(futures) < self.concurrency:
                    args = task_args[task_index % len(task_args)]
                    future = executor.submit(task, args)
                    futures.append(future)
                    task_index += 1
                
                # Collect completed futures
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                    futures.remove(future)
                
                time.sleep(0.01)  # Small sleep to prevent tight loop
            
            # Wait for remaining futures
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task error: {e}")
        
        return results
    
    def _execute_ramp_up_load(self, task: Callable, task_args: List[Any],
                             duration_seconds: int) -> List[Dict[str, Any]]:
        """
        Execute with ramp-up load pattern (gradually increasing concurrency).
        
        Args:
            task: Task to execute
            task_args: Task arguments
            duration_seconds: Duration to run
            
        Returns:
            List of results
        """
        results = []
        start_time = time.time()
        ramp_duration = duration_seconds * 0.3  # 30% of time for ramp-up
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            task_index = 0
            
            while (time.time() - start_time) < duration_seconds:
                elapsed = time.time() - start_time
                
                # Calculate current concurrency level
                if elapsed < ramp_duration:
                    current_concurrency = int(self.concurrency * (elapsed / ramp_duration))
                    current_concurrency = max(1, current_concurrency)
                else:
                    current_concurrency = self.concurrency
                
                # Submit tasks
                while len(futures) < current_concurrency:
                    args = task_args[task_index % len(task_args)]
                    future = executor.submit(task, args)
                    futures.append(future)
                    task_index += 1
                
                # Collect completed futures
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                    futures.remove(future)
                
                time.sleep(0.01)
            
            # Wait for remaining futures
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task error: {e}")
        
        return results
    
    def _execute_spike_load(self, task: Callable, task_args: List[Any],
                           duration_seconds: int) -> List[Dict[str, Any]]:
        """
        Execute with spike load pattern (periodic spikes in load).
        
        Args:
            task: Task to execute
            task_args: Task arguments
            duration_seconds: Duration to run
            
        Returns:
            List of results
        """
        results = []
        start_time = time.time()
        spike_interval = 60  # Spike every 60 seconds
        spike_duration = 10  # Spike lasts 10 seconds
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            task_index = 0
            
            while (time.time() - start_time) < duration_seconds:
                elapsed = time.time() - start_time
                
                # Determine if in spike period
                in_spike = (elapsed % spike_interval) < spike_duration
                current_concurrency = self.concurrency if in_spike else (self.concurrency // 4)
                current_concurrency = max(1, current_concurrency)
                
                # Submit tasks
                while len(futures) < current_concurrency:
                    args = task_args[task_index % len(task_args)]
                    future = executor.submit(task, args)
                    futures.append(future)
                    task_index += 1
                
                # Collect completed futures
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                    futures.remove(future)
                
                time.sleep(0.01)
            
            # Wait for remaining futures
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task error: {e}")
        
        return results
    
    def _execute_wave_load(self, task: Callable, task_args: List[Any],
                          duration_seconds: int) -> List[Dict[str, Any]]:
        """
        Execute with wave load pattern (sinusoidal variation in load).
        
        Args:
            task: Task to execute
            task_args: Task arguments
            duration_seconds: Duration to run
            
        Returns:
            List of results
        """
        import math
        
        results = []
        start_time = time.time()
        wave_period = 120  # Complete wave cycle every 120 seconds
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            task_index = 0
            
            while (time.time() - start_time) < duration_seconds:
                elapsed = time.time() - start_time
                
                # Calculate current concurrency using sine wave
                wave_position = (elapsed % wave_period) / wave_period * 2 * math.pi
                wave_factor = (math.sin(wave_position) + 1) / 2  # 0 to 1
                current_concurrency = int(self.concurrency * 0.25 + 
                                         self.concurrency * 0.75 * wave_factor)
                current_concurrency = max(1, current_concurrency)
                
                # Submit tasks
                while len(futures) < current_concurrency:
                    args = task_args[task_index % len(task_args)]
                    future = executor.submit(task, args)
                    futures.append(future)
                    task_index += 1
                
                # Collect completed futures
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                    futures.remove(future)
                
                time.sleep(0.01)
            
            # Wait for remaining futures
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task error: {e}")
        
        return results
    
    def get_active_threads(self) -> int:
        """Get number of active threads."""
        with self.lock:
            return self.active_threads
