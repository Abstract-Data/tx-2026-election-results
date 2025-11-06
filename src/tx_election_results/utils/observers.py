"""
Unified observer system for the Observer pattern.
Supports both migration/pipeline observers and API observers.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict
import logging


class BaseObserver(ABC):
    """Base observer interface with common error handling."""
    
    @abstractmethod
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Handle error event.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        pass


class PipelineObserver(BaseObserver):
    """Base observer interface for pipeline/migration events."""
    
    @abstractmethod
    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Handle progress update.
        
        Args:
            current: Current progress count
            total: Total items to process
            message: Optional progress message
        """
        pass
    
    @abstractmethod
    def on_complete(self, statistics: Dict[str, Any] = None) -> None:
        """
        Handle completion event.
        
        Args:
            statistics: Optional statistics dictionary
        """
        pass
    
    @abstractmethod
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """Handle error event."""
        pass


class APIObserver(BaseObserver):
    """Base observer interface for API events."""
    
    @abstractmethod
    def on_request(self, method: str, path: str, params: Dict[str, Any] = None) -> None:
        """
        Handle API request event.
        
        Args:
            method: HTTP method
            path: Request path
            params: Optional request parameters
        """
        pass
    
    @abstractmethod
    def on_response(self, status_code: int, response_time: float = None) -> None:
        """
        Handle API response event.
        
        Args:
            status_code: HTTP status code
            response_time: Optional response time in seconds
        """
        pass
    
    @abstractmethod
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """Handle API error event."""
        pass


class ProgressObserver(PipelineObserver):
    """Observer for logging pipeline/migration progress."""
    
    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Log progress update.
        
        Args:
            current: Current progress count
            total: Total items to process
            message: Optional progress message
        """
        percentage = (current / total * 100) if total > 0 else 0
        progress_msg = f"Progress: {current}/{total} ({percentage:.1f}%)"
        if message:
            progress_msg += f" - {message}"
        print(progress_msg)
    
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Log error event.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        error_msg = f"Error: {type(error).__name__}: {str(error)}"
        if context:
            error_msg += f" - Context: {context}"
        print(error_msg)
    
    def on_complete(self, statistics: Dict[str, Any] = None) -> None:
        """
        Log completion event.
        
        Args:
            statistics: Optional statistics dictionary
        """
        print("Pipeline completed!")
        if statistics:
            print("Statistics:")
            for key, value in statistics.items():
                print(f"  {key}: {value}")


class ErrorObserver(PipelineObserver):
    """Observer for handling and logging pipeline errors."""
    
    def __init__(self):
        """Initialize error observer with logging."""
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.ERROR)
    
    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """Handle progress update (no-op for error observer)."""
        pass
    
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Log error with full context.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        error_msg = f"Pipeline error: {type(error).__name__}: {str(error)}"
        if context:
            error_msg += f" - Context: {context}"
        self.logger.error(error_msg, exc_info=True)
    
    def on_complete(self, statistics: Dict[str, Any] = None) -> None:
        """Handle completion event (no-op for error observer)."""
        pass


class StatisticsObserver(PipelineObserver):
    """Observer for tracking pipeline statistics."""
    
    def __init__(self):
        """Initialize statistics observer."""
        self.stats: Dict[str, Any] = {
            "total_processed": 0,
            "total_errors": 0,
            "errors": [],
        }
    
    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Track progress statistics.
        
        Args:
            current: Current progress count
            total: Total items to process
            message: Optional progress message
        """
        self.stats["total_processed"] = current
        self.stats["total_items"] = total
    
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Track error statistics.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        self.stats["total_errors"] += 1
        error_info = {
            "type": type(error).__name__,
            "message": str(error),
            "context": context or {},
        }
        self.stats["errors"].append(error_info)
    
    def on_complete(self, statistics: Dict[str, Any] = None) -> None:
        """
        Finalize and report statistics.
        
        Args:
            statistics: Optional statistics dictionary to merge
        """
        if statistics:
            self.stats.update(statistics)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get collected statistics.
        
        Returns:
            Dictionary with pipeline statistics
        """
        return self.stats.copy()


class MetricsObserver(APIObserver):
    """Observer for tracking API metrics."""
    
    def __init__(self):
        """Initialize metrics observer."""
        self.metrics: Dict[str, Any] = {
            "total_requests": 0,
            "total_errors": 0,
            "response_times": [],
            "status_codes": {},
        }
    
    def on_request(self, method: str, path: str, params: Dict[str, Any] = None) -> None:
        """
        Track request metrics.
        
        Args:
            method: HTTP method
            path: Request path
            params: Optional request parameters
        """
        self.metrics["total_requests"] += 1
    
    def on_response(self, status_code: int, response_time: float = None) -> None:
        """
        Track response metrics.
        
        Args:
            status_code: HTTP status code
            response_time: Optional response time in seconds
        """
        # Track status codes
        status_code_str = str(status_code)
        self.metrics["status_codes"][status_code_str] = (
            self.metrics["status_codes"].get(status_code_str, 0) + 1
        )
        
        # Track response times
        if response_time is not None:
            self.metrics["response_times"].append(response_time)
    
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Track error metrics.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        self.metrics["total_errors"] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get collected metrics.
        
        Returns:
            Dictionary with API metrics
        """
        metrics = self.metrics.copy()
        if metrics["response_times"]:
            metrics["avg_response_time"] = sum(metrics["response_times"]) / len(
                metrics["response_times"]
            )
            metrics["max_response_time"] = max(metrics["response_times"])
            metrics["min_response_time"] = min(metrics["response_times"])
        return metrics


class RequestLogger(APIObserver):
    """Observer for logging API requests and responses."""
    
    def __init__(self):
        """Initialize request logger."""
        self.logger = logging.getLogger(__name__)
    
    def on_request(self, method: str, path: str, params: Dict[str, Any] = None) -> None:
        """
        Log API request.
        
        Args:
            method: HTTP method
            path: Request path
            params: Optional request parameters
        """
        self.logger.info(f"API Request: {method} {path}")
        if params:
            self.logger.debug(f"Request params: {params}")
    
    def on_response(self, status_code: int, response_time: float = None) -> None:
        """
        Log API response.
        
        Args:
            status_code: HTTP status code
            response_time: Optional response time in seconds
        """
        msg = f"API Response: {status_code}"
        if response_time is not None:
            msg += f" (response_time: {response_time:.3f}s)"
        self.logger.info(msg)
    
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Log API error.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        error_msg = f"API error: {type(error).__name__}: {str(error)}"
        if context:
            error_msg += f" - Context: {context}"
        self.logger.error(error_msg, exc_info=True)


# Backward compatibility aliases
MigrationObserver = PipelineObserver

