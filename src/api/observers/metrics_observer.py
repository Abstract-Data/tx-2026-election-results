"""Metrics observer for API metrics tracking."""
from typing import Any, Dict

from .base_observer import APIObserver


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

