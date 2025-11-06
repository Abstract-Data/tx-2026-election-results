"""Request logger observer for API requests."""
import logging
from typing import Any, Dict

from .base_observer import APIObserver

logger = logging.getLogger(__name__)


class RequestLogger(APIObserver):
    """Observer for logging API requests."""

    def __init__(self):
        """Initialize request logger with logging."""
        logging.basicConfig(level=logging.INFO)

    def on_request(self, method: str, path: str, params: Dict[str, Any] = None) -> None:
        """
        Log API request.

        Args:
            method: HTTP method
            path: Request path
            params: Optional request parameters
        """
        log_msg = f"{method} {path}"
        if params:
            log_msg += f" - Params: {params}"
        logger.info(log_msg)

    def on_response(self, status_code: int, response_time: float = None) -> None:
        """
        Log API response.

        Args:
            status_code: HTTP status code
            response_time: Optional response time in seconds
        """
        log_msg = f"Response: {status_code}"
        if response_time:
            log_msg += f" - Time: {response_time:.3f}s"
        logger.info(log_msg)

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
        logger.error(error_msg, exc_info=True)

