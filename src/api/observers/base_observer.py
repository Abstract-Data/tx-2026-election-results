"""Base observer interface for API events."""
from abc import ABC, abstractmethod
from typing import Any, Dict


class APIObserver(ABC):
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
        """
        Handle API error event.

        Args:
            error: Exception that occurred
            context: Optional context information
        """
        pass

