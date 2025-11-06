"""Base observer interface for Observer pattern."""
from abc import ABC, abstractmethod
from typing import Any, Dict


class MigrationObserver(ABC):
    """Base observer interface for migration events."""

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
    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Handle error event.

        Args:
            error: Exception that occurred
            context: Optional context information
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

