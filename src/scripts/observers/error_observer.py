"""Error observer for migration error handling."""
import logging
from typing import Any, Dict

from .base_observer import MigrationObserver

logger = logging.getLogger(__name__)


class ErrorObserver(MigrationObserver):
    """Observer for handling and logging migration errors."""

    def __init__(self):
        """Initialize error observer with logging."""
        logging.basicConfig(level=logging.ERROR)

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Handle progress update (no-op for error observer).

        Args:
            current: Current progress count
            total: Total items to process
            message: Optional progress message
        """
        pass

    def on_error(self, error: Exception, context: Dict[str, Any] = None) -> None:
        """
        Log error with full context.

        Args:
            error: Exception that occurred
            context: Optional context information
        """
        error_msg = f"Migration error: {type(error).__name__}: {str(error)}"
        if context:
            error_msg += f" - Context: {context}"
        logger.error(error_msg, exc_info=True)

    def on_complete(self, statistics: Dict[str, Any] = None) -> None:
        """
        Handle completion event (no-op for error observer).

        Args:
            statistics: Optional statistics dictionary
        """
        pass

