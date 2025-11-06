"""Statistics observer for migration statistics tracking."""
from typing import Any, Dict

from .base_observer import MigrationObserver


class StatisticsObserver(MigrationObserver):
    """Observer for tracking and reporting migration statistics."""

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
            Dictionary with migration statistics
        """
        return self.stats.copy()

