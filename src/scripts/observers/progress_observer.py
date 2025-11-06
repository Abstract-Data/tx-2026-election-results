"""Progress observer for migration progress tracking."""
from typing import Any, Dict

from .base_observer import MigrationObserver


class ProgressObserver(MigrationObserver):
    """Observer for logging migration progress."""

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
        print("Migration completed!")
        if statistics:
            print("Statistics:")
            for key, value in statistics.items():
                print(f"  {key}: {value}")

