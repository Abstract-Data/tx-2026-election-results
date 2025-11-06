"""Base strategy interface for likelihood scoring."""
from abc import ABC, abstractmethod
from typing import Dict, Any


class LikelihoodScoringStrategy(ABC):
    """Base interface for likelihood scoring strategies."""

    @abstractmethod
    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate likelihood scores based on voter data.

        Args:
            voter_data: Dictionary containing voter information (PRI24, PRI22, age, etc.)

        Returns:
            Dictionary with calculated likelihood scores
        """
        pass

