"""Factory for creating likelihood scorers."""
from .likelihood_scorer import LikelihoodScorer


class LikelihoodScorerFactory:
    """Factory for creating configured likelihood scorers."""

    @staticmethod
    def create_scorer() -> LikelihoodScorer:
        """
        Create a configured likelihood scorer with all strategy components.

        Returns:
            Configured LikelihoodScorer instance
        """
        return LikelihoodScorer()

