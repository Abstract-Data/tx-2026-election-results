"""Strategy for evaluating prediction accuracy."""
from typing import Dict, Any

from ...voter import PredictionAccuracy
from .base_strategy import LikelihoodScoringStrategy


class PredictionAccuracyStrategy(LikelihoodScoringStrategy):
    """Evaluate prediction accuracy by comparing predicted vs actual voting method."""

    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate prediction accuracy classification.

        Args:
            voter_data: Dictionary with voting_method_likelihood_early and actual_voted_early

        Returns:
            Dictionary with prediction_accuracy classification
        """
        voting_method_likelihood_early = voter_data.get("voting_method_likelihood_early", 0.5)
        actual_voted_early = voter_data.get("actual_voted_early")

        # If no actual data available
        if actual_voted_early is None:
            return {"prediction_accuracy": PredictionAccuracy.UNKNOWN}

        # Predicted early voting
        predicted_early = voting_method_likelihood_early > 0.5

        # Classify accuracy
        if predicted_early and actual_voted_early:
            prediction_accuracy = PredictionAccuracy.CORRECT_EARLY
        elif not predicted_early and not actual_voted_early:
            prediction_accuracy = PredictionAccuracy.CORRECT_ELECTION_DAY
        elif predicted_early and not actual_voted_early:
            prediction_accuracy = PredictionAccuracy.PREDICTED_EARLY_BUT_DIDNT
        else:  # not predicted_early and actual_voted_early
            prediction_accuracy = PredictionAccuracy.PREDICTED_ELECTION_DAY_BUT_VOTED_EARLY

        return {"prediction_accuracy": prediction_accuracy}

