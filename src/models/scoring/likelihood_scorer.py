"""Main likelihood scorer orchestrating all strategies."""
from typing import Dict, Any

from .strategies import (
    GeneralElectionLikelihoodStrategy,
    PredictionAccuracyStrategy,
    PrimaryLikelihoodStrategy,
    TurnoutScoreStrategy,
    VotingMethodLikelihoodStrategy,
)


class LikelihoodScorer:
    """Orchestrates all likelihood scoring strategies."""

    def __init__(self):
        """Initialize scorer with all strategy components."""
        self.primary_strategy = PrimaryLikelihoodStrategy()
        self.voting_method_strategy = VotingMethodLikelihoodStrategy()
        self.general_election_strategy = GeneralElectionLikelihoodStrategy()
        self.turnout_score_strategy = TurnoutScoreStrategy()
        self.prediction_accuracy_strategy = PredictionAccuracyStrategy()

    def score_voter(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all likelihood scores for a voter.

        Args:
            voter_data: Dictionary containing voter information

        Returns:
            Dictionary with all calculated likelihood scores
        """
        results = {}

        # Step 1: Primary likelihood
        primary_results = self.primary_strategy.calculate(voter_data)
        results.update(primary_results)
        voter_data.update(primary_results)

        # Step 2: Voting method likelihood
        voting_method_results = self.voting_method_strategy.calculate(voter_data)
        results.update(voting_method_results)
        voter_data.update(voting_method_results)

        # Step 3: General election likelihood
        general_results = self.general_election_strategy.calculate(voter_data)
        results.update(general_results)

        # Step 4: Turnout score
        turnout_results = self.turnout_score_strategy.calculate(voter_data)
        results.update(turnout_results)

        # Step 5: Prediction accuracy (if actual voting data available)
        if "actual_voted_early" in voter_data:
            accuracy_results = self.prediction_accuracy_strategy.calculate(voter_data)
            results.update(accuracy_results)

        return results

