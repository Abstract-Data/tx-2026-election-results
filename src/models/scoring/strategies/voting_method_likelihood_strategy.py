"""Strategy for calculating early vs election day voting likelihood."""
from typing import Dict, Any

from .base_strategy import LikelihoodScoringStrategy


class VotingMethodLikelihoodStrategy(LikelihoodScoringStrategy):
    """Calculate likelihood of voting early vs on election day."""

    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate voting method likelihood based on PRI codes and age.

        Args:
            voter_data: Dictionary with PRI24, PRI22, age, and other voter info

        Returns:
            Dictionary with voting_method_likelihood_early and voting_method_likelihood_election_day
        """
        pri24 = voter_data.get("pri24", "").strip().upper() if voter_data.get("pri24") else ""
        pri22 = voter_data.get("pri22", "").strip().upper() if voter_data.get("pri22") else ""
        age = voter_data.get("age")

        # Check for early voting pattern (ends with "E" or contains "E")
        def is_early_vote(pri_code: str) -> bool:
            """Check if PRI code indicates early voting."""
            if not pri_code:
                return False
            pri_code = pri_code.upper()
            # "RE" or "DE" indicates early vote
            return pri_code.endswith("E") or (len(pri_code) == 2 and "E" in pri_code)

        early24 = is_early_vote(pri24)
        early22 = is_early_vote(pri22)

        # Base likelihoods
        likelihood_early = 0.5
        likelihood_election_day = 0.5

        # Both elections show early voting
        if early24 and early22:
            likelihood_early = 0.85
            likelihood_election_day = 0.15
        # Both elections show election day voting
        elif not early24 and not early22 and (pri24 or pri22):
            likelihood_early = 0.15
            likelihood_election_day = 0.85
        # Mixed pattern
        elif early24 != early22:
            likelihood_early = 0.5
            likelihood_election_day = 0.5
        # One early, one unknown
        elif early24 or early22:
            likelihood_early = 0.65
            likelihood_election_day = 0.35

        # Age factor: older voters tend to vote early
        if age is not None:
            if age >= 65:
                likelihood_early = min(0.95, likelihood_early + 0.15)
                likelihood_election_day = max(0.05, likelihood_election_day - 0.15)
            elif age >= 55:
                likelihood_early = min(0.9, likelihood_early + 0.1)
                likelihood_election_day = max(0.1, likelihood_election_day - 0.1)
            elif age < 35:
                likelihood_early = max(0.1, likelihood_early - 0.1)
                likelihood_election_day = min(0.9, likelihood_election_day + 0.1)

        return {
            "voting_method_likelihood_early": likelihood_early,
            "voting_method_likelihood_election_day": likelihood_election_day,
        }

