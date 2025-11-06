"""Strategy for calculating overall turnout likelihood."""
from typing import Dict, Any

from .base_strategy import LikelihoodScoringStrategy


class TurnoutScoreStrategy(LikelihoodScoringStrategy):
    """Calculate overall likelihood to vote in general election."""

    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate turnout score based on multiple factors.

        Args:
            voter_data: Dictionary with age, PRI codes, voting history, etc.

        Returns:
            Dictionary with turnout_score (0.0-1.0)
        """
        age = voter_data.get("age")
        pri24 = voter_data.get("pri24", "").strip().upper() if voter_data.get("pri24") else ""
        pri22 = voter_data.get("pri22", "").strip().upper() if voter_data.get("pri22") else ""
        voting_method_likelihood_early = voter_data.get("voting_method_likelihood_early", 0.5)
        primary_likelihood_r = voter_data.get("primary_likelihood_r", 0.5)
        primary_likelihood_d = voter_data.get("primary_likelihood_d", 0.5)

        # Base score from age demographics
        base_score = 0.5
        if age is not None:
            if age >= 65:
                base_score = 0.85
            elif age >= 55:
                base_score = 0.75
            elif age >= 45:
                base_score = 0.65
            elif age >= 35:
                base_score = 0.55
            elif age >= 25:
                base_score = 0.45
            else:
                base_score = 0.35

        # Primary participation factor
        primary_participation = max(primary_likelihood_r, primary_likelihood_d)
        if primary_participation >= 0.8:
            primary_factor = 1.2
        elif primary_participation >= 0.6:
            primary_factor = 1.1
        elif primary_participation >= 0.4:
            primary_factor = 1.0
        else:
            primary_factor = 0.9

        # Historical voting pattern (has voted in primaries)
        has_voting_history = bool(pri24 or pri22)
        history_factor = 1.15 if has_voting_history else 1.0

        # Calculate final score
        turnout_score = base_score * primary_factor * history_factor
        turnout_score = min(1.0, max(0.0, turnout_score))

        return {"turnout_score": turnout_score}

