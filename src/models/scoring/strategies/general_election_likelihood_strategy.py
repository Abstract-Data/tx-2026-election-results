"""Strategy for classifying general election likelihood."""
from typing import Dict, Any

from ...voter import GeneralLikelihood
from .base_strategy import LikelihoodScoringStrategy


class GeneralElectionLikelihoodStrategy(LikelihoodScoringStrategy):
    """Classify voters into general election likelihood categories."""

    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify general election likelihood based on party affiliation consistency.

        Args:
            voter_data: Dictionary with PRI24, PRI22, and primary likelihoods

        Returns:
            Dictionary with general_likelihood classification
        """
        pri24 = voter_data.get("pri24", "").strip().upper() if voter_data.get("pri24") else ""
        pri22 = voter_data.get("pri22", "").strip().upper() if voter_data.get("pri22") else ""
        primary_likelihood_r = voter_data.get("primary_likelihood_r", 0.5)
        primary_likelihood_d = voter_data.get("primary_likelihood_d", 0.5)

        # Extract party component
        def extract_party(pri_code: str) -> str:
            """Extract party component from PRI code."""
            if not pri_code:
                return ""
            pri_code = pri_code.upper()
            if "R" in pri_code and "D" not in pri_code:
                return "R"
            elif "D" in pri_code and "R" not in pri_code:
                return "D"
            elif "UN" in pri_code:
                return "UN"
            return ""

        party24 = extract_party(pri24)
        party22 = extract_party(pri22)

        # Determine likelihood classification
        # Strong Republican pattern
        if party24 == "R" and party22 == "R" and primary_likelihood_r >= 0.8:
            general_likelihood = GeneralLikelihood.LIKELY_REPUBLICAN
        # Mostly Republican
        elif (party24 == "R" or party22 == "R") and primary_likelihood_r >= 0.6:
            general_likelihood = GeneralLikelihood.LEAN_REPUBLICAN
        # Strong Democratic pattern
        elif party24 == "D" and party22 == "D" and primary_likelihood_d >= 0.8:
            general_likelihood = GeneralLikelihood.LIKELY_DEMOCRAT
        # Mostly Democratic
        elif (party24 == "D" or party22 == "D") and primary_likelihood_d >= 0.6:
            general_likelihood = GeneralLikelihood.LEAN_DEMOCRAT
        # Mixed or unaffiliated
        else:
            general_likelihood = GeneralLikelihood.TOSS_UP

        return {"general_likelihood": general_likelihood}

