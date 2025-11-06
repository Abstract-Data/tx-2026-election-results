"""Strategy for calculating primary voting likelihood (R vs D)."""
from typing import Dict, Any

from .base_strategy import LikelihoodScoringStrategy


class PrimaryLikelihoodStrategy(LikelihoodScoringStrategy):
    """Calculate probability of voting in Republican vs Democratic primary."""

    def calculate(self, voter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate primary likelihood based on PRI24 and PRI22.

        Args:
            voter_data: Dictionary with PRI24, PRI22, and other voter info

        Returns:
            Dictionary with primary_likelihood_r and primary_likelihood_d
        """
        pri24 = voter_data.get("pri24", "").strip().upper() if voter_data.get("pri24") else ""
        pri22 = voter_data.get("pri22", "").strip().upper() if voter_data.get("pri22") else ""

        # Extract party component (R vs D, ignoring E/Election Day distinction)
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

        # Calculate likelihoods
        likelihood_r = 0.0
        likelihood_d = 0.0

        # Both elections show Republican preference
        if party24 == "R" and party22 == "R":
            likelihood_r = 0.9
            likelihood_d = 0.1
        # Both elections show Democratic preference
        elif party24 == "D" and party22 == "D":
            likelihood_r = 0.1
            likelihood_d = 0.9
        # One Republican, one Democratic (mixed)
        elif (party24 == "R" and party22 == "D") or (party24 == "D" and party22 == "R"):
            likelihood_r = 0.5
            likelihood_d = 0.5
        # One Republican, one unknown
        elif party24 == "R" or party22 == "R":
            likelihood_r = 0.7
            likelihood_d = 0.3
        # One Democratic, one unknown
        elif party24 == "D" or party22 == "D":
            likelihood_r = 0.3
            likelihood_d = 0.7
        # Both unaffiliated or unknown
        elif party24 == "UN" or party22 == "UN":
            likelihood_r = 0.5
            likelihood_d = 0.5
        # Both unknown
        else:
            likelihood_r = 0.5
            likelihood_d = 0.5

        return {
            "primary_likelihood_r": likelihood_r,
            "primary_likelihood_d": likelihood_d,
        }

