"""Precinct lookup module."""
from tx_election_results.precinct.lookup import (
    build_precinct_to_district_lookup,
    apply_precinct_lookup
)

__all__ = [
    "build_precinct_to_district_lookup",
    "apply_precinct_lookup",
]


