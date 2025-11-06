"""Data processing module."""
from tx_election_results.data.voterfile import process_voterfile
from tx_election_results.data.early_voting import process_early_voting
from tx_election_results.data.merge import merge_voter_data

__all__ = [
    "process_voterfile",
    "process_early_voting",
    "merge_voter_data",
]


