"""Analysis module."""
from tx_election_results.analysis.district_comparison import (
    calculate_party_gains_losses,
    create_party_gains_losses_visualizations,
    compare_old_vs_new_turnout,
    create_comparison_visualizations,
)
from tx_election_results.analysis.all_districts_gains_losses import generate_all_districts_gains_losses
from tx_election_results.analysis.party_transition_report import generate_party_transition_report
from tx_election_results.analysis.party_crosstab_report import generate_party_crosstab_report

__all__ = [
    "calculate_party_gains_losses",
    "create_party_gains_losses_visualizations",
    "compare_old_vs_new_turnout",
    "create_comparison_visualizations",
    "generate_all_districts_gains_losses",
    "generate_party_transition_report",
    "generate_party_crosstab_report",
]

