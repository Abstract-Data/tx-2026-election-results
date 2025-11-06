"""Date extraction utility for early voting filenames."""
import re
from datetime import date
from typing import Optional


def extract_date_from_filename(filename: str) -> Optional[date]:
    """
    Extract date from early voting filename.

    Filename format: "STATEWIDE...EarlyVoting.MM_DD_YYYY.csv"
    Example: "STATEWIDE.2025 NOVEMBER 4TH CONSTITUTIONAL AMENDMENT.EarlyVoting.10_20_2025.csv"

    Args:
        filename: Source filename

    Returns:
        Date object or None if date cannot be extracted
    """
    if not filename:
        return None

    # Pattern to match MM_DD_YYYY format at the end of filename
    pattern = r"(\d{1,2})_(\d{1,2})_(\d{4})\.csv$"
    match = re.search(pattern, filename)

    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        year = int(match.group(3))

        try:
            return date(year, month, day)
        except ValueError:
            # Invalid date (e.g., month > 12, day > 31)
            return None

    return None

