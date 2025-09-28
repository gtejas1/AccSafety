from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from unified_explore import _format_unified_table


def test_format_unified_table_encodes_ampersand_in_view_link():
    df = pd.DataFrame(
        [
            {
                "Location": "1st & Main",
                "Mode": "pedestrian",
                "Facility type": "intersection",
                "Source": "Eco (Pilot Counts)",
                "Total counts": 10,
                "Start date": "2023-01-01",
                "End date": "2023-01-02",
                "ViewHref": "/eco/dashboard?location=1st & Main",
            }
        ]
    )

    formatted = _format_unified_table(df)
    assert "%26" in formatted.loc[0, "View"]
