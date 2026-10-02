import pandas as pd

from issue_intelligence_platform.ml.data import prepare_dataset


def test_prepare_dataset_cleans_priority_and_builds_text() -> None:
    dataframe = pd.DataFrame(
        {
            "Summary": [
                "  Node fails  ",
                "Ignored issue",
                "Another ignored issue",
            ],
            "Description": [
                " startup error ",
                "description",
                "description",
            ],
            "Priority": [
                " High ",
                None,
                "   ",
            ],
        }
    )

    result = prepare_dataset(dataframe)

    assert len(result) == 1
    assert result.loc[0, "Priority"] == "High"
    assert result.loc[0, "text"] == "Node fails\nstartup error"
