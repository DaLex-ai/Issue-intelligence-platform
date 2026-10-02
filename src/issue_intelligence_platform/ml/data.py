from __future__ import annotations

import pandas as pd

DATASET_URL = (
    "https://raw.githubusercontent.com/av9ash/gitbugs/"
    "eadf2d6794990a1aaa26d0ae15391f29f8969301/"
    "cassandra/cassandra_bugs.csv"
)

REQUIRED_COLUMNS = {
    "Summary",
    "Issue id",
    "Status",
    "Priority",
    "Resolution",
    "Created",
    "Resolved",
    "Description",
}


def load_raw_dataset() -> pd.DataFrame:
    df = pd.read_csv(DATASET_URL)

    missing_columns = REQUIRED_COLUMNS.difference(df.columns)
    if missing_columns:
        raise ValueError(
            f"Dataset is missing required columns: {sorted(missing_columns)}"
        )

    return df


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df[["Summary", "Description", "Priority"]].copy()

    prepared["Summary"] = prepared["Summary"].fillna("").astype(str)
    prepared["Description"] = prepared["Description"].fillna("").astype(str)

    prepared = prepared.dropna(subset=["Priority"])

    prepared["Priority"] = prepared["Priority"].astype(str).str.strip()
    prepared = prepared[prepared["Priority"] != ""]

    prepared["text"] = (
        prepared["Summary"].str.strip() + "\n" + prepared["Description"].str.strip()
    )

    prepared = prepared[prepared["text"].str.strip() != ""]

    return prepared.reset_index(drop=True)
