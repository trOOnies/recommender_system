from typing import TYPE_CHECKING, List
if TYPE_CHECKING:
    from pandas import DataFrame


def order_cols(df: "DataFrame", cols: List[str]) -> "DataFrame":
    return df[cols + [c for c in df.columns if c not in cols]]
