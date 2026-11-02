from collections.abc import Sequence

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from . import stdhll as H

QTYPE_COLS = ("a_count", "aaaa_count", "mx_count", "ns_count", "other_type_count", "non_in_count")
RCODE_COLS = ("ok_count", "nx_count", "fail_count", "other_rcode_count")
HLL_SKETCH_COLS = ("v4client_hll", "v6client_hll")


def add_total_query_count_column(df: DataFrame, from_rcode_cols: bool = True, name: str = "query_count") -> DataFrame:
    """
    Add a column to the DataFrame representing the total query count.

    Parameters:
    - df: The input DataFrame.
    - from_rcode_cols: If True, sum the RCODE_COLS; otherwise, sum the QTYPE_COLS. \
        The result should be the same either way, but this option allows working with a reduced DataFrame.
    - name: The name of the new column to add.
    Returns:
    - A DataFrame with the new query count column added.
    """
    query_count_col = sum(F.col(col) for col in (RCODE_COLS if from_rcode_cols else QTYPE_COLS))
    return df.withColumn(name, query_count_col)


def merge_rows(
    df: DataFrame,
    group_by: list[str | Column] | Column | str | int,
    add_cols: Sequence[str] | None = QTYPE_COLS + RCODE_COLS,
    hll_merge_cols: Sequence[str] | None = HLL_SKETCH_COLS,
    avg_cols: Sequence[str] | None = None,
    other_aggs: Sequence[Column] | None = None,
) -> DataFrame:
    """
    Generic function to merge rows of a 5-minute histogram DataFrame by the specified group_by columns
    with the specified aggregations. Columns passed to add_cols, hll_merge_cols, and avg_cols retain their name
    after the aggregation. The other_aggs parameter allows specifying additional aggregation expressions that do
    not fit into the predefined categories.

    With the default parameters, all query counts (e.g. a_count, ok_count) are summed and the HLL sketches are merged.
    The HLL-derived counting columns (v{4,6}clients_avg, v{4,6}client_count_hll) are not included in the default parameters
    because the appropriate aggregation depends on the specific use case. E.g., when merging multiple creators together,
    it is appropriate to sum them, whereas for reducing the 5-minute bins into larger intervals, averaging the
    v{4,6}clients_avg columns is more suitable.

    Parameters:
    - df: The input DataFrame.
    - group_by: Columns to group by.
    - add_cols: Columns to sum.
    - hll_merge_cols: Columns containing HyperLogLog sketches to merge using hll_merge.
    - avg_cols: Columns to average.
    - other_aggs: Other aggregation expressions.
    Returns:
    - A DataFrame with rows merged according to the specified aggregations.
    """
    add_aggs = [F.sum(col).alias(col) for col in (add_cols or ())]
    hll_aggs = [H.hll_merge(F.col(col)).alias(col) for col in (hll_merge_cols or ())]
    avg_aggs = [F.avg(col).alias(col) for col in (avg_cols or ())]
    return df.groupBy(group_by).agg(*add_aggs, *hll_aggs, *avg_aggs, *(other_aggs or ()))
