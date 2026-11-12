import datetime as D
from collections.abc import Sequence

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from . import stdhll as H

QTYPE_COLS = ("a_count", "aaaa_count", "mx_count", "ns_count", "other_type_count", "non_in_count")
RCODE_COLS = ("ok_count", "nx_count", "fail_count", "other_rcode_count")
HLL_SKETCH_COLS = ("v4clients_hll", "v6clients_hll")


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
    sum_cols: Sequence[str] | None = QTYPE_COLS + RCODE_COLS,
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
    - sum_cols: Columns to sum.
    - hll_merge_cols: Columns containing HyperLogLog sketches to merge using hll_merge.
    - avg_cols: Columns to average.
    - other_aggs: Other aggregation expressions.
    Returns:
    - A DataFrame with rows merged according to the specified aggregations.
    """
    add_aggs = [F.sum(col).alias(col) for col in (sum_cols or ())]
    hll_aggs = [H.hll_merge(F.col(col)).alias(col) for col in (hll_merge_cols or ())]
    avg_aggs = [F.avg(col).alias(col) for col in (avg_cols or ())]
    return df.groupBy(group_by).agg(*add_aggs, *hll_aggs, *avg_aggs, *(other_aggs or ()))


def aggregate_timeseries_into_list(
    df: DataFrame,
    group_by: list[str | Column] | Column | str | int,
    value_column: str | Column,
    date_min: str | D.date,
    date_max: str | D.date,
    interval: int,
    timeseries_out_column_name: str,
) -> DataFrame:
    """
    Aggregate time series from a 5-minute (or multiple-of-5-minute) histogram DataFrame into lists
    ordered by date, hour, and minute. Bins that have no data will be filled with zeros.

    The input DataFrame is assumed to contain no duplicate values for the combination of date, hour, and minute
    within each group defined by group_by. Use merge_rows to combine duplicate rows before aggregation if necessary.

    The specified interval must match the granularity of the input data. A mismatch may lead to skipped bins
    or superfluous zeros in the resulting time series. The interval must be a multiple of 5 minutes.

    The lists contain all values from the specified date range, both including and between date_min and date_max.
    As such, the length of each time series list will be (1440 / interval * (number of days)).

    The input DataFrame may contain rows outside the specified date range, these will be ignored.

    Parameters:
    - df: The input DataFrame.
    - group_by: Columns to group by.
    - value_column: The column containing the values to aggregate into a list.
    - date_min: The minimum date to include in the aggregation.
    - date_max: The maximum date to include in the aggregation.
    - interval: The time interval in minutes of the input DataFrame.
    - timeseries_out_column_name: The name of the output column containing the aggregated time series.

    Returns:
    - A DataFrame with the aggregated time series as lists.
    """
    if isinstance(date_min, str):
        date_min = D.date.fromisoformat(date_min)
    if isinstance(date_max, str):
        date_max = D.date.fromisoformat(date_max)
    if not date_min <= date_max:
        raise ValueError("date_min must be less than or equal to date_max.")

    existing_columns = df.columns
    if "date" not in existing_columns:
        raise ValueError("The input DataFrame must contain a 'date' column.")
    if "hour" not in existing_columns:
        raise ValueError("The input DataFrame must contain a 'hour' column.")
    if "minute" not in existing_columns:
        raise ValueError("The input DataFrame must contain a 'minute' column.")
    if value_column not in existing_columns:
        raise ValueError("The input DataFrame must contain the given `value_column`.")
    if interval % 5 != 0:
        raise ValueError("The interval must be a multiple of 5.")

    df_filtered_date = df.filter(F.col("date").between(date_min, date_max))

    """
    Add an index column to the filtered DataFrame based on the date, hour, and minute columns.
    The index represents the position of each row in the aggregated time series.
    """
    df_with_index = df_filtered_date.withColumn(
        "_aggregate_timeseries_index",
        F.floor(
            (F.datediff(F.col("date"), F.lit(date_min)) * 1440 + F.col("hour") * 60 + F.col("minute")) / interval
        ).cast("int"),
    )

    """
    Aggregate the indexed rows into a map { index -> value }.
    """
    df_with_item_map = df_with_index.groupBy(group_by).agg(
        F.map_from_entries(F.collect_list(F.struct("_aggregate_timeseries_index", value_column))).alias(
            "_aggregate_timeseries_item_map"
        )
    )

    number_of_days = (date_max - date_min).days + 1
    min_index = 0
    max_index = (number_of_days * 1440 - 1) // interval

    """
    Fill in missing indices in the time series with a default value of 0.
    The time series sequence is now ordered by the index.
    """
    df_with_items_filled = df_with_item_map.withColumn(
        "_aggregate_timeseries_items_filled",
        F.transform(
            F.sequence(F.lit(min_index), F.lit(max_index)),
            lambda i: F.struct(
                i.alias("index"),
                F.coalesce(F.element_at("_aggregate_timeseries_item_map", i), F.lit(0)).alias("value"),
            ),
        ),
    )

    """
    Up to this point, the time series sequence still contains structs including the index.
    Extract just the values into the final output column.
    Drop the intermediate columns.
    """
    df_with_timeseries = df_with_items_filled.withColumn(
        timeseries_out_column_name, F.transform("_aggregate_timeseries_items_filled", lambda x: x["value"])
    ).drop("_aggregate_timeseries_index", "_aggregate_timeseries_item_map", "_aggregate_timeseries_items_filled")

    return df_with_timeseries
