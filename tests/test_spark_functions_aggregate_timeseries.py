import datetime as D
import random

import pytest

from pyspark_dnstapir.spark.functions import aggregate_timeseries_into_list


def test_aggregate_timeseries1(spark):
    data = [
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 0, 0, 5),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 0, 30, 3),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 1, 0, 7),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 2, 0, 2),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 4, 0, 4),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 23, 30, 1),
        (D.date.fromisoformat("2026-10-01"), "com", "example", 0, 0, 6),
        (D.date.fromisoformat("2026-10-01"), "com", "example", 0, 30, 2),
        (D.date.fromisoformat("2026-10-01"), "com", "example", 23, 30, 4),
    ]
    df = spark.createDataFrame(data, ["date", "label0", "label1", "hour", "minute", "query_count"])
    df1 = aggregate_timeseries_into_list(
        df,
        group_by=["label0", "label1"],
        value_column="query_count",
        date_min="2026-10-01",
        date_max="2026-10-01",
        interval=30,
        timeseries_out_column_name="query_counts",
    ).sort("label0", "label1")

    result = df1.collect()
    assert len(result) == 2

    examplecom = result[0].asDict()
    expected_examplecom = {
        "label0": "com",
        "label1": "example",
        "query_counts": [6, 2] + ([0] * 45) + [4],
    }

    assert examplecom == expected_examplecom

    dnstapirse = result[1].asDict()
    expected_dnstapirse = {
        "label0": "se",
        "label1": "dnstapir",
        "query_counts": [5, 3, 7, 0, 2, 0, 0, 0, 4] + ([0] * 38) + [1],
    }

    assert dnstapirse == expected_dnstapirse


def test_aggregate_timeseries2(spark):
    data = [
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 0, 45, 5),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 2, 15, 15),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 5, 0, 9),
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 18, 30, 7),
        (D.date.fromisoformat("2026-10-02"), "se", "dnstapir", 14, 45, 8),
        (D.date.fromisoformat("2026-10-02"), "se", "dnstapir", 20, 0, 12),
    ]
    random.Random(42).shuffle(data)

    df = spark.createDataFrame(data, ["date", "label0", "label1", "hour", "minute", "query_count"])
    df1 = aggregate_timeseries_into_list(
        df,
        group_by=["label0", "label1"],
        value_column="query_count",
        date_min=D.date.fromisoformat("2026-10-01"),
        date_max=D.date.fromisoformat("2026-10-02"),
        interval=15,
        timeseries_out_column_name="query_counts",
    )

    result = df1.collect()
    assert len(result) == 1
    dnstapirse = result[0].asDict()
    expected_dnstapirse = {
        "label0": "se",
        "label1": "dnstapir",
        "query_counts": ([0] * 3)
        + [5]
        + ([0] * 5)
        + [15]
        + ([0] * 10)
        + [9]
        + ([0] * 53)
        + [7]
        + ([0] * 80)
        + [8]
        + ([0] * 20)
        + [12]
        + ([0] * 15),
    }

    assert dnstapirse == expected_dnstapirse


def test_aggregate_timeseries_errors(spark):
    data = [
        (D.date.fromisoformat("2026-10-01"), "se", "dnstapir", 0, 45, 5),
    ]
    df = spark.createDataFrame(data, ["date", "label0", "label1", "hour", "minute", "query_count"])

    with pytest.raises(ValueError, match="date_min must be less than or equal to date_max."):
        aggregate_timeseries_into_list(
            df,
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-02",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError, match="The interval must be a multiple of 5."):
        aggregate_timeseries_into_list(
            df,
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="2026-10-01",
            interval=7,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError, match="The input DataFrame must contain a 'date' column."):
        aggregate_timeseries_into_list(
            df.drop("date"),
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError, match="The input DataFrame must contain a 'hour' column."):
        aggregate_timeseries_into_list(
            df.drop("hour"),
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError, match="The input DataFrame must contain a 'minute' column."):
        aggregate_timeseries_into_list(
            df.drop("minute"),
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError, match="The input DataFrame must contain the given `value_column`."):
        aggregate_timeseries_into_list(
            df.drop("query_count"),
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError):
        aggregate_timeseries_into_list(
            df,
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="invalid",
            date_max="2026-10-01",
            interval=15,
            timeseries_out_column_name="query_counts",
        )

    with pytest.raises(ValueError):
        aggregate_timeseries_into_list(
            df,
            group_by=["label0", "label1"],
            value_column="query_count",
            date_min="2026-10-01",
            date_max="invalid",
            interval=15,
            timeseries_out_column_name="query_counts",
        )
