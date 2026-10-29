import pyspark.sql.functions as F

import pyspark_dnstapir.spark.stdhll as H


def test_stdhll_init_cardinality_merge(spark):
    df = spark.createDataFrame([(1,), (2,), (2,)], ["value"])
    df1 = df.withColumn("hll", H.hll_init(F.col("value")))
    df2 = df1.withColumn("cardinality", H.hll_cardinality(F.col("hll")))
    df3 = df2.agg(
        H.hll_merge(F.col("hll")).alias("hll_merged"), F.sum(F.col("cardinality")).alias("cardinality_sum")
    ).withColumn("cardinality_merged", H.hll_cardinality(F.col("hll_merged")))
    df4 = df3.drop("hll_merged")

    row = df4.collect()[0]
    assert row["cardinality_sum"] == 3
    assert row["cardinality_merged"] == 2

    df5 = df.agg(H.hll_init_agg(F.col("value")).alias("hll_agg")).withColumn(
        "cardinality_agg", H.hll_cardinality(F.col("hll_agg"))
    )
    row = df5.collect()[0]
    assert row["cardinality_agg"] == 2


def test_stdhll_init_collection(spark):
    df = spark.createDataFrame([([1, 2, 2],), ([3, 4, 4],), ([4, 4],)], ["collection"])
    df1 = df.withColumn("hll", H.hll_init_collection(F.col("collection")))
    df2 = df1.withColumn("cardinality", H.hll_cardinality(F.col("hll")))

    cardinalities = [row["cardinality"] for row in df2.collect()]
    assert cardinalities == [2, 2, 1]

    df3 = df.agg(H.hll_init_collection_agg(F.col("collection")).alias("hll_agg")).withColumn(
        "cardinality_agg", H.hll_cardinality(F.col("hll_agg"))
    )
    row = df3.collect()[0]
    assert row["cardinality_agg"] == 4


def test_stdhll_row_merge_intersect(spark):
    df = spark.createDataFrame([([1, 2, 2], [3, 4, 4], [4, 4])], ["collection1", "collection2", "collection3"])
    df1 = (
        df.withColumn("hll1", H.hll_init_collection(F.col("collection1")))
        .withColumn("hll2", H.hll_init_collection(F.col("collection2")))
        .withColumn("hll3", H.hll_init_collection(F.col("collection3")))
    )

    df2 = df1.withColumn("hll_merged", H.hll_row_merge(F.col("hll1"), F.col("hll2"), F.col("hll3")))
    df3 = df2.withColumn("cardinality_merged", H.hll_cardinality(F.col("hll_merged")))

    row = df3.collect()[0]
    assert row["cardinality_merged"] == 4

    df4 = df3.withColumn("cardinality_intersect", H.hll_intersect_cardinality(F.col("hll2"), F.col("hll3")))
    row = df4.collect()[0]
    assert row["cardinality_intersect"] == 1
