from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import expr


def register_hll_functions(sparks: SparkSession):
    sparkc = sparks.sparkContext
    sparkc._jvm.com.swoop.alchemy.spark.expressions.hll.HLLFunctionRegistration.registerFunctions(sparks._jsparkSession)
    sparks.conf.set("com.swoop.alchemy.hll.implementation", "AGGREGATE_KNOWLEDGE")


def hll_init(c: Column) -> Column:
    """
    Creates a composable HLL sketch for each row
    """
    return expr(f"hll_init({unsafe_sql(c)})")


def hll_init_collection(c: Column) -> Column:
    """
    Creates a composable HLL sketch for each input row. Column must be a collection type (array, map), and collection
    elements are treated as individual values.
    """
    return expr(f"hll_init_collection({unsafe_sql(c)})")


def hll_init_agg(c: Column) -> Column:
    """
    Combines all input into a single HLL sketch
    """
    return expr(f"hll_init_agg({unsafe_sql(c)})")


def hll_init_collection_agg(c: Column) -> Column:
    """
    Combines all input into a single HLL sketch. Column must be a collection type (array, map), and collection
    elements are treated as individual values.
    """
    return expr(f"hll_init_collection_agg({unsafe_sql(c)})")


def hll_merge(c: Column) -> Column:
    """
    Aggregates all sketches into a single merged sketch that represents the union of the constituents.
    """
    return expr(f"hll_merge({unsafe_sql(c)})")


def hll_row_merge(*c: Column) -> Column:
    """
    Merges multiple "sketches" in one row into a single field.
    """
    return expr("hll_row_merge({})".format(", ".join(list(map(unsafe_sql, c)))))


def hll_cardinality(c: Column) -> Column:
    """
    Returns the estimated cardinality of an HLL "sketch"
    """
    return expr(f"hll_cardinality({unsafe_sql(c)})")


def hll_intersect_cardinality(left: Column, right: Column) -> Column:
    """
    Returns the estimated intersection cardinality of the binary representations produced by
    HyperLogLog. Computes a merged (unioned) sketch and uses the fact that |A intersect B| = (|A| + |B|) - |A union B|.
    Returns null if both sketches are null, but 0 if only one is
    """
    return expr("hll_intersect_cardinality({})".format(", ".join(list(map(unsafe_sql, [left, right])))))


def unsafe_sql(c: Column) -> str:
    return c._jc.expr.__call__().sql.__call__()
