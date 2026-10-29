import os

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

import pytest
from pyspark.sql import SparkSession

import pyspark_dnstapir.spark.stdhll as H


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-dnstapir-unit-tests")
        .config(
            "spark.jars.packages",
            "com.swoop:spark-alchemy_2.12:1.2.1",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    H.register_hll_functions(session)

    yield session

    session.stop()
