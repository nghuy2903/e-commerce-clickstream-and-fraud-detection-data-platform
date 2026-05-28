from pyspark.sql import SparkSession
from chispa import assert_df_equality


def test_dataframe_equality_smoke() -> None:
    spark = SparkSession.builder.master("local[1]").appName("chispa-smoke").getOrCreate()
    left_df = spark.createDataFrame([(1, "ok")], ["id", "status"])
    right_df = spark.createDataFrame([(1, "ok")], ["id", "status"])
    assert_df_equality(left_df, right_df)
    spark.stop()
