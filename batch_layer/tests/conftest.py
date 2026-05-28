import pytest
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local[2]") \
      .appName("chispa_tests") \
      .config("spark.driver.memory", "1g") \
      .config("spark.executor.memory", "1g") \
      .getOrCreate()