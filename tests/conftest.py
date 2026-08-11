import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()
