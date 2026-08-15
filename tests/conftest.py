import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    # Every standardize_*/unpivot_* transform relies on to_date()/CAST returning
    # null on a bad value (so the row can be filtered out) rather than raising —
    # that's only Spark's non-ANSI behavior. Set explicitly rather than relying
    # on whatever a given PySpark version defaults to (Spark 4.x defaults ANSI
    # mode on, which turns those nulls into raised exceptions instead).
    session.conf.set("spark.sql.ansi.enabled", "false")
    yield session
    session.stop()
