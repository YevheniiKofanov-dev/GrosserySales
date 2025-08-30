import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Transforming Customers Data
@dlt.view(
    name="countries_silver_view"
)
def countries_silver_view():
    df = spark.readStream.table("countries_bronze_csv")
    df = df.withColumn(
        "CountryID",
        df["CountryID"].cast("int")
    )
    df = df.filter(
        df["CountryID"].isNotNull() & df["CountryName"].isNotNull() & df["CountryCode"].isNotNull()
    )
    return df
