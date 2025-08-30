import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Transforming Customers Data
@dlt.view(
    name="cities_silver_view"
)
def categories_silver_view():
    df = spark.readStream.table("cities_bronze_csv")
    df = df.withColumn(
        "CityID",
        df["CityID"].cast("int")
    )
    df = df.filter(
        df["CityID"].isNotNull() & df["CityName"].isNotNull() & df["ZipCode"].isNotNull() & df["CountryID"].isNotNull()
    )
    return df
