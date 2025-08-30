import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="customers_silver_view"
)
def customers_silver_view():
    df = spark.readStream.table("customers_bronze_csv")
    df = df.withColumn(
        "CustomerID",
        df["CustomerID"].cast("int")
    ).withColumn(
        "CityID",
        df["CityID"].cast("int")
    )
    df = df.filter(
        df["CustomerID"].isNotNull() &
        df["FirstName"].isNotNull() &
        df["LastName"].isNotNull() &
        df["CityID"].isNotNull() &
        df["Address"].isNotNull()
    )
    return df
