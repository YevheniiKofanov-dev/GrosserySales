import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Transforming Customers Data
@dlt.view(
    name="categories_silver_view"
)
def categories_silver_view():
    df = spark.readStream.table("categories_bronze_csv")
    df = df.withColumn(
        "CategoryID",
        df["CategoryID"].cast("int")
    )
    df = df.filter(
        df["CategoryID"].isNotNull() & df["CategoryName"].isNotNull()
    )
    return df

