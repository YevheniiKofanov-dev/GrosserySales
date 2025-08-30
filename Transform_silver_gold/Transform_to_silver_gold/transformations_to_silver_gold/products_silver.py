import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="products_silver_view"
)
def products_silver_view():
    df = spark.readStream.table("products_bronze_csv")
    df = df.withColumn(
        "ProductID",
        df["ProductID"].cast("int")
    ).withColumn(
        "Price",
        df["Price"].cast("double")
    ).withColumn(
        "CategoryID",
        df["CategoryID"].cast("int")
    ).withColumn(
        "ModifyDate",
        to_timestamp(df["ModifyDate"], "yyyy-MM-dd HH:mm:ss.SSS")
    ).withColumn(
        "VitalityDays",
        df["VitalityDays"].cast("double")
    )
    df = df.filter(
        df["ProductID"].isNotNull() &
        df["ProductName"].isNotNull() &
        df["Price"].isNotNull() &
        df["CategoryID"].isNotNull() &
        df["ModifyDate"].isNotNull() &
        df["VitalityDays"].isNotNull()
    )
    
    return df

