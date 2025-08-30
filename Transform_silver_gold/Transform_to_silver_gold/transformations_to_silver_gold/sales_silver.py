import dlt
from pyspark.sql.functions import to_timestamp

@dlt.view(
    name="sales_silver_view"
)
def sales_silver_view():
    df = spark.readStream.table("sales_bronze_csv")
    df = df.withColumn(
        "SalesID", df["SalesID"].cast("int")
    ).withColumn(
        "SalesPersonID", df["SalesPersonID"].cast("int")
    ).withColumn(
        "CustomerID", df["CustomerID"].cast("int")
    ).withColumn(
        "ProductID", df["ProductID"].cast("int")
    ).withColumn(
        "Quantity", df["Quantity"].cast("int")
    ).withColumn(
        "Discount", df["Discount"].cast("double")
    ).withColumn(
        "TotalPrice", df["TotalPrice"].cast("double")
    ).withColumn(
        "SalesDate", to_timestamp(df["SalesDate"], "yyyy-MM-dd HH:mm:ss.SSS")
    )
    df = df.filter(
        df["ProductID"].isNotNull() &
        df["SalesID"].isNotNull() &
        df["TotalPrice"].isNotNull() &
        df["CustomerID"].isNotNull() &
        df["SalesDate"].isNotNull() &
        df["SalesPersonID"].isNotNull() &
        df["Quantity"].isNotNull() &
        df["Discount"].isNotNull()
    )
    return df