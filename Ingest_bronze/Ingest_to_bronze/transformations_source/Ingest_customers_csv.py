import dlt
from pyspark.sql.functions import current_timestamp

file_path = "abfss://raw-data@grosserysalesstore.dfs.core.windows.net/csv/customers/"

@dlt.table(
    name="customers_bronze_csv"
)
def customers_bronze_csv():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(file_path)
    )
    df = df.withColumn("inserted_datetime", current_timestamp())
    return df