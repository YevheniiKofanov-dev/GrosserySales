import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="employees_silver_view"
)
def employees_silver_view():
    df = spark.readStream.table("employees_bronze_csv")
    df = df.withColumn(
        "EmployeeID",
        df["employeeID"].cast("int")
    ).withColumn(
        "CityID",
        df["CityID"].cast("int")
    ).withColumn(
        "BirthDate",
        to_timestamp(df["BirthDate"], "yyyy-MM-dd HH:mm:ss.SSS")
    ).withColumn(
        "HireDate",
        to_timestamp(df["HireDate"], "yyyy-MM-dd HH:mm:ss.SSS")
    )
    df = df.filter(
        df["EmployeeID"].isNotNull() &
        df["FirstName"].isNotNull() &
        df["LastName"].isNotNull() &
        df["CityID"].isNotNull() &
        df["BirthDate"].isNotNull() &
        df["HireDate"].isNotNull()
    )
    return df

