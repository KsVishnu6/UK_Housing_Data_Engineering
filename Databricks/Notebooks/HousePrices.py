# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE

# COMMAND ----------

# 1. Read source data

house_price_df = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .option("dataAddress", "2a!A3:DR321")
    .option("inferSchema", True)
    .load("abfss://raw@ukhousingstorageadls.dfs.core.windows.net/medianpricepaidforadministrativegeographies.xlsx")
)

# COMMAND ----------

# 2. Select required fields

house_price_bronze = house_price_df.select(
    col("Local authority code").alias("la_code"),
    col("Local authority name").alias("la_name"),
    col("Year ending Mar 2025").alias("median_house_price")
)

# COMMAND ----------

# 3. Write Bronze data

house_price_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_housing.bronze.house_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER

# COMMAND ----------

from pyspark.sql.functions import *


try:

    # 1. Read Bronze

    house_prices = spark.table(
        "uk_housing.bronze.house_prices"
    )


    # 2. Check expected columns

    expected_columns = [
        "la_code",
        "la_name",
        "median_house_price"
    ]

    if house_prices.columns != expected_columns:
        raise Exception(
            f"COLUMN CHECK FAILED: Expected {expected_columns}, "
            f"found {house_prices.columns}"
        )


    # 3. Check Bronze data types

    expected_data_types = {
        "la_code": "string",
        "la_name": "string",
        "median_house_price": "bigint"
    }

    actual_data_types = dict(house_prices.dtypes)

    for column_name, expected_type in expected_data_types.items():

        if actual_data_types.get(column_name) != expected_type:
            raise Exception(
                f"DATA TYPE CHECK FAILED: '{column_name}' expected "
                f"{expected_type}, found "
                f"{actual_data_types.get(column_name)}"
            )


    # 4. Check required nulls

    required_columns = [
        "la_code",
        "la_name",
        "median_house_price"
    ]

    for column_name in required_columns:

        if (
            house_prices
            .filter(col(column_name).isNull())
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"NULL CHECK FAILED: "
                f"'{column_name}' contains null values"
            )


    # 5. Check blank strings

    for column_name in ["la_code", "la_name"]:

        if (
            house_prices
            .filter(trim(col(column_name)) == "")
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"BLANK CHECK FAILED: "
                f"'{column_name}' contains blank values"
            )


    # 6. Check LA code format

    if (
        house_prices
        .filter(
            ~col("la_code").rlike("^[EW][0-9]{8}$")
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid LA code found"
        )


    # 7. Check LA code uniqueness

    if (
        house_prices
        .groupBy("la_code")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "UNIQUENESS CHECK FAILED: Duplicate la_code found"
        )


    # 8. Check LA code / LA name consistency

    if (
        house_prices
        .groupBy("la_code")
        .agg(
            countDistinct("la_name").alias("name_count")
        )
        .filter(col("name_count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "CONSISTENCY CHECK FAILED: "
            "A la_code is associated with multiple la_name values"
        )


    # 9. Check house prices

    if (
        house_prices
        .filter(col("median_house_price") <= 0)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: "
            "Zero or negative median house price found"
        )


    # 10. Keep England for the analytical model

    house_prices_england = house_prices.filter(
        col("la_code").rlike("^E[0-9]{8}$")
    )


    if house_prices_england.limit(1).count() == 0:
        raise Exception(
            "SCOPE CHECK FAILED: No England house price records found"
        )


    # 11. Build Silver

    house_prices_silver = (
        house_prices_england
        .select(
            col("la_code").cast("string").alias("la_code"),
            col("la_name").cast("string").alias("la_name"),
            col("median_house_price")
                .cast("long")
                .alias("median_house_price")
        )
    )


    # 12. Write Silver

    (
        house_prices_silver
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "uk_housing.silver.house_prices"
        )
    )

    print("House Prices Silver table created successfully")


except Exception as e:

    print("PIPELINE FAILED")
    print(str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

try:
    # 1. Read Silver data

    house_prices = spark.table("uk_housing.silver.house_prices")

    # 2. Select required fields

    fact_house_prices = (
        house_prices
        .select(
            "la_code",
            "median_house_price"
        )
    )
    
    # 3. Update existing records or insert new records

    if spark.catalog.tableExists("uk_housing.gold.fact_house_prices"):

        target = DeltaTable.forName(
            spark,
            "uk_housing.gold.fact_house_prices"
        )

        (
            target.alias("target")
            .merge(
                fact_house_prices.alias("source"),
                "target.la_code = source.la_code"
            )
            .whenMatchedUpdate(set={
                "median_house_price": "source.median_house_price"
            })
            .whenNotMatchedInsert(values={
                "la_code": "source.la_code",
                "median_house_price": "source.median_house_price"
            })
            .execute()
        )

    else:
        fact_house_prices.write.mode("overwrite").saveAsTable(
            "uk_housing.gold.fact_house_prices"
        )

    print("Gold House Prices fact updated successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise