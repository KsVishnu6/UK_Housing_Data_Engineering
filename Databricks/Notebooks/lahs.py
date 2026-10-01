# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE

# COMMAND ----------

# 1. Read source data

lahs_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("abfss://raw@ukhousingstorageadls.dfs.core.windows.net/LAHS_open_data_1978-79_to_2024-25.csv")
)

# COMMAND ----------

# 2. Select required fields

lahs_bronze = lahs_df.select(
    "LAD24CD",
    "LAD24NM",
    "Year",
    "status",
    "a1a",
    "cc1a"
)

lahs_bronze.show(10, truncate=False)

# COMMAND ----------

# 3. Write Bronze data

lahs_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_housing.bronze.lahs")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER

# COMMAND ----------

from pyspark.sql.functions import *


try:

    # 1. Read Bronze

    lahs = spark.table("uk_housing.bronze.lahs")


    # 2. Check expected columns

    expected_columns = [
        "LAD24CD",
        "LAD24NM",
        "Year",
        "status",
        "a1a",
        "cc1a"
    ]

    if lahs.columns != expected_columns:
        raise Exception(
            f"COLUMN CHECK FAILED: Expected {expected_columns}, "
            f"found {lahs.columns}"
        )


    # 3. Check Bronze data types

    expected_data_types = {
        "LAD24CD": "string",
        "LAD24NM": "string",
        "Year": "string",
        "status": "string",
        "a1a": "string",
        "cc1a": "string"
    }

    actual_data_types = dict(lahs.dtypes)

    for column_name, expected_type in expected_data_types.items():

        if actual_data_types.get(column_name) != expected_type:
            raise Exception(
                f"DATA TYPE CHECK FAILED: '{column_name}' expected "
                f"{expected_type}, found "
                f"{actual_data_types.get(column_name)}"
            )


    # 4. Keep 2024-25 only

    lahs_2425 = lahs.filter(
        col("Year") == "2024-25"
    )

    if lahs_2425.limit(1).count() == 0:
        raise Exception(
            "SCOPE CHECK FAILED: No 2024-25 LAHS records found"
        )


    # 5. Check required nulls

    required_columns = [
        "LAD24CD",
        "LAD24NM",
        "Year",
        "status",
        "a1a",
        "cc1a"
    ]

    for column_name in required_columns:

        if (
            lahs_2425
            .filter(col(column_name).isNull())
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"NULL CHECK FAILED: "
                f"'{column_name}' contains null values"
            )


    # 6. Check blank strings

    for column_name in required_columns:

        if (
            lahs_2425
            .filter(trim(col(column_name)) == "")
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"BLANK CHECK FAILED: "
                f"'{column_name}' contains blank values"
            )


    # 7. Check LA code format

    if (
        lahs_2425
        .filter(
            ~col("LAD24CD").rlike("^E[0-9]{8}$")
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid LAD24CD found"
        )


    # 8. Check LA code uniqueness

    if (
        lahs_2425
        .groupBy("LAD24CD")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "UNIQUENESS CHECK FAILED: Duplicate LAD24CD found"
        )


    # 9. Check status values

    allowed_status = [
        "Submitted",
        "Saved"
    ]

    if (
        lahs_2425
        .filter(~col("status").isin(allowed_status))
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Unexpected status value found"
        )


    # 10. Check a1a format

    valid_value_pattern = r"^[0-9]+(,[0-9]{3})*$"

    if (
        lahs_2425
        .filter(
            ~col("a1a").rlike(valid_value_pattern)
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE FORMAT CHECK FAILED: Invalid a1a value found"
        )


    # 11. Check cc1a format

    if (
        lahs_2425
        .filter(
            ~col("cc1a").rlike(valid_value_pattern)
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE FORMAT CHECK FAILED: Invalid cc1a value found"
        )


    # 12. Check non-negative values

    if (
        lahs_2425
        .filter(
            regexp_replace(
                col("a1a"),
                ",",
                ""
            ).cast("long") < 0
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Negative a1a value found"
        )


    if (
        lahs_2425
        .filter(
            regexp_replace(
                col("cc1a"),
                ",",
                ""
            ).cast("long") < 0
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Negative cc1a value found"
        )


    # 13. Build Silver

    lahs_silver = (
        lahs_2425
        .select(
            col("LAD24CD").cast("string").alias("la_code"),
            col("LAD24NM").cast("string").alias("la_name"),
            col("Year").cast("string").alias("year"),
            col("status").cast("string").alias("status"),
            regexp_replace(
                col("a1a"),
                ",",
                ""
            ).cast("long").alias("a1a"),
            regexp_replace(
                col("cc1a"),
                ",",
                ""
            ).cast("long").alias("cc1a")
        )
    )


    # 14. Write Silver

    (
        lahs_silver
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "uk_housing.silver.lahs"
        )
    )

    print("LAHS Silver table created successfully")


except Exception as e:

    print("PIPELINE FAILED")
    print(str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

try:

    # 1. Read Silver data

    lahs = spark.table("uk_housing.silver.lahs")
    
    # 2. Select required fields

    fact_lahs = (
        lahs
        .select(
            "la_code",
            "year",
            "status",
            "a1a",
            "cc1a"
        )
    )

    # 3. Update existing records or insert new records
    
    if spark.catalog.tableExists("uk_housing.gold.fact_lahs"):

        target = DeltaTable.forName(
            spark,
            "uk_housing.gold.fact_lahs"
        )

        (
            target.alias("target")
            .merge(
                fact_lahs.alias("source"),
                """
                target.la_code = source.la_code
                AND target.year = source.year
                """
            )
            .whenMatchedUpdate(set={
                "status": "source.status",
                "a1a": "source.a1a",
                "cc1a": "source.cc1a"
            })
            .whenNotMatchedInsert(values={
                "la_code": "source.la_code",
                "year": "source.year",
                "status": "source.status",
                "a1a": "source.a1a",
                "cc1a": "source.cc1a"
            })
            .execute()
        )

    else:
        fact_lahs.write.mode("overwrite").saveAsTable(
            "uk_housing.gold.fact_lahs"
        )

    print("Gold LAHS fact updated successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise