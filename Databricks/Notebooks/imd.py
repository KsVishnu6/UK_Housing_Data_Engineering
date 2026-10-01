# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE

# COMMAND ----------

from pyspark.sql.functions import *

# 1. Read source data

imd_df = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .option("dataAddress", "IMD!A1:Z300")
    .option("inferSchema", True)
    .load("abfss://raw@ukhousingstorageadls.dfs.core.windows.net/File_10_-_IoD2025_Local_Authority_District_Summaries__lower-tier__v2.xlsx")
)

print(imd_df.columns)

# COMMAND ----------

# 2. Select required fields

imd_bronze = imd_df.select(
    col("Local Authority District code (2024)").alias("la_code"),
    col("Local Authority District name (2024)").alias("la_name"),
    col("IMD - Average score ").alias("imd_score"),
    col("IMD - Rank of average score ").alias("imd_rank")
)

# COMMAND ----------

# 3. Write Bronze data

imd_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_housing.bronze.imd")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER

# COMMAND ----------

from pyspark.sql.functions import *


try:

    # 1. Read Bronze

    imd = spark.table("uk_housing.bronze.imd")


    # 2. Check expected columns

    expected_columns = [
        "la_code",
        "la_name",
        "imd_score",
        "imd_rank"
    ]

    if imd.columns != expected_columns:
        raise Exception(
            f"COLUMN CHECK FAILED: Expected {expected_columns}, "
            f"found {imd.columns}"
        )


    # 3. Check Bronze data types

    expected_data_types = {
        "la_code": "string",
        "la_name": "string",
        "imd_score": "decimal(18,16)",
        "imd_rank": "bigint"
    }

    actual_data_types = dict(imd.dtypes)

    for column_name, expected_type in expected_data_types.items():

        if actual_data_types.get(column_name) != expected_type:
            raise Exception(
                f"DATA TYPE CHECK FAILED: '{column_name}' expected "
                f"{expected_type}, found "
                f"{actual_data_types.get(column_name)}"
            )


    # 4. Remove completely empty source rows

    imd_clean = imd.filter(
        ~(
            col("la_code").isNull() &
            col("la_name").isNull() &
            col("imd_score").isNull() &
            col("imd_rank").isNull()
        )
    )


    # 5. Check required nulls

    required_columns = [
        "la_code",
        "la_name",
        "imd_score",
        "imd_rank"
    ]

    for column_name in required_columns:

        if (
            imd_clean
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

    for column_name in ["la_code", "la_name"]:

        if (
            imd_clean
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
        imd_clean
        .filter(
            ~col("la_code").rlike("^E[0-9]{8}$")
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid la_code found"
        )


    # 8. Check LA code uniqueness

    if (
        imd_clean
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


    # 9. Check LA code / name consistency

    if (
        imd_clean
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


    # 10. Check IMD rank

    if (
        imd_clean
        .filter(col("imd_rank") <= 0)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid IMD rank found"
        )


    # 11. Check IMD rank uniqueness

    if (
        imd_clean
        .groupBy("imd_rank")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "UNIQUENESS CHECK FAILED: Duplicate IMD rank found"
        )


    # 12. Check IMD score

    if (
        imd_clean
        .filter(col("imd_score") < 0)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Negative IMD score found"
        )


    # 13. Build Silver

    imd_silver = (
        imd_clean
        .select(
            col("la_code").cast("string").alias("la_code"),
            col("la_name").cast("string").alias("la_name"),
            col("imd_score")
                .cast("decimal(10,3)")
                .alias("imd_score"),
            col("imd_rank")
                .cast("long")
                .alias("imd_rank")
        )
    )


    # 14. Write Silver

    (
        imd_silver
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("uk_housing.silver.imd")
    )

    print("IMD Silver table created successfully")


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

    imd = spark.table("uk_housing.silver.imd")

    # 2. Select required fields

    fact_imd = (
        imd
        .select(
            "la_code",
            "imd_score",
            "imd_rank"
        )
    )
    
    # 3. Update existing records or insert new records

    if spark.catalog.tableExists("uk_housing.gold.fact_imd"):

        target = DeltaTable.forName(
            spark,
            "uk_housing.gold.fact_imd"
        )

        (
            target.alias("target")
            .merge(
                fact_imd.alias("source"),
                "target.la_code = source.la_code"
            )
            .whenMatchedUpdate(set={
                "imd_score": "source.imd_score",
                "imd_rank": "source.imd_rank"
            })
            .whenNotMatchedInsert(values={
                "la_code": "source.la_code",
                "imd_score": "source.imd_score",
                "imd_rank": "source.imd_rank"
            })
            .execute()
        )

    else:
        fact_imd.write.mode("overwrite").saveAsTable(
            "uk_housing.gold.fact_imd"
        )

    print("Gold IMD fact updated successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise