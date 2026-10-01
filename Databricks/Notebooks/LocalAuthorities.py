# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE

# COMMAND ----------

# 1. Read source data

a1_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("abfss://raw@ukhousingstorageadls.dfs.core.windows.net/uk_local_authorities_current.csv")
)

display(a1_df)

# COMMAND ----------

# 2. Select required fields

a1_bronze = a1_df.select(
    col("local-authority-code"),
    col("official-name"),
    col("nice-name"),
    col("gss-code"),
    col("start-date"),
    col("end-date"),
    col("replaced-by"),
    col("nation"),
    col("region"),
    col("local-authority-type"),
    col("local-authority-type-name"),
    col("current-authority")
)

# COMMAND ----------

# 3. Remove combined authorities

a1_bronze = a1_bronze.filter(
    col("local-authority-type") != "COMB"
)

# COMMAND ----------

# 4. Write Bronze data

a1_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_housing.bronze.local_authorities")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER

# COMMAND ----------

from pyspark.sql.functions import *


try:

    # 1. Read Bronze

    la_df = spark.table("uk_housing.bronze.local_authorities")


    # 2. Check expected columns

    expected_columns = [
        "local-authority-code",
        "official-name",
        "nice-name",
        "gss-code",
        "start-date",
        "end-date",
        "replaced-by",
        "nation",
        "region",
        "local-authority-type",
        "local-authority-type-name",
        "current-authority"
    ]

    if la_df.columns != expected_columns:
        raise Exception(
            f"COLUMN CHECK FAILED: Expected {expected_columns}, "
            f"found {la_df.columns}"
        )


    # 3. Check Bronze data types

    expected_data_types = {
        "local-authority-code": "string",
        "official-name": "string",
        "nice-name": "string",
        "gss-code": "string",
        "start-date": "date",
        "end-date": "date",
        "replaced-by": "string",
        "nation": "string",
        "region": "string",
        "local-authority-type": "string",
        "local-authority-type-name": "string",
        "current-authority": "boolean"
    }

    actual_data_types = dict(la_df.dtypes)

    for column_name, expected_type in expected_data_types.items():

        if actual_data_types.get(column_name) != expected_type:
            raise Exception(
                f"DATA TYPE CHECK FAILED: '{column_name}' expected "
                f"{expected_type}, found {actual_data_types.get(column_name)}"
            )


    # 4. Check required nulls

    required_not_null = [
        "local-authority-code",
        "official-name",
        "nice-name",
        "nation",
        "local-authority-type",
        "local-authority-type-name",
        "current-authority"
    ]

    for column_name in required_not_null:

        if la_df.filter(col(column_name).isNull()).limit(1).count() > 0:
            raise Exception(
                f"NULL CHECK FAILED: '{column_name}' contains null values"
            )


    # 5. Check blank strings

    string_columns = [
        "local-authority-code",
        "official-name",
        "nice-name",
        "gss-code",
        "replaced-by",
        "nation",
        "region",
        "local-authority-type",
        "local-authority-type-name"
    ]

    for column_name in string_columns:

        if (
            la_df
            .filter(
                col(column_name).isNotNull() &
                (trim(col(column_name)) == "")
            )
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"BLANK CHECK FAILED: '{column_name}' contains blank values"
            )


    # 6. Check LA ID uniqueness

    if (
        la_df
        .groupBy("local-authority-code")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "UNIQUENESS CHECK FAILED: Duplicate local-authority-code found"
        )


    # 7. Check GSS code uniqueness

    if (
        la_df
        .filter(col("gss-code").isNotNull())
        .groupBy("gss-code")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "UNIQUENESS CHECK FAILED: Duplicate gss-code found"
        )


    # 8. Check GSS code format

    if (
        la_df
        .filter(
            col("gss-code").isNotNull() &
            ~col("gss-code").rlike("^[A-Z][0-9]{8}$")
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid gss-code format found"
        )


    # 9. Check nation values

    expected_nations = [
        "England",
        "Scotland",
        "Wales",
        "Northern Ireland"
    ]

    if (
        la_df
        .filter(~col("nation").isin(expected_nations))
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Unexpected nation value found"
        )


    # 10. Check local authority types

    expected_authority_types = [
        "CC",
        "CTY",
        "LBO",
        "MD",
        "NID",
        "NMD",
        "SCO",
        "SRA",
        "UA",
        "WPA"
    ]

    if (
        la_df
        .filter(
            ~col("local-authority-type").isin(expected_authority_types)
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Unexpected local-authority-type found"
        )


    # 11. Check authority type names

    type_name_check = (
        la_df
        .groupBy(
            "local-authority-type",
            "local-authority-type-name"
        )
        .count()
    )

    if (
        type_name_check
        .groupBy("local-authority-type")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "CONSISTENCY CHECK FAILED: "
            "local-authority-type has multiple type names"
        )


    # 12. Check date logic

    if (
        la_df
        .filter(
            col("start-date").isNotNull() &
            col("end-date").isNotNull() &
            (col("end-date") < col("start-date"))
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "DATE CHECK FAILED: end-date is before start-date"
        )


    # 13. Check current authority / end date

    if (
        la_df
        .filter(
            (col("current-authority") == True) &
            col("end-date").isNotNull()
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Current authority has an end-date"
        )


    # 14. Check non-current authority / end date

    if (
        la_df
        .filter(
            (col("current-authority") == False) &
            col("end-date").isNull()
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Non-current authority has no end-date"
        )


    # 15. Keep England for the analytical model

    la_df = la_df.filter(col("nation") == "England")


    # 16. Rename columns

    la_silver = (
        la_df
        .withColumnRenamed("local-authority-code", "la_id")
        .withColumnRenamed("official-name", "official_name")
        .withColumnRenamed("nice-name", "la_name")
        .withColumnRenamed("gss-code", "la_code")
        .withColumnRenamed("start-date", "start_date")
        .withColumnRenamed("end-date", "end_date")
        .withColumnRenamed("replaced-by", "replaced_by")
        .withColumnRenamed("local-authority-type", "local_authority_type")
        .withColumnRenamed(
            "local-authority-type-name",
            "local_authority_type_name"
        )
        .withColumnRenamed("current-authority", "current_authority")
    )


    # 17. Write Silver

    (
        la_silver
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("uk_housing.silver.local_authorities")
    )

    print("Local Authorities Silver table created successfully")


except Exception as e:

    print("PIPELINE FAILED")
    print(str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD

# COMMAND ----------

# MAGIC %md
# MAGIC DIMENSION

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

try:

    # 1. Read Silver data

    local_authorities = spark.table("uk_housing.silver.local_authorities")

    # 2. Select required fields

    dim_local_authority = (
        local_authorities
        .select(
            col("la_code"),
            col("la_name"),
            col("official_name"),
            col("nation"),
            col("region"),
            col("local_authority_type"),
            col("local_authority_type_name"),
            col("current_authority"),
            col("start_date"),
            col("end_date"),
            col("replaced_by")
        )
    )

    # 3. Update existing records or insert new records

    if spark.catalog.tableExists("uk_housing.gold.dim_local_authority"):

        target = DeltaTable.forName(
            spark,
            "uk_housing.gold.dim_local_authority"
        )

        (
            target.alias("target")
            .merge(
                dim_local_authority.alias("source"),
                "target.la_code = source.la_code"
            )
            .whenMatchedUpdate(set={
                "la_name": "source.la_name",
                "official_name": "source.official_name",
                "nation": "source.nation",
                "region": "source.region",
                "local_authority_type": "source.local_authority_type",
                "local_authority_type_name": "source.local_authority_type_name",
                "current_authority": "source.current_authority",
                "start_date": "source.start_date",
                "end_date": "source.end_date",
                "replaced_by": "source.replaced_by"
            })
            .whenNotMatchedInsert(values={
                "la_code": "source.la_code",
                "la_name": "source.la_name",
                "official_name": "source.official_name",
                "nation": "source.nation",
                "region": "source.region",
                "local_authority_type": "source.local_authority_type",
                "local_authority_type_name": "source.local_authority_type_name",
                "current_authority": "source.current_authority",
                "start_date": "source.start_date",
                "end_date": "source.end_date",
                "replaced_by": "source.replaced_by"
            })
            .execute()
        )

    else:
        dim_local_authority.write.mode("overwrite").saveAsTable(
            "uk_housing.gold.dim_local_authority"
        )

    print("Gold Local Authority dimension updated successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise