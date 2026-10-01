# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE

# COMMAND ----------

from pyspark.sql.functions import *

# 1. Read source data

homelessness_df = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("multiLine", True)
    .csv("abfss://raw@ukhousingstorageadls.dfs.core.windows.net/Statutory_Homelessness_Detailed_Local_Authority_Data_2024-2025_corrected-A1.csv")
)

# COMMAND ----------

# 2. Select English local-authority records and required fields

homelessness_bronze = (
    homelessness_df
    .filter(col("_c0").rlike("^E(06|07|08|09)[0-9]{6}$"))
    .select(
        col("_c0").alias("la_code"),
        col("_c1").alias("la_name"),
        col("_c4").alias("total_households_assessed"),
        col("_c6").alias("total_duty_households"),
        col("_c7").alias("prevention_duty_households"),
        col("_c9").alias("relief_duty_households")
    )
)

# COMMAND ----------

# 3. Write Bronze data

homelessness_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("uk_housing.bronze.homelessness")

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER

# COMMAND ----------

from pyspark.sql.functions import *


try:

    # 1. Read Bronze

    homelessness_df = spark.table(
        "uk_housing.bronze.homelessness"
    )


    # 2. Check expected columns

    expected_columns = [
        "la_code",
        "la_name",
        "total_households_assessed",
        "total_duty_households",
        "prevention_duty_households",
        "relief_duty_households"
    ]

    if homelessness_df.columns != expected_columns:
        raise Exception(
            f"COLUMN CHECK FAILED: Expected {expected_columns}, "
            f"found {homelessness_df.columns}"
        )


    # 3. Check Bronze data types

    expected_data_types = {
        "la_code": "string",
        "la_name": "string",
        "total_households_assessed": "string",
        "total_duty_households": "string",
        "prevention_duty_households": "string",
        "relief_duty_households": "string"
    }

    actual_data_types = dict(homelessness_df.dtypes)

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
        "total_households_assessed",
        "total_duty_households",
        "prevention_duty_households",
        "relief_duty_households"
    ]

    for column_name in required_columns:

        if (
            homelessness_df
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

    for column_name in required_columns:

        if (
            homelessness_df
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
        homelessness_df
        .filter(
            ~col("la_code").rlike(
                "^E(06|07|08|09)[0-9]{6}$"
            )
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "VALUE CHECK FAILED: Invalid English "
            "local authority code found"
        )


    # 7. Check LA code uniqueness

    if (
        homelessness_df
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


    # 8. Check household value formats

    household_columns = [
        "total_households_assessed",
        "total_duty_households",
        "prevention_duty_households",
        "relief_duty_households"
    ]

    valid_household_pattern = (
        r"^[0-9]+(,[0-9]{3})*$|^\.\.$"
    )

    for column_name in household_columns:

        if (
            homelessness_df
            .filter(
                ~col(column_name).rlike(valid_household_pattern)
            )
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"VALUE FORMAT CHECK FAILED: "
                f"Invalid value found in '{column_name}'"
            )


    # 9. Check negative values

    for column_name in household_columns:

        if (
            homelessness_df
            .filter(
                ~col(column_name).eqNullSafe("..") &
                (
                    regexp_replace(
                        col(column_name),
                        ",",
                        ""
                    ).cast("long") < 0
                )
            )
            .limit(1)
            .count()
            > 0
        ):
            raise Exception(
                f"VALUE CHECK FAILED: "
                f"Negative value found in '{column_name}'"
            )


    # 10. Check household business rule

    numeric_rows = homelessness_df.filter(
        col("total_duty_households").rlike(
            r"^[0-9]+(,[0-9]{3})*$"
        ) &
        col("prevention_duty_households").rlike(
            r"^[0-9]+(,[0-9]{3})*$"
        ) &
        col("relief_duty_households").rlike(
            r"^[0-9]+(,[0-9]{3})*$"
        )
    )

    if (
        numeric_rows
        .filter(
            regexp_replace(
                col("total_duty_households"),
                ",",
                ""
            ).cast("long")
            !=
            (
                regexp_replace(
                    col("prevention_duty_households"),
                    ",",
                    ""
                ).cast("long")
                +
                regexp_replace(
                    col("relief_duty_households"),
                    ",",
                    ""
                ).cast("long")
            )
        )
        .limit(1)
        .count()
        > 0
    ):
        raise Exception(
            "BUSINESS RULE CHECK FAILED: "
            "total_duty_households does not equal "
            "prevention_duty_households + "
            "relief_duty_households"
        )


    # 11. Create Silver DataFrame

    homelessness_silver = homelessness_df.select(
        "la_code",
        "la_name",
        "total_households_assessed",
        "total_duty_households",
        "prevention_duty_households",
        "relief_duty_households"
    )


    # 12. Write Silver

    (
        homelessness_silver
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("uk_housing.silver.homelessness")
    )

    print("Homelessness Silver table created successfully")


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

    homelessness = spark.table("uk_housing.silver.homelessness")

    # 2. Select required fields and convert household values to numeric

    fact_homelessness = (
        homelessness
        .select(
            col("la_code"),
            expr("try_cast(regexp_replace(total_households_assessed, ',', '') as long)").alias("total_households_assessed"),
            expr("try_cast(regexp_replace(total_duty_households, ',', '') as long)").alias("total_duty_households"),
            expr("try_cast(regexp_replace(prevention_duty_households, ',', '') as long)").alias("prevention_duty_households"),
            expr("try_cast(regexp_replace(relief_duty_households, ',', '') as long)").alias("relief_duty_households")
        )
    )

    # 3. Update existing records or insert new records

    if spark.catalog.tableExists("uk_housing.gold.fact_homelessness"):

        target = DeltaTable.forName(
            spark,
            "uk_housing.gold.fact_homelessness"
        )

        (
            target.alias("target")
            .merge(
                fact_homelessness.alias("source"),
                "target.la_code = source.la_code"
            )
            .whenMatchedUpdate(set={
                "total_households_assessed": "source.total_households_assessed",
                "total_duty_households": "source.total_duty_households",
                "prevention_duty_households": "source.prevention_duty_households",
                "relief_duty_households": "source.relief_duty_households"
            })
            .whenNotMatchedInsert(values={
                "la_code": "source.la_code",
                "total_households_assessed": "source.total_households_assessed",
                "total_duty_households": "source.total_duty_households",
                "prevention_duty_households": "source.prevention_duty_households",
                "relief_duty_households": "source.relief_duty_households"
            })
            .execute()
        )

    else:
        fact_homelessness.write.mode("overwrite").saveAsTable(
            "uk_housing.gold.fact_homelessness"
        )

    print("Gold Homelessness fact updated successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise