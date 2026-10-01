# Databricks notebook source
from pyspark.sql.functions import *

try:

    # 1. Create LA code mapping
    
    mapping_data = [
        ("E08000038", "E08000016", "Barnsley"),
        ("E08000039", "E08000019", "Sheffield")
    ]

    mapping = spark.createDataFrame(
        mapping_data,
        ["source_la_code", "gold_la_code", "la_name"]
    )

    # 2. Write Gold mapping table

    mapping.write.mode("overwrite").saveAsTable(
        "uk_housing.gold.la_code_mapping"
    )

    print("Gold LA code mapping created successfully")

except Exception as e:
    print("PIPELINE FAILED")
    print(str(e))
    raise