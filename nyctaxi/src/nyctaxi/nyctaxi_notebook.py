# Databricks notebook source
from pyspark.sql import functions as F

# --- parâmetros vindos do job (DABs) ---
dbutils.widgets.text("catalog", "dev")            # default só para dev local
dbutils.widgets.text("schema", "nyc_taxi_dabs")
dbutils.widgets.text("table",  "nyctaxi_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
table   = dbutils.widgets.get("table")

target_table = f"{catalog}.{schema}.{table}"


# 1) Ler tabela de amostra
df = spark.sql(
    f"""SELECT 
        date_format(DATE_TRUNC('month', tpep_pickup_datetime), 'MM/yyyy') AS month, 
        CASE 
            WHEN DAYOFWEEK(tpep_pickup_datetime) IN (1, 7) THEN 'Weekend' 
            ELSE 'Weekday' 
        END AS day_type, 
        CAST(SUM(fare_amount) AS decimal(20,2)) AS total_fare_amount 
    FROM 
    samples.nyctaxi.trips 
    GROUP BY 
        date_format(DATE_TRUNC('month', tpep_pickup_datetime), 'MM/yyyy'), 
        CASE 
            WHEN DAYOFWEEK(tpep_pickup_datetime) IN (1, 7) THEN 'Weekend' 
            ELSE 'Weekday' 
        END 
        ORDER BY 
        month, 
        day_type"""
)

(df.write
         .mode("overwrite")
         .saveAsTable(target_table))

print(f"Linhas gravadas: {df.count()}")
display(spark.table(target_table).limit(5))
