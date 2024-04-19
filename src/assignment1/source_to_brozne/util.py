# Databricks notebook source
from pyspark.sql.functions import udf, current_date

# COMMAND ----------

def read_csv(path):
  df = spark.read.csv(path, header=True, inferSchema=True)
  return df

# COMMAND ----------

def write_csv(df, path):
  df.write.format('csv').save(path)

# COMMAND ----------

def read_with_custom_schema(data, schema):
    df = spark.read.csv(data, schema)
    return df

# COMMAND ----------

# Camel to Snake case
def camel_to_snake_case(df):
    for cols in df.columns:
        df = df.withColumnRenamed(cols, cols.lower())
    return df


# COMMAND ----------

udf(camel_to_snake_case)

# COMMAND ----------

# adding current date
def add_current_date(df):
    df = df.withColumn("load_date", current_date())
    return df

# COMMAND ----------

