# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %run "/Users/smilingdavid001@gmail.com/assignment/assignment1/source_to_brozne/util"

# COMMAND ----------

# Employee Schema
employee_custom_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])

# COMMAND ----------

# Country schema
country_custom_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])


# COMMAND ----------

# Department Schema
department_custom_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])

# COMMAND ----------

employee_path = "/FileStore/for_assignment_files/Employee_Q1.csv"
employee_path = "/FileStore/for_assignment_files/Department_Q1.csv"
country_path = "/FileStore/for_assignment_files/Country_Q1.csv"

# COMMAND ----------

employee_df = read_with_custom_schema(employee_path,employee_custom_schema)
country_df = read_with_custom_schema(employee_path, country_custom_schema)
department_df = read_with_custom_schema(country_path, department_custom_schema)

# COMMAND ----------

employee_snake_case_df = camel_to_snake_case(employee_df)

# COMMAND ----------

department_snake_case_df = camel_to_snake_case(department_df)

# COMMAND ----------

country_snake_case_df = camel_to_snake_case(country_df)

# COMMAND ----------

# Adding load_date in df
employee_with_date_df = add_current_date(employee_snake_case_df)

# COMMAND ----------

department_with_date_df = add_current_date(department_snake_case_df)

# COMMAND ----------

country_with_date_df = add_current_date(country_snake_case_df)

# COMMAND ----------

# creating employee_info database
spark.sql('create database employee_info')
spark.sql('use employee_info')

# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignments/questoin1/silver/employee_info/dim_employee').saveAsTable('dim_employee')

# COMMAND ----------

