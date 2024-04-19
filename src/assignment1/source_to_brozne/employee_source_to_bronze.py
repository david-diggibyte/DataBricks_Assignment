# Databricks notebook source
# MAGIC %run "/Users/smilingdavid001@gmail.com/assignment/assignment1/source_to_brozne/util"

# COMMAND ----------

#path 
employee_path = "/FileStore/for_assignment_files/Employee_Q1.csv"
department_path = "/FileStore/for_assignment_files/Department_Q1.csv"
country_path = "/FileStore/for_assignment_files/Country_Q1.csv"

# COMMAND ----------

employee_df = read_csv(employee_path)
department_df = read_csv(department_path)
country_df = read_csv(country_path)

# COMMAND ----------

# Write to Location
employee_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/employee'
department_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/department'
country_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/country'

# COMMAND ----------

write_csv(employee_df, employee_write_path)
write_csv(department_df, department_write_path)
write_csv(country_df, country_write_path)