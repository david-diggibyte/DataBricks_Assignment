**Databricks Assignment**

**Question 1:**


1.Create 3 folders as source_to_bronze, bronze_to_silver, silver_to_gold.

2.Create 4 notebooks in this respective order.

* 2 Notebooks named in source_to_bronze as utils 
* 1 Notebook in bronze to silver as employee_bronze_to_silver 
* 1 Notebook in silver to gold as employee_silver_to_gold

3.Read the 3 datasets as Dataframe in employee_source_to_bronze, call utils notebook in this notebook, and write to a location in DBFS,
as /source_to_bronze/file_name.csv (employee, department_df, country_df) as CSV format.

4.In employee_bronze_to_silver, call utils notebook in this notebook.
Read the file located in DBFS location source_to_bronze with as data frame different read methods using custom schema.

5.convert the Camel case of the columns to the snake case using UDF.

6.Add the load_date column with the current date.

•	The primary key is EmployeeID, the Database name is Employee_info, Table name is dim_employee.

•	write the DF as a delta table to the location /silver/db_name/table_name.

7.In gold notebook employee_silver_to_gold, call utils notebook in this notebook
 Read the table stored in a silver layer as DataFrame and select the columns based on the following requirements.

8.Requirements:

•	Find the salary of each department in descending order.

•	Find the number of employees in each department located in each country.

•	List the department names along with their corresponding country names.

•	What is the average age of employees in each department?

•	Add the at_load_date column to data frames.

•	Write the df to dbfs location /gold/employee/table_name(fact_employee) with overwrite and replace where condition on at_load_date.


**Question 2:**


1.Fetch the data from the given API by passing the parameter as a page and retrieving the data till the data is empty.

2.Read the data frame with a custom schema

3.Flatten the dataframe

4.Derive a new column from email as site_address with values(reqres.in)

5.Add load_date with the current date

6.Write the data frame to location in DBFS as /db_name /table_name with 
Db_name as site_info and table_name as person_info with delta format and overwrite mode.
