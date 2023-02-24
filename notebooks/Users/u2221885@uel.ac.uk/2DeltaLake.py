# Databricks notebook source
#This is ADLS Gen-2 accountname and access key details
storageaccount=#enter the storage account before you run the notebook
acct_info=f"fs.azure.account.key.{storageaccount}.dfs.core.windows.net"
accesskey=#enter the access key before you run the notebook
print(acct_info)

# COMMAND ----------

#setting account credentials in notebook session configs
spark.conf.set(
acct_info,
accesskey)

# COMMAND ----------

cust_path = "abfss://rawdata@{storageaccount}.dfs.core.windows.net/customers/parquetFiles"

# COMMAND ----------

df_cust = (spark.read.format("parquet").load(cust_path))
df_cust.write.format("delta").mode("overwrite").save("abfss://rawdata@{storageaccount}.dfs.core.windows.net/customers/delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating Delta table
# MAGIC DROP TABLE IF EXISTS Customer;
# MAGIC CREATE TABLE Customer
# MAGIC USING delta
# MAGIC location "abfss://rawdata@{storageaccount}.dfs.core.windows.net/customers/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted Customer

# COMMAND ----------

ord_path = "abfss://rawdata@{storageaccount}.dfs.core.windows.net/orders/parquetFiles"
df_ord = (spark.read.format("parquet").load(ord_path))
df_ord.write.format("delta").mode("overwrite").save("abfss://rawdata@{storageaccount}.dfs.core.windows.net/orders/delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Orders;
# MAGIC CREATE TABLE Orders
# MAGIC USING delta
# MAGIC location "abfss://rawdata@{storageaccount}.dfs.core.windows.net/orders/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Getting total customer based on priority and total account balance by joining the delta tables
# MAGIC SELECT o.O_ORDERPRIORITY,count(o.O_CUSTKEY) As TotalCustomer,Sum(c.C_ACCTBAL) As CustAcctBal
# MAGIC FROM Orders o INNER JOIN Customer c on o.O_CUSTKEY=c.C_CUSTKEY
# MAGIC WHERE o.O_ORDERDATE>'1997-12-31'
# MAGIC GROUP BY o.O_ORDERPRIORITY
# MAGIC ORDER BY TotalCustomer DESC;

# COMMAND ----------

