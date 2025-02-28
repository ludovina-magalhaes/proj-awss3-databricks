# Databricks notebook source
# DBTITLE 1,Ler tabela da Camada Silver no SQL
# MAGIC %sql
# MAGIC select *
# MAGIC from bronze.onlineretail_bronze
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ler tabela da Camada Silver no PySpark
df_silver = spark.table('workspace.bronze.onlineretail_bronze')

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# DBTITLE 1,Eliminar nulos e duplicados
# Excluir nulos e duplicados
df_silver = df_silver.dropDuplicates()
df_silver = df_silver.dropna()

# COMMAND ----------

# DBTITLE 1,Criar uma coluna calculada
from pyspark.sql.functions import col
df_silver = df_silver.withColumn("TotalSales", col("Quantity") * col("UnitPrice"))

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# DBTITLE 1,Corrigir a coluna InvoiceData
from pyspark.sql.functions import split
# Extrair apenas a data da coluna  InvoiceData
df_silver = df_silver.withColumn("InvoiceDate", split(col("InvoiceDate"), " ")[0])

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# verificar os valores unicos da coluna InvoiceDate
df_silver.select("InvoiceDate").distinct().show()

# COMMAND ----------

# DBTITLE 1,Corrigir a data na coluna InvoiceDate
from pyspark.sql.functions import to_date

# Convert the date string to a proper timestamp format
df_silver = df_silver.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy"))

# Display the DataFrame
df_silver.display()  

# COMMAND ----------

# DBTITLE 1,Guardar o dataframe na Camada Silver
df_silver.write.mode("overwrite").saveAsTable("workspace.silver.onlineretail_silver")   