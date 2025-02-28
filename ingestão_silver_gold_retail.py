# Databricks notebook source
# DBTITLE 1,Ler os dados da tabela da Camada Silver
df_gold = spark.table("workspace.silver.onlineretail_silver")

# COMMAND ----------

df_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cálculo de Métricas

# COMMAND ----------

# DBTITLE 1,Vendas total por Cliente
from pyspark.sql.functions import sum, avg, count, round
sales_by_costumer = df_gold.groupBy("CustomerID").agg(
    round(sum("TotalSales"), 2).alias("TotalSales"),
    count("InvoiceNo").alias("NumPurchases"),
    round(avg("TotalSales"), 2).alias("AvgPurchasesValues")
)  

# COMMAND ----------

sales_by_costumer.display() 

# COMMAND ----------

# DBTITLE 1,Vendas por Produto
sales_per_product = df_gold.groupBy("StockCode").agg(
    round(sum("TotalSales"), 2).alias("TotalSales"),
    sum("Quantity").alias("TotalQuantitySold")
)

sales_per_product.display()

# COMMAND ----------

# DBTITLE 1,Venda Total por País
sales_per_country = df_gold.groupBy("Country").agg(
    round(sum("TotalSales"),2).alias("TotalSales"),
    sum("Quantity").alias("TotalQuantitySold")
)

sales_per_country.display()


# COMMAND ----------

# DBTITLE 1,Guardar as tabelas na Camada Gold
sales_per_country.write.mode("overwrite").saveAsTable("workspace.gold.sales_per_country")   
sales_by_costumer.write.mode("overwrite").saveAsTable("workspace.gold.sales_by_costumer")   
sales_per_product.write.mode("overwrite").saveAsTable("workspace.gold.sales_per_product")