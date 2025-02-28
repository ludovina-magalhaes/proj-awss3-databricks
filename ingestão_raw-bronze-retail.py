# Databricks notebook source
# DBTITLE 1,Carregamento de dados do csv
# Link: /Volumes/workspace/project_data/raw/OnlineRetail.csv

# Carregar os dados num dataframe Spark
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/workspace/project_data/raw/OnlineRetail.csv")

# COMMAND ----------

# DBTITLE 1,Exibir os dados
df.display()

# COMMAND ----------

# DBTITLE 1,Verificar os tipos de dados
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Alterar o tipo de dados
df = df.withColumn("CustomerID" , df["CustomerID"].cast("string"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Salvar os dados na Camada Silver

df.write.mode("overwrite").saveAsTable("Workspace.bronze.onlineretail_bronze")