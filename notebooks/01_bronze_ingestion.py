# Databricks notebook source
dbutils.fs.ls('/Volumes/my_data/default/my_data')

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType,TimestampType
schema = StructType([
    StructField("transaction_id", StringType(),  False),
    StructField("customer_id", StringType(), False),
    StructField("amount",           DoubleType(), False),
    StructField("currency",       StringType(),  False),
    StructField("status",         StringType(),  False),
    StructField("payment_type",   StringType(),  False),
    StructField("merchant",       StringType(),  False),
    StructField("timestamp",      StringType(),  True)
]
)

transaction_df= spark.read\
    .option("header", "true")\
        .option("encoding", "utf-8")\
            .schema(schema)\
                .csv("/Volumes/my_data/default/my_data/transactions.csv")





# COMMAND ----------

print(f"Loaded {transaction_df.count()} transactions")
transaction_df.printSchema


# COMMAND ----------

customers_df = spark.read\
    .option("header", "true")\
        .option("encoding", "utf-8")\
            .option("inferSchema", "true")\
                .csv("/Volumes/my_data/default/my_data/customers.csv")

print(f"Loaded {customers_df.count()} customers")


# COMMAND ----------

fx_rates_df = spark.read\
    .option("header", "true")\
        .option("inferSchema", "true")\
            .option("encoding", "utf-8")\
            .csv("/Volumes/my_data/default/my_data/fx_rates.csv")

print(f"Loaded {fx_rates_df.count()} fx rates")
print(fx_rates_df.head())


# COMMAND ----------

(
    transaction_df.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_transactions")
)
(
    customers_df.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_customers")
    )



(
    fx_rates_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.raw_fx_rates")
)

print("Bronze Delta tables created successfully")


# COMMAND ----------

spark.sql("SHOW TABLES IN bronze").show()

# COMMAND ----------

print("Transaction:" ,spark.table("bronze.raw_transactions").count(),"count")
print("Customers:" ,spark.table("bronze.raw_customers").count(),"count")
print("FX_Rates:" ,spark.table("bronze.raw_fx_rates").count(),"count")

# COMMAND ----------

spark.sql("DESCRIBE HISTORY bronze.raw_transactions").show(truncate=False)