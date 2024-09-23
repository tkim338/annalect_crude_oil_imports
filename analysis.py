import pyspark
from pyspark.sql import SparkSession
import kaggle

kaggle.api.dataset_download_files(
    dataset = "alistairking/u-s-crude-oil-imports", 
    path="us-crude-oil",
    unzip=True
)

spark = SparkSession.builder.master("local").getOrCreate()

df = spark.read.csv(
    "us-crude-oil", 
    header=True
)
df = df.withColumn(
    "quantity",
    df["quantity"].cast("int")
)


print("1. What are the top 5 destinations for oil produced in Albania?")

df.where("originName = 'Albania'") \
    .groupBy("destinationName") \
    .sum("quantity") \
    .orderBy("sum(quantity)", ascending=False) \
    .show()


print("2. For UK, which destinations have a total quantity greater than 100,000?")

df.where("originName = 'United Kingdom'") \
    .groupBy("destinationName") \
    .sum("quantity") \
    .where("sum(quantity) > 100000") \
    .show()


print("3. What was the most exported grade for each year and origin?")

grade_df = df.groupBy(["year", "originName", "gradeName"]) \
    .sum("quantity")
grade_df = grade_df.withColumn(
    "quantity",
    grade_df["sum(quantity)"]
)

max_qty_df = grade_df.groupBy(["year", "originName"]) \
    .max("sum(quantity)")
max_qty_df = max_qty_df.withColumn(
    "quantity",
    max_qty_df["max(sum(quantity))"]
)

max_qty_df.join(
    other=grade_df,
    on=["year", "originName", "quantity"]
).select([
    "year",
    "originName",
    "gradeName"
]).show()