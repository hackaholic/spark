import pyspark

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ch06") \
        .master("yarn") \
        .getOrCreate()

    df = spark.range(1000)

    print(df.where("id > 500").selectExpr("sum(id)").collect())

# spark-submit --master yarn --deploy-mode client ch16.py