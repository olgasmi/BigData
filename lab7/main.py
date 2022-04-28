from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col


def main():
    spark = SparkSession.builder.master("local[*]").appName("lab7").getOrCreate()
    names = spark.read.csv('names.csv', header=True)
    names = names.withColumn("height_in_feet", names.height / 30.48) \
        .withColumn("surname", split(col("name"), ' ').getItem(1)) \
        .withColumn("name", split(col("name"), ' ').getItem(0))
    names.show()


if __name__ == '__main__':
    main()
