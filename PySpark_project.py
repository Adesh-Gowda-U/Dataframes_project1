# Installing required packages
!pip install pyspark
!pip install findspark
!pip install pandas

import findspark
findspark.init()

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
	
# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('csv_file')

# Preview a few records
mtcars.head()

# We use the `createDataFrame` function to load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars)

# Let us look at the schema of the loaded spark dataframe
sdf.printSchema()

#We use the select() function to select a particular column of data. Here we show the mpg column.
sdf.select('mpg').show(5)

#We first filter to only retain rows with mpg > 18. We use the filter() function for this.
sdf.filter(sdf['mpg'] < 18).show(5)

#We create a new column called wtTon that has the weight from the wt column converted to metric tons.
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

#we compute the average weight of cars by their cylinders as shown below.
sdf.groupby(['cyl'])\
.agg({"wt": "AVG"})\
.show(5)

#We can also sort the output from the aggregation to get the most common cars.
car_counts = sdf.groupby(['cyl'])\
.agg({"wt": "count"})\
.sort("count(wt)", ascending=False)\
.show(5)

