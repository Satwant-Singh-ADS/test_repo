from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the sample data
sample_data = [
    Row(Member="John", Date="2023-01-01", c1=10.5, c2=20.2, c3=30.3),
    Row(Member="John", Date="2023-01-05", c1=15.2, c2=25.5, c3=35.7),
    Row(Member="John", Date="2023-01-10", c1=5.7, c2=15.1, c3=25.9),
    Row(Member="John", Date="2023-01-15", c1=12.6, c2=22.9, c3=32.4),
    Row(Member="John", Date="2023-01-20", c1=8.3, c2=18.7, c3=28.1),
    Row(Member="John", Date="2023-01-25", c1=16.8, c2=28.4, c3=39.2),
    Row(Member="John", Date="2023-01-30", c1=10.2, c2=20.8, c3=31.1),
    Row(Member="John", Date="2023-02-05", c1=14.5, c2=24.3, c3=36.5),
    Row(Member="John", Date="2023-02-10", c1=6.9, c2=16.7, c3=26.8),
    Row(Member="John", Date="2023-02-15", c1=11.3, c2=21.5, c3=32.8)
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("Member", StringType(), nullable=False),
    StructField("Date", DateType(), nullable=False),
    StructField("c1", DoubleType(), nullable=False),
    StructField("c2", DoubleType(), nullable=False),
    StructField("c3", DoubleType(), nullable=False)
])

# Create the DataFrame
df = spark.createDataFrame(sample_data, schema)

# Show the DataFrame
df.show()


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_lookback(df, member_col, date_col, columns, lookback_days):
    windowSpecs = [Window.partitionBy(member_col).orderBy(date_col).rangeBetween(-(lookback_days-1), 0) for _ in range(len(columns))]

    for i, column in enumerate(columns):
        lookback_col_name = f"Last_{lookback_days}_Days_{column}"
        df = df.withColumn(lookback_col_name, F.sum(column).over(windowSpecs[i]))

    return df

# Define the column names and lookback days
member_col = "Member"
date_col = "Date"
columns = ["c1", "c2", "c3"]
lookback_days = 30

# Calculate the lookback values
df = calculate_lookback(df, member_col, date_col, columns, lookback_days)

# Show the resulting DataFrame
df.show()





from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define the window specifications for last 30, 60, and 90 days at member level
windowSpec_30 = Window.partitionBy("Member").orderBy("Date").rangeBetween(-29, 0)
windowSpec_60 = Window.partitionBy("Member").orderBy("Date").rangeBetween(-59, 0)
windowSpec_90 = Window.partitionBy("Member").orderBy("Date").rangeBetween(-89, 0)

# Calculate the lookback values for each column at member level
df = df.withColumn("Last_30_Days_c1", F.sum("c1").over(windowSpec_30))
df = df.withColumn("Last_60_Days_c1", F.sum("c1").over(windowSpec_60))
df = df.withColumn("Last_90_Days_c1", F.sum("c1").over(windowSpec_90))

df = df.withColumn("Last_30_Days_c2", F.sum("c2").over(windowSpec_30))
df = df.withColumn("Last_60_Days_c2", F.sum("c2").over(windowSpec_60))
df = df.withColumn("Last_90_Days_c2", F.sum("c2").over(windowSpec_90))

df = df.withColumn("Last_30_Days_c3", F.sum("c3").over(windowSpec_30))
df = df.withColumn("Last_60_Days_c3", F.sum("c3").over(windowSpec_60))
df = df.withColumn("Last_90_Days_c3", F.sum("c3").over(windowSpec_90))

# Show the resulting DataFrame
df.show()



