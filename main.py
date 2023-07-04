from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define the start date and lookback days
start_date = '2022-12-15'
lookback_days = 15

# Convert the start date to a PySpark DateType
start_date = F.to_date(F.lit(start_date))

# Check if a row with the start date exists for each ID
id_with_start_date = df.filter(F.col("Date") == start_date).select("ID").distinct()

# Filter IDs that don't have a row with the start date
missing_ids = df.select("ID").distinct().subtract(id_with_start_date)

# Create a DataFrame with the dummy rows for missing IDs
dummy_df = missing_ids.withColumn("Date", F.lit(start_date)).withColumn("Amount", F.lit(None))

# Union the original DataFrame with the dummy DataFrame
df = df.union(dummy_df)

# Define the window specification for the lookback period
window_spec = Window.partitionBy("ID").orderBy(F.col("Date")).rangeBetween(-lookback_days, -1)

# Calculate the lookback amount for each ID, considering only the '15th December 2022' rows
df = df.withColumn("Lookback_Amount", F.when(F.col("Date") == start_date, F.sum("Amount").over(window_spec)).otherwise(None))

# Filter the DataFrame to include only dates from or after the start date
df = df.filter(F.col("Date") >= start_date)

# Show the resulting DataFrame
df.show()
