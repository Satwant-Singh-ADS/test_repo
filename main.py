from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

for col in numeric_columns:
    plt.figure(figsize=(8, 6))
    plt.hist(Df.select(col).dropna().toPandas()[col], bins=20, color='blue', alpha=0.7)
    plt.xlabel(col)
    plt.ylabel('Frequency')
    plt.title(f'Univariate Histogram of {col}')
    plt.grid(True)
    plt.show()

percentiles = [0.01, 0.02, 0.03, ..., 0.99]  # Specify the percentiles you want to compute

percentiles_data = []
for col_name in numeric_columns:
    percentiles_values = Df.approxQuantile(col_name, percentiles, 0.0)
    percentiles_data.append([col_name] + percentiles_values)

percentile_columns = ["Column"] + [f"P{int(p * 100)}" for p in percentiles]
percentiles_df = spark.createDataFrame(percentiles_data, percentile_columns)

target_col = "Target"  # Replace with your actual target column name
target_values = [0, 1]

for col in numeric_columns:
    plt.figure(figsize=(8, 6))
    for value in target_values:
        subset = Df.filter(Df[target_col] == value).select(col, target_col).dropna().toPandas()
        plt.scatter(subset[col], subset[target_col], label=f"{target_col} = {value}")
    plt.xlabel(col)
    plt.ylabel(target_col)
    plt.title(f'Bivariate Scatter Plot of {col} vs {target_col}')
    plt.legend()
    plt.grid(True)
    plt.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create a Spark session
spark = SparkSession.builder.appName("ImputationExample").getOrCreate()

# Replace 'your_dataframe' with your actual DataFrame name
# Replace 'your_numeric_column' with the actual column name you want to process

# Calculate the percentile values
percentile_995 = your_dataframe.approxQuantile("your_numeric_column", [0.995], 0.0)[0]

# Impute negative values with -1 and values above 99.5 percentile with 99.5 percentile value * 1.1
imputed_df = your_dataframe.withColumn(
    "your_numeric_column",
    expr(
        "CASE WHEN your_numeric_column < 0 THEN -1 "
        f"WHEN your_numeric_column > {percentile_995} THEN {percentile_995 * 1.1} "
        "ELSE your_numeric_column END"
    )
)

# Show the resulting DataFrame
imputed_df.show()

# Stop the Spark session
spark.stop()
