from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("GreedyCoverage").getOrCreate()

# Sample data (your dataframe will come from the actual source)
data = [(1, 1), (1, 2), (1, 5), (2, 2)]
df = spark.createDataFrame(data, ["account_id", "sequence_id"])

# Function to find the minimum set of sequence_ids that cover all account_ids
def find_sequence_cover(df):
    # Initialize a set to track covered account ids
    covered_account_ids = set()
    
    # Initialize a list to store the selected sequence_ids
    selected_sequence_ids = []
    
    # Repeat the process until all account_ids are covered
    while len(covered_account_ids) < df.select("account_id").distinct().count():
        # Group by sequence_id and count distinct account_ids for each sequence_id
        coverage_df = df.groupBy("sequence_id").agg(
            countDistinct("account_id").alias("covered_accounts")
        )
        
        # Sort sequence_ids by number of accounts they cover, descending
        coverage_df = coverage_df.orderBy(col("covered_accounts").desc())
        
        # Get the sequence_id with the maximum coverage
        best_sequence = coverage_df.first()
        best_sequence_id = best_sequence["sequence_id"]
        
        # Add the selected sequence_id to the list
        selected_sequence_ids.append(best_sequence_id)
        
        # Get the account_ids covered by this sequence_id
        covered_accounts = df.filter(df.sequence_id == best_sequence_id).select("account_id").distinct().rdd.flatMap(lambda x: x).collect()
        
        # Add covered account_ids to the covered set
        covered_account_ids.update(covered_accounts)
        
        # Remove the covered account_ids and the selected sequence_id from the dataframe
        df = df.filter(~df.account_id.isin(covered_accounts))
        
        # If all account_ids are covered, stop
        if len(covered_account_ids) == df.select("account_id").distinct().count():
            break
    
    return selected_sequence_ids

# Find the sequence_ids that cover all account_ids
selected_sequence_ids = find_sequence_cover(df)
print("Selected Sequence IDs:", selected_sequence_ids)
