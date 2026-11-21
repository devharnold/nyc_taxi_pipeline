import pandas as pd

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_parquet(prefix + 'yellow_tripdata_2021-01.parquet', nrows=100)

# Display first rows
df.head()

# Check data types
df.dtypes

# Check data shape
df.shape