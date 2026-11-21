import duckdb

url = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/green_tripdata_2021-01.parquet"
)

df = duckdb.query(
    f"""
    SELECT *
    FROM read_parquet('{url}')
    LIMIT 100
    """
).df()

print("\nFirst rows:")
print(df.head())

print("\nData types:")
print(df.dtypes)

print("\nShape:")
print(df.shape)
