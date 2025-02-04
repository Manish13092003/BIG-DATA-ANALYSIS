import dask.dataframe as dd

# Specify the dtypes for the problematic columns
dtype = {
    'improvement_surcharge': 'float64',
    'tolls_amount': 'float64'
}

# Load the dataset with specified dtypes
df = dd.read_csv(r"F:\New folder\yellow_tripdata_2023-01.csv", dtype=dtype)

# Show the first few rows
print("First few rows of the dataset:")
print(df.head())

# Show basic information about the dataset
print("\nDataset Info:")
print(df.info())

# Perform a basic analysis (example: calculate average trip distance)
avg_trip_distance = df['trip_distance'].mean().compute()
print(f"\nAverage Trip Distance: {avg_trip_distance}")

# Convert 'tpep_pickup_datetime' to datetime format
df['tpep_pickup_datetime'] = dd.to_datetime(df['tpep_pickup_datetime'])

# 1. Find the total revenue per day
df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
daily_revenue = df.groupby('pickup_date')['total_amount'].sum().compute()
print("\nDaily Revenue:")
print(daily_revenue)

# 2. Weekly revenue (group by week)
df['pickup_week'] = df['tpep_pickup_datetime'].dt.isocalendar().week
weekly_revenue = df.groupby('pickup_week')['total_amount'].sum().compute()
print("\nWeekly Revenue:")
print(weekly_revenue)

# 3. Monthly revenue (group by month)
df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
monthly_revenue = df.groupby('pickup_month')['total_amount'].sum().compute()
print("\nMonthly Revenue:")
print(monthly_revenue)

# 4. Yearly revenue (group by year)
df['pickup_year'] = df['tpep_pickup_datetime'].dt.year
yearly_revenue = df.groupby('pickup_year')['total_amount'].sum().compute()
print("\nYearly Revenue:")
print(yearly_revenue)

# 5. Find the number of trips per day
trips_per_day = df.groupby('pickup_date')['VendorID'].count().compute()
print("\nNumber of trips per day:")
print(trips_per_day)

# 6. Average fare per vendor
avg_fare_per_vendor = df.groupby('VendorID')['total_amount'].mean().compute()
print("\nAverage Fare per Vendor:")
print(avg_fare_per_vendor)

# 7. Monthly profit (revenue minus tolls and surcharge)
df['monthly_profit'] = df['total_amount'] - (df['tolls_amount'] + df['improvement_surcharge'])
monthly_profit = df.groupby('pickup_month')['monthly_profit'].sum().compute()
print("\nMonthly Profit:")
print(monthly_profit)

# 8. Annual profit (revenue minus tolls and surcharge)
df['annual_profit'] = df['total_amount'] - (df['tolls_amount'] + df['improvement_surcharge'])
annual_profit = df.groupby('pickup_year')['annual_profit'].sum().compute()
print("\nAnnual Profit:")
print(annual_profit)
