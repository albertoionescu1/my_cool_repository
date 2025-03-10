# my_cool_repository

The Python code I used:

```python
import pandas as pd
import numpy as np

# Load data from the .csv files

activity = pd.read_csv('Downloads/data/activity.csv', parse_dates=['from_date', 'to_date'])
customers = pd.read_csv('Downloads/data/customers.csv')
acq = pd.read_csv('Downloads/data/acq_orders.csv')

# Quality checks
print("\nNull check for the 'activity' dataframe:")
print(activity.isnull().sum())
print("\nNull check for the 'customers' dataframe:")
print(customers.isnull().sum())
print("\nNull check for the 'acq' dataframe:")
print(acq.isnull().sum())

# I'm assuming each customer belongs to one taxonomy group - further explanations below
# So I'm grouping acquisition orders by customer_id and select the minimum taxonomy value
acq_aggregate = acq.groupby("customer_id", as_index=False).agg({"taxonomy_business_category_group": "min"})

# Merge the dataframes: activity, customers, and the aggregated acquisition table
activity_overall = activity.merge(customers, on="customer_id", how="left") \
                           .merge(acq_aggregate, on="customer_id", how="left")

# Create a new column 'last_active_month' by truncating the 'to_date' to the first day of that month
activity_overall["last_active_month"] = activity_overall["to_date"].apply(lambda d: d.replace(day=1))

# Now, I am expanding each subscription into monthly active rows by
# generating a list of months (starting at the first of the month) for each subscription
# during which the subscription was active.
# For the subscripiton, I am using customer_id instead of subscription_id because I want to also capture
# customers that cancel and return to purchase another subsctiption. However, using subscription_id would allow me
# to understand better the performance of each taxonomy_group, but that is out of scope.
# Further, this is also the reason I assigned one taxonomy group per customers.
expanded_rows = []
for _, row in activity_overall.iterrows():
    start = row["from_date"].replace(day=1)
    end = row["to_date"].replace(day=1)
    months = pd.date_range(start, end, freq='MS')
    for m in months:
        expanded_rows.append({
            "customer_id": row["customer_id"],
            "active_month": m,
            "customer_country": row["customer_country"],
            "taxonomy_business_category_group": row["taxonomy_business_category_group"],
            "last_active_month": row["last_active_month"]
        })
df_expanded = pd.DataFrame(expanded_rows)

# Remove duplicates so that each customer appears only once per active month and segment
# Segment is a dimension based on month, country and taxonomy group
df_expanded = df_expanded.drop_duplicates(subset=["customer_id", "active_month",
                                                  "customer_country", "taxonomy_business_category_group"])

# Remove any rows where the taxonomy group is null
df_expanded = df_expanded[df_expanded["taxonomy_business_category_group"].notnull()]

# I saw the data for August 2024 is incomplete and may skew the analysis, so I'm removing it
df_expanded = df_expanded[~((df_expanded["active_month"].dt.year == 2024) & (df_expanded["active_month"].dt.month == 8))]

# Next, identify each customer's first active month per segment
# This first active month will be used to flag new customers.
first_active = df_expanded.groupby(["customer_id", "customer_country", "taxonomy_business_category_group"],
                as_index=False)["active_month"].min().rename(columns={"active_month": "first_active_month"})
df_expanded = df_expanded.merge(first_active, 
              on=["customer_id", "customer_country", "taxonomy_business_category_group"], how="left")

# Flag new customers with 1 if the active month equals the first active month, otherwise 0
# So if a customers signs up again at a latter stage for another subscription, they won't be treated as 'new'
df_expanded["is_new"] = (df_expanded["active_month"] == df_expanded["first_active_month"]).astype(int)

# Compute monthly active counts, as well as new customers, per segment
monthly_figures = df_expanded.groupby(["active_month", "customer_country", "taxonomy_business_category_group"]).agg(
                  total_active_customers=("customer_id", "nunique"), new_customers=("is_new", "sum")).reset_index()
monthly_figures = monthly_figures.sort_values(
                  by=["customer_country", "taxonomy_business_category_group", "active_month"])

# For each segment, I'm determining the starting customers as the previous month's total active customers
monthly_figures["starting_customers"] = monthly_figures.groupby(["customer_country", "taxonomy_business_category_group"]
                                                               )["total_active_customers"].shift(1)

# Calculate retention rate AS: THE PERCENTAGE OF PREVIOUS MONTH'S CUSTOMERS, EXCLUDING NEW ONES, WHO REMAIN ACTIVE
# in the current month
monthly_figures["retention_rate"] = (
    (monthly_figures["total_active_customers"] - monthly_figures["new_customers"])
    / monthly_figures["starting_customers"]
) * 100

# Calculate churn rate AS: 1 - RETENTION RATE
monthly_figures["churn_rate"] = 100 - monthly_figures["retention_rate"]

# For the first month in each segment (with no previous month data), set retention to 100% and churn to 0%
monthly_figures["retention_rate"] = monthly_figures["retention_rate"].fillna(100)
monthly_figures["churn_rate"] = monthly_figures["churn_rate"].fillna(0)

# Rename columns for Looker:
# 'taxonomy_business_category_group' -> 'Taxonomy_Group'
# 'retention_rate' -> 'Retention_Rate'
# 'churn_rate' -> 'Churn_Rate'
# 'active_month' -> 'Date_Year_Month'
# 'customer_country' -> 'Country'
monthly_figures = monthly_figures.rename(columns={
    "taxonomy_business_category_group": "Taxonomy_Group",
    "retention_rate": "Retention_Rate",
    "churn_rate": "Churn_Rate",
    "active_month": "Date_Year_Month",
    "customer_country": "Country"
})

# Save the DataFrame to the final .csv
monthly_figures.to_csv("Downloads/data/monthly_retention_analysis.csv", index=False)
