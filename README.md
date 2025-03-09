# my_cool_repository

```python
##The Python code I used:

import pandas as pd
import numpy as np

#Load data
activity = pd.read_csv('Downloads/data/activity.csv', parse_dates=['from_date', 'to_date'])
customers = pd.read_csv('Downloads/data/customers.csv')
acq = pd.read_csv('Downloads/data/acq_orders.csv')

# In order to be able to filter the Looker graphs, I'm assuming that every customer belongs to one group:
acq_aggregate = acq.groupby("customer_id", as_index=False).agg({"taxonomy_business_category_group": "min"})

# Merge the dataframes
activity_overall = activity.merge(customers, on="customer_id", how="left") \
               .merge(acq_aggregate, on="customer_id", how="left")

# Create a new column 'last_active_month' by truncating the 'to_date' to the first day of that month.
activity_overall["last_active_month"] = activity_overall["to_date"].apply(lambda d: d.replace(day=1))

# Next, turn the subscriptions into monthly active rows

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
first_active = df_expanded.groupby(["customer_id", "customer_country", "taxonomy_business_category_group"],
                as_index=False)["active_month"].min().rename(columns={"active_month": "first_active_month"})
df_expanded = df_expanded.merge(first_active, on=["customer_id", "customer_country",
                                                  "taxonomy_business_category_group"], how="left")

# Flag new customers with 1 if the active month is the first active month, otherwise 0
df_expanded["is_new"] = (df_expanded["active_month"] == df_expanded["first_active_month"]).astype(int)

# Flag lost customers - a customer is considered lost if the current active month is their last_active_month.
df_expanded["is_lost"] = (df_expanded["active_month"] == df_expanded["last_active_month"]).astype(int)

# Compute monthly active counts per segment, as well as new and lost customers
monthly_figures = df_expanded.groupby(["active_month", "customer_country", "taxonomy_business_category_group"]).agg(
    total_active_customers=("customer_id", "nunique"),
    new_customers=("is_new", "sum"),
    lost_customers=("is_lost", "sum")).reset_index()
monthly_figures = monthly_figures.sort_values(by=["customer_country",
                                                  "taxonomy_business_category_group", "active_month"])

# For each segment, I'm determining the starting customers as the previous month's total active customers
monthly_figures["starting_customers"] = monthly_figures.groupby(["customer_country",
                                        "taxonomy_business_category_group"])["total_active_customers"].shift(1)

# Calculate retention rate AS: THE PERCENTAGE OF PREVIOUS MONTH'S CUSTOMERS, EXCLUDING NEW ONES, WHO REMAIN ACTIVE
monthly_figures["retention_rate"] = ((monthly_figures["total_active_customers"] - monthly_figures["new_customers"]) /
                                    monthly_figures["starting_customers"]) * 100

# Calculate churn rate AS: 1 - RETENTION RATE
monthly_figures["churn_rate"] = 100 - monthly_figures["retention_rate"]

# For the first month in each segment, set retention to 100% and churn to 0%
monthly_figures["retention_rate"] = monthly_figures["retention_rate"].fillna(100)
monthly_figures["churn_rate"] = monthly_figures["churn_rate"].fillna(0)

#Rename columns
monthly_figures = monthly_figures.rename(columns={
    "taxonomy_business_category_group": "Taxonomy_Group",
    "retention_rate": "Retention_Rate",
    "churn_rate": "Churn_Rate",
    "active_month": "Date_Year_Month",
    "customer_country": "Country"
})

#Save the results
monthly_figures.to_csv("Downloads/data/monthly_retention_analysis.csv", index=False)
