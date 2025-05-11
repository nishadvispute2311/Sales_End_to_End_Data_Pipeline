from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Example log message

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("NishTech Sales Dashboard") \
    .getOrCreate()

logging.info("Starting Spark KPI job")


# Output path
BASE_DIR = "/app"

# Set output directory path
output_dir = os.path.join(BASE_DIR, "yn_processed_kpis_cron")
os.makedirs(output_dir, exist_ok=True)

def get_output_dir(dir_name):
    # Define base path assuming /app is your working directory inside the container
    final_output_dir = os.path.join(BASE_DIR, "yn_processed_kpis_cron", dir_name)
    return final_output_dir

# Load CSVs

# yn_sales_project_final/csv_consumed_data_kafka/
cust_df = spark.read.csv(os.path.join(BASE_DIR, 'csv_consumed_data_kafka', 'yn-customers-0812_data.csv'), header=True, inferSchema=True)
product_df = spark.read.csv(os.path.join(BASE_DIR, 'csv_consumed_data_kafka', 'yn-products-0812_data.csv'), header=True, inferSchema=True)
orders_df = spark.read.csv(os.path.join(BASE_DIR, 'csv_consumed_data_kafka', 'yn-orders-0812_data.csv'), header=True, inferSchema=True)
order_items_df = spark.read.csv(os.path.join(BASE_DIR, 'csv_consumed_data_kafka', 'yn-order_items-0812_data.csv'), header=True, inferSchema=True)

# timestamp transform
cust_df_v1 = cust_df.withColumn("parsed_ts", to_timestamp("signup_date")).withColumn("cust_activation_date", date_format("parsed_ts", "yyyy-MM-dd HH:mm:ss")).drop("parsed_ts").drop("signup_date")
order_df_v1 = orders_df.withColumn("parsed_ts", to_timestamp("order_date")).withColumn("ordered_date", date_format("parsed_ts", "yyyy-MM-dd HH:mm:ss")).drop("parsed_ts").drop("order_date")

# --🛒 Total Orders Count of all orders placed 
# --👥 New Customers Count of signups in the time window - last 3 hrs
# --💵 Total Sales Amount Sum of total_amount from orders 
# --📦 Total Items Sold Sum of quantity in order_items 
# --🛍 Total Unique Products Sold Count of distinct product_id 
# --💲 Average Order Value (AOV) Total Sales / Total Orders 
# --Customer Acquisition Cost (CAC): Average cost incurred to acquire a new customer. 
# -- Sales Conversion Rate: Percentage of website visitors or leads that result in completed sales. AVG VALUE
# -- Customer Retention Rate: Percentage of customers who made a repeat purchase within a specific time period.
# --1. Sales Revenue: Total revenue generated over a specific period. 
# --2. Sales Growth Rate: Percentage increase or decrease in sales revenue compared to the previous period.
# --Inventory Turnover: Number of times inventory is sold and replaced in a given period.

# Total Orders Count of all orders placed 
total_orders_count = orders_df.count()

# New Customers Count of signups in the time window - last 3 hrs
customers_in_last_3hrs = cust_df_v1.filter(col("cust_activation_date") >= expr("current_timestamp() + INTERVAL 5 HOURS 30 MINUTES - INTERVAL 3 HOURS")).count()

# Total Sales Amount Sum of total_amount from orders
total_sales_amount = order_df_v1.agg(sum("total_amount").alias("Total_sales_amount")).collect()[0]["Total_sales_amount"]

# Total Items Sold Sum of quantity in order_items
total_items_sold = order_items_df.agg(sum("quantity").alias("Items_sold")).collect()[0]["Items_sold"]

# Total Unique Products Sold Count of distinct product_id
total_products_sold = order_items_df.select("product_id").distinct().count()

# Average Order Value (AOV) Total Sales / Total Orders
aov = total_sales_amount/total_orders_count

# Customer Acquisition Cost (CAC): Average cost incurred to acquire a new customer
total_acquisition_cost = 50000
# CAC = total cost / number of new customers
cust_count = cust_df_v1.count()
cac = total_acquisition_cost / cust_count if cust_count else 0
print(f"Customer Acquisition Cost (CAC): ₹{cac:.2f}")

# Sales Conversion Rate: Percentage of website visitors or leads that result in completed sales. AVG VALUE
first_order_date = order_df_v1.groupBy("customer_id").agg(min("ordered_date").alias("first_purchase_date"))
date_difference = cust_df_v1.join(first_order_date,on="customer_id",how="inner").withColumn("interval_for_first_order_hour",round((unix_timestamp("first_purchase_date") - unix_timestamp("cust_activation_date")) / 3600, 2)).select("customer_id","name","cust_activation_date","first_purchase_date","interval_for_first_order_hour")
avg_time = date_difference.agg(avg('interval_for_first_order_hour').alias('avg_value')).collect()[0]['avg_value']
print(f"Average Cutomer to order conversion time : {avg_time:.2f} hour")

# Customer Retention Rate: Percentage of customers who made a repeat purchase within a specific time period.
customer_df = order_df_v1.groupBy("customer_id").agg(count("order_id").alias("order_count"))
repeat_customer_df = customer_df.filter("order_count > 1")
repeat_customer_count = repeat_customer_df.select("customer_id").distinct().count()
total_customer_count = order_df_v1.select("customer_id").distinct().count()
percent_repeat_customers = (repeat_customer_count/total_customer_count)*100
print(f"Repeat Customers: {percent_repeat_customers:.2f}%")

# Sales Revenue: Total revenue generated over a specific period.
orders_in_last3hrs = order_df_v1.filter(col("ordered_date") >= expr("current_timestamp() + INTERVAL 5 HOURS 30 MINUTES - INTERVAL 3 HOURS"))
sales_in_last3hrs = orders_in_last3hrs.agg(sum("total_amount").alias("sales_revenue")).collect()[0]["sales_revenue"] or 0.0
print(f"Sales Revenue (Last 3 Hours, IST): ₹ {sales_in_last3hrs}")

# Sales Growth Rate: Percentage increase or decrease in sales revenue compared to the previous period.
current_time_frame = expr("current_timestamp() + INTERVAL 5 HOURS 30 MINUTES - INTERVAL 3 HOURS")
previous_time_start = expr("current_timestamp() + INTERVAL 5 HOURS 30 MINUTES - INTERVAL 6 HOURS")
previous_time_end = expr("current_timestamp() + INTERVAL 5 HOURS 30 MINUTES - INTERVAL 3 HOURS")
current_sales = order_df_v1.filter(col("ordered_date") >= current_time_frame).agg(sum("total_amount").alias("sales_in_last3hrs")).collect()[0]["sales_in_last3hrs"] or 0.0
previous_sales = order_df_v1.filter((col("ordered_date") >= previous_time_start) & (col("ordered_date") < previous_time_end)).agg(sum("total_amount").alias("sales_in_last3_6hrs")).collect()[0]["sales_in_last3_6hrs"] or 0.0
if previous_sales == 0:
    growth_rate = float('inf') if current_sales > 0 else 0
else:
    growth_rate = ((current_sales - previous_sales) / previous_sales) * 100

print(f"Sales Growth Rate: {growth_rate:.2f}%")


# Inventory Turnover: Number of times inventory is sold and replaced in a given period.

order_items_df = order_items_df.withColumn("unit_price", col("unit_price").cast("double"))
products_df = product_df.withColumn("price", col("price").cast("double"))
# Approximate cost price as 70% of selling price
products_with_cogs = products_df.withColumn("cost_price", col("price") * 0.7)
# Join to get cost price into order_items
order_items_enriched = order_items_df.join(products_with_cogs, on="product_id", how="left")
# Calculate COGS = quantity * cost_price
order_items_enriched = order_items_enriched.withColumn("cogs", col("quantity") * col("cost_price"))
# Sum COGS and quantity sold
cogs_total = order_items_enriched.agg({"cogs": "sum"}).collect()[0][0] or 0.0
quantity_total = order_items_enriched.agg({"quantity": "sum"}).collect()[0][0] or 1  # avoid division by zero
# Assume Average Inventory = total quantity sold / 2 (very rough approximation)
average_inventory = quantity_total / 2
inventory_turnover = cogs_total / average_inventory if average_inventory != 0 else 0
print(f"Inventory Turnover: {inventory_turnover:.2f}")

# save kpis in csv
static_card_kpi_data = [(total_orders_count, customers_in_last_3hrs, total_sales_amount, total_items_sold, total_products_sold, aov, cac, avg_time, percent_repeat_customers, sales_in_last3hrs, growth_rate, inventory_turnover)]
static_card_kpi_data_columns = ["total_orders_count", "customers_in_last_3hrs", "total_sales_amount", "total_items_sold", "total_products_sold", "aov", "cac", "avg_time", "percent_repeat_customers", "sales_in_last3hrs", "growth_rate", "inventory_turnover"]

kpi_df = spark.createDataFrame(static_card_kpi_data,static_card_kpi_data_columns)
kpi_df_path = get_output_dir("static_summary_kpis")
kpi_df.coalesce(1).write.option("header",True).mode("overwrite").csv(kpi_df_path)


# Customer Lifetime Value (CLV): Predicted revenue generated from a customer over their lifetime. 
# Sales by Region: Distribution of sales across different geographic regions. 
# Seasonal Sales Trends: Analysis of sales patterns and fluctuations based on seasonal factors. 
# churn risk customers
# top products sold in last 5 mins
# category revenue
# Membership revenue

# Customer Lifetime Value (CLV): Predicted revenue generated from a customer over their lifetime.
yn_customer_ltv_path = get_output_dir("yn_customer_ltv")
orders_customer_data = order_df_v1.join(cust_df_v1, "customer_id")
ltv_per_customer = orders_customer_data.groupBy(window("ordered_date", "10 day"), "customer_id", "name").agg(round(sum("total_amount"), 2).alias("customer_ltv"))
ranked_ltv = ltv_per_customer.withColumn("rank", row_number().over(Window.partitionBy("window").orderBy(col("customer_ltv").desc()))).filter(col("rank") <= 5).orderBy("window", "rank")
# Assuming ranked_ltv has the 'window' column
ranked_ltv = ranked_ltv.withColumn("window_start", col("window.start"))
ranked_ltv = ranked_ltv.withColumn("window_end", col("window.end"))
# Drop the 'window' column as it's no longer needed
ranked_ltv = ranked_ltv.drop("window")
# Now write the result as CSV
ranked_ltv.coalesce(1).write.mode("overwrite").option("header", True).csv(yn_customer_ltv_path)

# Sales by Region: Distribution of sales across different geographic regions.
orders_with_location = order_df_v1.join(cust_df_v1, on="customer_id", how="inner")
location_revenue = orders_with_location.groupBy("location").agg(round(sum("total_amount"),2).alias("total_revenue")).orderBy("total_revenue", ascending=False).limit(10)
location_revenue.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_regional_revenue")

# Membership revenue
cust_order_data = cust_df_v1.join(order_df_v1,on="customer_id",how="inner")
member_revenue_data = cust_order_data.groupBy("membership_type").agg(round(sum("total_amount"),2).alias("revenue_generated")).orderBy(col("revenue_generated").desc())
member_revenue_data.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_membership_revenue")

# Seasonal Sales Trends: Analysis of sales patterns and fluctuations based on seasonal factors. 
orders_df_time = order_df_v1.withColumn("order_month", month("ordered_date")).withColumn("order_weekday", dayofweek("ordered_date")).withColumn("order_hour", hour("ordered_date"))
# Monthly Sales Trend
monthly_sales_df = orders_df_time.groupBy("order_month").agg(sum("total_amount").alias("monthly_sales")).orderBy("order_month")
monthly_sales_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_monthly_sales_df")
# Weekly Sales Trend (1 = Sunday, 7 = Saturday)
weekly_sales_df = orders_df_time.groupBy("order_weekday").agg(sum("total_amount").alias("weekday_sales")).orderBy("order_weekday")
weekly_sales_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_weekly_sales_df")
# Hourly Sales Trend
hourly_sales_df = orders_df_time.groupBy("order_hour").agg(sum("total_amount").alias("hourly_sales")).orderBy("order_hour")
hourly_sales_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_hourly_sales_df")


# Real-Time Top N Products Track top-selling products per 5 min window order_items, products
orders_in_last_5mins = order_df_v1.filter(col("ordered_date") >= expr("current_timestamp() - INTERVAL 15 MINUTES"))
ordered_items_last_5mins = orders_in_last_5mins.join(order_items_df,on="order_id",how="inner")
products_last_5mins = ordered_items_last_5mins.groupBy("product_id").agg(count("quantity").alias("sold_quantity"))
final_df = products_last_5mins.join(products_df,on="product_id",how="left").orderBy("sold_quantity",ascending=False).select("name","sold_quantity").limit(10)
final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_products_last_5mins")


# Revenue by Product Category
product_items_data = product_df.join(order_items_df,on="product_id",how="inner")
category_wise_revenue = product_items_data.withColumn("revenue",expr("quantity * unit_price")).groupBy("category").agg(round(sum("revenue"),2).alias("revenue_generated")).orderBy(col("revenue_generated").desc())
category_wise_revenue.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_category_wise_revenue")

# Churn Risk Detection (Streaming) Customers who haven't placed order in X days Stateful l
order_last_date = order_df_v1.groupBy("customer_id").agg(max("ordered_date").alias("last_ordered_date"))
churn_data = order_last_date.withColumn("days_since_last_order",datediff(current_timestamp(),col("last_ordered_date"))).filter(col("days_since_last_order") >= 7).orderBy(col("days_since_last_order").desc())
churn_data.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/yn_churn_data")

logging.info("✅ Spark job executed successfully")
# Stop SparkSession
spark.stop()










#  docker run -it --rm -v "${PWD}/yn_sales_project_final:/app" -w /app bitnami/spark spark-submit spark_kpi_file_yn.py --Final_executed
# docker stop kpi-cron
# docker build -f Dockerfile.cron -t spark-kpi-cron .
# docker rm -f kpi-cron
# docker run -d --name kpi-cron spark-kpi-cron
# docker logs -f kpi-cron