import streamlit as st
import pandas as pd
import glob
import os
import time
import plotly.express as px


# Page config
st.set_page_config(page_title="NishTech Sales Dashboard", layout="wide")
st.title("📊 Real-Time Sales KPI Dashboard")

# @st.cache_data
def load_kpi_data():
    # Look for any file named part-*.csv inside the summary_kpis folder
    kpi_files = glob.glob("yn_processed_kpis/static_summary_kpis/part-*.csv")

    if kpi_files:
        df = pd.read_csv(kpi_files[0])  # Read the first part file
        return {
            "total_orders_count": int(df["total_orders_count"][0]),
            "customers_in_last_3hrs": int(df["customers_in_last_3hrs"][0]),
            "total_sales_amount": float(df["total_sales_amount"][0]),
            "total_items_sold": int(df["total_items_sold"][0]),
            "total_products_sold": int(df["total_products_sold"][0]),
            "aov": float(df["aov"][0]),
            "cac": float(df["cac"][0]),
            "avg_time": int(df["avg_time"][0]),
            "percent_repeat_customers": float(df["percent_repeat_customers"][0]),
            "sales_in_last3hrs": float(df["sales_in_last3hrs"][0]),
            "growth_rate": float(df["growth_rate"][0]),
            "inventory_turnover": int(df["inventory_turnover"][0])
        }
    else:
        return {k: 0 for k in [
            "total_orders_count", "customers_in_last_3hrs", "total_sales_amount",
            "total_items_sold", "total_products_sold", "aov",
            "cac", "avg_time", "percent_repeat_customers", 
            "sales_in_last3hrs", "growth_rate", "inventory_turnover"
        ]}

kpis = load_kpi_data()

# KPI Cards
st.markdown("### 🔍 KPIs Overview")
col1, col2, col3, col4 = st.columns(4)
col5, col6, col7, col8 = st.columns(4)
col9, col10, col11, col12 = st.columns(4)

col1.metric("🛒 Total Orders", f"{kpis['total_orders_count']}")
col2.metric("👥 Customers (Last 3 Hrs)", f"{kpis['customers_in_last_3hrs']}")
col3.metric("💰 Total Sales", f"${kpis['total_sales_amount']:,.2f}")
col4.metric("📦 Total Items Sold", f"{int(kpis['total_items_sold'])}")
col5.metric("🧾 Unique Products Sold", f"{kpis['total_products_sold']}")
col6.metric("📐 Avg. Order Value", f"${kpis['aov']:,.2f}")
col7.metric("📈 Avg. Customer Aquisition Cost", f"${kpis['cac']:,.2f}")
col8.metric("⏱️ Avg. Customer Conversion Time", f"{kpis['avg_time']:,.2f} Hr")
col9.metric("📈 Repeat Customers", f"{kpis['percent_repeat_customers']:,.2f}%")
col10.metric("💲 Sales (Last 3 Hrs)", f"${kpis['sales_in_last3hrs']:,.2f}")
col11.metric("🛒 Growth Rate", f"{kpis['growth_rate']:,.2f}%")
col12.metric("📦 Inventory Turnover", f"{kpis['inventory_turnover']:,.2f}")
st.divider()


def load_csv_data(foldername):
    kpi_files = glob.glob(f"yn_processed_kpis/{foldername}/part-*.csv")
    return pd.read_csv(kpi_files[0]) if kpi_files else pd.DataFrame()

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Category Revenue", "🔥 Top Products", "🌍 Location Revenue", "👥 Customer LTV", "📈 Churn Risk Customers"])

with tab1:
    st.subheader("📊 Revenue by Product Category")
    st.dataframe(load_csv_data("yn_category_wise_revenue"))

with tab2:
    st.subheader("🏆 Most Sold Products (In Last 5 Mins)")
    st.dataframe(load_csv_data("yn_products_last_5mins"))

with tab3:
    st.subheader("🌍 Revenue by Location")
    st.dataframe(load_csv_data("yn_regional_revenue"))

with tab4:
    st.subheader("💸 Top 5 Customers by LTV (10-day window)")
    st.dataframe(load_csv_data("yn_customer_ltv"))

with tab5:
    st.subheader("📈 Churn Risk Customers")
    st.dataframe(load_csv_data("yn_churn_data"))

st.divider()
# st.caption("Powered by Streamlit • Data from processed_kpis/")


import streamlit as st
import pandas as pd
import altair as alt

# Load and clean CSVs
def load_graph_data():
    kpi_files_hourly = glob.glob(f"yn_processed_kpis/yn_hourly_sales_df/part-*.csv")
    kpi_files_weekly = glob.glob(f"yn_processed_kpis/yn_weekly_sales_df/part-*.csv")
    kpi_files_monthly = glob.glob(f"yn_processed_kpis/yn_monthly_sales_df/part-*.csv")
    hourly = pd.read_csv(kpi_files_hourly[0]).dropna().astype({'order_hour': int, 'hourly_sales': float}) if kpi_files_hourly else pd.DataFrame()
    weekly = pd.read_csv(kpi_files_weekly[0]).dropna().astype({'order_weekday': int, 'weekday_sales': float}) if kpi_files_weekly else pd.DataFrame()
    monthly = pd.read_csv(kpi_files_monthly[0]).dropna().astype({'order_month': int, 'monthly_sales': float}) if kpi_files_monthly else pd.DataFrame()
    return hourly, weekly, monthly

hourly_df, weekly_df, monthly_df = load_graph_data()

# Title
st.title("Sales Trends Dashboard")
location_file = glob.glob(f"yn_processed_kpis/yn_regional_revenue/part-*.csv")
location_df = pd.read_csv(location_file[0]) if location_file else pd.DataFrame()


# Layout: Side-by-side using columns
col1, col2 = st.columns(2)

with col1:
    fig1 = px.pie(location_df, values='total_revenue', names='location',
              title='Revenue Share by Location', hole=0.4)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    view = st.selectbox("Select Time Granularity", ["Hourly", "Weekly", "Monthly"])
    if view == "Hourly":
        st.subheader("Hourly Sales")
        chart = alt.Chart(hourly_df).mark_line(point=True).encode(
            x=alt.X('order_hour:O', title='Hour of Day'),
            y=alt.Y('hourly_sales:Q', title='Sales'),
            tooltip=['order_hour', 'hourly_sales']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)

    elif view == "Weekly":
        st.subheader("Weekly Sales")
        day_names = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
        weekly_df['weekday'] = weekly_df['order_weekday'].map(day_names)
        chart = alt.Chart(weekly_df).mark_bar().encode(
            x=alt.X('weekday', sort=["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]),
            y=alt.Y('weekday_sales:Q', title='Sales'),
            tooltip=['weekday', 'weekday_sales']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)

    elif view == "Monthly":
        st.subheader("Monthly Sales")
        chart = alt.Chart(monthly_df).mark_bar().encode(
            x=alt.X('order_month:O', title='Month'),
            y=alt.Y('monthly_sales:Q', title='Sales'),
            tooltip=['order_month', 'monthly_sales']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)

st.divider()


st.title("Revenue Analysis by Category and Membership Type")

# Load and clean data
category_data = load_csv_data("yn_category_wise_revenue")
membership_data = load_csv_data("yn_membership_revenue")
category_df = category_data.dropna()
membership_df = membership_data.dropna()

# Rename for consistency
category_df = category_df.rename(columns={"category": "label", "revenue_generated": "revenue"})
category_df['percent'] = category_df['revenue'] / category_df['revenue'].sum()
membership_df = membership_df.rename(columns={"membership_type": "label", "revenue_generated": "revenue"})

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏅 Revenue by Membership Type")
    chart2 = alt.Chart(membership_df).mark_bar().encode(
        x=alt.X('label:N', title='Membership Type'),
        y=alt.Y('revenue:Q', title='Revenue'),
        tooltip=['label', 'revenue'],
        color=alt.Color('label:N', legend=None)
    ).properties(width=350, height=400)
    st.altair_chart(chart2, use_container_width=True)

with col2:
    st.subheader("🔸 Revenue Distribution By Category")
    pie_fig = px.pie(
        category_df,
        names='label',
        values='revenue',
        hole=0.4,
        title="Revenue Share by Category"
    )
    pie_fig.update_traces(textinfo='percent+label', pull=[0.02]*len(category_df))
    pie_fig.update_layout(height=500)
    st.plotly_chart(pie_fig, use_container_width=True)

st.divider()

countdown = st.empty()
for i in range(30, 0, -1):
    countdown.text(f"Refreshing in {i} seconds...")
    time.sleep(1)

st.rerun()

# For streamlit
# docker build -t streamlit-dashboard .
# D:\kafka-docker-project\spark_project> docker run -it --rm -p 8501:8501 -v "${PWD}:/app" streamlit-dashboard
