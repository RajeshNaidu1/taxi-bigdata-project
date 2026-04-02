import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import plotly.express as px

DB_URL = "postgresql+psycopg2://rajesh@localhost:5432/taxi_db"

engine = create_engine(DB_URL)

st.set_page_config(page_title="Taxi Analytics Dashboard", layout="wide")
st.title("Taxi Big Data Project Dashboard")

# Load tables from PostgreSQL
trips_by_day = pd.read_sql("SELECT * FROM analytics.trips_by_day", engine)
revenue_by_day = pd.read_sql("SELECT * FROM analytics.revenue_by_day", engine)
peak_hours = pd.read_sql("SELECT * FROM analytics.peak_hours", engine)
payment_type_summary = pd.read_sql("SELECT * FROM analytics.payment_type_summary", engine)
top_pickup_locations = pd.read_sql("SELECT * FROM analytics.top_pickup_locations", engine)

# KPIs
total_trips = trips_by_day["total_trips"].sum()
total_revenue = revenue_by_day["total_revenue"].sum()

col1, col2 = st.columns(2)
col1.metric("Total Trips", f"{int(total_trips):,}")
col2.metric("Total Revenue", f"${total_revenue:,.2f}")

# Trips by day
st.subheader("Trips by Day")
fig1 = px.line(trips_by_day, x="pickup_date", y="total_trips", markers=True)
st.plotly_chart(fig1, use_container_width=True)

# Revenue by day
st.subheader("Revenue by Day")
fig2 = px.line(revenue_by_day, x="pickup_date", y="total_revenue", markers=True)
st.plotly_chart(fig2, use_container_width=True)

# Peak hours
st.subheader("Peak Pickup Hours")
fig3 = px.bar(peak_hours, x="pickup_hour", y="total_trips")
st.plotly_chart(fig3, use_container_width=True)

# Payment type summary
st.subheader("Payment Type Summary")
st.dataframe(payment_type_summary, use_container_width=True)

# Top pickup locations
st.subheader("Top Pickup Locations")
fig4 = px.bar(top_pickup_locations, x="PULocationID", y="total_trips")
st.plotly_chart(fig4, use_container_width=True)