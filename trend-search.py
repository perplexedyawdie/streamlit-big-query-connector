import streamlit as st
from BigQueryConnector import BigQueryConnection
import datetime
conn = st.experimental_connection("bigquery", type=BigQueryConnection)

st.title('International Google Search Trends')
country = st.selectbox(
    'Select a country',
    ('France', 'Taiwan', 'Argentina', 'Colombia', 'United Kingdom', 'Ukraine', 'Portugal', 'Indonesia', 'Netherlands', 'Japan', 'Canada', 'Malaysia', 'Norway', 'Switzerland', 'South Africa', 'Mexico', 'Philippines', 'Belgium', 'Thailand', 'Vietnam', 'Italy', 'Poland', 'Saudi Arabia', 'Austria', 'Romania', 'Hungary', 'Australia', 'Turkey', 'South Korea', 'Germany', 'India', 'Nigeria', 'New Zealand', 'Czech Republic', 'Chile', 'Brazil', 'Israel', 'Finland', 'Egypt', 'Sweden', 'Denmark'))
start_date = st.date_input("Start Date", datetime.date(2023, 4, 6))
end_date = st.date_input("End Date", datetime.date(2023, 7, 6))
query = f"""
SELECT
  term
FROM
  `bigquery-public-data.google_trends.international_top_rising_terms`
WHERE
  country_name = '{country}' 
  AND week BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
  term
LIMIT 10
"""
# data = conn.query(query)

def handle_search():
    if country and start_date and end_date:
        data = conn.query(query)
        st.dataframe(data, use_container_width=True, hide_index=True)
    else:
        st.warning("Please enter all the required values!")

st.button('Search', on_click=handle_search, use_container_width=True, type="primary")


# print(data)