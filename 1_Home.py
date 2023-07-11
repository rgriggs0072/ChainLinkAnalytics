
# Contents of ~/my_app/streamlit_app.py
import streamlit as st
#from streamlit_option_menu import option_menu
import pandas as pd
from PIL import Image
import plotly.express as px
import numpy as np
import time
import streamlit.components.v1 as components


# Set page to wide display to give more room
st.set_page_config(layout="wide")
padding_top = 0
st.snow() 

# This function sets the logo and company name inside the sidebar
def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo

my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height = 100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")






# Set Page Header   
st.header("Manage Chain Resets and Gap Analysis")
# Set custom CSS for hr element
st.markdown("""
        <style>
            hr {
                margin-top: 0.5rem;
                margin-bottom: 0.5rem;
                height: 3px;
                background-color: #333;
                border: none;
            }
        </style>
    """, unsafe_allow_html=True)

# Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)


# Load data
df = px.data.gapminder()
df = df[(df['continent']=='Asia') & (df['year'] >= 1932) & (df['year'] <= 2007)]

# Create animated chart using Plotly
fig = px.scatter(df, x='gdpPercap', y='lifeExp', size='pop', color='country', log_x=True, hover_name='country', animation_frame='year', range_x=[200, 100000], range_y=[20, 90])

# Create a container for the chart
chart_container = st.container()

# Display the chart in the container
with chart_container:
    chart = st.plotly_chart(fig, use_container_width=True)

# Animate the chart for 5 seconds
end_time = time.time() + 5
while time.time() < end_time:
    for frame in fig.frames:
        with chart_container:
            chart.plotly_chart({"data": frame.data, "layout": frame.layout})
        time.sleep(0.5)


