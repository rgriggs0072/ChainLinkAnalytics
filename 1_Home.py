from cgitb import text
import streamlit as st
from streamlit_option_menu import option_menu
import snowflake.connector
import pandas as pd
from PIL import Image
import plotly.express as px
import numpy as np
import time
import altair as alt
import streamlit.components.v1 as components

# Set page to wide display to give more room
st.set_page_config(layout="wide")
padding_top = 0
# st.snow()

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# This function sets the logo and company name inside the sidebar
def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo

my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height=100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")

# Set Page Header
st.header("Manage Chain Resets and Gap Analysis")
# Set custom CSS for hr element
st.markdown(
    """
        <style>
            hr {
                margin-top: 0.0rem;
                margin-bottom: 0.5rem;
                height: 3px;
                background-color: #333;
                border: none;
            }
        </style>
    """,
    unsafe_allow_html=True,
)

# Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)

# Fetch data from the SCHMATIC_SUMMARY view
def fetch_schematic_summary_data():
    # Load Snowflake credentials from the secrets.toml file
    snowflake_creds = st.secrets["snowflake"]
    
    # Establish a new connection to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_creds["account"],
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        warehouse=snowflake_creds["warehouse"],
        database=snowflake_creds["database"],
        schema=snowflake_creds["schema"]
    )

    sql_query = """
    SELECT 
        UPC,
        PRODUCT_NAME,
        "Total_In_Schematic",
        "Purchased",
        "Purchased_Percentage"
    FROM 
        DATASETS.DATASETS.SCHEMATIC_SUMMARY;
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["UPC", "Product Name", "Total In Schematic", "Purchased", "Purchased Percentage"])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Format the Purchased Percentage column as percentage with two decimal places
    df["Purchased Percentage"] = df["Purchased Percentage"].apply(lambda x: f"{float(x):.2f}%")


    return df

# Fetch schematic summary data
schematic_summary_data = fetch_schematic_summary_data()

# Add centered and styled title above the scatter chart
st.markdown("<h1 style='text-align: center; font-size: 18px;'>By Product, in Schematic Compared Against Product Sold In and Percentage</h1>", unsafe_allow_html=True)

# Create a scatter chart using Altair
scatter_chart = alt.Chart(schematic_summary_data).mark_circle().encode(
    x='Total In Schematic',
    y='Purchased',
    color='Product Name',
    tooltip=['Product Name', 'UPC', 'Total In Schematic', 'Purchased', 'Purchased Percentage']
).interactive()

# Display the scatter chart using Streamlit
st.altair_chart(scatter_chart, use_container_width=True)

# Fetch data from the new view that includes chain name, product name, and purchased percentage
def fetch_chain_schematic_data():

    # Load Snowflake credentials from the secrets.toml file
    snowflake_creds = st.secrets["snowflake"]
    # Establish a new connection to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_creds["account"],
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        warehouse=snowflake_creds["warehouse"],
        database=snowflake_creds["database"],
        schema=snowflake_creds["schema"]
    )    

    sql_query = """
    SELECT 
        CHAIN_NAME,
        "Total_In_Schematic",
        "Purchased",
        "Purchased_Percentage"
    FROM 
        DATASETS.DATASETS.CHAIN_SCHEMATIC_SUMMARY
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["CHAIN_NAME", "Total_In_Schematic", "Purchased", "Purchased_Percentage"])

    cursor.close()
    conn.close()

    # Format the Purchased Percentage column as percentage with two decimal places
    df["Purchased_Percentage"] = df["Purchased_Percentage"].apply(lambda x: f"{float(x):.2f}%")


    return df


#===============================================================================================================================================
# Provides Execution by Chain barchart
#===============================================================================================================================================
# Fetch chain schematic data
chain_schematic_data = fetch_chain_schematic_data()

# Add centered and styled title above the scatter chart
st.markdown("<h1 style='text-align: left; font-size: 18px;'>By Chain, in Schematic Compared Against Product Sold In and Percentage</h1>", unsafe_allow_html=True)

# Clean and convert the Purchased_Percentage column to numeric
chain_schematic_data['Purchased_Percentage'] = chain_schematic_data['Purchased_Percentage'].str.replace('%', '').astype(float)

# Calculate summary statistics
total_in_schematic = chain_schematic_data['Total_In_Schematic'].sum()
total_purchased = chain_schematic_data['Purchased'].sum()
average_percentage = chain_schematic_data['Purchased_Percentage'].mean()

# Create a bar chart using Altair with percentage labels on bars
bar_chart = alt.Chart(chain_schematic_data).mark_bar().encode(
    x='CHAIN_NAME',
    y='Total_In_Schematic',
    color=alt.Color('Purchased_Percentage', scale=alt.Scale(scheme='viridis')),
    tooltip=['CHAIN_NAME', 'Total_In_Schematic', 'Purchased', 'Purchased_Percentage']
).properties(
    width=800,  # Adjust the width as needed
    height=400,  # Adjust the height as needed
).configure_title(
    align='center',
    fontSize=16  # Adjust the font size as needed
).encode(
    text=alt.Text('Purchased_Percentage:Q', format='.2f')  # Format the percentage label
).configure_mark(
    fontSize=14  # Adjust the font size of the percentage label
)


#=======================================================================================================================================
# Create two columns to display barchart in column 1 and execution summary in column two
#=======================================================================================================================================

# Create a layout with two columns
col1, col2 = st.columns([2, 1])
#======================================================================================================================================
# call above code to get the data and display barchart
#=========================================================================================================================================

# Display the bar chart in the first column
col1.altair_chart(bar_chart, use_container_width=False)

#===============================================================================================================================
# Calculate the pruchased percentage and display in the summary data
#==============================================================================================================================================

# Calculate the overall purchased percentage
overall_percentage = (total_purchased / total_in_schematic) * 100

# Display the summary data in the second column
col2.markdown("<div style='text-align: center; margin-top: 10px;'>", unsafe_allow_html=True)
#col2.markdown(f"<div style='{summary_style}'>", unsafe_allow_html=True)
col2.write("Execution Summary:")
col2.write(f"Total In Schematic: {total_in_schematic}")
col2.write(f"Total Purchased: {total_purchased}")
col2.write(f"Overall Purchased Percentage: {overall_percentage:.2f}%")
col2.markdown("</div>", unsafe_allow_html=True)
col2.markdown("</div>", unsafe_allow_html=True)





## Fetch chain schematic data
#chain_schematic_data = fetch_chain_schematic_data()

## Add centered and styled title above the scatter chart
#st.markdown("<h1 style='text-align: center; font-size: 18px;'>By Chain, in Schematic Compared Against Product Sold In and Percentage</h1>", unsafe_allow_html=True)

## Create a scatter chart using Altair
#scatter_chart = alt.Chart(chain_schematic_data).mark_circle().encode(
#    x='Total_In_Schematic',
#    y='Purchased',
#    color='CHAIN_NAME',
#    tooltip=['CHAIN_NAME', 'Total_In_Schematic', 'Purchased', 'Purchased_Percentage']
#).properties(
#    #title="By Chain, Product in Schematic Compared to Sold in and Percentage"
#).configure_title(
#    align='center',
#    fontSize=16  # Adjust the font size as needed
#).interactive()

## Display the scatter chart using Streamlit
#st.altair_chart(scatter_chart, use_container_width=True)