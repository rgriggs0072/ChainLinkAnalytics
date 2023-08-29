#from cgitb import text
import streamlit as st
from streamlit_option_menu import option_menu
import snowflake.connector
import pandas as pd
from PIL import Image
import plotly.express as px
import numpy as np
import base64
import time
import altair as alt
import streamlit.components.v1 as components

# Set page to wide display to give more room
st.set_page_config(layout="wide")
padding_top = 0




with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    




@st.cache_data
def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

img = get_img_as_base64("./images/DeltaPacific_Logo.jpg")

page_bg_img = f"""
<style>
[data-testid="stSidebar"] > div:first-child {{
    background-image: url("data:image/jpg;base64,{img}");
    background-size: 200px;
    background-repeat: no-repeat;
    background-position: top calc(140px + 40%);  /* Adjust the space here */
}}
</style>
"""

st.markdown(page_bg_img, unsafe_allow_html=True)
#st.sidebar.header("Configuration")

# # This function sets the logo and company name inside the sidebar
# def add_logo(logo_path, width, height):
#     """Read and return a resized logo"""
#     logo = Image.open(logo_path)
#     modified_logo = logo.resize((width, height))
#     return modified_logo

# my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height=100)
# st.sidebar.image(my_logo)
# st.sidebar.subheader("Delta Pacific Beverage Co.")

# Set Page Header
st.header("Delta Pacific Beverage Chain Dashboard")
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
col1, col2 = st.columns([1, 2])
#======================================================================================================================================
# call above code to get the data and display barchart
#=========================================================================================================================================

# Add centered and styled title above the scatter chart in the second column
col2.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary by Chain</h1>", unsafe_allow_html=True)

# Display the scatter chart in the second column

#===============================================================================================================================
# Calculate the pruchased percentage and display in the summary data
#==============================================================================================================================================

# Calculate the overall purchased percentage
overall_percentage = (total_purchased / total_in_schematic) * 100

# Display the summary data in the first column
col1.markdown("<div style='text-align: center; margin-top: 10px;'>", unsafe_allow_html=True)
#col2.markdown(f"<div style='{summary_style}'>", unsafe_allow_html=True)
col1.write("Execution Summary:")
col1.write(f"Total In Schematic: {total_in_schematic}")
col1.write(f"Total Purchased: {total_purchased}")
col1.write(f"Overall Purchased Percentage: {overall_percentage:.2f}%")
col1.markdown("</div>", unsafe_allow_html=True)
col1.markdown("</div>", unsafe_allow_html=True)

# Display the bar chart in the first column
col2.altair_chart(bar_chart, use_container_width=False)

#============================================================================================================================================
# End Barchart and summary by chain
#=============================================================================================================================================

#------------------------------------------------------------------------------------------------------------------------------------------

#============================================================================================================================================
# Build scatter chart and summary by supplier
#=============================================================================================================================================



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

# Fetch supplier names from the supplier_county table
def fetch_supplier_names():
    query = "SELECT DISTINCT supplier FROM supplier_county order by supplier"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    supplier_names = [row[0] for row in results]
    cursor.close()
    return supplier_names

# Create a sidebar select widget for selecting suppliers
#selected_suppliers = st.sidebar.multiselect("Select Suppliers", fetch_supplier_names())

# Close the Snowflake connection
#conn.close()



# Fetch schematic summary data for selected suppliers
def fetch_supplier_schematic_summary_data(selected_suppliers):
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

    supplier_conditions = ", ".join([f"'{supplier}'" for supplier in selected_suppliers])
    
    sql_query = f"""
    SELECT 
        SUPPLIER_Name,
        "Total_In_Schematic",
        "Purchased",
        "Purchased_Percentage"
        
    FROM 
        DATASETS.DATASETS.supplier_schematic_summary
    WHERE
        SUPPLIER_name IN ({supplier_conditions});
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["Supplier_Name","Total In Schematic", "Purchased", "Purchased Percentage"])

    # Close the cursor and connection
    cursor.close()
    conn.close()
   
    # Format the Purchased Percentage column as percentage with two decimal places
    df["Purchased Percentage"] = df["Purchased Percentage"].apply(lambda x: f"{float(x):.2f}%")

    return df

#.........


#......................
# Fetch schematic summary data for selected suppliers
def fetch_scatter_supplier_schematic_summary_data(selected_suppliers):
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

    supplier_conditions = ", ".join([f"'{supplier}'" for supplier in selected_suppliers])
    
    sql_query = f"""
    SELECT 
        UPC,
         PRODUCT_NAME,
        "Total_In_Schematic",
        "Purchased",
        "Purchased_Percentage"
        
    FROM 
        DATASETS.DATASETS.schematic_summary
    WHERE
        SUPPLIER IN ({supplier_conditions});
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["UPC","PRODUCT_NAME", "Total In Schematic", "Purchased", "Purchased Percentage"])

    # Close the cursor and connection
    cursor.close()
    conn.close()
   
    # Format the Purchased Percentage column as percentage with two decimal places
    df["Purchased Percentage"] = df["Purchased Percentage"].apply(lambda x: f"{float(x):.2f}%")

    return df

#...................................................

# Add centered and styled title above the bar chart
st.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary by Supplier</h1>", unsafe_allow_html=True)

# Create a sidebar select widget for selecting suppliers
selected_suppliers = st.sidebar.multiselect("Select Suppliers", fetch_supplier_names())

# Fetch supplier schematic summary data for selected suppliers if there are any
supplier_schematic_summary_data = None
if selected_suppliers:
    supplier_schematic_summary_data = fetch_supplier_schematic_summary_data(selected_suppliers)

# Display the bar chart if there is data
if supplier_schematic_summary_data is not None:
    # Create a bar chart using Altair
    supplier_bar_chart = alt.Chart(supplier_schematic_summary_data).mark_bar().encode(
        x='Supplier_Name',
        y='Total In Schematic',
        color=alt.Color('Purchased Percentage', scale=alt.Scale(scheme='viridis')),
        tooltip=['Supplier_Name', 'Total In Schematic', 'Purchased', 'Purchased Percentage']
    ).interactive()

    # Display the supplier bar chart
    st.altair_chart(supplier_bar_chart, use_container_width=True)
else:
    # If supplier_schematic_summary_data is None, display a message
    st.write("Please select one or more suppliers to view the chart")

# Add centered and styled title above the scatter chart
st.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary by Product by Supplier</h1>", unsafe_allow_html=True)

# Fetch schematic summary data for selected suppliers if there are any
scatter_schematic_summary_data = None
if selected_suppliers:
    scatter_schematic_summary_data = fetch_scatter_supplier_schematic_summary_data(selected_suppliers)

# Check if scatter_schematic_summary_data is not None before creating the scatter chart
if scatter_schematic_summary_data is not None:
    # Create a scatter chart using Altair
    scatter_chart = alt.Chart(scatter_schematic_summary_data).mark_circle().encode(
        x='Total In Schematic',
        y='Purchased',
        color='PRODUCT_NAME',
        tooltip=['PRODUCT_NAME', 'UPC', 'Total In Schematic', 'Purchased', 'Purchased Percentage']
    ).interactive()

    # Display the scatter chart
    st.altair_chart(scatter_chart, use_container_width=True)
else:
    # If scatter_schematic_summary_data is None, create an empty DataFrame with appropriate columns
    empty_data = pd.DataFrame(columns=["Product Name", "UPC", "Total In Schematic", "Purchased", "Purchased Percentage"])
  
    # Create a scatter chart with the empty DataFrame
    scatter_chart = alt.Chart(empty_data).mark_circle().encode(
        x='Total In Schematic',
        y='Purchased'
    ).properties(
        title="Please select one or more suppliers to view the chart"
    )
    
    # Display the scatter chart
    st.altair_chart(scatter_chart, use_container_width=True)



 #...

