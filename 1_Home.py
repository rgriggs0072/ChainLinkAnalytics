#from cgitb import text
import streamlit as st
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
st.set_page_config(
    layout="wide",
    initial_sidebar_state="collapsed")
padding_top = 0



#==================================================================================================================
# Read the style css formatting information
#==================================================================================================================
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
   
#==================================================================================================================
#  End read the style css formating information
#==================================================================================================================

#------------------------------------------------------------------------------------------------------------------

#==================================================================================================================
# Create the sidebar place for holding client logo and setting page header
#==================================================================================================================

st.sidebar.header("Dashboard For")

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

#==================================================================================================================
# End block for Create the sidebar place for holding client logo
#==================================================================================================================

#-------------------------------------------------------------------------------------------------------------------

#===========================================================================================================================================
# Create three columns to display  Salesperson Store count in column 1,  execution summary in column two and chain barchart in column three
#===========================================================================================================================================

# Create a layout with two columns
col1, col2, col3 = st.columns([20, 30, 50], gap="small")

#===========================================================================================================================================
# end block for Create three columns to display  Salesperson Store count in column 1,  execution summary in column two and chain barchart in column three
#===========================================================================================================================================

#--------------------------------------------------------------------------------------------------------------------------------------------

#===========================================================================================================================================
# Function that will connect to DB and pull data to display the Execution Summary Data in column 2
#===========================================================================================================================================

## Function to calculate and display execution summary
def display_execution_summary():

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

    # Retrieve data from your view
    query = "SELECT SUM(\"In_Schematic\") AS total_in_schematic, SUM(\"PURCHASED_YES_NO\") AS purchased, SUM(\"PURCHASED_YES_NO\") / COUNT(*) AS purchased_percentage FROM GAP_REPORT;"
    df = pd.read_sql(query, conn)

    # Calculate total gaps
    total_gaps = df['TOTAL_IN_SCHEMATIC'].iloc[0] - df['PURCHASED'].iloc[0]

    # Calculate the purchased percentage as a float
    purchased_percentage = float(df['PURCHASED_PERCENTAGE'].iloc[0])

    # Convert the percentage to the desired format (e.g., 59.98%)
    formatted_percentage = f"{purchased_percentage * 100:.2f}%"

    # Close the connection
    conn.close()

    # Return the calculated values
    return df['TOTAL_IN_SCHEMATIC'].iloc[0], df['PURCHASED'].iloc[0], total_gaps, formatted_percentage

#===========================================================================================================================================
# End Block for Function that will connect to DB and pull data to display the Execution Summary Data in column 2
#===========================================================================================================================================

#-------------------------------------------------------------------------------------------------------------------------------------------

#===========================================================================================================================================
# Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in column 3
#===========================================================================================================================================

# Function to fetch data for the bar chart
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

    # Fetch data for the bar chart (modify the query to match your view)
    query = "SELECT CHAIN_NAME, SUM(\"In_Schematic\") AS total_in_schematic, SUM(\"PURCHASED_YES_NO\") AS purchased, SUM(\"PURCHASED_YES_NO\") / COUNT(*) AS purchased_percentage FROM CHAINLINK_DEVELOPMENT.CHAINLINK_DEV.EXECUTION_SUMMARY GROUP BY CHAIN_NAME;"
    df = pd.read_sql(query, conn)
    df = pd.read_sql(query, conn)
    #st.write(df)
    # Close the connection
    conn.close()
    
    return df


#===========================================================================================================================================
# Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in column 3
#===========================================================================================================================================

#-------------------------------------------------------------------------------------------------------------------------------------------

#===============================================================================================================================================
# Call function fetch_chain_schematic_data() to get data for bar chart and display it in column 3 
#===============================================================================================================================================
# Fetch chain schematic data
chain_schematic_data = fetch_chain_schematic_data()
#st.write(chain_schematic_data)

# Clean and convert the Purchased_Percentage column to numeric
chain_schematic_data['PURCHASED_PERCENTAGE'] = chain_schematic_data['PURCHASED_PERCENTAGE'].astype(str)
chain_schematic_data['PURCHASED_PERCENTAGE'] = chain_schematic_data['PURCHASED_PERCENTAGE'].str.replace('%', '').astype(float)


# Calculate summary statistics
total_in_schematic = chain_schematic_data['TOTAL_IN_SCHEMATIC'].sum()
total_purchased = chain_schematic_data['PURCHASED'].sum()
average_percentage = chain_schematic_data['PURCHASED_PERCENTAGE'].mean()

# Create a bar chart using Altair with percentage labels on bars
bar_chart = alt.Chart(chain_schematic_data).mark_bar().encode(
    x='CHAIN_NAME',
    y='TOTAL_IN_SCHEMATIC',
    color=alt.Color('PURCHASED_PERCENTAGE', scale=alt.Scale(scheme='viridis')),
    tooltip=['CHAIN_NAME', 'TOTAL_IN_SCHEMATIC', 'PURCHASED', 'PURCHASED_PERCENTAGE']
).properties(
    width=800,  # Adjust the width as needed
    height=400,  # Adjust the height as needed
).configure_title(
    align='center',
    fontSize=16  # Adjust the font size as needed
).encode(
    text=alt.Text('PURCHASED_PERCENTAGE:Q', format='.2f')  # Format the percentage label
).configure_mark(
    fontSize=14  # Adjust the font size of the percentage label
)

# Display the bar chart in the third column
col3.altair_chart(bar_chart, use_container_width=False)

#===============================================================================================================================================
# Call function fetch_chain_schematic_data() to get data for bar chart and display it in column 3 
#===============================================================================================================================================

#-----------------------------------------------------------------------------------------------------------------------------------------------

#===============================================================================================================================================
# This block will call salesperson data from table and display the salesperson and number of stores they susport in column 1
#===============================================================================================================================================


# Load Snowflake credentials from the secrets.toml file
snowflake_creds = st.secrets["snowflake"]
# Connect to Snowflake (replace with your Snowflake credentials)
conn = snowflake.connector.connect(
       account=snowflake_creds["account"],
       user=snowflake_creds["user"],
       password=snowflake_creds["password"],
       warehouse=snowflake_creds["warehouse"],
       database=snowflake_creds["database"],
       schema=snowflake_creds["schema"]
)    

# Execute the SQL query to retrieve the salesperson's store count
query = pd.read_sql_query('''
                          SELECT SALESPERSON, TOTAL_STORES FROM CHAINLINK_DEVELOPMENT.CHAINLINK_DEV.SALESPERSON_STORE_COUNT
                          ''',conn)

# cursor.close()
conn.close()

# Create a DataFrame from the query results
salesperson_df = pd.DataFrame(query, columns=['SALESPERSON', 'TOTAL_STORES'])

# Rename the columns
salesperson_df = salesperson_df.rename(columns={'SALESPERSON': 'Salesperson', 'TOTAL_STORES': 'Stores'})

# Limit the number of displayed rows to 6
limited_salesperson_df = salesperson_df.head(100)

# Define the maximum height for the table container
max_height = '365px'

## Adjust the width of the table by changing the 'width' property
table_style = f"max-height: {max_height}; overflow-y: auto; background-color: #EEEEEE; padding: 1% 2% 2% 0%; border-radius: 10px; border-left: 0.5rem solid #9AD8E1 !important; box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important; width: 100%;"

# Wrap the table in an HTML div with the specified style
table_html = limited_salesperson_df.to_html(classes=["table", "table-striped"], escape=False, index=False)

# Add style to the table tag to allow automatic column width adjustment
table_with_scroll = f"<div style='{table_style}'><table style='table-layout: auto;'><colgroup><col style='width: 60%;'><col style='width: 30%;'></colgroup>{table_html}</table></div>"

# Display the table in col1 with custom formatting
with col1:
    # Display the table with custom formatting
    st.markdown(table_with_scroll, unsafe_allow_html=True)
  

#===============================================================================================================================================
# End  block will call salesperson data from table and display the salesperson and number of stores they susport in column 1
#===============================================================================================================================================

#-----------------------------------------------------------------------------------------------------------------------------------------------
#===========================================================================================================================================
# Call display_execution_summary() to get the execution summary data and display it for the user in column 2
#===========================================================================================================================================

# Fetch the data from the function
total_in_schematic, total_purchased, total_gaps, formatted_percentage = display_execution_summary()

 #Display the values in col2
with col2:
    # Add centered and styled title above the content in the second column
    col2.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary</h1>", unsafe_allow_html=True)

    # Center-align each markdown line
    st.markdown(f"<p style='text-align: center;'>Total In Schematic: {total_in_schematic}</p>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center;'>Total Purchased: {total_purchased}</p>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center;'>Total Gaps: {total_gaps}</p>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center;'>Overall Purchased Percentage: {formatted_percentage}</p>", unsafe_allow_html=True)

#===========================================================================================================================================
# End block to Call display_execution_summary() to get the execution summary data and display it for the user in column 2
#===========================================================================================================================================

#----------------------------------------------------------------------------------------------------------------------------------------------


#===============================================================================================================================================
# snowflake toml connection information for pulling data from tables
#===============================================================================================================================================


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

#===============================================================================================================================================
# End snowflake toml connection information for pulling data from tables
#===============================================================================================================================================

#-----------------------------------------------------------------------------------------------------------------------------------------------

#===============================================================================================================================================
# Function to pull supplier data to populate sidebar dropdown
#===============================================================================================================================================
# Fetch supplier names from the supplier_county table
def fetch_supplier_names():
    query = "SELECT DISTINCT supplier FROM supplier_county order by supplier"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    supplier_names = [row[0] for row in results]
    cursor.close()
    return supplier_names

#===============================================================================================================================================
# End Function to pull supplier data to populate sidebar dropdown
#===============================================================================================================================================


#----------------------------------------------------------------------------------------------------------------------------------

#=================================================================================================================================================
# Function to pull data to populate the product by supplier chart once suppliers have been selected
#================================================================================================================================================


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

#=================================================================================================================================================
# End Function to pull data to populate the product by supplier chart once suppliers have been selected
#================================================================================================================================================

#-------------------------------------------------------------------------------------------------------------------------------------------------

#================================================================================================================================================
# Function to pull product by supplier scatter chart once the supplier have been selected from the sidebar selection widget
#=================================================================================================================================================

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

#================================================================================================================================================
# End Function to pull product by supplier scatter chart once the supplier have been selected from the sidebar selection widget
#=================================================================================================================================================

#--------------------------------------------------------------------------------------------------------------------------------------------------

#==================================================================================================================================================
# This Block of codes creates the sidebar multi select widget for selecting suppliers then calls function to get supplier data then display it in
# the barchart for each supplier.  Additonally it calls the function to get the data for the supplier to populate the scatter chart for each product
# for the selected supplier
#====================================================================================================================================================
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

#==================================================================================================================================================
# End This Block of codes creates the sidebar multi select widget for selecting suppliers then calls function to get supplier data then display it in
# the barchart for each supplier.  Additonally it calls the function to get the data for the supplier to populate the scatter chart for each product
# for the selected supplier
#====================================================================================================================================================



