import snowflake.connector
import pandas as pd
import streamlit as st
from PIL import Image
import openpyxl
from io import BytesIO




#======================================================================================================================================
# Set page to always show in wide format
st.set_page_config(layout="wide")
padding_top = 10

#======================================================================================================================================  
# deals with padding for page

padding = 0
st.markdown(f""" <style>
    .reportview-container .main .sidebar=content{{
        padding-top: {0}rem;
        padding-right: {padding}rem;
        padding-left: {padding}rem;
        padding-bottom: {padding}rem;
    }} </style> """, unsafe_allow_html=True)

#======================================================================================================================================
# Funtion to add the load image to 
def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo
#=====================================================================================================================================
# Get the image and add to the sidebar calling function above also puts compacny name in sidebase
my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height = 100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")

# Sets the page header

# Set Page Header
st.header("Misc Reports and Analytics")

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
# Dividing sections of the page starting with the header  There will be a line between all elements
# Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)


#======================================================================================================================================

# Load Product Data function to the product table within snowflake
#======================================================================================================================================
# Load Product Data function to the product table within Snowflake
def fetch_product_analysis_data():
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

    # Execute the SQL query against the PRODUCT_ANALYSIS view and fetch the results into a DataFrame
    sql_query = """
    SELECT
        STORE_NAME,
        PRODUCT_NAME,
        SALESPERSON,
        UPC,
        _COUNT AS ProductCount
    FROM
        PRODUCT_ANALYSIS;  
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["Store", "Product", "Salesperson", "UPC", "ProductCount"])
    df['Salesperson'].fillna('Unknown', inplace=True)

    #st.write(df)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df
#===================================================================================================================================

# Button to call the product analysis data function above
#====================================================================================================================================

# Button to fetch and display the product analysis data
if st.button("Fetch Product Analysis Pivot Data"): 
    with st.spinner('Getting Product Analysis Data From Snowflake ...'):
    # Fetch the data
     df = fetch_product_analysis_data()

#====================================================================================================================================
#Create the excel pivot table and provide download button    
     # Pivot table creation
     pivot_table = pd.pivot_table(df, values="ProductCount", index=["UPC", "Product","Salesperson" ], columns="Store", fill_value=0)

     # Add total by salesperson
    pivot_table["Total"] = pivot_table.sum(axis=1)



    # Save the pivot table as an Excel file
    excel_file_path = "product_analysis_pivot.xlsx"
    pivot_table.to_excel(excel_file_path)

# Download button for the Excel file
    st.download_button(
        label="Download Product Analysis Report",
        data=open(excel_file_path, "rb").read(),
        file_name=excel_file_path,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

  
    





#====================================================================================================================================

# Function to pull Schematic Summary Data from snowflake

#===================================================================================================================================

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

    # Execute the SQL query against the schematic_summary view and fetch the results into a DataFrame
    sql_query = """
    SELECT 
    "dg_upc" AS UPC,
    SUPPLIER,
    PRODUCT_NAME,
    SUM("In_Schematic") AS Total_In_Schematic,
    SUM(PURCHASED_YES_NO) AS Total_Purchased,
    (SUM(PURCHASED_YES_NO) / SUM("In_Schematic")) * 100 AS Purchased_Percentage
     
FROM
    GAP_REPORT_TMP2
    where "sc_STATUS" = 'Yes'
    group by supplier, "dg_upc", product_name
    order by supplier;
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=["UPC", "SUPPLIER", "PRODUCT_NAME", "Total_In_Schematic", "Total_Purchased", "PURCHASED_PERCENTAGE"])
    #st.write(df)
    # Ensure 'PURCHASED_PERCENTAGE' is treated as a numeric (float) type
    df['PURCHASED_PERCENTAGE'] = df['PURCHASED_PERCENTAGE'].astype(float)
    # Remove rows with NaN values in 'PURCHASED_PERCENTAGE'
    df['Total_Purchased'].fillna(0, inplace=True)
    df['PURCHASED_PERCENTAGE'].fillna(0, inplace=True)
    # Perform rounding and formatting
    df['PURCHASED_PERCENTAGE'] = (df['PURCHASED_PERCENTAGE']).apply(lambda x: f"{x:.2f}%")

    #st.write(df)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df

#==================================================================================================================================
# Button to call the Scehmatic Summary data function above and create the excel file and a button to download the file
#====================================================================================================================================

# Button to fetch and display the product analysis data
if st.button("Fetch Schematic Summary Data"): 
    with st.spinner('Getting Schematic Summary Data From Snowflake ...'):
    # Fetch Schematic Summary Data
     df = fetch_schematic_summary_data()

# Download button for the Excel file
    excel_file_path = "SCHEMATIC_SUMMARY_DATA.xlsx"
    df.to_excel(excel_file_path, index=False)

    st.download_button(
        label="Download Schematic Summary Data",
        data=open(excel_file_path, "rb").read(),
        file_name=excel_file_path,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    