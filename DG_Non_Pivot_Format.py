from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
import streamlit as st
import pandas as pd


def format_non_pivot_table(workbook, stream, selected_option):
    df = pd.DataFrame(workbook.active.values)

    # Track issues
    rows_with_missing_values = []
    rows_with_apostrophe_issues = []
    rows_with_upc_hyphens = []

    smart_quote = "\u2019"

    for index, row in df.iterrows():
        missing_fields = []

        # Pull relevant values
        store_name = str(row[0]) if not pd.isna(row[0]) else ""
        store_number = row[1]
        upc = str(row[2]) if not pd.isna(row[2]) else ""
        chain_name = str(row[10]) if len(row) > 10 and not pd.isna(row[10]) else ""

        # --- Normalize store name
        normalized_store_name = store_name.replace("'", "").replace(smart_quote, "")
        if normalized_store_name != store_name:
            rows_with_apostrophe_issues.append(index)
            df.at[index, 0] = normalized_store_name

        # --- Normalize UPC
        if "-" in upc:
            cleaned_upc = upc.replace("-", "")
            df.at[index, 2] = cleaned_upc
            rows_with_upc_hyphens.append(index)

        # --- Validate required fields
        if not store_name:
            missing_fields.append("STORE NAME")
        if pd.isna(store_number):
            missing_fields.append("STORE NUMBER")
        if not upc:
            missing_fields.append("UPC")
        if not chain_name:
            missing_fields.append("CHAIN NAME")

        if missing_fields:
            rows_with_missing_values.append(
                f"Row {index + 1}: Missing {', '.join(missing_fields)}"
            )

    # 🚫 Blocking validation failure
    if rows_with_missing_values:
        st.session_state['warnings_present'] = True
        with st.expander("❌ Missing Required Values", expanded=True):
            for msg in rows_with_missing_values:
                st.error(msg)
        st.error("Please fix these errors and re-upload the file.")
        return None

    # ✅ No blocking issues
    st.session_state['warnings_present'] = False

    # ℹ️ Informative cleanup messages
    if rows_with_apostrophe_issues:
        st.info(f"Cleaned apostrophes or smart quotes from {len(rows_with_apostrophe_issues)} store name(s).")

    if rows_with_upc_hyphens:
        st.info(f"Removed hyphens from {len(rows_with_upc_hyphens)} UPC(s) in the sheet.")

    # ✅ Success message
    st.success("✅ Formatting complete. File cleaned and ready for upload.")

    # Save cleaned DataFrame to new workbook
    wb_cleaned = Workbook()
    ws = wb_cleaned.active
    for r in dataframe_to_rows(df, index=False, header=False):
        ws.append(r)

    # Construct downloadable file
    chain_name_for_file = df.iloc[0, 10] if df.shape[1] > 10 else "CHAIN"
    cleaned_filename = f"{chain_name_for_file}_formatted_dg_file.xlsx"
    cleaned_stream = BytesIO()
    wb_cleaned.save(cleaned_stream)

    # 📥 Download button
   # st.download_button("📥 Download Cleaned File", cleaned_stream.getvalue(), file_name=cleaned_filename)

    return wb_cleaned
