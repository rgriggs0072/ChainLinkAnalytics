import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import base64

def verify_snowflake_connection():
    try:
        # Load Snowflake credentials from secrets
        creds = st.secrets["snowflake_secure"]

        # Decode base64 key and load as private key
        key_bytes = base64.b64decode(creds["private_key_base64"])
        private_key_obj = serialization.load_pem_private_key(
            key_bytes,
            password=creds["private_key_passphrase"].encode()
        )

        # Convert key to DER format for Snowflake
        private_key = private_key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Establish connection
        conn = snowflake.connector.connect(
            user=creds["user"],
            account=creds["account"],
            private_key=private_key,
            warehouse=creds["warehouse"],
            database=creds["database"],
            schema=creds["schema"],
            role=creds["role"]
        )

        # Run diagnostic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
        results = cursor.fetchone()
        cursor.close()
        conn.close()

        # Display results
        st.success("✅ Snowflake connection verified!")
        st.markdown(f"**User**: `{results[0]}`")
        st.markdown(f"**Role**: `{results[1]}`")
        st.markdown(f"**Warehouse**: `{results[2]}`")
        st.markdown(f"**Database**: `{results[3]}`")
        st.markdown(f"**Schema**: `{results[4]}`")

    except Exception as e:
        st.error("❌ Failed to connect to Snowflake.")
        st.exception(e)



if st.button("🔌 Test Snowflake Connection"):
    verify_snowflake_connection()
