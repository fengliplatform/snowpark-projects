# Import python packages
import streamlit as st
import pandas as pd

from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Keypair Rotation :key:")
st.write(
    """ As required by enterprise security policy, please 
    rotate your application keypairs.
    """
)

# Get the current credentials
session = get_active_session()

# current user name
current_user = pd.DataFrame(session.sql("select current_user()").collect()).iloc[0,0]
st.write(f"Current user: {current_user}\n")

# Click Rotate button to rotate your PKI keypair
st.write("""
         Click button to rotate your PKI keypair. Following will happen:\n
         1 Your new PKI keypair will be generated in enterprise AWS account\n
         2 Your new private key will be saved into Secrets Manager \n
           where your application continue retrieving the new private key.\n
         3 Your new public key will be set for your Snowflake user\n
        """)
if st.button("Rotate Keypair"):
    session.sql(f"call db1.public.set_user_public_key('{current_user}')").collect()
    st.write("Success")

