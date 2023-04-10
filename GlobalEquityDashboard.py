###########################################
########## Package Imports
###########################################
import pandas as pd, numpy as np
import DatastreamDSWS as DSWS
from datetime import datetime as dt
import streamlit as st





##############################################
######## Initialise Connection
##############################################

# Initialise login credentials
username=st.secrets.credentials.username
password=st.secrets.credentials.password

# Create connection using the username and password
ds = DSWS.Datastream(username = username, password = password)



#####################################################################################
######################### Initialise Mappings ##############################
#####################################################################################

# Sector Name to Sector DS code mapping
dict_sectors = {
    "Technology": "TECNOWD",
    "Financials": "FINANWD",
    "Consumer Discretionary": "CNDISWD",
    "Industrials": "INDUSWD",
    "Healthcare": "HLTHCWD",
    "Consumer Staples": "COSTPWD",
    "Energy": "ENEGYWD",
    "Basic Materials": "BMATRWD",
    "Telecom": "TELCMWD",
    "Utilities": "UTILSWD",
    "Real Estate": "RLESTWD"
}

# Sector Name to IBES Code Mapping
dict_ibes = {
    "Technology": "@:AFM1IT",
    "Financials": "@:AFM1FN",
    "Consumer Discretionary": "@:AFM1CD",
    "Industrials": "@:AFM1ID",
    "Healthcare": "@:AFM1HC",
    "Consumer Staples": "@:AFM1CS",
    "Energy": "@:AFM1E1",
    "Basic Materials": "@:AFM1M1",
    "Telecom": "@:AFM1T1",
    "Utilities": "@:AFM1U1",
    "Real Estate": "@:AFM1RE" #"@:AFM2R2"
}





# Sector Names List
input_sectors = ['Technology', 'Financials', 'Consumer Discretionary', 'Industrials', 'Healthcare',
                 'Consumer Staples', 'Energy', 'Basic Materials', 'Telecom', 'Utilities', 'Real Estate']



# Initialise Parameters
start = "-0d"
end = "-0d"
start_90 = '-90d'
end_90 = '-90d'
freq_d = "D"
freq_m = "M"


# Arrow Color code
UP = '<span style="color:green;">&#x25B2;</span>'
DOWN = '<span style="color:red;">&#x25BC;</span>'

