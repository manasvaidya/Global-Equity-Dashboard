###########################################
########## Package Imports
###########################################
import pandas as pd, numpy as np
import DatastreamDSWS as DSWS
from datetime import datetime as dt





##############################################
######## Initialise Connection
##############################################

# Initialise login credentials
username=st.secrets.credentials.username
password=st.secrets.credentials.password

# Create connection using the username and password
ds = DSWS.Datastream(username = username, password = password)


