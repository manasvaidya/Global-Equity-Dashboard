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



# Create empty data frame
df = pd.DataFrame(input_sectors, columns = ['sector'])

# Create DS Sector Codes column
df['sector_ticker'] = df['sector'].map(dict_sectors)

# Create G#L modified DS sector codes column
df['sector_ticker_gl'] = 'G#L' + df['sector_ticker']

# Create IBES ticker codes column
df['sector_ticker_ibes'] = df['sector'].map(dict_ibes)

# Create Tickers
tickers = ','.join(df['sector_ticker'])
tickers_gl = ','.join(df['sector_ticker_gl'].to_list())
tickers_ibes = ','.join(df['sector_ticker_ibes'].to_list())





########################################################################################################
#################################### PERFORMANCE #######################################################
########################################################################################################




############################################# MTD (%) Metric ##########################################

# MTD parameters
fields_mtd = ['PCHV#(X,MTD)']

# Create MTD metric dataframe
df_mtd = ds.get_data(tickers = tickers, 
            start = start, 
            end = end,
            freq = freq_d,
            fields=fields_mtd
           )

# Merge to main dataframe
df = df.merge(df_mtd[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'mtd_performance'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





############################################# YTD (%) Metric ##########################################

# MTD parameters
fields_ytd = ['PCHV#(X,YTD)']

# Create MTD metric dataframe
df_ytd = ds.get_data(tickers = tickers, 
            start = start, 
            end = end,
            freq = freq_d,
            fields=fields_ytd
           )

# Merge to main dataframe
df = df.merge(df_ytd[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'ytd_performance'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





############################################# 1Y Performance Metric ##########################################

# MTD parameters
fields_1y = ['PCH#(X,1Y)']

# Create MTD metric dataframe
df_1y = ds.get_data(tickers = tickers, 
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_1y
           )

# Merge to main dataframe
df = df.merge(df_1y[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': '1y_performance'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



#################################### 3Y Performance ########################################

# MTD parameters
fields_3y = ['GRFL#(X,3Y)']

  
## Extract MTD Performance Data
df_3y = ds.get_data(tickers = tickers, 
                    start = start, 
                    end = end,
                    freq=freq_d,
                    fields = fields_3y
                   )

# Merge to main dataframe
df = df.merge(df_3y[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': '3y_cagr'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






########################################################################################################
#################################### TECHNICALS #######################################################
########################################################################################################




############################################# RSI Metric ##########################################

# RSI parameters
fields_rsi = ['RSI#(X,14D)']

# Create RSI metric dataframe
df_rsi = ds.get_data(tickers = tickers, 
            start = start, 
            end = end,
            freq = freq_d,
            fields=fields_rsi
           )

# Merge to main dataframe
df = df.merge(df_rsi[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'rsi_14'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




############################################# Breadth Metric ##########################################

# Breadth Parameters
fields_breadth = ['(LIST#(X,IF#(X-MAV#(X,200D),GT,ZERO),AVG))*100.00']

# Create Breadth metric dataframe
df_breadth = ds.get_data(tickers = tickers_gl,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_breadth
           )

# Merge to main dataframe
df = df.merge(df_breadth[['Instrument', 'Value']], left_on = 'sector_ticker_gl', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'breadth'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Create 90days past Breadth metric dataframe
df_breadth_90past = ds.get_data(tickers = tickers_gl,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_breadth
           )

# Merge to main dataframe
df = df.merge(df_breadth_90past[['Instrument', 'Value']], left_on = 'sector_ticker_gl', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'breadth_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['breadth_viz'] = (df['breadth'].astype(str)
                  + np.where(df['breadth'].gt(df['breadth_90past']), UP, DOWN)
                   )




############################################# 200d Rel Metric ##########################################

# 200d Rel Metric Parameters
fields_200d = ['100*(REB#(X)/MAV#(REB#(X),200D)-1.00)']

# Create 200d Rel metric dataframe
df_200d = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_200d
           )

# Merge to main dataframe
df = df.merge(df_200d[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'rel_200d'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Create 90 day past 200d Rel metric dataframe
df_200d_90past = ds.get_data(tickers = tickers,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_200d
           )

# Merge to main dataframe
df = df.merge(df_200d_90past[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'rel_200d_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





# Convert to string and add arrow
df['rel_200d_viz'] = (df['rel_200d'].astype(str)
                  + np.where(df['rel_200d'].gt(df['rel_200d_90past']), UP, DOWN)
                   )


############################################# A/D Line Metric ##########################################

# A/D Line Parameters
fields_ad = ["100.000*MAV#(X(RS)/(X(FS)+X(RS)),1M)"]


# Create A/D Line Rel metric dataframe
df_ad = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_ad
           )

# Merge to main dataframe
df = df.merge(df_ad[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'ad_line'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)


# Create A/D Line Rel metric dataframe
df_ad_90past = ds.get_data(tickers = tickers,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_ad
           )

# Merge to main dataframe
df = df.merge(df_ad_90past[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'ad_line_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['ad_line_viz'] = (df['ad_line'].astype(str)
                  + np.where(df['ad_line'].gt(df['ad_line_90past']), UP, DOWN)
                   )



