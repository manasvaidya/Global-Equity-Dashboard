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







########################################################################################################
#################################### CYCLYCALITY #######################################################
########################################################################################################



############################################# Sector Beta ##########################################

# Metric Parameters
fields_beta = ['REGB#(LN#(TOTMKWD/LAG#(TOTMKWD,1M)),LN#(X/LAG#(X,1M)),60M)']

# Create Sector Beta metric dataframe
df_beta = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_m,
            fields = fields_beta
           )


# Merge to main dataframe
df = df.merge(df_beta[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'beta'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





############################################# Rolling Corr ##########################################

# Metric Parameters
fields_corr = ['CORR#(ACH#(GXCESIR,1M),PCH#(X/TOTMKWD,1M),60M)']

# Create Rolling Corr metric dataframe
df_corr = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_m,
            fields = fields_corr
           )


# Merge to main dataframe
df = df.merge(df_corr[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'rolling_corr'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






############################################# DXY Corr ##########################################

# Metric Parameters
fields_dxy = ['CORR#(PCH#(X,4W),PCH#(NDXYSPT,4W),200D)']

# Create Rolling Corr metric dataframe
df_corr = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_dxy
           )


# Merge to main dataframe
df = df.merge(df_corr[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'dxy_corr'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)








########################################################################################################
#################################### EARNINGS #######################################################
########################################################################################################




################################# 3M Moving Avg Earnings Revision Ratio ######################################

# Metric Parameters
fields_earnings_rev_3m = ['(MAV#(PAD#((X(A12UPE)-X(A12DNE))/(X(A12UPE)+X(A12DNE))),3M))*100.00']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_earnings_rev_3m = ds.get_data(tickers = tickers_ibes,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_earnings_rev_3m
           )


# Merge to main dataframe
df = df.merge(df_earnings_rev_3m[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'earnings_rev_3m'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






################################# 90 days past 3M Moving Avg Earnings Revision Ratio ######################################

# Create 90 days past 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_earnings_rev_3m_90past = ds.get_data(tickers = tickers_ibes,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_earnings_rev_3m
           )


# Merge to main dataframe
df = df.merge(df_earnings_rev_3m_90past[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'earnings_rev_3m_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['earnings_rev_3m_viz'] = (df['earnings_rev_3m'].astype(str)
                  + np.where(df['earnings_rev_3m'].gt(df['earnings_rev_3m_90past']), UP, DOWN)
                   )




################################# EPS Growth ######################################

# Metric Parameters
fields_eps = ['PCH#(X(A12TE),1Y)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_eps = ds.get_data(tickers = tickers_ibes,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_eps
           )


# Merge to main dataframe
df = df.merge(df_eps[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'eps'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)







################################# 90 days past EPS Growth ######################################

# Create 90 days past EPS Growth metric dataframe
df_eps_90past = ds.get_data(tickers = tickers_ibes,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_eps
           )


# Merge to main dataframe
df = df.merge(df_eps_90past[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'eps_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



# Convert to string and add arrow
df['eps_viz'] = (df['eps'].astype(str)
                  + np.where(df['eps'].gt(df['eps_90past']), UP, DOWN)
                   )






################################# Earning Growth Expectations ######################################

# Metric Parameters
fields_earnings_growth_exp = ['X(A12GRO)']

# Create Earnings Growth Expecations metric dataframe
df_earnings_growth_exp = ds.get_data(tickers = tickers_ibes,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_earnings_growth_exp
           )


# Merge to main dataframe
df = df.merge(df_earnings_growth_exp[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'earning_growth_exp'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



################################# 90 days past Earning Growth Expectations ######################################

# Create 90 days past Earnigns Growth Expectations metric dataframe
df_earnings_growth_exp_90past = ds.get_data(tickers = tickers_ibes,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_earnings_growth_exp
           )


# Merge to main dataframe
df = df.merge(df_earnings_growth_exp[['Instrument', 'Value']], left_on = 'sector_ticker_ibes', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'earning_growth_exp_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



# Convert to string and add arrow
df['earning_growth_exp_viz'] = (df['earning_growth_exp'].astype(str)
                  + np.where(df['earning_growth_exp'].gt(df['earning_growth_exp_90past']), UP, DOWN)
                   )








################################# Sales Growth ######################################

# Metric Parameters
fields_sales_growth = ['MAV#(PCH#(X(DWSL),1Y),3M)']


# Create Sales Growth metric dataframe
df_sales_growth = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_sales_growth
           )


# Merge to main dataframe
df = df.merge(df_sales_growth[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'sales_growth'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



################################# 90 days past Sales Growth ######################################

# Create 90 days past Sales Growth metric dataframe
df_sales_growth_90past = ds.get_data(tickers = tickers,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_sales_growth
           )


# Merge to main dataframe
df = df.merge(df_sales_growth_90past[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'sales_growth_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['sales_growth_viz'] = (df['sales_growth'].astype(str)
                  + np.where(df['sales_growth'].gt(df['sales_growth_90past']), UP, DOWN)
                   )







############################################# Net Profit Margins ##########################################

# Metric Parameters
fields_netpro = ['X(DWNM)*1.00']

# Create Sector Beta metric dataframe
df_netpro = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_netpro
           )


# Merge to main dataframe
df = df.merge(df_netpro[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'net_profit_margin'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)


############################################# 90 days Past Net Profit Margins ##########################################


# Create Sector Beta metric dataframe
df_netpro_90past = ds.get_data(tickers = tickers,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_netpro
           )


# Merge to main dataframe
df = df.merge(df_netpro_90past[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'net_profit_margin_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['net_profit_margin_viz'] = (df['net_profit_margin'].astype(str)
                  + np.where(df['net_profit_margin'].gt(df['net_profit_margin_90past']), UP, DOWN)
                   )



################################### Net Profit Margins Z Score ########################################

# Metric Parameters
fields_netpro_z = ['(X(DWNM)-AVG#(X(DWNM),-20Y,))/SDN#(X(DWNM),-20Y,)']

# Create Sector Beta metric dataframe
df_netpro_z = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_netpro_z
           )


# Merge to main dataframe
df = df.merge(df_netpro_z[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'net_profit_margin_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# Dividend Yield ######################################

# Metric Parameters
fields_dividend = ['X(DY)']


# Create Sales Growth metric dataframe
df_dividend = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_dividend
           )


# Merge to main dataframe
df = df.merge(df_dividend[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'dividend'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)









########################################################################################################
#################################### VALUATION #######################################################
########################################################################################################



################################# P/E Ratio DataStream ######################################

# Metric Parameters
fields_pe_ds = ['X(PE)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_pe_ds = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_pe_ds
           )


# Merge to main dataframe
df = df.merge(df_pe_ds[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'pe_ds'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



################################# Forward P/E Ratio DataStream ######################################

# Metric Parameters
fields_fwd_pe_ds = ['X(DIPE)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_fwd_pe_ds = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_fwd_pe_ds
           )


# Merge to main dataframe
df = df.merge(df_fwd_pe_ds[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'fwd_pe_ds'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






################################# Forward P/E Ratio Z-Score DataStream ######################################

# Metric Parameters
fields_fwd_pe_ds_zscore = ['(X(DIPE)-AVG#(X(DIPE),-20Y,))/SDN#(X(DIPE),-20Y,)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_fwd_pe_ds_zscore = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_fwd_pe_ds_zscore
           )


# Merge to main dataframe
df = df.merge(df_fwd_pe_ds_zscore[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'fwd_pe_ds_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






################################# Price to Book Ratio DataStream ######################################

# Metric Parameters
fields_price_book = ['X(BP)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_book = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_book
           )


# Merge to main dataframe
df = df.merge(df_price_book[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_book'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)






################################# Price to Book Ratio ZScore DataStream ######################################

# Metric Parameters
fields_price_book_zscore = ['(X(BP)-AVG#(X(BP),-20Y,))/SDN#(X(BP),-20Y,)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_book_zscore = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_book_zscore
           )


# Merge to main dataframe
df = df.merge(df_price_book_zscore[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_book_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# Price to Cash Ratio DataStream ######################################

# Metric Parameters
fields_price_cash = ['X(PC)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_cash = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_cash
           )


# Merge to main dataframe
df = df.merge(df_price_cash[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_cash'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# Price to Cash Ratio Z Score DataStream ######################################

# Metric Parameters
fields_price_cash_zscore = ['(X(PC)-AVG#(X(PC),-20Y,))/SDN#(X(PC),-20Y,)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_cash_zscore = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_cash_zscore
           )


# Merge to main dataframe
df = df.merge(df_price_cash_zscore[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_cash_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# Price to Sales Ratio DataStream ######################################

# Metric Parameters
fields_price_sales = ['E062(X)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_sales = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_sales
           )


# Merge to main dataframe
df = df.merge(df_price_sales[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_sales'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




################################# Price to Sales Ratio ZScore DataStream ######################################

# Metric Parameters
fields_price_sales_zscore = ['(E062(X)-AVG#(E062(X),-20Y,))/SDN#(E062(X),-20Y,)']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_price_sales_zscore = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_price_sales_zscore
           )


# Merge to main dataframe
df = df.merge(df_price_sales_zscore[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'price_sales_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# Valuation ZScore ######################################
df['valuation_zscore'] = df[['fwd_pe_ds_zscore', 
                             'price_book_zscore', 
                             'price_cash_zscore',
                             'price_sales_zscore']].mean(axis=1)





################################# Tupper Fwd PE ######################################

# Metric Parameters
fields_tupper_fwd_pe = ['REBE#(X/TOTMKWD,MTE)-(REBE#(X/TOTMKWD,MTE))/(X(DIPE)/TOTMKWD(DIPE))']

# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_tupper_fwd_pe = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_tupper_fwd_pe
           )


# Merge to main dataframe
df = df.merge(df_tupper_fwd_pe[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'tupper_fwd_pe'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)





################################# 90 days Past Tupper Fwd PE ######################################


# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_tupper_fwd_pe_90past = ds.get_data(tickers = tickers,
            start = start_90, 
            end = end_90,
            freq = freq_d,
            fields = fields_tupper_fwd_pe
           )


# Merge to main dataframe
df = df.merge(df_tupper_fwd_pe_90past[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'tupper_fwd_pe_90past'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




# Convert to string and add arrow
df['tupper_fwd_pe_viz'] = (df['tupper_fwd_pe'].astype(str)
                  + np.where(df['tupper_fwd_pe'].gt(df['tupper_fwd_pe_90past']), UP, DOWN)
                   )







########################################################################################################
#################################### OPERATIONS #######################################################
########################################################################################################



############################################# Return on Equity ##########################################

# Metric Parameters
fields_reteq = ['X(DWRE)']

# Create Sector Beta metric dataframe
df_reteq = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_reteq
           )


# Merge to main dataframe
df = df.merge(df_reteq[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'return_on_equity'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)



############################################# Return on Equity Z Score ########################################

# Metric Params
fields_reteq_z = ['(X(DWRE)-AVG#(X(DWRE),-20Y,))/SDN#(X(DWRE),-20Y,)']

# Create Sector Beta metric dataframe
df_reteq_z = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_reteq_z
           )


# Merge to main dataframe
df = df.merge(df_reteq_z[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'return_on_equity_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




############################################# Operating Margins ##########################################

# Metric Parameters
fields_opmrg = ['X(DWEB)/X(DWSL)*100.00']

# Create Sector Beta metric dataframe
df_opmrg = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_opmrg
           )


# Merge to main dataframe
df = df.merge(df_opmrg[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'ops_margin'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




################################# Operating Margins Z Score ######################################

# Metric Parameters
fields_opmrg_z = ['(E063(X)-AVG#(E063(X),-20Y,))/SDN#(E063(X),-20Y,)']

# Create Sector Beta metric dataframe
df_opmrg_z = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_opmrg_z
           )


# Merge to main dataframe
df = df.merge(df_opmrg_z[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'ops_margin_zscore'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)







############################################# Operations ZScore ##########################################
df['operations_zscore'] = df[['return_on_equity_zscore', 
                              'net_profit_margin_zscore', 
                              'ops_margin_zscore']].mean(axis=1)





################################# Market Cap ######################################


# Metric Parameters
fields_mkt_cap = ['(X(MV)/TOTMKWD(MV))*100.00']


# Create 3 Months Moving Avg Earnings Revision Ratio metric dataframe
df_mkt_cap = ds.get_data(tickers = tickers,
            start = start, 
            end = end,
            freq = freq_d,
            fields = fields_mkt_cap
           )


# Merge to main dataframe
df = df.merge(df_mkt_cap[['Instrument', 'Value']], left_on = 'sector_ticker', right_on = 'Instrument', how='left')
df.rename(columns = {'Value': 'mkt_cap'}, inplace=True)
df.drop(columns = ['Instrument'], inplace=True)




########################################################################################################
#################################### POST PROCESSING ###################################################
########################################################################################################

# Post Processing
df.index=df['sector']


df = df[['mkt_cap', 
         'mtd_performance', 'ytd_performance', '1y_performance', '3y_cagr',
         'rsi_14', 'breadth_viz', 'rel_200d_viz', 'ad_line_viz',
         'beta', 'rolling_corr', 'dxy_corr',
         'earnings_rev_3m_viz', 'eps_viz', 'earning_growth_exp_viz', 'sales_growth_viz', 'net_profit_margin_viz',
         'dividend',
         'pe_ds', 'fwd_pe_ds', 'price_book', 'price_cash', 'price_sales', 'valuation_zscore',
         'tupper_fwd_pe_viz',
         'return_on_equity', 'net_profit_margin', 'ops_margin', 'operations_zscore']]

def get_super(x):
    normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-=()"
    super_s = "ᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾQᴿˢᵀᵁⱽᵂˣʸᶻᵃᵇᶜᵈᵉᶠᵍʰᶦʲᵏˡᵐⁿᵒᵖ۹ʳˢᵗᵘᵛʷˣʸᶻ⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾"
    res = x.maketrans(''.join(normal), ''.join(super_s))
    return x.translate(res)





# Rename Index
df.index.name = None


# Rename Columns
df.columns = pd.MultiIndex.from_tuples([
                                  ('', 'Market Cap (%)'),
                                  ('Performance', 'MTD (%)'), 
                                  ('Performance', 'YTD (%)'), 
                                  ('Performance', '1-Year (%)'), 
                                  ('Performance', '3Yr CAGR (%)'), 
                                  ('Technicals', '14-Day RSI'), 
                                  ('Technicals', 'Breadth' + get_super("+") + ' (%)'), 
                                  ('Technicals', 'Rel. to 200Day (%)'), 
                                  ('Technicals', 'Advance/Decline Line'), 
                                  ('Cyclicality', 'Sector Beta'),
                                  ('Cyclicality', 'Citi Eco. Surprice Corr. to Rel. Per.'),
                                  ('Cyclicality', 'DXY Correlation'),
                                  ('Earnings', 'Earnings Revision Ratio' + get_super("++")),
                                  ('Earnings', 'EPS Growth (YoY) (%)'),
                                  ('Earnings', '12-Mth Fwd EPS Growth Exp. (%)'),
                                  ('Earnings', 'Sales Growth (YoY) (%)'),
                                  ('Earnings', 'Profit Margin (%)'),
                                  ('', 'Dividend Yield'),
                                  ('Valuation', 'Price Earnings'),
                                  ('Valuation', 'Forward PE'),
                                  ('Valuation', 'Price to Book'),
                                  ('Valuation', 'Price to Cash'),
                                  ('Valuation', 'Price to Sales'),
                                  ('Valuation', 'Valuation Z-Score'),
                                  ('', 'Tupper Pre./Dis/ (%)'),
                                  ('Operations', 'Return on Equity (%)'),
                                  ('Operations', 'Net Profit Margin (%)'),
                                  ('Operations', 'Operating Margin (%)'),
                                  ('Operations', 'Operation Z-Score')])


df = df.T





######################################################################################
################### Format Output
######################################################################################

def make_pretty(styler):
    # Set Decimal Precision
    styler.format(precision=1)
    
    
    # Create Title
    caption_styles = [dict(selector="caption",
            props=[("text-align", "centre"),
                   ("font-size", "120%"),
                   ("color", 'black'),
                  ('caption-side', 'top')])]
    
    styler.set_caption("Global Equity Sector Dashboard [Source: Refinitiv DataStream, Acorn MC Ltd]").set_table_styles(caption_styles, overwrite=False)
    
    
    # Create border for entire table
    styler.set_table_styles([{'selector' : '',
                            'props' : [('border','1px solid black')]}], overwrite=False)
    
    # Background color for all rows
    styler.set_table_styles([{'selector': 'td',
                              'props': [('background-color', 'white')]}], overwrite=False)
    
    # Set border color between columns
    styler.set_table_styles([
        {'selector': 'td', 'props': 'border-left: 1px solid black'},
        {'selector': 'td', 'props': 'border-right: 1px solid black'}
    ]
    , overwrite=False, axis=0)
    
    # Background color for column headers and row index
    styler.set_table_styles([
        {'selector': 'th:not(.index_name)', 'props': 'background-color: #CCCEE7; color: black;'}
    ], overwrite=False)
    
    
    # Set border color between headers
    styler.set_table_styles({("Technology"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    styler.set_table_styles({("Consumer Discretionary"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    
    styler.set_table_styles({("Healthcare"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    styler.set_table_styles({("Energy"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    styler.set_table_styles({("Telecom"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    styler.set_table_styles({("Real Estate"): [
        {'selector': 'th', 'props': 'border-left: 1px solid black'},
        {'selector': 'th', 'props': 'border-right: 1px solid black'},
    ]}, overwrite=False, axis=0)
    
    
    
    
    def index_level0(s):
        return np.where(s.isin(['Performance', 'Cyclicality','']), 
                        "border-bottom: 1px solid black; border-top: 1px solid black;", "")
    
    styler.apply_index(index_level0)  
    
    
    def index_level1_bottom(s):
        return np.where(s.isin(['Market Cap (%)', '3Yr CAGR (%)', 'Advance/Decline Line','DXY Correlation', 
                                'Profit Margin (%)','Dividend Yield', 'Valuation Z-Score', 
                                'Tupper Pre./Dis/ (%)', 'Operation Z-Score']), 
                        "border-bottom: 1px solid black;", "")
    styler.apply_index(index_level1_bottom)
    
    
    def index_level1_top(s):
        return np.where(s.isin(['Market Cap (%)']), 
                        "border-top: 1px solid black;", "")
    styler.apply_index(index_level1_top)
    
    
    
    
    return styler
    

result = df.style.pipe(make_pretty)

 
# # CSS to inject contained in a string
# hide_table_row_index = """
#             <style>
#             thead tr th:first-child {display:none}
#             tbody th {display:none}
#             </style>
#             """

# # Inject CSS with Markdown
# st.markdown(hide_table_row_index, unsafe_allow_html=True)


#st.dataframe(result, use_container_width=True)
st.table(result)


