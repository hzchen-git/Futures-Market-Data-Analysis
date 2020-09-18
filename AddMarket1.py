# -*- coding: utf-8 -*-
"""
Created on Sun Jul  5 14:31:43 2020

@author: Hongze
"""
import os
import pandas as pd
import numpy as np
from datetime import time
path = 'D:\\Hongze\\Dropbox\\SpreadData\\'

#def shortOrLong1hr(row):
#    if (row['BidPrice2']-row['AskPrice1']) > (-row['1hr Average'] + 2*row['1hr Std']):
#        return -1
#    elif(row['AskPrice2']-row['BidPrice1']) <  (-row['1hr Average'] - 2*row['1hr Std']):
#        return 1
#    else:
#        return None

#def getCost(row):
#    if row['Strategy'].isnull():
#        return 0
#    elif df.Strategy==1:
#        return row['AskPrice2'] - row['BidPrice1']
#    else:
#        return row['BidPrice2'] - row['AskPrice1']

def findCloseIndex(df):
    i = len(df)-2
    limit = time(hour=14,minute=59,second=55,microsecond=0)
    while True:
        curr = df.Time[i]
        currTime = time(hour=int(curr[0:2]), minute=int(curr[3:5]), second=int(curr[6:8]), microsecond=int(df.MilliSecond[i])*1000)
        #Should be no repeats, so...
        if currTime == limit:
            return i
        elif currTime < limit:
            return i+1
        i-=1
    
    
AllInstrumentCode = list(['cu','au','ag','ni','rb','al','fu','ru','bu','sn','ss','sp','zn','pb','hc'])
AllTestDates = list(['20200103','20200106','20200107','20200108','20200109','20200110','20200113','20200114','20200115','20200116','20200117','20200120','20200121','20200122','20200123'])
# Add 3 columns
for date in AllTestDates:
    for code in AllInstrumentCode:
        print('Date=' + date, 'Code=' + code)
        file = [i for i in os.listdir(path + code) if 'ProcessedSpread_' + date in i][0]
        df = pd.read_csv(path+code+'\\'+file, index_col = 0)
        #remember to make moving average negative.
         
        close = -df.at[findCloseIndex(df), '1hr Average']

        print(list(df[(df.AskPrice1==0) | (df.BidPrice1 == 0) | (df.AskPrice2 == 0) | (df.BidPrice2 == 0)].index))        

        #temp columns
        df['AskPrice1N'] = np.where(df['AskVolume1']!=0, df['AskPrice1'], df['UpperLimitPrice1'])
        df['BidPrice1N'] = np.where(df['BidVolume1']!=0, df['BidPrice1'], df['LowerLimitPrice1'])
        df['AskPrice2N'] = np.where(df['AskVolume2']!=0, df['AskPrice2'], df['UpperLimitPrice2'])
        df['BidPrice2N'] = np.where(df['BidVolume2']!=0, df['BidPrice2'], df['LowerLimitPrice2'])
        
        #df['Strategy'] = df.apply(shortOrLong1hr, axis=1)
        df['Strategy'] = np.where(
            (df['BidPrice2N']-df['AskPrice1N']) > (-df['1hr Average'] + 2*df['1hr Std']), float(-1), np.where(
                (df['AskPrice2N']-df['BidPrice1N']) <  (-df['1hr Average'] - 2*df['1hr Std']), float(1), float('NaN')))
        #print(df.Strategy)
        
        df['Cost'] = np.where(
            df.Strategy.isnull(), 0, np.where(
                df.Strategy==-1, df.BidPrice2N - df.AskPrice1N, df.AskPrice2N - df.BidPrice1N)) 
        #print(df.Cost)
        #0 if df.Strategy.isnull() else df.BidPrice2N - df.AskPrice1N if df.Strategy==-1 else df.AskPrice2N - df.BidPrice1N
    
        df['Closing Difference'] = np.where(
            df.Strategy.isnull(), 0, np.where(
                df.Strategy==-1, df.Cost-close, close-df.Cost))
        #print(df['Closing Difference'])
        df.drop(columns=['AskPrice1N','BidPrice1N','AskPrice2N','BidPrice2N'], inplace=True)
        df.to_csv(path+code+'\\MarketSpread1'+file[15:] )
        