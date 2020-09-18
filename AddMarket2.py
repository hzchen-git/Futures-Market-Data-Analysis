# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 18:50:36 2020

@author: Hongze
"""

import os
import pandas as pd
import numpy as np
from datetime import time


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
        
def createMarketCSV(date, code, open_std, close_std, duration='1hr', max_holdings=10):
    if os.environ['COMPUTERNAME'] != 'HONGZE-PC':
        path = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
    else:
        path = 'D:\\Hongze\\Dropbox\\SpreadData\\'
    file = [i for i in os.listdir(path + code) if 'ProcessedSpread_' + date in i][0]
    df = pd.read_csv(path+code+'\\'+file, index_col = 0)
    limit = findCloseIndex(df)
    df['AskPrice1N'] = np.where(df['AskVolume1']!=0, df['AskPrice1'], df['UpperLimitPrice1'])
    df['BidPrice1N'] = np.where(df['BidVolume1']!=0, df['BidPrice1'], df['LowerLimitPrice1'])
    df['AskPrice2N'] = np.where(df['AskVolume2']!=0, df['AskPrice2'], df['UpperLimitPrice2'])
    df['BidPrice2N'] = np.where(df['BidVolume2']!=0, df['BidPrice2'], df['LowerLimitPrice2'])
    
    df['ShortPrice']=df['BidPrice2N']-df['AskPrice1N']
    df['LongPrice']=df['AskPrice2N']-df['BidPrice1N']
    df['OS Threshold']=-df[duration+' Average'] + open_std*df[duration+' Std']
    df['OL Threshold']=-df[duration+' Average'] - open_std*df[duration+' Std']
    df['CS Threshold']=-df[duration+' Average'] - close_std*df[duration+' Std']
    df['CL Threshold']=-df[duration+' Average'] + close_std*df[duration+' Std']
    
    df['Open'] = np.where(
            df['ShortPrice'] > df['OS Threshold'], float(-1), np.where(
                df['LongPrice'] <  df['OL Threshold'], float(1), float('NaN')))
    df['Close'] = np.where(
            df['ShortPrice'] > df['CS Threshold'], float(-1), np.where(
                df['LongPrice'] <  df['CL Threshold'], float(1), float('NaN')))
    # +1 = buy, -1 = sell
    strategy = ["" for i in range(len(df))]
    # open short, open long, close short, close long, or blank
    holdings = 0 # can go between +/- 10
    h_list = []
    account = [float(0) for i in range(len(df))]
    # Might need to loop to keep track of holdings...
    for i in range(limit):
        if holdings != 0:
            #Try to close
            if df.Close[i] == 1 and holdings < 0:
                strategy[i] = 'close short' #buy
                holdings += 1
                #only possible if holdings<0
                account[i] = -(df.at[i,'LongPrice']) #if you buy, it's negative!
                h_list.append(holdings)
                continue
            elif df.Close[i] == -1 and holdings > 0:
                strategy[i] = 'close long' #sell
                holdings -= 1
                #holdings >0
                account[i] = (df.at[i,'ShortPrice'])
                h_list.append(holdings)
                continue
        if holdings < max_holdings and holdings > -max_holdings:
            #Now try to open
            if df.Open[i] == 1:
                strategy[i] = 'open long'
                holdings += 1
                account[i] = -(df.at[i,'LongPrice'])
                h_list.append(holdings)
                continue
            elif df.Open[i] == -1:
                strategy[i] = 'open short'
                holdings -= 1
                account[i] = (df.at[i,'ShortPrice'])
                h_list.append(holdings)
                continue
        #still here? then did neither.
        h_list.append(holdings)
        
    # Finally, when you reach the limit, close all: 
    if holdings > 0:
        strategy[limit] = 'close long'
        account[limit] = (df.at[limit,'ShortPrice'])*holdings
    elif holdings < 0:
        strategy[limit] = 'close short'
        account[limit] = -(df.at[limit,'LongPrice'])*-holdings
    # if holdings = 0, nothing needs to be changed
    h_list.extend([0 for i in range(limit, len(df))])
    
    df['Strategy'] = strategy
    df['Holdings'] = h_list
    df['Account'] = account
    df['Profit'] = df.Account.expanding().sum()
    #df.drop(columns=['Open', "Close", 'AskPrice1N','BidPrice1N','AskPrice2N','BidPrice2N','ShortPrice','LongPrice','OS Threshold','OL Threshold', 'CS Threshold', 'CL Threshold'], inplace=True)
    return df
                

def main():
    AllInstrumentCode = list(['cu'])
    AllTestDates = list(['20200103','20200106','20200107','20200108','20200109','20200110','20200113','20200114','20200115','20200116','20200117','20200120','20200121','20200122','20200123'])#list(['20200103'])
    destdir = 'D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\'
    #destdir = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
    open_std = 2
    close_std = 0
    for code in AllInstrumentCode:
        for date in AllTestDates:
            print(date, code)
            df = createMarketCSV(date, code, open_std, close_std)
            df.to_csv(destdir+'MarketSpread_'+code+'_'+date+'.csv')

if __name__=='__main__':
    main()