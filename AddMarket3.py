# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 at some time

@author: Hongze
"""

import os
import pandas as pd
import numpy as np
from datetime import time
from GetInstrumentInfo import GetStepsize


def findCloseIndex(df, code=None):
    i = len(df)-2
    if code == 'T':
        limit = time(hour=15,minute=14,second=55,microsecond=0)
    else:
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
        
def createMarketCSV(date, code, duration, open_std=2, close_std=0, max_holdings=10, target_step=2):
    if duration not in ['5m','15m','30m','1hr','1d']:
        raise Exception('invalid duration parameter')
    if os.environ['COMPUTERNAME'] != 'HONGZE-PC':
        path = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
    else:
        path = 'D:\\Hongze\\Dropbox\\SpreadData\\'
    if os.path.isdir(path) == False:
        path = 'C:\\Users\\xingt\\Dropbox\\SpreadData\\'
    file = [i for i in os.listdir(path + code) if 'ProcessedSpread_' + date in i][0]
    df = pd.read_csv(path+code+'\\'+file, index_col = 0)
    limit = findCloseIndex(df, code=code)
    df['AskPrice1N'] = np.where(df['AskVolume1']!=0, df['AskPrice1'], df['UpperLimitPrice1'])
    df['BidPrice1N'] = np.where(df['BidVolume1']!=0, df['BidPrice1'], df['LowerLimitPrice1'])
    df['AskPrice2N'] = np.where(df['AskVolume2']!=0, df['AskPrice2'], df['UpperLimitPrice2'])
    df['BidPrice2N'] = np.where(df['BidVolume2']!=0, df['BidPrice2'], df['LowerLimitPrice2'])
    
    df['ShortPrice']=df['BidPrice2N']-df['AskPrice1N']
    df['LongPrice']=df['AskPrice2N']-df['BidPrice1N']
    df['OS Threshold']=df[duration+' Average'] + open_std*df[duration+' Std']
    df['OL Threshold']=df[duration+' Average'] - open_std*df[duration+' Std']
    df['CS Threshold']=df[duration+' Average'] - close_std*df[duration+' Std']
    df['CL Threshold']=df[duration+' Average'] + close_std*df[duration+' Std']
    
    df['Open'] = np.where(
            df['ShortPrice'] > df['OS Threshold'], -1, np.where(
                df['LongPrice'] <  df['OL Threshold'], 1, np.nan))
    df['Close'] = np.where(
            df['ShortPrice'] > df['CS Threshold'], -1, np.where(
                df['LongPrice'] <  df['CL Threshold'], 1, np.nan))
    # +1 = buy, -1 = sell
    df['Expected Profit'] = df[duration+' Std'] * (open_std-close_std)
    target = float(target_step * GetStepsize(code))
    df['Target Profit'] = target
    df['EP>=TP'] = df['Expected Profit'] >= df['Target Profit']
    
    strategy = ["" for i in range(len(df))]
    trades = [0 for i in range(len(df))]
    # open short, open long, close short, close long, or blank
    holdings = 0 # can go between +/- the limit
    account = [float(0) for i in range(len(df))]
    h_list = [0] + [np.nan for i in range(1, limit)] + [0 for i in range(limit, len(df))]
    
    indexlist = df[(df.index>=1) & (df.index<limit) & ((df['Close'].notna()) | ((df['Open'].notna()) & (df['EP>=TP']==True)))].index
    #indexlist = df.iloc[1:limit][(df['Close'].notna()) | ((df['Open'].notna()) & (df['EP>=TP']==True))].index
    
    for i in indexlist:
        if holdings != 0:
            #Try to close
            if df.Close[i] == 1 and holdings < 0:
                trades[i] = min(int(0.1*max_holdings), abs(holdings), df.at[i,'AskVolume2'], df.at[i,'BidVolume1'])
                if trades[i] != 0:
                    strategy[i] = 'close short' #buy
                    holdings += trades[i]
                    account[i] = -(df.at[i,'LongPrice'])*trades[i] #if you buy, it's negative!
                    h_list[i] = holdings
                continue
            elif df.Close[i] == -1 and holdings > 0:
                trades[i] = min(int(0.1*max_holdings), abs(holdings), df.at[i,'BidVolume2'], df.at[i,'AskVolume1'])
                if trades[i] != 0:
                    strategy[i] = 'close long' #sell
                    holdings -= trades[i]
                    account[i] = (df.at[i,'ShortPrice'])*trades[i]
                    h_list[i] = holdings
                continue
        if holdings < max_holdings and holdings > -max_holdings:
            #Now try to open
            if df.Open[i] == 1 and df['EP>=TP'][i] == True:
                trades[i] = min(int(0.1*max_holdings), max_holdings-abs(holdings), df.at[i,'AskVolume2'], df.at[i,'BidVolume1'])
                if trades[i] != 0:
                    strategy[i] = 'open long'
                    holdings += trades[i]
                    account[i] = -(df.at[i,'LongPrice'])*trades[i]
                    h_list[i] = holdings
                continue
            elif df.Open[i] == -1 and df['EP>=TP'][i] == True:
                trades[i] = min(int(0.1*max_holdings), max_holdings-abs(holdings), df.at[i,'BidVolume2'], df.at[i,'AskVolume1'])
                if trades[i] != 0:
                    strategy[i] = 'open short'
                    holdings -= trades[i]
                    account[i] = (df.at[i,'ShortPrice'])*trades[i]
                    h_list[i] = holdings
                continue

    # Finally, when you reach the limit, close all: 
    if holdings > 0:
        strategy[limit] = 'close long'
        account[limit] = (df.at[limit,'MidSpread'])*holdings
        trades[limit] = holdings
    elif holdings < 0:
        strategy[limit] = 'close short'
        account[limit] = -(df.at[limit,'MidSpread'])*-holdings
        trades[limit] = -holdings
    # if holdings = 0, nothing needs to be changed
    
    df['Strategy'] = strategy
    df['Trades'] = trades
    df['Holdings'] = h_list
    df['Holdings'].fillna(method='ffill', inplace=True)
    df['Account'] = account
    df['Profit'] = df.Account.expanding().sum()
    df['Total Trades'] = df.Trades.expanding().sum()
    if df['Total Trades'].iat[-1] % 2 != 0:
        raise Exception('Total Trades is somehow odd')
    #df.drop(columns=['Open', "Close", 'AskPrice1N','BidPrice1N','AskPrice2N','BidPrice2N','ShortPrice','LongPrice','OS Threshold','OL Threshold', 'CS Threshold', 'CL Threshold'], inplace=True)
    return df.astype({'Holdings':'int64', 'Total Trades': 'int64'})
                

def main():
    AllInstrumentCode = list(['T'])
    AllTestDates = list(['20200525'])
    #AllTestDates = [i for i in list(os.listdir('D:\\Hongze\\Dropbox\\2020')) if len(i)==8 and i>'20200102']
    # destdir  = 'D:\\Hongze\\Dropbox\\SpreadData\\cu\\Trading\\'
    # destdir = 'C:\\Users\\xingt\\Desktop\\'
    destdir = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
    open_std = 2
    close_std = 0
    for code in AllInstrumentCode:
        for date in AllTestDates:
            print(date, code)
            df = createMarketCSV(date, code, open_std=open_std, close_std=close_std, max_holdings = 50, duration='5m',target_step=2)
            df.to_csv(destdir+'MarketSpread_'+code+'_'+date+'.csv')

if __name__=='__main__':
    main()