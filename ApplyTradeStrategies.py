# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 14:44:19 2020

@author: Hongze
"""

import os
import copy
import pandas as pd
from AddMarket3 import createMarketCSV#, findCloseIndex

def getSpreadName(code, date):
    if os.environ['COMPUTERNAME'] != 'HONGZE-PC':
        path = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
    else:
        path = 'D:\\Hongze\\Dropbox\\SpreadData\\'
    if os.path.isdir(path) == False:
        path = 'C:\\Users\\xingt\\Dropbox\\SpreadData\\'
    path += code
    spreads = [i for i in os.listdir(path) if code in i and date in i]
    if not spreads: #empty list
        return None
    return (spreads[0])[spreads[0].index('_') : len(spreads[0])-4]
    # Just the stuff including and after the first _, minus the .csv

def main():
    OPEN_STD = 2
    CLOSE_STD = 0
    TARGET_STEP = 2
    durations=['5m','15m','30m','1hr','1d']
    AllInstrumentCode = list(['cu','au','ag','ni','rb','al','ru','zn','sc','IF','IC','T'])
    AllTestDates = [i for i in list(os.listdir('D:\\Hongze\\Dropbox\\2020')) if len(i)==8 and i>'20200102']
    for code in AllInstrumentCode:
        spreaddir = 'D:\\Hongze\\Dropbox\\SpreadData\\' + code
        destdir = spreaddir + '\\Trading'
        if os.path.isdir(spreaddir) == False:
            spreaddir = 'C:\\Users\\xingt\\Dropbox\\SpreadData\\' + code
            destdir = 'C:\\Users\\xingt\\Desktop\\' + code + '\\Trading' + str(OPEN_STD)
        for duration in durations:
            print('Code='+code+' Duration='+duration)
            #Have to make a separate file per instrument
            if os.path.isfile(destdir+'\\Trade_Strategy_'+duration+'.csv'):
                print('File already exists; skipping...\n')
                continue
            if code.isupper():
                mh = 50
            else:
                mh = 200
            present_dates = copy.deepcopy(AllTestDates)
            dfs=list([])
            for date in AllTestDates:
                print(date)
                spreadname = getSpreadName(code,date)
                if spreadname is None or not os.path.isfile(spreaddir+'\\ProcessedSpread'+ spreadname +'.csv'):
                    print('Date not found. Skipping...')
                    present_dates.remove(date)
                    continue
                dfs.append(createMarketCSV(date, code, duration, open_std=OPEN_STD, close_std=CLOSE_STD, max_holdings=mh, target_step=TARGET_STEP)) #access via indices
            #now start creating a file.
            print('Creating Trade CSV...')
            trades = [int(d['Total Trades'][len(d)-1]/2) for d in dfs] #should be even
            max_holdings = [max(abs(d.Holdings.min()), abs(d.Holdings.max())) for d in dfs]
            profit = [d.Profit[len(d)-1] for d in dfs]
            o = [OPEN_STD for d in dfs]
            c = [CLOSE_STD for d in dfs]
            dur = [duration for d in dfs]
            mhs = [mh for d in dfs]
            ts = [float(TARGET_STEP) for d in dfs]
            df_out = pd.DataFrame({'Date' : present_dates,'Open Std':o,'Close Std':c,'Avg Duration':dur,'Holdings Limit':mhs,'Targeted Profit Steps':ts,"Trading Volume" : trades, 'Daily Max Holdings' : max_holdings, 'Daily Profit' : profit})
            
            print('Outputting\n')
            
            if not os.path.isdir(destdir):
                os.mkdir(destdir)
            top_three_trades = sorted([i for i in range(len(dfs))], key=lambda i: trades[i], reverse=True)[:3]
            top_three_profit = sorted([i for i in range(len(dfs))], key=lambda i: profit[i], reverse=True)[:3]
            for i in list(set(top_three_trades+top_three_profit)):
                filepath = destdir+'\\MarketSpread' + getSpreadName(code, present_dates[i]) + '_' + duration + '.csv'
                if not os.path.isfile(filepath):
                    dfs[i].to_csv(filepath)
            df_out.to_csv(destdir+'\\Trade_Strategy_'+duration+'.csv')

if __name__ =='__main__':
    main()