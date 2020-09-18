# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 15:51:01 2020

@author: Hongze
"""

import os
import pandas as pd
from collections import deque
from AddMarket3 import createMarketCSV, findCloseIndex
from ApplyTradeStrategies import getSpreadName
from GetInstrumentInfo import GetStepsize


OPEN_STD = 2
CLOSE_STD = 0
MAX_HOLDINGS = 200 #ignore this one; see below
TARGET_STEP = 2
DATAPATH = 'D:\\Hongze\\Dropbox\\SpreadData\\'
AllTestDates = [i for i in list(os.listdir('D:\\Hongze\\Dropbox\\2020')) if len(i)==8 and i>'20200102']


def createTradeSpread(date, code, duration, open_std=OPEN_STD, close_std=CLOSE_STD, max_holdings=MAX_HOLDINGS, target_step=TARGET_STEP):
    spreaddir = DATAPATH + code
    #Insert some no-repeats implementation here
    spreadname = getSpreadName(code,date)
    if spreadname is None or not os.path.isfile(spreaddir+'\\ProcessedSpread'+ spreadname +'.csv'):
        raise Exception('File not found')
    df=createMarketCSV(date, code, duration, open_std=open_std, close_std=close_std, max_holdings=max_holdings, target_step=target_step)    
    limit = findCloseIndex(df,code=code) # since we're just adding columns, this should still work
    stepsize = GetStepsize(code)
    
    time, close_time, long_short, profit, fz, ftz, tz, hz, norm_stepsize, success = list(), list(), list(), list(), list(), list(), list(), list(), list(), list()
    stack = deque() #LIFO
    indexlist = df[df['Trades']!=0].index
    for i in indexlist:
        trades = df.at[i,'Trades'] #number of trades in that tick
        unit_account = df.at[i,'Account']/trades
        if 'open' in df.at[i,'Strategy']:
            for trade in range(trades):
                #store info in 7-tuples: open price, 5z, 15z, 30z, hz, 1dstd, long/short
                tp = (unit_account, df.at[i,'Time']+'.'+str(df.at[i,'MilliSecond']).zfill(3), df.at[i,'5m z'], df.at[i,'15m z'], df.at[i,'30m z'], df.at[i,'1hr z'], df.at[i,'1d Std'], df.at[i,'Open'])
                stack.append(tp)
        elif 'close' in df.at[i,'Strategy']:
            # Assumption: must close existing long/short before opening short/long
            for trade in range(trades):
                op, ot, f, ft, t, h, odstd, ls= stack.pop()
                profit.append(op+unit_account)
                time.append(ot)
                close_time.append(df.at[i,'Time']+'.'+str(df.at[i,'MilliSecond']).zfill(3))
                fz.append(f)
                ftz.append(ft)
                tz.append(t)
                hz.append(h)
                norm_stepsize.append(odstd/stepsize)
                if i != limit:
                    success.append(True)
                else:
                    success.append(False)
                if ls==1:
                    long_short.append('Long')
                elif ls==-1:
                    long_short.append('Short')
        else:
            raise Exception('This shouldn\'t happen.')
    if len(stack) != 0:
        raise Exception('Stack not empty')
    return pd.DataFrame({'Date':[date for i in range(len(time))], 'Open Time':time, 'Close Time':close_time, 'Long/Short':long_short, 'Profit':profit, 'Successful':success, '1d Std/Stepsize':norm_stepsize, '5m z':fz, '15m z':ftz, '30m z': tz, '1hr z': hz})
    
    
def combineTradeSpreads(code, duration, open_std=OPEN_STD, close_std=CLOSE_STD, max_holdings=MAX_HOLDINGS, target_step=TARGET_STEP):
    #something about present dates
    spreaddir = DATAPATH + code
    dfs = list()
    for date in AllTestDates:
        print(date)
        spreadname = getSpreadName(code,date)
        if spreadname is None or not os.path.isfile(spreaddir+'\\ProcessedSpread'+ spreadname +'.csv'):
            print('Date not found. Skipping')
            continue
        dfs.append(createTradeSpread(date, code, duration, open_std=open_std, close_std=close_std, max_holdings=max_holdings, target_step=target_step))
    df_out = pd.concat(dfs, ignore_index=True).astype({'Successful':'bool'})
    return df_out

def main():
    AllInstrumentCode = list(['cu','au','ag','ni','rb','al','ru','zn','sc','IF','IC','T'])
    #durations=['5m','15m','30m','1hr','1d']
    durations = ['1d']
    for code in AllInstrumentCode:
        print('\nCode='+code)
        destdir = DATAPATH + code + '\\Trading'
        if not os.path.exists(destdir):
            os.mkdir(destdir)
        if code.isupper():
            mh = 50
        else:
            mh = 200
        for duration in durations: 
            print('Duration='+duration)
            filepath = destdir + '\\Trades_'+code+'_'+duration+'.csv'
            if os.path.isfile(filepath):
                print('File already exists; skipping\n')
                continue
            df = combineTradeSpreads(code, duration, max_holdings = mh)
            print('Outputting\n')
            df.to_csv(filepath)

if __name__ == '__main__':
    main()