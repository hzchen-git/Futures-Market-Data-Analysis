# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 17:39:05 2020

@author: Hongze
"""

import os
import pandas as pd
from GetInstrumentInfo import GetExchangeCode, GetMultiplier
from SaveSpreadData2 import findActiveCodes, calculateBuySellVolume
epsilon=1e-10

fadd = 'D:\\Hongze\\Dropbox'
if os.path.isdir(fadd) == False :
    fadd = 'C:\\Users\\Hongze\\Dropbox'
#AllInstrumentCode = ['AP','CF','CJ','FG','MA','OI','RM','SA','SF','SM','SR','TA','UR','ZC']
AllInstrumentCode = ['OI']
#AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i>'20200520']
AllTestDates = ['20200511']
for d in range(len(AllTestDates)):
    fname = AllTestDates[d]
    print('Date = ' + fname+'\n')
    lastread=None #just to initialize
    for j in range(len(AllInstrumentCode)):
    #Access/Prepare files
        InstrumentCode = AllInstrumentCode[j] 
        print('Code = ' + InstrumentCode)
        ExchangeCode = GetExchangeCode(InstrumentCode)
        cfname = fadd + "\\" + fname[0:4] + "\\" +  fname + "\\" + ExchangeCode + "_" + fname + ".csv"
        if os.path.isfile(cfname) == False: 
            print('Source file not found; skipping\n')
            continue
        destdir = fadd + "\\SpreadData\\"+InstrumentCode
        if not os.path.exists(destdir):
            os.makedirs(destdir) #create destination directory
        pattern = "RawSpread_" + fname
        #if len([i for i in os.listdir(destdir) if pattern in i]) > 0:
        #    print('File already exists; skipping\n')
        #    continue
        if lastread==None or j == 0 or GetExchangeCode(InstrumentCode) != GetExchangeCode(AllInstrumentCode[lastread]) or 'df' not in locals():
            df = pd.read_csv(cfname,sep=',',encoding = 'ISO-8859-1', error_bad_lines=False)
            df = df[(abs(df[df.columns[24]]) >= epsilon) | (abs(df[df.columns[26]]) >= epsilon)]
            df.index=range(len(df))
            lastread=j #fixed a bug here.
        
        activeCodes = findActiveCodes(df, InstrumentCode)
        if activeCodes is None: 
            print("Nothing created for " + InstrumentCode + " on " + fname)
            continue
        else:
            activeCode1, activeCode2 = activeCodes
            
        df1 = df[df[df.columns[2]] == activeCode1].reset_index().drop(columns='index')
        if len(df1) < 10: 
            print('Nothing created!')
            continue
        multiplier = GetMultiplier(InstrumentCode)
        #dfObj1 = pd.DataFrame(columns=['Time','MilliSecond','LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','TotalVolume1','Amount1','UpperLimitPrice1','LowerLimitPrice1'])
        #dfObj2 = pd.DataFrame(columns=['Time','MilliSecond','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','TotalVolume2','Amount2','UpperLimitPrice2','LowerLimitPrice2'])
        dfObj1 = pd.DataFrame()
        
        dfObj1['Time'] = df1[df1.columns[21]]
        dfObj1['MilliSecond'] = df1[df1.columns[22]]
        dfObj1['LastPrice1'] = df1[df1.columns[5]]
        dfObj1['BidVolume1'] = df1[df1.columns[24]]
        dfObj1['BidPrice1'] = df1[df1.columns[23]]
        dfObj1['AskPrice1'] = df1[df1.columns[25]]
        dfObj1['AskVolume1'] = df1[df1.columns[26]]
        dfObj1['TotalVolume1'] = df1[df1.columns[12]]
        dfObj1['Amount1'] = df1[df1.columns[13]]
        dfObj1['UpperLimitPrice1'] = df1[df1.columns[17]]
        dfObj1['LowerLimitPrice1'] = df1[df1.columns[18]]
        
        dfObj1['TradeVolume1'] = dfObj1.TotalVolume1.diff()
        dfObj1.at[0,'TradeVolume1'] = dfObj1['TotalVolume1'][0]
        dfObj1['TradeAmount1'] = dfObj1.Amount1.diff() #temporary column
        dfObj1.at[0,'TradeAmount1'] = dfObj1['Amount1'][0]
        dfObj1['AveragePrice1'] = dfObj1['TradeAmount1']/dfObj1['TradeVolume1']/multiplier
        dfObj1.drop(columns='TradeAmount1')
        
        dfObj1['BuyVolume1'], dfObj1['SellVolume1'] = calculateBuySellVolume(dfObj1, instrument='1')
        
        
        destfile = destdir + "\\RawSpread_" + fname + "_" + activeCode1 + ".csv"
        dfObj1.to_csv(destfile)
        print('Complete.\n')