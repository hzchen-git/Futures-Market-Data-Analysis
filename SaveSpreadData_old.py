# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 09:13:55 2020

@author: xingtan
"""

import os
import csv
import numpy as np
import pandas as pd
import array as arr
from datetime import date
from GetInstrumentInfo import GetMultiplier, GetStepsize, GetExchangeCode, GetInstrument, GetTimeInHalfSeconds
#AllInstrumentCode = GetInstrument('ZDX_D')
AllInstrumentCode = list(['sc'])
#AllInstrumentCode = list(['ag','au','rb','ni'])'
fadd = 'D:\\Dropbox\\Dropbox\\Backup\\Futures Data\\'
if os.path.isdir(fadd) == False :
    fadd = 'C:\\Users\\xingt\\Dropbox\\Backup\\Futures Data\\'
fadd2 = fadd + '2020'
AllTestDates = os.listdir(fadd2)
#AllTestDates = list(['20200318','20200319','20200320','20200323','20200324'])
AllTestDates = list(['20200401'])
epsilon = 1e-10
for fname in AllTestDates:
#    if fname < '20200108':
#        continue
    print('Date = ' + fname)
    for j in range(len(AllInstrumentCode)):   
        InstrumentCode = AllInstrumentCode[j]       
        ExchangeCode = GetExchangeCode(InstrumentCode)
        
        cfname = fadd + "\\" + fname[0:4] + "\\" +  fname + "\\" + ExchangeCode + "_" + fname + ".csv"
            
        if os.path.isfile(cfname) == False: continue
    
        destdir = fadd + "SpreadData\\"+InstrumentCode
        if not os.path.exists(destdir):
            os.makedirs(destdir)
        # pattern = "RawSpread_" + fname         
        # if len([i for i in os.listdir(destdir) if pattern in i]) > 0:continue

        if j == 0 or GetExchangeCode(InstrumentCode) != GetExchangeCode(AllInstrumentCode[j-1]):
            df = pd.read_csv(cfname,sep=',',encoding = "ISO-8859-1", engine='python', error_bad_lines=False)
            i_drop = [i for i in range(len(df)) if abs(df.iat[i,24]) < epsilon and abs(df.iat[i,26]) < epsilon]
            df = df.drop(i_drop)
            df.index=range(len(df))
                
    
        AllInstrument = list(set(df.iloc[:,2]))
        unique = list()
        volumes = arr.array('i', [])

        mylist = list(df.iloc[:,2])
        for i in range(len(AllInstrument)):
            if len(AllInstrument[i])==4-(ExchangeCode=='CZCE')+len(InstrumentCode) and AllInstrument[i][0:len(InstrumentCode)] == InstrumentCode:
                lastindex = len(mylist) - mylist[::-1].index(AllInstrument[i]) - 1
                lastvolume = df.iloc[lastindex,12] #colume 12 = volume
                if lastvolume >= 0 :
                    unique.append(AllInstrument[i])
                    volumes.extend([int(lastvolume)])
            
        if len(volumes) < 2: continue
    
        i1 = volumes.index(max(volumes))
        i2 = volumes.index(np.unique(volumes)[-2])
        activeCode1 = unique[i1]
        activeCode2 = unique[i2]
        
        indexes = [i for i in range(len(df)) if df.iloc[i,2] == activeCode1 or df.iloc[i,2] == activeCode2]        
        
        if len(indexes) < 10: continue
    
        multiplier = GetMultiplier(InstrumentCode)
        stepsize = GetStepsize(InstrumentCode)
        dfObj = pd.DataFrame(columns=['Time', 'MilliSecond', 'Spread', 'MidSpread', 'LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','TotalVolume1','TotalVolume2','Amount1','Amount2'])
        k1 = 0
        k2 = 0
        for i in indexes:
            if df.iat[i,2] != activeCode1 and df.iat[i,2] != activeCode2:
                continue
            if df.iat[i,2] == activeCode1 :
                if k1 == k2 - 1 and k1 >= 1 and GetTimeInHalfSeconds(dfObj.at[k1,'Time'], dfObj.at[k1,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                    dfObj.at[k1,'LastPrice1'] = dfObj.at[k1-1,'LastPrice1']
                    dfObj.at[k1,'BidVolume1'] = dfObj.at[k1-1,'BidVolume1']
                    dfObj.at[k1,'BidPrice1'] = dfObj.at[k1-1,'BidPrice1']
                    dfObj.at[k1,'AskPrice1'] = dfObj.at[k1-1,'AskPrice1'] 
                    dfObj.at[k1,'AskVolume1'] = dfObj.at[k1-1,'AskVolume1']
                    dfObj.at[k1,'TotalVolume1'] = dfObj.at[k1-1,'TotalVolume1']
                    dfObj.at[k1,'TradeVolume1']  = 0
                    dfObj.at[k1,'Amount1'] = dfObj.at[k1-1,'Amount1']
                    dfObj.at[k1,'Spread'] = dfObj.at[k1,'LastPrice1'] -  dfObj.at[k1,'LastPrice2']
                    dfObj.at[k1,'MidSpread'] =  ( dfObj.at[k1,'BidPrice1']+dfObj.at[k1,'AskPrice1'] -  dfObj.at[k1,'BidPrice2'] -  dfObj.at[k1,'AskPrice2'] ) /2 
                    k1 = k1 + 1
                
                dfObj.at[k1,'LastPrice1'] = df.iat[i,5]
                dfObj.at[k1,'BidVolume1'] = df.iat[i,24]
                dfObj.at[k1,'BidPrice1'] = df.iat[i,23]
                dfObj.at[k1,'AskPrice1'] = df.iat[i,25]    
                dfObj.at[k1,'AskVolume1'] = df.iat[i,26] 
                dfObj.at[k1,'TotalVolume1'] = df.iat[i,12]
                dfObj.at[k1,'Amount1'] = df.iat[i,13]
                if k1 == 0:
                    dfObj.at[k1,'TradeVolume1'] = df.iat[i,12]
                    if df.iat[i,12] > 0 :
                        dfObj.at[k1,'AveragePrice1'] = df.iat[i,13]/df.iat[i,12]/multiplier
                elif df.iat[i,12] - dfObj.at[k1-1,'TotalVolume1'] > 0:
                    dfObj.at[k1,'TradeVolume1'] = df.iat[i,12] - dfObj.at[k1-1,'TotalVolume1']
                    dfObj.at[k1,'AveragePrice1'] =( df.iat[i,13]- dfObj.at[k1-1,'Amount1'])/ dfObj.at[k1,'TradeVolume1'] /multiplier
                else:
                    dfObj.at[k1,'TradeVolume1'] = 0
                    
                if k1 >= k2:
                    dfObj.at[k1,'Time'] = df.iat[i,21]
                    dfObj.at[k1,'MilliSecond'] = df.iat[i,22]
                elif k1 == k2 - 1 and k1 >= 1:
                    if GetTimeInHalfSeconds(dfObj.at[k1,'Time'], dfObj.at[k1,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                        print('incorrect time')
                        break
                
                if k1 > k2:
                    if k2 == 0:
                        print('error k2 = 0')
                        break
                    dfObj.at[k2,'LastPrice2'] = dfObj.at[k2-1,'LastPrice2']
                    dfObj.at[k2,'BidVolume2'] = dfObj.at[k2-1,'BidVolume2']
                    dfObj.at[k2,'BidPrice2'] = dfObj.at[k2-1,'BidPrice2']
                    dfObj.at[k2,'AskPrice2'] = dfObj.at[k2-1,'AskPrice2'] 
                    dfObj.at[k2,'AskVolume2'] = dfObj.at[k2-1,'AskVolume2']
                    dfObj.at[k2,'TotalVolume2'] = dfObj.at[k2-1,'TotalVolume2']
                    dfObj.at[k2,'TradeVolume2']  = 0
                    dfObj.at[k2,'Amount2'] = dfObj.at[k2-1,'Amount2']
                    dfObj.at[k2,'Spread'] = dfObj.at[k2,'LastPrice1'] -  dfObj.at[k2,'LastPrice2']
                    dfObj.at[k2,'MidSpread'] =  ( dfObj.at[k2,'BidPrice1']+dfObj.at[k2,'AskPrice1'] -  dfObj.at[k2,'BidPrice2'] -  dfObj.at[k2,'AskPrice2'] ) /2 
                    k2 = k2 + 1
                k1 = k1 + 1
                if k1 == k2:
                     dfObj.at[k1-1,'Spread'] = dfObj.at[k1-1,'LastPrice1'] -  dfObj.at[k2-1,'LastPrice2']
                     dfObj.at[k1-1,'MidSpread'] =  ( dfObj.at[k1-1,'BidPrice1']+dfObj.at[k1-1,'AskPrice1'] -  dfObj.at[k2-1,'BidPrice2'] -  dfObj.at[k2-1,'AskPrice2'] ) /2 
            elif df.iat[i,2] == activeCode2 :
                if k2 == k1 - 1 and k2 >= 1 and GetTimeInHalfSeconds(dfObj.at[k2,'Time'], dfObj.at[k2,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                    dfObj.at[k2,'LastPrice2'] = dfObj.at[k2-1,'LastPrice2']
                    dfObj.at[k2,'BidVolume2'] = dfObj.at[k2-1,'BidVolume2']
                    dfObj.at[k2,'BidPrice2'] = dfObj.at[k2-1,'BidPrice2']
                    dfObj.at[k2,'AskPrice2'] = dfObj.at[k2-1,'AskPrice2'] 
                    dfObj.at[k2,'AskVolume2'] = dfObj.at[k2-1,'AskVolume2']
                    dfObj.at[k2,'TotalVolume2'] = dfObj.at[k2-1,'TotalVolume2']
                    dfObj.at[k2,'TradeVolume2']  = 0
                    dfObj.at[k2,'Amount2'] = dfObj.at[k2-1,'Amount2']
                    dfObj.at[k2,'Spread'] = dfObj.at[k2,'LastPrice1'] -  dfObj.at[k2,'LastPrice2']
                    dfObj.at[k2,'MidSpread'] =  ( dfObj.at[k2,'BidPrice1']+dfObj.at[k2,'AskPrice1'] -  dfObj.at[k2,'BidPrice2'] -  dfObj.at[k2,'AskPrice2'] ) /2 
                    k2 = k2 + 1
                
                dfObj.at[k2,'LastPrice2'] = df.iat[i,5]
                dfObj.at[k2,'BidVolume2'] = df.iat[i,24]
                dfObj.at[k2,'BidPrice2'] = df.iat[i,23]
                dfObj.at[k2,'AskPrice2'] = df.iat[i,25]    
                dfObj.at[k2,'AskVolume2'] = df.iat[i,26]
                dfObj.at[k2,'TotalVolume2'] = df.iat[i,12]
                dfObj.at[k2,'Amount2'] = df.iat[i,13]
                if k2 == 0:
                    dfObj.at[k2,'TradeVolume2'] = df.iat[i,12]
                    if df.iat[i,12] > 0 :
                        dfObj.at[k2,'AveragePrice2'] = df.iat[i,13]/df.iat[i,12]/multiplier
                elif df.iat[i,12] - dfObj.at[k2-1,'TotalVolume2'] > 0:
                    dfObj.at[k2,'TradeVolume2'] = df.iat[i,12] - dfObj.at[k2-1,'TotalVolume2']
                    dfObj.at[k2,'AveragePrice2'] =( df.iat[i,13]- dfObj.at[k2-1,'Amount2'])/ dfObj.at[k2,'TradeVolume2'] /multiplier
                else:
                    dfObj.at[k2,'TradeVolume2']  = 0
                    
                if k2 >= k1:
                    dfObj.at[k2,'Time'] = df.iat[i,21]
                    dfObj.at[k2,'MilliSecond'] = df.iat[i,22]
                elif k2 == k1 - 1 and k2 >= 1:
                    if GetTimeInHalfSeconds(dfObj.at[k2,'Time'], dfObj.at[k2,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                        print('incorrect time')
                        break
                if k2 > k1:
                    if k1 == 0:
                        print('error k1 = 0')
                        break
                    dfObj.at[k1,'LastPrice1'] = dfObj.at[k1-1,'LastPrice1']
                    dfObj.at[k1,'BidVolume1'] = dfObj.at[k1-1,'BidVolume1']
                    dfObj.at[k1,'BidPrice1'] = dfObj.at[k1-1,'BidPrice1']
                    dfObj.at[k1,'AskPrice1'] = dfObj.at[k1-1,'AskPrice1'] 
                    dfObj.at[k1,'AskVolume1'] = dfObj.at[k1-1,'AskVolume1']
                    dfObj.at[k1,'TotalVolume1'] = dfObj.at[k1-1,'TotalVolume1']
                    dfObj.at[k1,'TradeVolume1']  = 0
                    dfObj.at[k1,'Amount1'] = dfObj.at[k1-1,'Amount1']
                    dfObj.at[k1,'Spread'] = dfObj.at[k1,'LastPrice1'] -  dfObj.at[k1,'LastPrice2']
                    dfObj.at[k1,'MidSpread'] =  ( dfObj.at[k1,'BidPrice1']+dfObj.at[k1,'AskPrice1'] -  dfObj.at[k1,'BidPrice2'] -  dfObj.at[k1,'AskPrice2'] ) /2 
                    k1 = k1 + 1
                k2 = k2 + 1
                if k1 == k2:
                     dfObj.at[k1-1,'Spread'] = dfObj.at[k1-1,'LastPrice1'] -  dfObj.at[k2-1,'LastPrice2']
                     dfObj.at[k1-1,'MidSpread'] =  ( dfObj.at[k1-1,'BidPrice1']+dfObj.at[k1-1,'AskPrice1'] -  dfObj.at[k2-1,'BidPrice2'] -  dfObj.at[k2-1,'AskPrice2'] ) /2
        
        destfile = destdir + "\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + ".csv"
        dfObj.to_csv(destfile)