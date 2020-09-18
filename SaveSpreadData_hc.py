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

def findActiveCodes(df, InstrumentCode):
    ExchangeCode=GetExchangeCode(InstrumentCode)
    AllInstrument = list(set(df.iloc[:,2])) #list of all unique instrument codes in spread
    unique = list()
    volumes = arr.array('i', []) #new array of ints

    mylist = list(df.iloc[:,2]) #the instrument codes column, in a list
    for i in range(len(AllInstrument)):
        if len(AllInstrument[i])==4-(ExchangeCode=='CZCE')+len(InstrumentCode) and AllInstrument[i][0:len(InstrumentCode)] == InstrumentCode: #check that it is the instrument code
            lastindex = len(mylist) - mylist[::-1].index(AllInstrument[i]) - 1
            lastvolume = df.iloc[lastindex,12] #colume 12 = volume
            if lastvolume >= 0 : #cumulative volume; last is highest
                unique.append(AllInstrument[i])
                volumes.extend([int(lastvolume)])
    if len(volumes) < 2: return None #quit
    i1 = volumes.index(max(volumes)) #Highest Volume
    i2 = volumes.index(np.unique(volumes)[-2]) #Second Highest Volume
    return unique[i1], unique[i2]
    

def copyPrevious1(dfObj, k1):
    dfObj.at[k1,'LastPrice1'] = dfObj.at[k1-1,'LastPrice1']
    dfObj.at[k1,'BidVolume1'] = dfObj.at[k1-1,'BidVolume1']
    dfObj.at[k1,'BidPrice1'] = dfObj.at[k1-1,'BidPrice1']
    dfObj.at[k1,'AskPrice1'] = dfObj.at[k1-1,'AskPrice1'] 
    dfObj.at[k1,'AskVolume1'] = dfObj.at[k1-1,'AskVolume1']
    dfObj.at[k1,'TotalVolume1'] = dfObj.at[k1-1,'TotalVolume1']
    dfObj.at[k1,'TradeVolume1']  = 0
    dfObj.at[k1,'Amount1'] = dfObj.at[k1-1,'Amount1']
    dfObj.at[k1,'UpperLimitPrice1'] = dfObj.at[k1-1,'UpperLimitPrice1']
    dfObj.at[k1,'LowerLimitPrice1'] = dfObj.at[k1-1,'LowerLimitPrice1']
    dfObj.at[k1,'Spread'] = dfObj.at[k1,'LastPrice1'] -  dfObj.at[k1,'LastPrice2']
    dfObj.at[k1,'MidSpread'] =  ( dfObj.at[k1,'BidPrice1']+dfObj.at[k1,'AskPrice1'] -  dfObj.at[k1,'BidPrice2'] -  dfObj.at[k1,'AskPrice2'] ) /2 

def copyPrevious2(dfObj, k2):
    dfObj.at[k2,'LastPrice2'] = dfObj.at[k2-1,'LastPrice2']
    dfObj.at[k2,'BidVolume2'] = dfObj.at[k2-1,'BidVolume2']
    dfObj.at[k2,'BidPrice2'] = dfObj.at[k2-1,'BidPrice2']
    dfObj.at[k2,'AskPrice2'] = dfObj.at[k2-1,'AskPrice2'] 
    dfObj.at[k2,'AskVolume2'] = dfObj.at[k2-1,'AskVolume2']
    dfObj.at[k2,'TotalVolume2'] = dfObj.at[k2-1,'TotalVolume2']
    dfObj.at[k2,'TradeVolume2']  = 0
    dfObj.at[k2,'Amount2'] = dfObj.at[k2-1,'Amount2']
    dfObj.at[k2,'UpperLimitPrice2'] = dfObj.at[k2-1,'UpperLimitPrice2']
    dfObj.at[k2,'LowerLimitPrice2'] = dfObj.at[k2-1,'LowerLimitPrice2']
    dfObj.at[k2,'Spread'] = dfObj.at[k2,'LastPrice1'] -  dfObj.at[k2,'LastPrice2']
    dfObj.at[k2,'MidSpread'] =  ( dfObj.at[k2,'BidPrice1']+dfObj.at[k2,'AskPrice1'] -  dfObj.at[k2,'BidPrice2'] -  dfObj.at[k2,'AskPrice2'] ) /2 

def createSpread(df, activeCode1, activeCode2, InstrumentCode):        
    df = df[(df[df.columns[2]] == activeCode1) | (df[df.columns[2]] == activeCode2)].drop_duplicates()
    #indexes = [i for i in range(len(df)) if df.iloc[i,2] == activeCode1 or df.iloc[i,2] == activeCode2]      
    #print(len(df))
    #pick out rows with appropriate activeCodes
    if len(df) < 10: return None #quit
    #dfObj = resulting dataframe
    multiplier = GetMultiplier(InstrumentCode)
    #stepsize = GetStepsize(InstrumentCode) #completely pointless here
    dfObj = pd.DataFrame(columns=['Time', 'MilliSecond', 'Spread', 'MidSpread', 'LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','TotalVolume1','TotalVolume2','Amount1','Amount2', 'UpperLimitPrice1', 'LowerLimitPrice1', "UpperLimitPrice2", "LowerLimitPrice2"])
    k1 = 0 #track two indices separately
    k2 = 0
    for i in range(len(df)):
        #print(i)
        if df.iat[i,2] != activeCode1 and df.iat[i,2] != activeCode2:
            continue #get rid of wrong active codes, if they somehow made it in?
            
        if df.iat[i,2] == activeCode1 :
            if k1 == k2 - 1 and k1 >= 1 and GetTimeInHalfSeconds(dfObj.at[k1,'Time'], dfObj.at[k1,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                copyPrevious1(dfObj, k1)
                k1 = k1 + 1
                
            #filling in:
            dfObj.at[k1,'LastPrice1'] = df.iat[i,5]
            dfObj.at[k1,'BidVolume1'] = df.iat[i,24]
            dfObj.at[k1,'BidPrice1'] = df.iat[i,23]
            dfObj.at[k1,'AskPrice1'] = df.iat[i,25]    
            dfObj.at[k1,'AskVolume1'] = df.iat[i,26] 
            dfObj.at[k1,'TotalVolume1'] = df.iat[i,12]
            dfObj.at[k1,'Amount1'] = df.iat[i,13]
            dfObj.at[k1,'UpperLimitPrice1'] = df.iat[i,17]
            dfObj.at[k1,'LowerLimitPrice1'] = df.iat[i,18]
            if k1 == 0:
                dfObj.at[k1,'TradeVolume1'] = df.iat[i,12]
                if df.iat[i,12] > 0 :
                    dfObj.at[k1,'AveragePrice1'] = df.iat[i,13]/df.iat[i,12]/multiplier
            elif df.iat[i,12] - dfObj.at[k1-1,'TotalVolume1'] > 0:
                dfObj.at[k1,'TradeVolume1'] = df.iat[i,12] - dfObj.at[k1-1,'TotalVolume1']
                dfObj.at[k1,'AveragePrice1'] =( df.iat[i,13]- dfObj.at[k1-1,'Amount1'])/ dfObj.at[k1,'TradeVolume1'] /multiplier
            else:
                dfObj.at[k1,'TradeVolume1'] = 0
            #Time checks  
            if k1 >= k2:
                dfObj.at[k1,'Time'] = df.iat[i,21]
                dfObj.at[k1,'MilliSecond'] = df.iat[i,22]
            elif k1 == k2 - 1 and k1 >= 1:
                if GetTimeInHalfSeconds(dfObj.at[k1,'Time'], dfObj.at[k1,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                    raise Exception('incorrect time')
                    #break
            
            if k1 > k2:
                if k2 == 0:
                    raise Exception('error k2 = 0')
                    #break
                copyPrevious2(dfObj, k2)
                k2 = k2 + 1
                
            k1 = k1 + 1
            if k1 == k2:
                 dfObj.at[k1-1,'Spread'] = dfObj.at[k1-1,'LastPrice1'] -  dfObj.at[k2-1,'LastPrice2']
                 dfObj.at[k1-1,'MidSpread'] =  ( dfObj.at[k1-1,'BidPrice1']+dfObj.at[k1-1,'AskPrice1'] -  dfObj.at[k2-1,'BidPrice2'] -  dfObj.at[k2-1,'AskPrice2'] ) /2 
        
        elif df.iat[i,2] == activeCode2 :
            if k2 == k1 - 1 and k2 >= 1 and GetTimeInHalfSeconds(dfObj.at[k2,'Time'], dfObj.at[k2,'MilliSecond']) != GetTimeInHalfSeconds( df.iat[i,21],  df.iat[i,22] ):
                copyPrevious2(dfObj, k2)
                k2 = k2 + 1
            
            dfObj.at[k2,'LastPrice2'] = df.iat[i,5]
            dfObj.at[k2,'BidVolume2'] = df.iat[i,24]
            dfObj.at[k2,'BidPrice2'] = df.iat[i,23]
            dfObj.at[k2,'AskPrice2'] = df.iat[i,25]    
            dfObj.at[k2,'AskVolume2'] = df.iat[i,26]
            dfObj.at[k2,'TotalVolume2'] = df.iat[i,12]
            dfObj.at[k2,'Amount2'] = df.iat[i,13]
            dfObj.at[k2,'UpperLimitPrice2'] = df.iat[i,17]
            dfObj.at[k2,'LowerLimitPrice2'] = df.iat[i,18]
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
                    raise Exception('incorrect time')
                    #break
            if k2 > k1:
                if k1 == 0:
                    raise Exception('error k1 = 0')
                    #break
                
                copyPrevious1(dfObj, k1)
                k1 = k1 + 1
            k2 = k2 + 1
            if k1 == k2:
                 dfObj.at[k1-1,'Spread'] = dfObj.at[k1-1,'LastPrice1'] -  dfObj.at[k2-1,'LastPrice2']
                 dfObj.at[k1-1,'MidSpread'] =  ( dfObj.at[k1-1,'BidPrice1']+dfObj.at[k1-1,'AskPrice1'] -  dfObj.at[k2-1,'BidPrice2'] -  dfObj.at[k2-1,'AskPrice2'] ) /2
    
    return dfObj

def main():
    #AllInstrumentCode = GetInstrument('ZDX_D')
    #AllInstrumentCode = list(['sc'])
    AllInstrumentCode = list(['cu','au','ag','ni','rb','al','fu','ru','bu','sn','ss','sp','zn','pb','hc'])
    fadd = 'D:\\Hongze\\Dropbox'
    #fadd = 'C:\\Users\\Hongze\\Dropbox\\' #file path
    #if os.path.isdir(fadd) == False :
    #    fadd = 'C:\\Users\\xingt\\Dropbox\\Backup\\Futures Data\\'
    #fadd2 = fadd + '2020'
    #AllTestDates = os.listdir(fadd2)
    #AllTestDates = list(['20200103'])
    AllTestDates = list(['20200102','20200103','20200106','20200107','20200108','20200109','20200110','20200113','20200114','20200115','20200116','20200117','20200120','20200121','20200122','20200123'])
    epsilon = 1e-10
    #--------------------------------------------------------------------------------
    for fname in AllTestDates: #run once for each date
    #    if fname < '20200108':
    #        continue
        print('Date = ' + fname)
        for j in range(len(AllInstrumentCode)):   #run once for each instrument code
            
        #Access/Prepare files
            InstrumentCode = AllInstrumentCode[j] 
            print('Code = ' + InstrumentCode)
            ExchangeCode = GetExchangeCode(InstrumentCode)
            cfname = fadd + "\\" + fname[0:4] + "\\" +  fname + "\\" + ExchangeCode + "_" + fname + ".csv"
            if os.path.isfile(cfname) == False: 
                print('Source file not found; skipping')
                continue
            destdir = fadd + "\\SpreadData\\"+InstrumentCode
            if not os.path.exists(destdir):
                os.makedirs(destdir) #create destination directory
            pattern = "RawSpread_" + fname         
            #if len([i for i in os.listdir(destdir) if pattern in i]) > 0:
                #print('File already exists; skipping')
                #continue
            
        #Create df (raw data)
            if j == 0 or GetExchangeCode(InstrumentCode) != GetExchangeCode(AllInstrumentCode[j-1]) or 'df' not in locals():
                df = pd.read_csv(cfname,sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
                #python vs c? engine='python',
                #i_drop = [i for i in range(len(df)) if abs(df.iat[i,24]) < epsilon and abs(df.iat[i,26]) < epsilon]
                #df = df.drop(i_drop) #remove rows with small volumes
                df = df[(abs(df[df.columns[24]]) >= epsilon) | (abs(df[df.columns[26]]) >= epsilon)]
                df.index=range(len(df))
    
            activeCodes = findActiveCodes(df, InstrumentCode)
            if activeCodes is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                continue
            else:
                activeCode1, activeCode2 = activeCodes
                
            destfile = destdir + "\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + "_1.csv"
            #if os.path.isfile(destfile):
            #    print('File already exists; skipping')
            #    continue
    #-------------------------------------------------------------------------------------------------------------        
            #see above
            dfObj = createSpread(df, activeCode1, activeCode2, InstrumentCode)
            if dfObj is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                continue
            dfObj = dfObj.drop_duplicates().reset_index(drop=True)
            dfObj.to_csv(destfile)
            print('Complete.')
         
if __name__ == '__main__':
    main()
