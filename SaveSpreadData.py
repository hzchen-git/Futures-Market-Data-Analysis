# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 09:13:55 2020

@author: xingtan
"""

import os
import copy
#import csv
import numpy as np
import pandas as pd
import array as arr
from datetime import datetime, time, date, timedelta
from GetInstrumentInfo import GetMultiplier, GetExchangeCode#, GetTimeInHalfSeconds

def findActiveCodes(df, InstrumentCode):
    ExchangeCode=GetExchangeCode(InstrumentCode)
    AllInstrument = list(set(df.iloc[:,2])) #list of all unique instrument codes in spread
    unique = list()
    volumes = arr.array('i', []) #new array of ints

    mylist = list(df.iloc[:,2]) #the instrument codes column, in a list
    for i in range(len(AllInstrument)):
        if len(AllInstrument[i])==4-(ExchangeCode=='CZCE')+len(InstrumentCode) and (AllInstrument[i][0:len(InstrumentCode)]) == InstrumentCode: #check that it is the instrument code
            lastindex = len(mylist) - mylist[::-1].index(AllInstrument[i]) - 1
            lastvolume = df.iloc[lastindex,12] #colume 12 = volume
            if lastvolume >= 0 : #cumulative volume; last is highest
                unique.append(AllInstrument[i])
                volumes.extend([int(lastvolume)])
    if len(volumes) < 2: 
        return None #quit
    i1 = volumes.index(max(volumes)) #Highest Volume
    i2 = volumes.index(np.unique(volumes)[-2]) #Second Highest Volume
    return unique[i1], unique[i2]

def matchMerge(df1, df2):
    df1['count'] = df1.groupby('TM').cumcount()
    df2['count'] = df2.groupby('TM').cumcount()
    df1.drop(columns='TM',inplace=True)
    df2.drop(columns='TM',inplace=True)
    df = pd.merge(df1, df2, how='outer', on=['Time','MilliSecond','count'], sort=True)
    #df['TradeVolume1'].fillna(value=0, inplace=True)
    #df['TradeVolume2'].fillna(value=0, inplace=True)
    #df['AveragePrice1'].fillna(value=-1,inplace=True)
    #df['AveragePrice2'].fillna(value=-1,inplace=True)
    df.fillna(method='ffill', inplace=True)
    df.drop(columns='count', inplace=True)
    return df

def calculateBuySellVolume(df, instrument=1):
    instrument=str(instrument)
    # returns the two new series; can append on your own
    prev_bid = df['BidPrice'+instrument].shift(1)
    prev_ask = df['AskPrice'+instrument].shift(1)
    
    buyvolume = pd.Series(np.where((df['TradeVolume'+instrument]==0) | (prev_ask==0) | (df['AveragePrice'+instrument]<=prev_bid), 0, np.where((prev_bid==0) | (df['AveragePrice'+instrument]>=prev_ask), df['TradeVolume'+instrument], df['TradeVolume'+instrument]*(df['AveragePrice'+instrument]-prev_bid)/(prev_ask-prev_bid))))
    sellvolume = df['TradeVolume'+instrument] - buyvolume
    buyvolume[0] = np.nan
    sellvolume[0] = np.nan
    return buyvolume, sellvolume

def CZCEfix(df):
    # MilliSecond is currently 0
    df['count'] = df.groupby('Time').cumcount()
    df['MilliSecond'] = np.where(df['count']==0, 0, 500) # first 0, rest 500
    df.drop_duplicates(subset=['Time', 'MilliSecond'], keep='last', ignore_index=True, inplace=True)
    df.drop(columns='count', inplace=True)


def findFirstTick(df):
    t = df['Time'].mask((df['Time'].str.contains('^!'))|(df['Time'].str.contains('^#')), other=df['Time'].str.slice(start=1))
    t = t.mask(t.str.contains('^a'), other=t.str.slice(start=1))
    i=0
    while True:
        curr=t[i]
        currTime = time(hour=int(curr[0:2]), minute=int(curr[3:5]), second=int(curr[6:8]))
        if (currTime >= time(hour=9) and currTime < time(hour=10)) or (currTime >= time(hour=21) and currTime < time(hour=22)):
            return i
        if i>100:
            raise Exception("Uh oh. Where's the first tick?")
        i+=1
      
def deleteFirstTicks(df):
    first = findFirstTick(df)
    if first==0:
        return df
    else:
        return df.iloc[first-1:].reset_index(drop=True)
    
        
def createSpread1(df, activeCode1, activeCode2, InstrumentCode, june_1=False):
    df1 = df[df[df.columns[2]] == activeCode1].reset_index().drop(columns='index')
    df2 = df[df[df.columns[2]] == activeCode2].reset_index().drop(columns='index')
    if len(df1) + len(df2) < 10: 
        return None
    multiplier = GetMultiplier(InstrumentCode)
    #dfObj1 = pd.DataFrame(columns=['Time','MilliSecond','LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','TotalVolume1','Amount1','UpperLimitPrice1','LowerLimitPrice1'])
    #dfObj2 = pd.DataFrame(columns=['Time','MilliSecond','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','TotalVolume2','Amount2','UpperLimitPrice2','LowerLimitPrice2'])
    dfObj1 = pd.DataFrame()
    dfObj2 = pd.DataFrame()
    
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
    
    #TradeVolume and Buy/Sellvolume has been moved after merge
    
    dfObj2['Time'] = df2[df2.columns[21]]
    dfObj2['MilliSecond'] = df2[df2.columns[22]]
    dfObj2['LastPrice2'] = df2[df2.columns[5]]
    dfObj2['BidVolume2'] = df2[df2.columns[24]]
    dfObj2['BidPrice2'] = df2[df2.columns[23]]
    dfObj2['AskPrice2'] = df2[df2.columns[25]]
    dfObj2['AskVolume2'] = df2[df2.columns[26]]
    dfObj2['TotalVolume2'] = df2[df2.columns[12]]
    dfObj2['Amount2'] = df2[df2.columns[13]]
    dfObj2['UpperLimitPrice2'] = df2[df2.columns[17]]
    dfObj2['LowerLimitPrice2'] = df2[df2.columns[18]]
    
    # Fix the time column for sorting, hackily
    #dfObj1 = deleteFirstTicks(dfObj1)
    dfObj1['Time'].where(dfObj1['Time']>'17:00:00', other='a'+dfObj1['Time'], inplace=True)
    dfObj1['Time'].mask(dfObj1.index<findFirstTick(dfObj1), other='!'+dfObj1['Time'], inplace=True)
#    = np.where(dfObj1['Time'].str.contains('^2'), dfObj1['Time'], 'a'+dfObj1['Time'])
    #dfObj2 = deleteFirstTicks(dfObj2)
    dfObj2['Time'].where(dfObj2['Time']>'17:00:00', other='a'+dfObj2['Time'], inplace=True)
    dfObj2['Time'].mask(dfObj2.index<findFirstTick(dfObj2), other='!'+dfObj2['Time'], inplace=True)
#    = np.where(dfObj2['Time'].str.contains('^2'), dfObj2['Time'], 'a'+dfObj2['Time'])
    #print(len(dfObj1), len(dfObj2))
    
    exchangeCode = GetExchangeCode(InstrumentCode)
    
    if not june_1:
        dfObj1 = dfObj1.drop_duplicates().reset_index(drop=True)
        dfObj2 = dfObj2.drop_duplicates().reset_index(drop=True)
    elif exchangeCode != 'DCE':
        d1 = dfObj1[dfObj1.Time < 'a09:00:00'].drop_duplicates()
        d2 = dfObj2[dfObj2.Time < 'a09:00:00'].drop_duplicates()
        dfObj1 = dfObj1[dfObj1.Time >= 'a09:00:00'].drop_duplicates(subset=['Time','MilliSecond','LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','Amount1','UpperLimitPrice1','LowerLimitPrice1'])
        dfObj2 = dfObj2[dfObj2.Time >= 'a09:00:00'].drop_duplicates(subset=['Time','MilliSecond','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','Amount2','UpperLimitPrice2','LowerLimitPrice2'])
        dfObj1 = d1.append(dfObj1, ignore_index=True)
        dfObj2 = d2.append(dfObj2, ignore_index=True)
    else:
        dfObj1 = dfObj1.drop_duplicates(subset=['Time','MilliSecond','LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','Amount1','UpperLimitPrice1','LowerLimitPrice1'])
        dfObj2 = dfObj2.drop_duplicates(subset=['Time','MilliSecond','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','Amount2','UpperLimitPrice2','LowerLimitPrice2'])
    
    #dfObj1.to_csv('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\dfObj1.csv')
    #dfObj2.to_csv('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\dfObj2.csv')
    
    # HANDLE DCE/CZCE:
    if exchangeCode=='DCE':
        dfObj1['MS1'] = dfObj1['MilliSecond']
        dfObj1['MilliSecond'] = np.where(dfObj1['MilliSecond'] < 500, 0, 500)
        dfObj2['MS2'] = dfObj2['MilliSecond']
        dfObj2['MilliSecond'] = np.where(dfObj2['MilliSecond'] < 500, 0, 500)
    elif exchangeCode=='CZCE':
        CZCEfix(dfObj1)
        CZCEfix(dfObj2)
    
    # HANDLE THE DUPLICATES
    dfObj1['TM'] = list(zip(dfObj1['Time'],dfObj1['MilliSecond']))
    dfObj2['TM'] = list(zip(dfObj2['Time'],dfObj2['MilliSecond']))
    
    l1 = list(dfObj1[dfObj1.duplicated(subset='TM', keep=False)].TM)
    l2 = list(dfObj2[dfObj2.duplicated(subset='TM', keep=False)].TM)
    s = set(l1).intersection(set(l2))
    print(s)
    #print(l1, l2)
    #print(len(list(s)),len(l1)+len(l2)-len(list(s)))
    if len(s) > 0:
        dup1 = dfObj1[dfObj1['TM'].isin(s)]
        dup2 = dfObj2[dfObj2['TM'].isin(s)]
        dfObj1 = dfObj1[dfObj1['TM'].isin(s)==False]
        dfObj2 = dfObj2[dfObj2['TM'].isin(s)==False]
        #print(len(dup1),len(dfObj1),len(dup2), len(dfObj2))
    dfObj1.drop(columns='TM',inplace=True)
    dfObj2.drop(columns='TM',inplace=True)
     
    #now merge!
    dfObj = pd.merge(dfObj1, dfObj2, how='outer', on=['Time','MilliSecond'], sort=True)
    #Note: merge does not sort by default
    if len(s) > 0:
        if len(dup1)==0 or len(dup2)==0:
            raise Exception('How did this happen?')
        dfObj = dfObj.append(matchMerge(dup1,dup2),ignore_index=True).sort_values(by=['Time','MilliSecond']).reset_index(drop=True)
    
    #dfObj.to_csv('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\dfObj.csv')
    
    #dfObj.iloc[:findFirstTick(dfObj)] = dfObj.iloc[:findFirstTick(dfObj)].fillna(method='ffill')
    dfObj.fillna(method='ffill', inplace=True)
    
    # In case the first few ticks are missing:
    if True in list(dfObj.iloc[0].isna()):
        print("There's some ticks missing from the start.")
        dfObj[['UpperLimitPrice1','UpperLimitPrice2','LowerLimitPrice1','LowerLimitPrice2']] = dfObj[['UpperLimitPrice1','UpperLimitPrice2','LowerLimitPrice1','LowerLimitPrice2']].fillna(method='bfill')
        dfObj['LastPrice1'].fillna(value=dfObj['UpperLimitPrice1'], inplace=True)
        dfObj['LastPrice2'].fillna(value=dfObj['UpperLimitPrice2'], inplace=True)
        dfObj.fillna(value=0, inplace=True)
    #append the duplicates back
    
    if exchangeCode=='DCE' or findFirstTick(dfObj)!=1:
        dfObj = deleteFirstTicks(dfObj)
    
    #TradeVolume and AveragePrice are calculated with TotalVolume and amount, which carries between rows
    dfObj['TradeVolume1'] = dfObj.TotalVolume1.diff()
    dfObj.at[0,'TradeVolume1'] = dfObj['TotalVolume1'][0]
    dfObj['TradeAmount1'] = dfObj.Amount1.diff() #temporary column
    dfObj.at[0,'TradeAmount1'] = dfObj['Amount1'][0]
    dfObj['AveragePrice1'] = dfObj['TradeAmount1']/dfObj['TradeVolume1']/multiplier #nan if no trades
    dfObj.drop(columns='TradeAmount1', inplace=True)
    dfObj['BuyVolume1'], dfObj['SellVolume1'] = calculateBuySellVolume(dfObj, instrument=1)
    dfObj['TradeVolume2'] = dfObj.TotalVolume2.diff()
    dfObj.at[0,'TradeVolume2'] = dfObj['TotalVolume2'][0]
    dfObj['TradeAmount2'] = dfObj.Amount2.diff() #temporary column
    dfObj.at[0,'TradeAmount2'] = dfObj['Amount2'][0]
    dfObj['AveragePrice2'] = dfObj['TradeAmount2']/dfObj['TradeVolume2']/multiplier
    dfObj.drop(columns='TradeAmount2', inplace=True)
    dfObj['BuyVolume2'], dfObj['SellVolume2'] = calculateBuySellVolume(dfObj, instrument=2)
    
    #dfObj['TradeVolume1'].fillna(value=0, inplace=True)
    #dfObj['TradeVolume2'].fillna(value=0, inplace=True)
    #dfObj['AveragePrice1'].fillna(value=-1,inplace=True)
    #dfObj['AveragePrice2'].fillna(value=-1,inplace=True)
    dfObj['BuyVolume1'].fillna(value=0, inplace=True)
    dfObj['SellVolume1'].fillna(value=0, inplace=True)
    dfObj['BuyVolume2'].fillna(value=0, inplace=True)
    dfObj['SellVolume2'].fillna(value=0, inplace=True)
    #now restore averageprice back to normal.
    #dfObj['AveragePrice1'].replace(to_replace=-1, value=np.nan, inplace=True)
    #dfObj['AveragePrice2'].replace(to_replace=-1, value=np.nan, inplace=True)
    
    #restore the time column
    dfObj['Time'].mask(dfObj['Time'].str.contains('!'), other=dfObj['Time'].str.slice(start=1), inplace=True)
    dfObj['Time'].mask(dfObj['Time'].str.contains('a'), other=dfObj['Time'].str.slice(start=1), inplace=True)
    
    #finally, the few columns that are missing.
    dfObj['Spread'] = -(dfObj['LastPrice1'] - dfObj['LastPrice2']) #2-1 now!
    
    dfObj['AskPrice1N'] = np.where(dfObj['AskVolume1']!=0, dfObj['AskPrice1'], dfObj['UpperLimitPrice1'])
    dfObj['BidPrice1N'] = np.where(dfObj['BidVolume1']!=0, dfObj['BidPrice1'], dfObj['LowerLimitPrice1'])
    dfObj['AskPrice2N'] = np.where(dfObj['AskVolume2']!=0, dfObj['AskPrice2'], dfObj['UpperLimitPrice2'])
    dfObj['BidPrice2N'] = np.where(dfObj['BidVolume2']!=0, dfObj['BidPrice2'], dfObj['LowerLimitPrice2'])
    
    dfObj['MidSpread'] = (dfObj['BidPrice2N'] + dfObj['AskPrice2N'] - dfObj['BidPrice1N'] - dfObj['AskPrice1N'])/2
    dfObj['MidPrice1'] = (dfObj['BidPrice1N']+dfObj['AskPrice1N'])/2
    dfObj['MidPrice2'] = (dfObj['BidPrice2N']+dfObj['AskPrice2N'])/2        

    #and order and clean up.
    
    if exchangeCode=='DCE':
        dfObj = dfObj[['Time', 'MilliSecond', 'MS1', 'MS2', 'Spread', 'MidSpread', 'LastPrice1', 'MidPrice1', 'BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','BuyVolume1','SellVolume1', 'MidPrice2', 'LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','BuyVolume2','SellVolume2','TotalVolume1','TotalVolume2','Amount1','Amount2', 'UpperLimitPrice1', 'LowerLimitPrice1', "UpperLimitPrice2", "LowerLimitPrice2"]]
    else:
        dfObj = dfObj[['Time', 'MilliSecond', 'Spread', 'MidSpread', 'LastPrice1', 'MidPrice1', 'BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','BuyVolume1','SellVolume1', 'MidPrice2', 'LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','BuyVolume2','SellVolume2','TotalVolume1','TotalVolume2','Amount1','Amount2', 'UpperLimitPrice1', 'LowerLimitPrice1', "UpperLimitPrice2", "LowerLimitPrice2"]]
    #print(null_counts[null_counts > 0].sort_values(ascending=False))
    dfObj = dfObj.astype({'BidVolume1':'int64','AskVolume1':'int64','TradeVolume1':'int64','BidVolume2':'int64','AskVolume2':'int64','TradeVolume2':'int64','TotalVolume1':'int64','TotalVolume2':'int64','Amount1':'int64','Amount2':'int64'})
    return dfObj

    

def main():
    fadd = 'D:\\Hongze\\Dropbox'
    if os.path.isdir(fadd) == False :
        fadd = 'C:\\Users\\Hongze\\Dropbox'
    AllInstrumentCode = list(['cu','au','ag','ni','rb','al','ru','zn','bu','fu','hc','pb','sn','sp','ss','sc','IF','IC','T'])
    AllInstrumentCode.extend(['cs','eb','eg','jd','jm','pp','pg','rr','a','b','c','i','j','l','m','p','v','y'])
    AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8]
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i<'20200323']
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i>='20200323' and i<'20200521']
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i>='20200521']
    #AllInstrumentCode = list(['OI'])
    #AllTestDates = list(['20200529'])
    epsilon=1e-10
    with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\passes.txt', 'w') as txtfile:
        txtfile.write('No RawSpread created for code/date:\n')
    
    datedict = dict()
    for c in AllInstrumentCode:
        datedict[c] = copy.deepcopy(AllTestDates)
    
    #--------------------------------------------------------------------------------
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
            if len([i for i in os.listdir(destdir) if pattern in i]) > 0:
                print('File already exists; skipping\n')
                continue
            
        #Create df (raw data)
            if lastread==None or j == 0 or GetExchangeCode(InstrumentCode) != GetExchangeCode(AllInstrumentCode[lastread]) or 'df' not in locals():
                df = pd.read_csv(cfname,sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
                df = df[(abs(df[df.columns[24]]) >= epsilon) | (abs(df[df.columns[26]]) >= epsilon)]
                df.index=range(len(df))
                lastread=j #fixed a bug here.
    
            activeCodes = findActiveCodes(df, InstrumentCode)
            if activeCodes is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\passes.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (insufficient uniques)\n')
                continue
            else:
                activeCode1, activeCode2 = activeCodes
                
    #-------------------------------------------------------------------------------------------------------------
            dfObj = createSpread1(df, activeCode1, activeCode2, InstrumentCode, fname=='20200601')
            if dfObj is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\passes.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (not long enough)\n')
                continue
            
            if len(dfObj[(dfObj['AskVolume1']==0) | (dfObj['AskVolume2']==0) | (dfObj['BidVolume1']==0) | (dfObj['BidVolume2']==0)]) > len(dfObj)/2:
                print("Too many zero volume ticks; skipping...\n")
                if fname in datedict.get(InstrumentCode):
                    datedict.get(InstrumentCode).remove(fname)
                with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\passes.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (too many 0 volumes)\n')
                if d < len(AllTestDates)-1:
                    datedict.get(InstrumentCode).remove(AllTestDates[d+1])
                continue
            
            #If the above is false but still removed from list previously
            if fname not in datedict.get(InstrumentCode):
                print('Creating alternate spread for '+InstrumentCode+' on '+fname)
                with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\passes.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (previous day too many 0 volumes)\n')
                if not os.path.exists(destdir+'\\Alternates'):
                    os.mkdir(destdir+'\\Alternates')
                destfile = destdir + "\\Alternates\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + ".csv"
            else:
                destfile = destdir + "\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + ".csv"
            #destfile='D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\cu0102.csv'
            dfObj.to_csv(destfile)
            print('Complete.\n')
            #exit()

if __name__ == '__main__':
    main()
