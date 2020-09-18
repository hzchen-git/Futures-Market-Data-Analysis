# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 21:47:22 2020

@author: Hongze
"""

import os
import copy
import pandas as pd
import numpy as np
from GetInstrumentInfo import GetExchangeCode, GetMultiplier
from SaveSpreadData2 import findActiveCodes, findFirstTick, calculateBuySellVolume, deleteFirstTicks

def createSingleSpread(df, activeCode, instrument=1):
    inst=str(instrument)
    df = df[df[df.columns[2]] == activeCode].reset_index(drop=True)
    dfObj = pd.DataFrame()
    dfObj['Time'] = df[df.columns[21]]
    #dfObj['MilliSecond'] = df[df.columns[22]]
    if instrument!=1 and instrument!=2:
        raise Exception('Invalid instrument number') 
    dfObj['LastPrice'+inst] = df[df.columns[5]]
    dfObj['BidVolume'+inst] = df[df.columns[24]]
    dfObj['BidPrice'+inst] = df[df.columns[23]]
    dfObj['AskPrice'+inst] = df[df.columns[25]]
    dfObj['AskVolume'+inst] = df[df.columns[26]]
    dfObj['TotalVolume'+inst] = df[df.columns[12]]
    dfObj['Amount'+inst] = df[df.columns[13]]
    dfObj['UpperLimitPrice'+inst] = df[df.columns[17]]
    dfObj['LowerLimitPrice'+inst] = df[df.columns[18]]
    return dfObj

def addSingleColumns(df, InstrumentCode, instrument=1):
    inst=str(instrument)
    multiplier = GetMultiplier(InstrumentCode)
    df['TradeVolume'+inst] = df['TotalVolume'+inst].diff()
    df.at[0,'TradeVolume'+inst] = df['TotalVolume'+inst][0]
    df['TradeAmount'+inst] = df['Amount'+inst].diff() #temporary column
    df.at[0,'TradeAmount'+inst] = df['Amount'+inst][0]
    df['AveragePrice'+inst] = df['TradeAmount'+inst]/df['TradeVolume'+inst]/multiplier
    df.drop(columns=('TradeAmount'+inst), inplace=True)
    df['BuyVolume'+inst], df['SellVolume'+inst] = calculateBuySellVolume(df, instrument=instrument)

def createSpread(df_curr, df_prev, activeCode1, activeCode2, InstrumentCode):
    df1_curr = createSingleSpread(df_curr, activeCode1, instrument=1)
    df2_curr = createSingleSpread(df_curr, activeCode2, instrument=2)
    if df_prev is not None:
        df1_prev = createSingleSpread(df_prev, activeCode1, instrument=1)
        df2_prev = createSingleSpread(df_prev, activeCode2, instrument=2)
    # concat only the relevant parts
        df1_prev = df1_prev[(df1_prev['Time']>='20:00:00') & (df1_prev.index>=findFirstTick(df1_prev))]
        df1_prev['Time'] = '!'+df1_prev['Time']
        df2_prev = df2_prev[(df2_prev['Time']>='20:00:00') & (df2_prev.index>=findFirstTick(df2_prev))]
        df2_prev['Time'] = '!'+df2_prev['Time']
    
    df1_curr['Time'].mask(df1_curr.index<findFirstTick(df1_curr), other='#'+df1_curr['Time'], inplace=True)
    df1_curr = df1_curr[df1_curr['Time']<'20:00:00']
    df2_curr['Time'].mask(df2_curr.index<findFirstTick(df2_curr), other='#'+df2_curr['Time'], inplace=True)
    df2_curr = df2_curr[df2_curr['Time']<'20:00:00']
    
    
    if df_prev is not None:
        #Checking mechanism for limit prices
        if not df1_prev.empty and (df1_curr['UpperLimitPrice1'].iat[0]!=df1_prev['UpperLimitPrice1'].iat[-1] or df1_curr['LowerLimitPrice1'].iat[0]!=df1_prev['LowerLimitPrice1'].iat[-1]):
            raise Exception("Limit prices don\'t match for instrument 1")
        if not df2_prev.empty and (df2_curr['UpperLimitPrice2'].iat[0]!=df2_prev['UpperLimitPrice2'].iat[-1] or df2_curr['LowerLimitPrice2'].iat[0]!=df2_prev['LowerLimitPrice2'].iat[-1]):
            raise Exception("Limit prices don\'t match for instrument 2")
        print("Prev1: "+str(len(df1_prev))+", Curr1: "+str(len(df1_curr)))
        dfObj1 = pd.concat([df1_prev, df1_curr], ignore_index=True)
        print("Prev2: "+str(len(df2_prev))+", Curr2: "+str(len(df2_curr)))
        dfObj2 = pd.concat([df2_prev, df2_curr], ignore_index=True)
        
    else:
        #What to do if no previous file (i.e. 1/3)
        print('Curr1: '+str(len(df1_curr)))
        dfObj1 = df1_curr
        print('Curr2: '+str(len(df1_curr)))
        dfObj2 = df2_curr
    # Hopefully dropping duplicates also handles the June 1 problem.
    dfObj1['tempT'] = dfObj1['Time'].mask((dfObj1['Time'].str.contains('!')) | (dfObj1['Time'].str.contains('#')), other=dfObj1['Time'].str.slice(start=1))
    dfObj1.drop_duplicates(subset=['tempT','LastPrice1','BidVolume1','BidPrice1','AskPrice1','AskVolume1','Amount1','UpperLimitPrice1','LowerLimitPrice1'], inplace=True, ignore_index=True)
    dfObj2['tempT'] = dfObj2['Time'].mask((dfObj2['Time'].str.contains('!')) | (dfObj2['Time'].str.contains('#')), other=dfObj2['Time'].str.slice(start=1))
    dfObj2.drop_duplicates(subset=['tempT','LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','Amount2','UpperLimitPrice2','LowerLimitPrice2'], inplace=True, ignore_index=True)

    if len(dfObj1) + len(dfObj2) < 20:
        return None
    dfObj1.drop(columns=['tempT'], inplace=True)
    dfObj2.drop(columns=['tempT'], inplace=True)
    
    #NOW we can merge. But first, handle duplicate seconds.
    dfObj1['count'] = dfObj1.groupby('Time').cumcount()
    dfObj2['count'] = dfObj2.groupby('Time').cumcount()
    print('Seconds with more than 2 ticks: '+ str(len(dfObj1[dfObj1['count']>=2]))+', '+str(len(dfObj1[dfObj1['count']>=2])))
    
    # Since more than 1 tick in a second is common, integrate match-merge here
    
    dfObj = pd.merge(dfObj1, dfObj2, how='outer', on=['Time', 'count'], sort=True)
    #dfObj.iloc[:findFirstTick(dfObj)] = dfObj.iloc[:findFirstTick(dfObj)].fillna(method='ffill') # a workaround for start ticks
    dfObj.fillna(method='ffill', inplace=True)
    
    dfObj['Time'].mask((dfObj['Time'].str.contains('!')) | (dfObj['Time'].str.contains('#')), other=dfObj['Time'].str.slice(start=1), inplace=True)
    
    if True in list(dfObj.iloc[0].isna()):
        print("There's some ticks missing from the start.") #Hopefully won't happen...
        dfObj[['UpperLimitPrice1','UpperLimitPrice2','LowerLimitPrice1','LowerLimitPrice2']] = dfObj[['UpperLimitPrice1','UpperLimitPrice2','LowerLimitPrice1','LowerLimitPrice2']].fillna(method='bfill')
        dfObj['LastPrice1'].fillna(value=dfObj['UpperLimitPrice1'], inplace=True)
        dfObj['LastPrice2'].fillna(value=dfObj['UpperLimitPrice2'], inplace=True)
        dfObj.fillna(value=0, inplace=True)
    if findFirstTick(dfObj)!=1:
        print("Removing extra start ticks...")
        dfObj = deleteFirstTicks(dfObj)
    
    addSingleColumns(dfObj, InstrumentCode, instrument=1)
    addSingleColumns(dfObj, InstrumentCode, instrument=2)
    
    dfObj['BuyVolume1'].fillna(value=0, inplace=True)
    dfObj['SellVolume1'].fillna(value=0, inplace=True)
    dfObj['BuyVolume2'].fillna(value=0, inplace=True)
    dfObj['SellVolume2'].fillna(value=0, inplace=True)
    
    dfObj['Spread'] = -(dfObj['LastPrice1'] - dfObj['LastPrice2'])
    dfObj['AskPrice1N'] = np.where(dfObj['AskVolume1']!=0, dfObj['AskPrice1'], dfObj['UpperLimitPrice1'])
    dfObj['BidPrice1N'] = np.where(dfObj['BidVolume1']!=0, dfObj['BidPrice1'], dfObj['LowerLimitPrice1'])
    dfObj['AskPrice2N'] = np.where(dfObj['AskVolume2']!=0, dfObj['AskPrice2'], dfObj['UpperLimitPrice2'])
    dfObj['BidPrice2N'] = np.where(dfObj['BidVolume2']!=0, dfObj['BidPrice2'], dfObj['LowerLimitPrice2'])
    dfObj['MidSpread'] = (dfObj['BidPrice2N'] + dfObj['AskPrice2N'] - dfObj['BidPrice1N'] - dfObj['AskPrice1N'])/2
    dfObj['MidPrice1'] = (dfObj['BidPrice1N']+dfObj['AskPrice1N'])/2
    dfObj['MidPrice2'] = (dfObj['BidPrice2N']+dfObj['AskPrice2N'])/2
    dfObj = dfObj[['Time', 'Spread', 'MidSpread', 'LastPrice1', 'MidPrice1', 'BidVolume1','BidPrice1','AskPrice1','AskVolume1','TradeVolume1','AveragePrice1','BuyVolume1','SellVolume1', 'MidPrice2', 'LastPrice2','BidVolume2','BidPrice2','AskPrice2','AskVolume2','TradeVolume2','AveragePrice2','BuyVolume2','SellVolume2','TotalVolume1','TotalVolume2','Amount1','Amount2', 'UpperLimitPrice1', 'LowerLimitPrice1', "UpperLimitPrice2", "LowerLimitPrice2"]]
    dfObj = dfObj.astype({'BidVolume1':'int64','AskVolume1':'int64','TradeVolume1':'int64','BidVolume2':'int64','AskVolume2':'int64','TradeVolume2':'int64','TotalVolume1':'int64','TotalVolume2':'int64','Amount1':'int64','Amount2':'int64'})
    return dfObj
    
def main():
    fadd = 'D:\\Hongze\\Dropbox'
    if os.path.isdir(fadd) == False :
        fadd = 'C:\\Users\\Hongze\\Dropbox'
    AllInstrumentCode = ['AP','CF','CJ','FG','MA','OI','RM','SA','SF','SM','SR','TA','UR','ZC']
    AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8]
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i<'20200323']
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i>='20200323' and i<'20200521']
    #AllTestDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8 and i>='20200521']
    actualAllDates = [i for i in list(os.listdir(fadd+'\\2020')) if len(i)==8]
    #AllInstrumentCode = list(['ZC'])
    #AllTestDates = list(['20200515'])
    epsilon=1e-10
    with open(fadd+'\\SpreadData\\_Test Data\\passesCZCE.txt', 'w') as txtfile:
        txtfile.write('No RawSpread created for code/date:\n')
    
    datedict = dict()
    for c in AllInstrumentCode:
        datedict[c] = copy.deepcopy(AllTestDates)
    
    #--------------------------------------------------------------------------------
    for d in range(len(AllTestDates)):
        fname = AllTestDates[d]
        if actualAllDates.index(fname)>0:
            pname = actualAllDates[actualAllDates.index(fname)-1]
        else:
            pname = None # should only happen if 1/2
        print('Date = ' + fname+'\n')
        lastread=None #just to initialize
        for j in range(len(AllInstrumentCode)):
        #Access/Prepare files
            InstrumentCode = AllInstrumentCode[j] 
            print('Code = ' + InstrumentCode)
            ExchangeCode = GetExchangeCode(InstrumentCode)
            cfname = fadd + "\\" + fname[0:4] + "\\" +  fname + "\\" + ExchangeCode + "_" + fname + ".csv"
            if pname is not None:
                cfname_prev = fadd + "\\" + pname[0:4] + "\\" +  pname + "\\" + ExchangeCode + "_" + pname + ".csv"
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
            if lastread==None or j == 0 or GetExchangeCode(InstrumentCode) != GetExchangeCode(AllInstrumentCode[lastread]) or 'df_curr' not in locals():
                df_curr = pd.read_csv(cfname,sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
                df_curr = df_curr[(abs(df_curr[df_curr.columns[24]]) >= epsilon) | (abs(df_curr[df_curr.columns[26]]) >= epsilon)].reset_index(drop=True)
                if pname is not None:
                    df_prev = pd.read_csv(cfname_prev,sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
                    df_prev = df_prev[(abs(df_prev[df_prev.columns[24]]) >= epsilon) | (abs(df_prev[df_prev.columns[26]]) >= epsilon)].reset_index(drop=True)
                else:
                    df_prev = None # nothing read if no previous day
                lastread=j #fixed a bug here.
                
            activeCodes = findActiveCodes(df_curr, InstrumentCode)
            if activeCodes is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                with open(fadd+'\\SpreadData\\_Test Data\\passesCZCE.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (insufficient uniques)\n')
                continue
            else:
                activeCode1, activeCode2 = activeCodes
                
    #-------------------------------------------------------------------------------------------------------------
            dfObj = createSpread(df_curr, df_prev, activeCode1, activeCode2, InstrumentCode)
            if dfObj is None: 
                print("Nothing created for " + InstrumentCode + " on " + fname)
                with open(fadd+'\\SpreadData\\_Test Data\\passesCZCE.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (not long enough)\n')
                continue
            
            if len(dfObj[(dfObj['AskVolume1']==0) | (dfObj['AskVolume2']==0) | (dfObj['BidVolume1']==0) | (dfObj['BidVolume2']==0)]) > len(dfObj)/2:
                print("Too many zero volume ticks; skipping...\n")
                if fname in datedict.get(InstrumentCode):
                    datedict.get(InstrumentCode).remove(fname)
                with open(fadd+'\\SpreadData\\_Test Data\\passesCZCE.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (too many 0 volumes)\n')
                if d < len(AllTestDates)-1:
                    datedict.get(InstrumentCode).remove(AllTestDates[d+1])
                continue
            
            #If the above is false but still removed from list previously
            if fname not in datedict.get(InstrumentCode):
                print('Creating alternate spread for '+InstrumentCode+' on '+fname)
                with open(fadd+'\\SpreadData\\_Test Data\\passesCZCE.txt', 'a') as txtfile:
                    txtfile.write(InstrumentCode + ': ' + fname+' (previous day too many 0 volumes)\n')
                if not os.path.exists(destdir+'\\Alternates'):
                    os.mkdir(destdir+'\\Alternates')
                destfile = destdir + "\\Alternates\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + ".csv"
            else:
                destfile = destdir + "\\RawSpread_" + fname + "_" + activeCode1 +  "_" + activeCode2 + ".csv"
            dfObj.to_csv(destfile)
            print('Complete.\n')

if __name__ == '__main__':
    main()
