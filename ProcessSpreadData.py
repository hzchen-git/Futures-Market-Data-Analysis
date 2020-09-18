# -*- coding: utf-8 -*-
"""
Created on Sun Jun 28 18:41:04 2020

@author: Hongze
"""
#import sys
import os
#import csv
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from GetInstrumentInfo import GetExchangeCode
from SaveSpreadData import createSpread1
from SaveSpreadDataCZCE import createSpread
epsilon=1e-10

def calculateTickStats(df, minutes):
    ticks = minutes * 120
    minutes = str(minutes)
    # midspread stats
    df[minutes+'m Average M'] = df['MidSpread'].rolling(ticks).mean().shift(1)
    df[minutes+'m Std M'] = df['MidSpread'].rolling(ticks).std().shift(1)
    df[minutes+'m Average M'].fillna(value=df['1d+ Average M'], inplace=True)
    df[minutes+'m Std M'].fillna(value=df['1d+ Std M'], inplace=True)
    #df[minutes+'m z'] = (df[minutes+'m Average M']-df['1d Average M'])/df['1d Std M']
    
    # midprice stats 1
    df[minutes+'m Average1'] = df['MidPrice1'].rolling(ticks).mean().shift(1)
    df[minutes+'m Std1'] = df['MidPrice1'].rolling(ticks).std().shift(1)
    df[minutes+'m Average1'].fillna(value=df['1d+ Average1'], inplace=True)
    df[minutes+'m Std1'].fillna(value=df['1d+ Std1'], inplace=True)
    
    # midprice stats 2
    df[minutes+'m Average2'] = df['MidPrice2'].rolling(ticks).mean().shift(1)
    df[minutes+'m Std2'] = df['MidPrice2'].rolling(ticks).std().shift(1)
    df[minutes+'m Average2'].fillna(value=df['1d+ Average2'], inplace=True)
    df[minutes+'m Std2'].fillna(value=df['1d+ Std2'], inplace=True)
    
    #aggregate buy/sell
    df[minutes+'m Agg BuyVolume1'] = df['BuyVolume1'].rolling(ticks).sum().shift(1)
    df[minutes+'m Agg BuyVolume1'].mask(df[minutes+'m Agg BuyVolume1']<epsilon, other=0, inplace=True)
    df[minutes+'m Agg SellVolume1'] = df['SellVolume1'].rolling(ticks).sum().shift(1)
    df[minutes+'m Agg SellVolume1'].mask(df[minutes+'m Agg SellVolume1']<epsilon, other=0, inplace=True)
    df[minutes+'m Ratio1'] = ((df[minutes+'m Agg BuyVolume1']-df[minutes+'m Agg SellVolume1'])/(df[minutes+'m Agg BuyVolume1']+df[minutes+'m Agg SellVolume1'])).mask((df[minutes+'m Agg BuyVolume1']+df[minutes+'m Agg SellVolume1'])==0, other=0)
    #df[minutes+'m Ratio1'].mask(df[minutes+'m Agg BuyVolume1'] + df[minutes+'m Agg SellVolume1'] < 2*epsilon, other=0, inplace=True)
    df[minutes+'m Agg BuyVolume2'] = df['BuyVolume2'].rolling(ticks).sum().shift(1)
    df[minutes+'m Agg BuyVolume2'].mask(df[minutes+'m Agg BuyVolume2']<epsilon, other=0, inplace=True)
    df[minutes+'m Agg SellVolume2'] = df['SellVolume2'].rolling(ticks).sum().shift(1)
    df[minutes+'m Agg SellVolume2'].mask(df[minutes+'m Agg SellVolume2']<epsilon, other=0, inplace=True)
    df[minutes+'m Ratio2'] = ((df[minutes+'m Agg BuyVolume2']-df[minutes+'m Agg SellVolume2'])/(df[minutes+'m Agg BuyVolume2']+df[minutes+'m Agg SellVolume2'])).mask((df[str(minutes)+'m Agg BuyVolume2']+df[minutes+'m Agg SellVolume2'])==0, other=0)
    #df[minutes+'m Ratio2'].mask(df[minutes+'m Agg BuyVolume2'] + df[minutes+'m Agg SellVolume2'] < 2*epsilon, other=0, inplace=True)

def calculateCZCEStats(df, minutes):
    seconds = str(minutes * 60) + 's'
    minutes = str(minutes)
    df_temp = df[['seconds', 'MidSpread','MidPrice1','MidPrice2','BuyVolume1','SellVolume1','BuyVolume2', 'SellVolume2']].set_index('seconds')
    #df_comb = pd.DataFrame(index=pd.Index(df.seconds, name='seconds'))
    #df_comb = pd.DataFrame(index=pd.Index(df.seconds.unique(), name='seconds')) #a dataframe indexed by 'seconds', so compatible with new columns
    #seconds should be increasing...right?
    
    dtroll=df_temp['MidSpread'].rolling(seconds, closed='left')
    df[minutes+'m Average M'] = dtroll.mean().fillna(value=df['1d+ Average M']).reset_index(drop=True)
    df[minutes+'m Std M'] = dtroll.std().fillna(value=df['1d+ Std M']).reset_index(drop=True)
    
    dtroll=df_temp['MidPrice1'].rolling(seconds, closed='left')
    df[minutes+'m Average1'] = dtroll.mean().fillna(value=df['1d+ Average1']).reset_index(drop=True)
    df[minutes+'m Std1'] = dtroll.std().fillna(value=df['1d+ Std1']).reset_index(drop=True)

    dtroll=df_temp['MidPrice2'].rolling(seconds, closed='left')
    df[minutes+'m Average2'] = dtroll.mean().fillna(value=df['1d+ Average2']).reset_index(drop=True)
    df[minutes+'m Std2'] = dtroll.std().fillna(value=df['1d+ Std2']).reset_index(drop=True)

    df[minutes+'m Agg BuyVolume1'] = df_temp['BuyVolume1'].rolling(seconds, closed='left').sum().reset_index(drop=True)
    df[minutes+'m Agg SellVolume1'] = df_temp['SellVolume1'].rolling(seconds, closed='left').sum().reset_index(drop=True)
    df[minutes+'m Agg BuyVolume2'] = df_temp['BuyVolume2'].rolling(seconds, closed='left').sum().reset_index(drop=True)
    df[minutes+'m Agg SellVolume2'] = df_temp['SellVolume2'].rolling(seconds, closed='left').sum().reset_index(drop=True)
    
    #df_comb = df_comb.groupby('seconds').first().shift() #decrease number of groupby calls
    #use the first, because the others will consider the previous rows in the same second
    # df_comb now has all the required columns, except with seconds as its index
    #df_comb.reset_index(inplace=True)
    #df = df.merge(df_comb, how='left', on='seconds', validate='m:1') #same second, same values
    
    df[minutes+'m Agg BuyVolume1'].mask(df[minutes+'m Agg BuyVolume1']<epsilon, other=0, inplace=True)
    df[minutes+'m Agg SellVolume1'].mask(df[minutes+'m Agg SellVolume1']<epsilon, other=0, inplace=True)
    df[minutes+'m Agg BuyVolume2'].mask(df[minutes+'m Agg BuyVolume2']<epsilon, other=0, inplace=True)
    df[minutes+'m Agg SellVolume2'].mask(df[minutes+'m Agg SellVolume2']<epsilon, other=0, inplace=True)
    df[minutes+'m Ratio1'] = ((df[minutes+'m Agg BuyVolume1']-df[minutes+'m Agg SellVolume1'])/(df[minutes+'m Agg BuyVolume1']+df[minutes+'m Agg SellVolume1'])).mask((df[minutes+'m Agg BuyVolume1']+df[minutes+'m Agg SellVolume1'])==0, other=0)
    df[minutes+'m Ratio2'] = ((df[minutes+'m Agg BuyVolume2']-df[minutes+'m Agg SellVolume2'])/(df[minutes+'m Agg BuyVolume2']+df[minutes+'m Agg SellVolume2'])).mask((df[minutes+'m Agg BuyVolume2']+df[minutes+'m Agg SellVolume2'])==0, other=0)
    #return df
    
# Hopefully this doesn't slow things down too much.
def toDT(z, prev=False):
    if len(z)==3:
        d,t,m = z
        s = d + " " + t + ' ' + str(m*1000)
        dt = datetime.strptime(s,'%Y%m%d %H:%M:%S %f')
    elif len(z)==2:
        d,t = z
        s = d+" "+t
        dt = datetime.strptime(s,'%Y%m%d %H:%M:%S')
    else:
        raise Exception('toDT error')
    if t[0]=='2':
        dt -= timedelta(days=1)
    if prev==True:
        return dt - timedelta(days=1)
    return dt

#path = 'C:\\Users\\Hongze\\Dropbox\\SpreadData'
path = 'D:\\Hongze\\Dropbox\\SpreadData'
#dataPath = 'C:\\Users\\Hongze\\Dropbox\\2020'
dataPath = 'D:\\Hongze\\Dropbox\\2020'
AllInstrumentCode = list(['cu','au','ag','ni','rb','al','ru','zn','bu','fu','hc','pb','sn','sp','ss','sc','IF','IC','T'])
AllInstrumentCode.extend(['cs','eb','eg','jd','jm','pp','pg','rr','a','b','c','i','j','l','m','p','v','y'])
AllInstrumentCode.extend(['AP','CF','CJ','FG','MA','OI','RM','SA','SF','SM','SR','TA','UR','ZC'])
#AllInstrumentCode = list(['au'])
AllTestDates = [i for i in list(os.listdir(dataPath)) if len(i)==8 and i > '20200102']
#AllTestDates = [i for i in list(os.listdir(dataPath)) if len(i)==8 and i>'20200102' and i<'20200321']
#AllTestDates = [i for i in list(os.listdir(dataPath)) if len(i)==8 and i>='20200321' and i<'20200518']
#AllTestDates = [i for i in list(os.listdir(dataPath)) if len(i)==8 and i>='20200518']
#AllTestDates = list(['20200103'])
print(AllTestDates)
with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\processed.txt', 'w') as txtfile:
    txtfile.write('No ProcessedSpread created for:\n')
for testDate in AllTestDates:
    for code in AllInstrumentCode:
        print('Date=' + testDate, 'Code='+code)
        codepath = path + "\\" + code + "\\"
        pathfiles = [i for i in os.listdir(codepath) if len(i) >=20]
        filename = None
        for file in pathfiles:
            if testDate == file[10:18]: #must start with RawSpread...
                filename = file
                break
        if filename is None:
            #raise Exception('File not found for ' + code + ' on ' + testDate)
            print('File not found. Skipping...\n')
            with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\processed.txt', 'a') as txtfile:
                txtfile.write('Processed ' +code+ ': ' + testDate +' (file not found)\n')
            continue

# Brief intermission: don't want to redo work
        destfile = codepath + "ProcessedSpread" + filename[9:]
        if os.path.isfile(destfile):
            print(destfile + " already exists; skipping")
            continue

# Now we have the filename. Make sure the previous day is here
        dateList = [i for i in list(os.listdir(dataPath)) if len(i)==8]
        alternatePath = codepath + 'Alternates\\'
        if not os.path.exists(alternatePath):
            os.mkdir(alternatePath)
        for i in range(len(dateList)): #get rid of any files that aren't folders
            if (dateList[i])[0:4] != '2020':
                dateList.pop(i)
        currIndex = dateList.index(testDate) #hope it doesn't throw a ValueError
        if currIndex == 0:
            raise Exception("No previous day data found for "+ testDate)
            #print('No Previous day data found')
            #with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\processed.txt', 'a') as txtfile:
            #    txtfile.write('Processed ' +code+ ': ' + testDate +' (no previous day data)\n')
            continue
        previousDate = dateList[currIndex-1]  #should give previous day's 8 digit string       
        prevfilename = filename[:10] + previousDate + filename[18:]
        
        if os.path.isfile(codepath + prevfilename):#search for file
            print('Found file: ' + prevfilename)
            df_prev = pd.read_csv(codepath + prevfilename, index_col=0)
        elif os.path.isfile(alternatePath + prevfilename):
            print('Found alternate file: ' + prevfilename)
            df_prev = pd.read_csv(alternatePath + prevfilename, index_col=0)
        else: 
            print("Creating spread for " + prevfilename + '...')
            #Since we can't guarantee the length of the codes...
            underscore = filename.index('_', 19)
            code1 = filename[19:underscore]
            code2 = filename[underscore+1:-4]
            # actually works for CZCE too!
            exchangeCode = GetExchangeCode(code)
            cfname = dataPath + "\\" +  previousDate + "\\" + exchangeCode + "_" + previousDate + ".csv"
            previous_day = pd.read_csv(cfname,sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
            if exchangeCode == 'CZCE':
                if currIndex == 1:
                    #just one day
                    df_prev = createSpread(previous_day, None, code1, code2, code) #previous file doesn't have 20s, current file does
                else:
                    #go another day back
                    previouserDate = dateList[currIndex-2]
                    previouser_day = pd.read_csv(dataPath + "\\" +  previouserDate + "\\" + exchangeCode + "_" + previouserDate + ".csv",sep=',',encoding = "ISO-8859-1", error_bad_lines=False)
                    df_prev = createSpread(previous_day, previouser_day, code1, code2, code)
            else:
                df_prev = createSpread1(previous_day, code1, code2, code, previousDate=='20200601') #wait for it
            if df_prev is None:
                #raise Exception("No spread created.")
                print('File creation failed. Skipping...')
                with open('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\processed.txt', 'a') as txtfile:
                    txtfile.write('Processed ' +code+ ': ' + testDate +' (file creation failed)\n')
                continue
            print("File created.")
            df_prev.to_csv(alternatePath + prevfilename)
                
        df_curr = pd.read_csv(codepath + filename, index_col=0)
        # We have two day's worth of data now. Now to start manipulating:
        df_curr['keep'] = True
        df_prev['keep'] = False
        
        #remove the first tick
        
        if GetExchangeCode(code)=='CZCE':
            df_curr['DateTime'] = list(zip([testDate for i in range(len(df_curr))], df_curr['Time']))
            df_prev['DateTime'] = list(zip([testDate for i in range(len(df_prev))], df_prev['Time']))
        else:
            df_curr['DateTime'] = list(zip([testDate for i in range(len(df_curr))], df_curr['Time'], df_curr['MilliSecond']))
            df_prev['DateTime'] = list(zip([testDate for i in range(len(df_prev))], df_prev['Time'], df_prev['MilliSecond']))
        df_curr['DateTime'] = df_curr['DateTime'].apply(toDT)
        df_prev['DateTime'] = df_prev['DateTime'].apply(toDT, prev=True)
        
        first = df_curr.iloc[[0]]
        df = pd.concat([df_prev.iloc[1:], df_curr.iloc[1:]], ignore_index=True)
        print(len(df_prev), len(df_curr), len(df_prev) + len(df_curr))
# Arrays to add to for new columns!
        #print("Creating new columns")
        #a5, a15, a30, ah, ad, s5, s15, s30, sh, sd = [0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))],[0 for i in range(len(df_prev))]
        #save some time for the unneeded columns
        print("Calculating avg and std")
        #ad,sd = [0 for i in range(len(df_prev))], [0 for i in range(len(df_prev))]
        #for i in range(len(df_prev), len(df)):
            #currentTime = df.at[i,'DateTime'] #28 is the Datetime column
    
            #print(df[(df['DateTime'] >= (currentTime - timedelta(minutes=60))) & (df['DateTime'] < currentTime)])
            #five = df[(df.DateTime > currentTime - timedelta(minutes=5)) & (df.DateTime <= currentTime)].MidSpread
            #fifteen = df[(df.DateTime > currentTime - timedelta(minutes=15)) & (df.DateTime <= currentTime)].MidSpread
            #thirty = df[(df.DateTime > currentTime - timedelta(minutes=30)) & (df.DateTime <= currentTime)].MidSpread
            #hour = df[(df.DateTime > currentTime - timedelta(hours=1)) & (df.DateTime <= currentTime)].MidSprea
            #day = df[(df['DateTime'] >= (currentTime - pd.Timedelta(days=1))) & (df['DateTime'] < currentTime)].MidSpread
            
            #five = df.MidSpread[max(0, i-600) : i]
            #fifteen = df.MidSpread[max(0, i-1800) : i]
            #thirty = df.MidSpread[max(0, i-3600) : i]
            #hour = df.MidSpread[max(0, i-7200) : i]
            #day = df.MidSpread[:i]
            #a5.append(five.mean())
            #s5.append(five.std()) 
            #a15.append(fifteen.mean())
            #s15.append(fifteen.std())
            #a30.append(thirty.mean())
            #s30.append(thirty.std())
            #ah.append(hour.mean())
            #sh.append(hour.std())
            #print(len(day))
            #ad.append(day.mean())
            #sd.append(day.std())
        
        #df['1d Average'] = ad
        #df['1d Std'] = sd
        
        #df['MidPrice1'] = (df['BidPrice1']+df['AskPrice1'])/2
        #df['MidPrice2'] = (df['BidPrice2']+df['AskPrice2'])/2       
        
        # Create 1d statistics
        df_temp = df[['DateTime','MidSpread','MidPrice1','MidPrice2','BuyVolume1','SellVolume1','BuyVolume2', 'SellVolume2']].set_index('DateTime')
        dtroll = df_temp['MidSpread'].rolling('1d', closed='left') 
        #df_temp['sum'] = dtroll.sum()
        #df_temp['count'] = dtroll.count()
        #df_temp['var'] = dtroll.var(ddof=0)
        #df_temp['avg'] = (df_temp['sum']-df_temp['MidSpread'])/(df_temp['count']-1)
        #df['1d Average M'] = list(df_temp['avg'])
        #df['1d Std M'] = list(((df_temp['var']*df_temp['count']) - (df_temp['MidSpread']-df_temp['avg'])**2)/(df_temp['count']-2))
        #df['1d Std M'] = np.where(df['1d Std M']>=0, df['1d Std M']**(1/2), np.nan)
        df['1d Average M'] = dtroll.mean().reset_index(drop=True)
        df['1d Std M'] = dtroll.std().reset_index(drop=True)
        
        dtroll = df_temp['MidPrice1'].rolling('1d', closed='left')
        #df_temp['sum'] = dtroll.sum()
        #df_temp['count'] = dtroll.count()
        #df_temp['var'] = dtroll.var(ddof=0)
        #df_temp['avg'] = (df_temp['sum']-df_temp['MidPrice1'])/(df_temp['count']-1)
        #df['1d Average1'] = list(df_temp['avg'])
        #df['1d Std1'] = list(((df_temp['var']*df_temp['count']) - (df_temp['MidPrice1']-df_temp['avg'])**2)/(df_temp['count']-2))
        #df['1d Std1'] = np.where(df['1d Std1']>=0, df['1d Std1']**(1/2), np.nan)
        df['1d Average1'] = dtroll.mean().reset_index(drop=True)
        df['1d Std1'] = dtroll.std().reset_index(drop=True)
        
        dtroll = df_temp['MidPrice2'].rolling('1d', closed='left')
        #df_temp['sum'] = dtroll.sum()
        #df_temp['count'] = dtroll.count()
        #df_temp['var'] = dtroll.var(ddof=0)
        #df_temp['avg'] = (df_temp['sum']-df_temp['MidPrice2'])/(df_temp['count']-1)
        #df['1d Average2'] = list(df_temp['avg'])
        #df['1d Std2'] = list(((df_temp['var']*df_temp['count']) - (df_temp['MidPrice2']-df_temp['avg'])**2)/(df_temp['count']-2))
        #df['1d Std2'] = np.where(df['1d Std2']>=0, df['1d Std2']**(1/2), np.nan)
        df['1d Average2'] = dtroll.mean().reset_index(drop=True)
        df['1d Std2'] = dtroll.std().reset_index(drop=True)
        
        df['1d Agg BuyVolume1'] = df_temp['BuyVolume1'].rolling('1d', closed='left').sum().reset_index(drop=True)
        df['1d Agg BuyVolume1'].mask(df['1d Agg BuyVolume1']<epsilon, other=0, inplace=True)
        df['1d Agg SellVolume1'] = df_temp['SellVolume1'].rolling('1d', closed='left').sum().reset_index(drop=True)
        df['1d Agg SellVolume1'].mask(df['1d Agg SellVolume1']<epsilon, other=0, inplace=True)
        df['1d Ratio1'] = ((df['1d Agg BuyVolume1']-df['1d Agg SellVolume1'])/(df['1d Agg BuyVolume1']+df['1d Agg SellVolume1'])).mask((df['1d Agg BuyVolume1']+df['1d Agg SellVolume1'])==0, other=0)
        #df['1d Ratio1'].mask(df['1d Agg BuyVolume1'] + df['1d Agg SellVolume1'] < 2*epsilon, other=0, inplace=True)
        df['1d Agg BuyVolume2'] = df_temp['BuyVolume2'].rolling('1d', closed='left').sum().reset_index(drop=True)
        df['1d Agg BuyVolume2'].mask(df['1d Agg BuyVolume2']<epsilon, other=0, inplace=True)
        df['1d Agg SellVolume2'] = df_temp['SellVolume2'].rolling('1d', closed='left').sum().reset_index(drop=True)
        df['1d Agg SellVolume2'].mask(df['1d Agg SellVolume2']<epsilon, other=0, inplace=True)
        df['1d Ratio2'] = ((df['1d Agg BuyVolume2']-df['1d Agg SellVolume2'])/(df['1d Agg BuyVolume2']+df['1d Agg SellVolume2'])).mask((df['1d Agg BuyVolume2']+df['1d Agg SellVolume2'])==0, other=0)
        #df['1d Ratio2'].mask(df['1d Agg BuyVolume2'] + df['1d Agg SellVolume2'] < 2*epsilon, other=0, inplace=True)
        
        df['1d+ Average M'] = df['MidSpread'].expanding().mean().shift(1)
        df['1d+ Std M'] = df['MidSpread'].expanding().std().shift(1)
        df['1d+ Average1'] = df['MidPrice1'].expanding().mean().shift(1)
        df['1d+ Std1'] = df['MidPrice1'].expanding().std().shift(1)
        df['1d+ Average2'] = df['MidPrice2'].expanding().mean().shift(1)
        df['1d+ Std2'] = df['MidPrice2'].expanding().std().shift(1)
        
        if GetExchangeCode(code) != 'CZCE':
            for m in [1,2,3,5,15,30,60]:
                calculateTickStats(df, m)
        else:
            # add a new time stamp thing
            changes = pd.Series(np.where(df['Time']==df['Time'].shift(1), 0, 1))
            changes = changes.cumsum()
            df['seconds'] = changes.apply(lambda x: pd.Timestamp(x, unit='s'))
            for m in [1,2,3,5,15,30,60]:
                calculateCZCEStats(df, m)
            df.drop(columns='seconds', inplace=True)
        
# Finally, create new columns from lists.
        print("Done.")
        print()
        #df['5m Average'], df['5m Std'], df['15m Average'], df['15m Std'], df['30m Average'], df['30m Std'], df['1hr Average'], df['1hr Std'], df['1d Average'], df['1d Std'] = a5,s5,a15,s15,a30,s30,ah,sh,ad,sd
        df = pd.concat([first, df[df.keep==True]], ignore_index=True)
        df.drop(columns=['keep', '1d+ Average M', '1d+ Std M', '1d+ Average1', '1d+ Average2', '1d+ Std1', '1d+ Std2', 'DateTime','Amount1', 'Amount2', 'TotalVolume1', 'TotalVolume2'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(destfile)
        #df.to_csv('D:\\Hongze\\Dropbox\\SpreadData\\_Test Data\\testprocessed2.csv')
        #exit(1)
#to be deleted later
#os.system("shutdown /h")
