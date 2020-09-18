# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 09:36:32 2020

@author: xingtan
"""

def GetMultiplier(InstrumentCode):
    if InstrumentCode == 'cu': return 5
    elif InstrumentCode == 'au': return 1000
    elif InstrumentCode == 'ag': return 15
    elif InstrumentCode == 'ni': return 1
    elif InstrumentCode == 'rb': return 10
    elif InstrumentCode == 'al': return 5
    elif InstrumentCode == 'fu': return 10
    elif InstrumentCode == 'ru': return 10
    elif InstrumentCode == 'bu': return 10
    elif InstrumentCode == 'sn': return 1
    elif InstrumentCode == 'ss': return 5
    elif InstrumentCode == 'sp': return 10
    elif InstrumentCode == 'zn': return 5
    elif InstrumentCode == 'pb': return 5
    elif InstrumentCode == 'hc': return 10
    elif InstrumentCode == 'sc': return 1000
    elif InstrumentCode == 'IF': return 300
    elif InstrumentCode == 'IC': return 200
    elif InstrumentCode == 'T': return 10000
    elif InstrumentCode == 'SR': return 10
    elif InstrumentCode == 'CF': return 5
    elif InstrumentCode == 'TA': return 5
    elif InstrumentCode == 'MA': return 10
    elif InstrumentCode == 'AP': return 10
    elif InstrumentCode == 'CY': return 5
    elif InstrumentCode == 'ZC': return 100
    elif InstrumentCode == 'FG': return 20
    elif InstrumentCode == 'UR': return 20
    elif InstrumentCode == 'SA': return 20
    elif InstrumentCode == 'OI': return 10
    elif InstrumentCode == 'RM': return 10
    elif InstrumentCode == 'SF': return 5
    elif InstrumentCode == 'SM': return 5
    elif InstrumentCode == 'CJ': return 5
    elif InstrumentCode == 'm': return 10
    elif InstrumentCode == 'jd': return 10
    elif InstrumentCode == 'l': return 5
    elif InstrumentCode == 'i': return 100
    elif InstrumentCode == 'y': return 10
    elif InstrumentCode == 'p': return 10
    elif InstrumentCode == 'c': return 10
    elif InstrumentCode == 'cs': return 10
    elif InstrumentCode == 'v': return 5
    elif InstrumentCode == 'eg': return 10
    elif InstrumentCode == 'pp': return 5
    elif InstrumentCode == 'eb': return 5
    elif InstrumentCode == 'jm': return 60
    elif InstrumentCode == 'a': return 10
    elif InstrumentCode == 'j': return 100
    elif InstrumentCode == 'pg': return 20
    elif InstrumentCode == 'rr': return 10
    elif InstrumentCode == 'b': return 10
    else: return -1
    
def GetStepsize(InstrumentCode):
    if InstrumentCode == 'cu': return 10
    elif InstrumentCode == 'au': return 0.02
    elif InstrumentCode == 'ag': return 1
    elif InstrumentCode == 'ni': return 10
    elif InstrumentCode == 'rb': return 1
    elif InstrumentCode == 'al': return 5
    elif InstrumentCode == 'fu': return 1
    elif InstrumentCode == 'ru': return 5
    elif InstrumentCode == 'bu': return 2
    elif InstrumentCode == 'sn': return 10
    elif InstrumentCode == 'ss': return 5
    elif InstrumentCode == 'sp': return 2
    elif InstrumentCode == 'zn': return 5
    elif InstrumentCode == 'pb': return 5
    elif InstrumentCode == 'hc': return 1
    elif InstrumentCode == 'sc': return 0.1
    elif InstrumentCode == 'IF': return 0.2
    elif InstrumentCode == 'IC': return 0.2
    elif InstrumentCode == 'T': return 0.005
    elif InstrumentCode == 'SR': return 1
    elif InstrumentCode == 'CF': return 5
    elif InstrumentCode == 'TA': return 2
    elif InstrumentCode == 'MA': return 1
    elif InstrumentCode == 'AP': return 1
    elif InstrumentCode == 'CY': return 5
    elif InstrumentCode == 'ZC': return 0.2
    elif InstrumentCode == 'FG': return 1
    elif InstrumentCode == 'UR': return 1
    elif InstrumentCode == 'SA': return 1
    elif InstrumentCode == 'OI': return 2
    elif InstrumentCode == 'RM': return 1
    elif InstrumentCode == 'SF': return 2
    elif InstrumentCode == 'SM': return 2
    elif InstrumentCode == 'CJ': return 5
    elif InstrumentCode == 'm': return 1
    elif InstrumentCode == 'jd': return 1
    elif InstrumentCode == 'l': return 5
    elif InstrumentCode == 'i': return 0.5
    elif InstrumentCode == 'y': return 2
    elif InstrumentCode == 'p': return 2
    elif InstrumentCode == 'c': return 1
    elif InstrumentCode == 'cs': return 1
    elif InstrumentCode == 'v': return 5
    elif InstrumentCode == 'eg': return 1
    elif InstrumentCode == 'pp': return 1
    elif InstrumentCode == 'eb': return 1
    elif InstrumentCode == 'jm': return 0.5
    elif InstrumentCode == 'a': return 1
    elif InstrumentCode == 'j': return 0.5
    elif InstrumentCode == 'pg': return 1
    elif InstrumentCode == 'rr': return 1
    elif InstrumentCode == 'b': return 1
    else: return -1
    
def GetExchangeCode(InstrumentCode):
    if InstrumentCode == 'cu': return  'SHFE'
    elif InstrumentCode == 'au': return  'SHFE'
    elif InstrumentCode == 'ag': return  'SHFE'
    elif InstrumentCode == 'ni': return  'SHFE'
    elif InstrumentCode == 'rb': return  'SHFE'
    elif InstrumentCode == 'al': return  'SHFE'
    elif InstrumentCode == 'fu': return  'SHFE'
    elif InstrumentCode == 'ru': return  'SHFE'
    elif InstrumentCode == 'bu': return  'SHFE'
    elif InstrumentCode == 'sn': return  'SHFE'
    elif InstrumentCode == 'ss': return  'SHFE'
    elif InstrumentCode == 'sp': return  'SHFE'
    elif InstrumentCode == 'zn': return  'SHFE'
    elif InstrumentCode == 'pb': return  'SHFE'
    elif InstrumentCode == 'hc': return  'SHFE'
    elif InstrumentCode == 'sc': return  'INE'
    elif InstrumentCode == 'IF': return  'CFFEX'
    elif InstrumentCode == 'IC': return  'CFFEX'
    elif InstrumentCode == 'T': return  'CFFEX'
    elif InstrumentCode == 'SR': return 'CZCE'
    elif InstrumentCode == 'CF': return 'CZCE'
    elif InstrumentCode == 'TA': return 'CZCE'
    elif InstrumentCode == 'MA': return 'CZCE'
    elif InstrumentCode == 'AP': return 'CZCE'
    elif InstrumentCode == 'CY': return 'CZCE'
    elif InstrumentCode == 'ZC': return 'CZCE'
    elif InstrumentCode == 'FG': return 'CZCE'
    elif InstrumentCode == 'UR': return 'CZCE'
    elif InstrumentCode == 'SA': return 'CZCE'
    elif InstrumentCode == 'OI': return 'CZCE'
    elif InstrumentCode == 'RM': return 'CZCE'
    elif InstrumentCode == 'SF': return 'CZCE'
    elif InstrumentCode == 'SM': return 'CZCE'
    elif InstrumentCode == 'CJ': return 'CZCE'
    elif InstrumentCode == 'm': return 'DCE'
    elif InstrumentCode == 'jd': return 'DCE'
    elif InstrumentCode == 'l': return 'DCE'
    elif InstrumentCode == 'i': return 'DCE'
    elif InstrumentCode == 'y': return 'DCE'
    elif InstrumentCode == 'p': return 'DCE'
    elif InstrumentCode == 'c': return 'DCE'
    elif InstrumentCode == 'cs': return 'DCE'
    elif InstrumentCode == 'v': return 'DCE'
    elif InstrumentCode == 'eg': return 'DCE'
    elif InstrumentCode == 'pp': return 'DCE'
    elif InstrumentCode == 'eb': return 'DCE'
    elif InstrumentCode == 'jm': return 'DCE'
    elif InstrumentCode == 'a': return 'DCE'
    elif InstrumentCode == 'j': return 'DCE'
    elif InstrumentCode == 'pg': return 'DCE'
    elif InstrumentCode == 'rr': return 'DCE'
    elif InstrumentCode == 'b': return 'DCE'
    else: return -1
    
def GetInstrument(Code):
    if Code == 'SHFE':
        return list(['cu','au','ag','ni','rb','al','fu','ru','bu','sn','ss','sp','zn','hc'])
    elif Code == 'CFFEX':
        return list(['IF','IC','T'])
    elif Code == 'CZCE':
        return list(['SR','CF','TA','MA','AP','CY','ZC','FG','UR','SA','OI','RM','SF','SM','CJ'])
    elif Code == 'DCE':
        return list(['m','jd','l','i','y','p','c','cs','v','eg','pp','eb','jm','pg','rr','b'])
    elif Code == 'INE':
        return list(['sc'])
    else:
        return -1
    
def GetTimeInHalfSeconds(UpdateTime, MilliSecond):
    hour = 0
    minute = 0
    second = 0
    if UpdateTime[0] == '2':
        if UpdateTime[1] >= '1':
            hour = int(UpdateTime[1]) - 1
        elif  UpdateTime[1] == '0':
            return -1 #20:00 --> -1
    elif UpdateTime[0] == '0': #past midnight
        if UpdateTime[1] <= '3': #Up to 4 am
            hour = int(UpdateTime[1]) + 3 #0 to 7?
    minute = int(UpdateTime[3:5]) #UpdateTime: hh:mm:ss
    second = int(UpdateTime[6:8])
    if MilliSecond >= 500:
        return hour*7200+minute*120+second*2+1
    else:
        return hour*7200+minute*120+second*2
    #Three different sessions. When were they again?
            