# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 16:44:27 2020

@author: Hongze
"""

import os
import numpy as np
import pandas as pd
import statistics
from GetInstrumentInfo import GetInstrument
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso
from sklearn.linear_model import Ridge
from sklearn import preprocessing
# from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from matplotlib.pyplot import plot
from statsmodels.graphics.gofplots import qqplot

path = 'D:\\Hongze\\Dropbox\\SpreadData\\'
if os.path.isdir(path) == False:
    path = 'C:\\Users\\xingt\\Dropbox\\SpreadData\\'
    
codes = GetInstrument('DCE') + GetInstrument('SHFE')
codes.remove('fu')
codes.remove('rr')
codes = ['rb']
def createPredictiveData(code, start_tick = 0):
    #use processed spreads
    files = [i for i in os.listdir(path + code) if 'ProcessedSpread_' in i]
    X_list, y_list = list(), list()
    for file in files:
        print(file[16:24])
        df = pd.read_csv(path+code+'\\'+file) #new dataframe...
        # 5 minute slices
        df = df[(df.index >= start_tick) & (df.index % 600 == start_tick)]
        if len(df) < 3:
            continue
        X = df[['1d Ratio1','1d Ratio2']].iloc[1:-1]
        for i in ['1m','2m','3m','5m','15m','30m','60m']:
            # j = int(i[:-1])
            # X[i+ ' z'] = df[i+ ' z'].iloc[1:-1]
            # X[i+ ' D'] = df[i+ ' z'].iloc[1:-1] - df[i+ ' z'].iloc[:-2].values            
            X[i+ ' Ratio1'] = df[i+ ' Ratio1'].iloc[1:-1]
            X[i+ ' Ratio2'] = df[i+ ' Ratio2'].iloc[1:-1]
        y = (df['5m Average1'].iloc[2:] - df['5m Average1'].iloc[1:-1].values)/ df['5m Average1'].iloc[1:-1].values  #all but the first tick, so shifted by 1
        X_list.append(X) # independent variable: the durations' z variables
        y_list.append(y) # dependent variable: 5m z five minutes from now
        
    Xs = pd.concat(X_list, ignore_index=True)
    ys = pd.concat(y_list, ignore_index=True)
    df = Xs
    df['Outcome'] = ys
    return df

def createPredictiveData1(code, start_tick = 0):
    files = [i for i in os.listdir(path + code) if 'ProcessedSpread_' in i]
    X_list, y_list = list(), list()
    for file in files:
        print(file[16:24])
        df = pd.read_csv(path+code+'\\'+file) #new dataframe...
        # 5 minute slices
        df = df[(df.index >= start_tick) & (df.index % 600 == start_tick)]
        if len(df) < 3:
            continue
        l = list()
        for i in ['1m','2m','3m','5m','15m','30m','60m','1d']:
            l.extend([i+' Ratio1', i+' Ratio2'])
        X = df[l].iloc[1:-1]
        y = (df['5m Average1'].iloc[2:] - df['5m Average1'].iloc[1:-1].values)/df['5m Average1'].iloc[1:-1].values
        X_list.append(X)
        y_list.append(y)
    Xs = pd.concat(X_list, ignore_index=True)
    ys = pd.concat(y_list, ignore_index=True)
    df = Xs
    df['Outcome'] = ys
    return df
        
def createLinearRegression(code, start_tick = 0, return_X = False):
    #use processed spreads
    files = [i for i in os.listdir(path + code) if 'ProcessedSpread_' in i]
    X_list, y_list = list(), list()
    for file in files:
        print(file[16:24])
        df = pd.read_csv(path+code+'\\'+file) #new dataframe...
        # 5 minute slices
        df = df[(df.index >= start_tick) & (df.index % 600 == start_tick)]
        if len(df) < 2:
            continue
        X = df[["5m z", "15m z", "30m z", "1hr z"]].iloc[:-1]#all but the last tick
        y = df['5m z'].iloc[1:] #all but the first tick, so shifted by 1
        X_list.append(X) # independent variable: the durations' z variables
        y_list.append(y) # dependent variable: 5m z five minutes from now
    # after acquiring all relevant ticks...
    print('Creating Linear Regression...')
    Xs = pd.concat(X_list, ignore_index=True)
    ys = pd.concat(y_list, ignore_index=True)
    print(len(Xs), len(ys))
    if len(Xs)==0 or len(ys)==0:
        raise Exception('Not enough ticks')
    reg = LinearRegression()
    reg.fit(Xs, ys)
    print('r^2='+str(reg.score(Xs,ys)))
    if return_X:
        return reg, Xs
    else:
        return reg


def write_to_csv(start_tick = 0):
    for code in codes:
        print('Code='+code)        
        df=createPredictiveData(code, start_tick)
        print('Outputting\n')
        if os.path.isdir(path+code) == False:
            os.mkdir(path+code)
        if os.path.isdir(path+code+'\\Prediction') == False:
            os.mkdir(path+code+'\\Prediction')
        df.to_csv(path+code+'\\Prediction\\PredictiveData5ma_'+code + '.csv')

def readPredictiveData(codes):
    df_list = list()
    for code in codes:
        df = pd.read_csv(path+code+'\\Prediction\\PredictiveData1ma_'+code+'.csv', index_col = 0)
        
        df_list.append(df)
    dfs = pd.concat(df_list, ignore_index=True)
    return dfs
    

def runPredictiveRegression(dfs):
    dfs = dfs.sample(frac=1).reset_index(drop=True)#shuffle rows
    l = [i for i in dfs.columns if  i != 'Outcome' ]
    
    Xs = dfs.loc[:, l]
    # Xs = dfs.loc[:, dfs.columns != 'Outcome']
    ys = dfs['Outcome']   
    k  = int(len(dfs)*0.8) #80% data to train, 20% for out-of-sample test
    X_train = Xs[:-k]
    y_train = ys[:-k]
    
    X_test = Xs[-k:]
    y_test = ys[-k:]
    
    # X_test.columns =  Xs.columns 
    # Create linear regression object
    # regr = LinearRegression()
    regr = Lasso(alpha = 0.0005)
    # regr = Ridge(alpha = 1000)#best model so far
    
    # Train the model using the training sets
    regr.fit(X_train, y_train)
    
    # The coefficients
    # print('Coefficients:', regr.coef_)
    # print('Intercept:', regr.intercept_)
    
    
    yinsample_pred = regr.predict(X_train)
    # Make predictions using the testing set
    y_pred = regr.predict(X_test)
    
    # The mean squared error
    # print('Mean squared error: %.2f' % mean_squared_error(y_test, y_pred))
    # The coefficient of determination: 1 is perfect prediction
    print('In-sample R^2: %.2f%% and out-of-sample R^2: %.2f%%'% (100*regr.score(X_train,y_train),100*r2_score(y_test, y_pred)) )

    return 100*r2_score(y_test, y_pred)


def main():
    outsampleR2 = []
    for i in range(10):
        outsampleR2.append(runPredictiveRegression(dfs))
    print('min R^2 is %.2f%%' % min(outsampleR2) + ' max R^2 is %.2f%%' % max(outsampleR2))
    print('median R^2 is %.2f%%' % statistics.median(outsampleR2))
if __name__=='__main__':
    dfs = readPredictiveData(codes)
    main()
    # write_to_csv(300)