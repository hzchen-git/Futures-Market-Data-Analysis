# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 22:51:00 2020

@author: Hongze
"""

import os
import pandas as pd
from LinearRegression import createPredictiveData1

def main():
    codes = ['cu','au','ag','ni','rb','al','ru','zn','sc','IF','IC','T']
    ratios = list()
    for i in ['1m','2m','3m','5m','15m','30m','60m','1d']:
        ratios.extend([i+' Ratio1', i+' Ratio2'])
    df = pd.DataFrame(index=codes, columns=ratios)
    
    for code in codes:
        print('Code='+code)
        df_pred = createPredictiveData1(code)
        print('Filling\n')
        for col in [i for i in df_pred.columns if i != 'Outcome']:
            df.at[code, col] = df_pred[col].corr(df_pred['Outcome'])
    print('Outputting...')
    destpath = 'D:\\Hongze\\Dropbox\\SpreadData\\_Test Data'
    if os.path.isdir(destpath)  == False:           
        destpath = 'C:\\Users\\xingt\\Dropbox\\SpreadData\\_Test Data'
    df.to_csv(destpath + '\\Ratio Correlations.csv')
if __name__=='__main__':
    main()