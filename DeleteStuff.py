# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 18:44:27 2020

@author: Hongze
"""

import os
import shutil
#path = 'C:\\Users\\Hongze\\Dropbox\\SpreadData\\'
path = 'D:\\Hongze\\Dropbox\\SpreadData\\'
dirs = [i for i in os.listdir(path) if len(i)==2 or len(i)==1]
#dirs = ['AP','CF','CJ','FG','MA','OI','RM','SA','SF','SM','SR','TA','UR','ZC']

for d in dirs:
    #if os.path.isdir(path+d+'\\Alternates'):
    #    shutil.rmtree(path+d+'\\Alternates')
    files = [i for i in os.listdir(path+d) if 'ProcessedSpread' in i and '20200103' in i]    
    for file in files:
        os.remove(path+d+'\\'+file)   
    #files = [i for i in os.listdir(path+d) if '20200601' in i or ('20200602' in i and 'Processed' in i)]
    #for file in files:
    #    os.remove(path+d+'\\'+file)
    #files = [i for i in os.listdir(path+d+'\\Alternates') if '20200601' in i]
    #for file in files:
    #    os.remove(path+d+'\\'+file)