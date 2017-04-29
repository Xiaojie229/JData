# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 23:24:07 2017

@author: XIAOJIELI
"""

import pickle
import pandas as pd

def get_sku_data():
    return pickle.load(open('./cache/pickle/sku.pkl','rb'))

def get_action_data():
    action_data=pickle.load(open('./cache/pickle/action_all.pkl','rb'))
    action_data['time']=action_data['time'].map(pd.to_datetime)    
    return action_data
    
    
def get_sku_basic_feature():
    
    
    
def get_sku_feature():
    sku_data=get_sku_data()
    