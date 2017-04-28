# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 09:17:28 2017

@author: XiaojieLi
"""

import gc
import pandas as pd

def get_head_label(df,start_date,mid_date,end_date):
    """
    提取时间段内的表头与正负标签
    """
    df_train=df[(df.time>=mid_date)&(df.time<end_date)]
    df_positive=df_train[df_train.type==4][['user_id','sku_id']].drop_duplicates()
    df_positive['label']=1
    
    df_train1=df[(df.time>=start_date)&(df.time<mid_date)]
    df_negative=df_train1[['user_id','sku_id']].drop_duplicates()
    
    del df_train,df_train1
    gc.collect()
    
    df_label=pd.merge(df_negative,df_positive,how='left',on=['user_id','sku_id'])
    df_label=df_label.fillna(0)
    return df_label
    
    
def get_user_head_label(df,start_date,mid_date,end_date):
    """
    提取时间段内用户表头与购买标签
    """
    df_train=df[(df.time>=mid_date)&(df.time<end_date)]
    df_positive=df_train[(df_train.type==4)&(df_train.cate==8)][['user_id']].drop_duplicates()
    df_positive['label']=1
    
    df_train1=df[(df.time>=start_date)&(df.time<mid_date)]
    df_negative=df_train1[(df_train1.cate==8)][['user_id']].drop_duplicates()
    
    del df_train,df_train1
    gc.collect()
    
    df_label=pd.merge(df_negative,df_positive,how='left',on=['user_id'])
    df_label=df_label.fillna(0)
    return df_label 
    
    
    