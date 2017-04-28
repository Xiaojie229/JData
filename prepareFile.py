# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 22:51:24 2017

@author: XiaojieLi
"""

import os
import gc
import pickle
import pandas as pd

def prepare_action_all():
#    os.mkdir('./cache')
    os.mkdir('./cache/pickle')
    action2=pd.read_csv('./data/JData_Action_201602.csv',encoding='gbk')
    action2['user_id']=action2['user_id'].map(int)
    action3=pd.read_csv('./data/JData_Action_201603.csv',encoding='gbk')
    action3['user_id']=action3['user_id'].map(int)
    action4=pd.read_csv('./data/JData_Action_201604.csv',encoding='gbk')
    action4['user_id']=action4['user_id'].map(int)
    action_all=pd.concat([action2,action3,action4])
    action_all.to_csv('./cache/action_all.csv',index=False)    
    file=open('./cache/pickle/action_all.pkl','wb')
    pickle.dump(action_all,file)
    
    action_0315_0415=action_all[pd.to_datetime(action_all.time)>=pd.to_datetime('2016-03-16 00:00:00')]
    action_0315_0415.to_csv('./cache/action_0315_0415.csv',index=False)
    file=open('./cache/pickle/action_0315_0415.pkl','wb')
    pickle.dump(action_0315_0415,file)
    del action2,action3,action_all,action_0315_0415
    gc.collect()
    
    action_0401_0415=action4[pd.to_datetime(action4.time)>=pd.to_datetime('2016-04-01 00:00:00')]
    action_0401_0415.to_csv('./cache/action_0401_0415.csv',index=False)
    file=open('./cache/pickle/action_0401_0415.pkl','wb')
    pickle.dump(action_0401_0415,file)
    del action_0401_0415
    gc.collect()
    
    user=pd.read_csv('./data/JData_User.csv',encoding='gbk')   
    file=open('./cache/pickle/user.pkl','wb')
    pickle.dump(user,file)
    del user
    gc.collect()
    sku=pd.read_csv('./data/JData_Product.csv',encoding='gbk')   
    file=open('./cache/pickle/sku.pkl','wb')
    pickle.dump(sku,file)
    del sku
    gc.collect()
    comment=pd.read_csv('./data/JData_Comment.csv',encoding='gbk')   
    file=open('./cache/pickle/comment.pkl','wb')
    pickle.dump(comment,file)
    
    
if __name__=="__main__":
    prepare_action_all()