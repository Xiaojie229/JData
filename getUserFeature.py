# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 23:22:16 2017

@author: XiaojieLi
"""

import pickle
import gc
import pandas as pd
from datetime import timedelta
from datetime import datetime
from getLabel import get_user_head_label

def get_action_data():
    action_data=pickle.load(open('./cache/pickle/action_0315_0415.pkl','rb'))    #先统计一个月内的，最后可以是整个数据集
    action_data['time']=action_data['time'].map(pd.to_datetime)    
    return action_data

def get_user_data():
    return pickle.load(open('./cache/pickle/user.pkl','rb'))
    
def convert_age(age):
    if age==u'-1':
        return 0
    elif age==u'15岁以下':
        return 1
    elif age==u'16-25岁':
        return 2
    elif age==u'26-35岁':
        return 3
    elif age==u'36-45岁':
        return 4
    elif age==u'46-55岁':
        return 5
    elif age=='56岁以上':
        return 6
    else:
        return -1

def get_user_basic_feature(user_data,end_date):
    """
    用户基本特征
    """
    df=user_data[['user_id','user_reg_tm']]
    df=df.map(pd.to_datetime)
    df.user_reg_tm=(end_date-df.user_reg_tm)
    user_data.age=user_data.age.map(convert_age)
    age_df=pd.get_dummies(user_data['age'],prefix='age')
    user_data=pd.concat([user_data['user_id'],age_df,user_data['sex'],user_data['user_lv_cd'],df['user_reg_tm']],axis=1)
    user_data.to_csv('./cache/user_basic_feature.csv',index=False)
    file=open('./cache/pickle/user_basic_feature.pkl','wb')
    pickle.dump(user_data,file)
    user_data=pickle.load(open('./cache/pickle/user_basic_feature.pkl','rb'))
    return user_data    
    
def get_user_action_feature_detail(action_df,start_date,end_date):
    str_start_data=start_date.strftime('%Y_%m_%d_%H_%M_%S')
    str_end_data=end_date.strftime('%Y_%m_%d_%H_%M_%S')
    action_df=action_df[(action_df.time>=start_date)&(action_df.time<end_date)]

#   购买量，点击量，加购量等    
    type_df=action_df[['user_id','type']]
    df=pd.get_dummies(type_df['type'],prefix='type')
    type_df=pd.concat([type_df,df],axis=1)
    type_df=type_df.groupby('user_id',as_index=False).sum()
    del type_df['type']
    user_head=type_df[['user_id']]
    del type_df['user_id']
    type_df.rename(columns=lambda x: x.replace(x,x+"_%s-%s"%(str_start_data,str_end_data)),inplace=True)
    type_df=pd.concat([user_head,type_df],axis=1)
    type_df.to_csv('./cache/%s_%s_type_df.csv'%(str_start_data,str_end_data),index=False)
    file=open('./cache/pickle/%s_%s_type_df.pkl'%(str_start_data,str_end_data),'wb')
    pickle.dump(type_df,file)
    
#   购买量占行为比率
    ratio=['user_id','buy_type1_ratio','buy_type2_ratio','buy_type3_ratio','buy_type5_ratio','buy_type6_ratio']
    buy_ratio=pd.concat([action_df['user_id'],df],axis=1)
    buy_ratio=buy_ratio.groupby(['user_id'],as_index=False).sum()
    buy_ratio['buy_type1_ratio']=buy_ratio['type_4']/buy_ratio['type_1']
    buy_ratio['buy_type2_ratio']=buy_ratio['type_4']/buy_ratio['type_2'] 
    buy_ratio['buy_type3_ratio']=buy_ratio['type_4']/buy_ratio['type_3']
    buy_ratio['buy_type5_ratio']=buy_ratio['type_4']/buy_ratio['type_5']
    buy_ratio['buy_type6_ratio']=buy_ratio['type_4']/buy_ratio['type_6']
    buy_ratio=buy_ratio[ratio]
    user_head=buy_ratio[['user_id']]
    del buy_ratio['user_id']
    buy_ratio.rename(columns=lambda x: x.replace(x,x+"_%s-%s"%(str_start_data,str_end_data)),inplace=True)
    buy_ratio=pd.concat([user_head,buy_ratio],axis=1)
    buy_ratio.to_csv('./cache/%s_%s_buy_ratio.csv'%(str_start_data,str_end_data),index=False)
    file=open('./cache/pickle/%s_%s_buy_ratio.pkl'%(str_start_data,str_end_data),'wb')
    pickle.dump(buy_ratio,file)
    
    feature=pd.merge(type_df,buy_ratio,how='left',on='user_id')
    del buy_ratio,df
    gc.collect()

#   成交数与其总访问数之比
    type_df=action_df[['user_id','type']]
    df=pd.get_dummies(type_df['type'],prefix='type')
    type_df=pd.concat([type_df,df],axis=1)
    del type_df['type']
    buy_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_1','type_2','type_3','type_5','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    buy_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,buy_type_ratio,how='left',on='user_id')
    del buy_type_ratio
    gc.collect()
#   浏览数与其总访问数之比
    view_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_2','type_3','type_4','type_5','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    view_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,view_type_ratio,how='left',on='user_id')
    del view_type_ratio
    gc.collect()
#   加购与其总访问数之比
    cat1_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_1','type_3','type_4','type_5','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    cat1_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,cat1_type_ratio,how='left',on='user_id')
    del cat1_type_ratio
    gc.collect()
#   去购与其总访问数之比
    cat2_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_1','type_2','type_4','type_5','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    cat2_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,cat2_type_ratio,how='left',on='user_id')
    del cat2_type_ratio
    gc.collect()
#   关注与其总访问数之比
    care_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_1','type_2','type_3','type_4','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    care_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,care_type_ratio,how='left',on='user_id')
    del care_type_ratio
    gc.collect()
#   关注与其总访问数之比
    click_type_ratio=action_df[['user_id']]
    type_data=type_df[['type_1','type_2','type_3','type_4','type_6']]
    type_data=type_data.sum(axis=1)
    ratio=type_df['type_4']
    click_type_ratio['type_ratio']=ratio/type_data
    feature=pd.merge(feature,click_type_ratio,how='left',on='user_id')
    del click_type_ratio
    gc.collect()
       
#   查看类别总量    
    cate_df=action_df[['user_id','cate']]
    df1=pd.get_dummies(cate_df['cate'],prefix='cate')
    cate_df=pd.concat([cate_df,df1],axis=1)
    cate_df=cate_df.groupby('user_id',as_index=False).sum()
    del cate_df['cate']
    feature=pd.merge(feature,cate_df,how='left',on='user_id')
    del cate_df,df1
    gc.collect()
    
#   购买类别总量
    cate_buy_df=action_df[['user_id','type','cate']]
    cate_buy_df=cate_buy_df[cate_buy_df.type==4]
    df2=pd.dummies(cate_buy_df['cate'],prefix='cate_buy')
    cate_buy_df=pd.concat([cate_buy_df,df2],axis=1)
    cate_buy_df=cate_buy_df.groupby('user_id',as_index=False).sum()
    del cate_buy_df['cate'],cate_buy_df['type'],df2
    feature=pd.merge(feature,cate_buy_df,how='left',on='user_id')
    del cate_buy_df
    gc.collect()
    
# TODO 购买类别数/访问类别数
    user_head=feature[['user_id']]
    del feature['user_id']
    feature.rename(columns=lambda x: x.replace(x,x+"_%s-%s"%(str_start_data,str_end_data)),inplace=True)
    feature=pd.concat([user_head,feature],axis=1)
    feature.fillna(0)
    feature.to_csv('./cache/%s_%s_feature.csv'%(str_start_data,str_end_data),index=False)
    file=open('./cache/pickle/%s_%s_feature.pkl'%(str_start_data,str_end_data),'wb')
    pickle.dump(feature,file)
    return feature    

def get_user_action_feature(action_data,end_date):
    """
    提取用户行为特征
    """
    # 用户在这些时间段内的行为特征
    user_action_feature=None
    for i in (1,2,3,5,7,14,21,30):
        start_date=end_date-timedelta(days=i)
        if user_action_feature is None:
            user_action_feature=get_user_action_feature_detail(action_data,start_date,end_date)
        else:
            user_action_feature=pd.merge(user_action_feature,get_user_action_feature_detail(action_data,start_date,end_date),
                                    how='left',on='user_id')
    for i in (1,2,3):
        end_date=end_date-timedelta(days=i)
        start_date=end_date-timedelta(days=1)
        user_action_feature=pd.merge(user_action_feature,get_user_action_feature_detail(action_data,start_date,end_date),
                                    how='left',on='user_id')
    user_action_feature.to_csv('./cache/user_action_feature.csv',index=False)
    file=open('./cache/pickle/user_action_feature.pkl','wb')
    pickle.dump(user_action_feature,file)
    return user_action_feature
    
def get_user_feature():
    end_date=pd.to_datetime('2016-04-13 00:00:00')
    mid_date=pd.to_datetime('2016-04-10 00:00:00')
    start_date=pd.to_datetime('2016-04-05 00:00:00')  
    action_data=get_action_data()
    user_head=get_user_head_label(action_data,start_date,mid_date,end_date)
    user_data=get_user_data()
    user_data=pd.merge(user_head,user_data,how='left',on='user_id')
    user_basic_feature=get_user_basic_feature(user_data,end_date)
    user_action_feature=get_user_action_feature(action_data,end_date)
    user_feature=pd.merge(user_basic_feature,user_action_feature,how='left',on='user_id')
    del user_basic_feature,user_action_feature
    gc.collect()
    user_feature.to_csv('./cache/user_feature.csv',index=False)
    file=open('./cache/pickle/user_feature.pkl','wb')
    pickle.dump(user_feature,file)
    return user_feature
    
def test():
    pass
    
if __name__=="__main__":
    get_user_feature()
#    test()