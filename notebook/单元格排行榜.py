# -*- coding: utf-8 -*-
'''
按照0.076的格子遍历所有的商店
'''

import pandas as pd
import geohash
import sqlite3 as sql
import datetime
import sys
import sqlalchemy
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from itertools import *
from pandas import ExcelWriter
from geopy.distance import vincenty

database_path = '../database/'
database_files = ['2016-04-02-data.db']

distance = 2.0


# load database
def load_database(db_name):
    '''加载数据库
    db_name:SQLITE3数据库的文件名
    '''

    engine = sqlalchemy.create_engine('sqlite:///' + database_path + db_name)
    restaurants_db = pd.read_sql_table('restaurants', engine)
    menus_db = pd.read_sql_table('menus', engine)

    # 计算营业额
    menus_db['revenue'] = menus_db['price'] * menus_db['month_sales']

    # 合并到主数据库
    revenue_db = menus_db.loc[:, ['restaurant_id', 'revenue']].groupby(
        'restaurant_id').sum().reset_index(drop=False)
    restaurants_db = pd.merge(
        restaurants_db,
        revenue_db,
        left_on='id',
        right_on='restaurant_id',
        how='left')

    # 计算菜单平均价
    mean_db = menus_db.loc[:, ['restaurant_id', 'price']].groupby(
        'restaurant_id').mean().reset_index(drop=False).rename(columns={
            'price': 'mean_price'
        })
    restaurants_db = pd.merge(restaurants_db, mean_db, on='restaurant_id')

    # 计算平均价格
    restaurants_db['average_price'] = restaurants_db[
        'revenue'] / restaurants_db['month_sales']

    restaurants_db['revenue'] = restaurants_db['revenue'].fillna(0)
    del restaurants_db['restaurant_id']

    return restaurants_db


def load_all_database():
    '''加载所有的数据库文件'''
    database_list = []
    for db_file in database_files:
        database_list.append(load_database(db_file))
    return database_list

# 加载所有的数据库文件
db_list = load_all_database()


def merge_databases(database_list):
    '''合并数据库
    database_list:数据库的列表
    '''
    full_db = database_list[0]
    for db in database_list[1:]:
        full_db = full_db.append(db)

    full_db = full_db.sort_values(
        'revenue', ascending=False).drop_duplicates(subset='id')
    print('Number of', full_db.shape[0], 'Records')
    return full_db


def create_geohash_db(full_db):
    '''生成GEOHASH数据
    full_db:餐厅数据库
    '''
    geohash_db = pd.DataFrame()
    geohash_db['geohash'] = full_db.apply(lambda row: geohash.encode(
        float(row.latitude), float(row.longitude), 7), axis=1)
    geohash_db = geohash_db.drop_duplicates(subset='geohash')
    # 为地点生成经纬度
    geohash_db['latitude'] = geohash_db.apply(
        lambda row: geohash.decode(row.geohash)[0], axis=1)
    geohash_db['longitude'] = geohash_db.apply(
        lambda row: geohash.decode(row.geohash)[1], axis=1)
    return geohash_db

# 合并数据
full_db = merge_databases(db_list)

# 生成Geohash数据库
geohash_db = create_geohash_db(full_db)

geohash_db = geohash_db.head(10)


def cell_ranking(geohash_db_row):
    full_db['distance'] = full_db.apply(
        lambda row: vincenty((row.latitude, row.longitude), (geohash_db_row.latitude, geohash_db_row.longitude)).km,
        axis=1)
    db = full_db[full_db.distance < distance]
    return pd.Series({
        'month_sales': db['month_sales'].sum(),
        'revenue': db['revenue'].sum()
    })


geohash_db = geohash_db.merge(
    geohash_db.apply(
        cell_ranking, axis=1), left_index=True, right_index=True)



def write_to_sqlite(db):
    engine = sqlalchemy.create_engine('sqlite:///' + database_path +
                                      'geohash.db')
    db.to_sql('ranking', engine)


write_to_sqlite(geohash_db)
